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
//! - Phase 1 (claim + fold, one short transaction) is [`drain_once`]'s own
//!   opening block, reusing [`super::claim::claim`],
//!   [`super::claim::owned_bucket_filter`], and [`super::fold::fold`]
//!   directly — there is nothing 1-1-specific about claiming or folding, so
//!   this module adds no wrapper around them.
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
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use tokio_postgres::types::{PgLsn, ToSql};
use tokio_postgres::{GenericClient, Transaction};

use crate::defs::ast::{Expr, GroupByKey, KeySpace, TransformDef, ValueType, group_by_contains};
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
use super::apply_aggregate::{self, AggregateTargetPlan};
use super::claim;
use super::converge;
use super::error::StagingError;
use super::fold::{self, FoldedChange};
use super::liveness::FenceMissBackoff;
use super::quarantine;
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
/// *common* case #134's own doc section measured — never pays the
/// fallback's live-recompute cost in place of the fast path's true delta.
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
    /// Downstream propagation would have staged a `Recompute` row past
    /// [`MAX_HOP_GEN`]. Named rather than silently truncated: an operator
    /// needs to know a wave ran away, and which target tables it ran away
    /// through, rather than have the tail of it quietly disappear.
    HopBoundExceeded { hop_gen: i32, tables: Vec<String> },
    /// A folded record names a source table Postgres no longer has
    /// (`42P01` from a live query against it) — issue #16's "one sanctioned
    /// exception to immutability": no retry or per-key quarantine can
    /// resolve this, since the table itself is gone, not any one row.
    /// [`drain_once`] routes this to [`quarantine::purge_dropped_table`]
    /// rather than the ordinary isolate/evict path.
    SourceTableDropped { source_table: String },
    /// [`super::quarantine::resume_column`] (or, indirectly,
    /// [`crate::app::Trellis::resume_column`]) was asked to resume a
    /// `(transform, column)` pair with no currently-paused `column_status`
    /// row — resuming a column that isn't paused is caller error, not a
    /// silent no-op. Also reused for "no such column on this definition at
    /// all," so an address naming a real transform but the wrong field name
    /// gets a specific error rather than silently doing nothing.
    ColumnNotPaused { transform: String, column: String },
    /// [`super::quarantine::resume_column`] was asked to resume a column
    /// whose owning definition is not currently [`crate::defs::model::TransformStatus::Live`]
    /// — most concretely, a definition still `Backfilling` behind an
    /// in-flight `backfill_chunks` queue nothing is draining. `resume_column`
    /// takes one snapshot of the *source* table and only clears
    /// `column_status` after writing it back, so any row a still-running
    /// backfill chunk inserts into the target *during* that window is never
    /// in the snapshot and never revisited once the column is unpaused —
    /// permanently stranding that row's column at NULL/default while
    /// `resume_column` reports success. This branch's cascade pause
    /// (`defs::catalog::column_dependents`, unlike the `status = 'live'`
    /// filtered paths CDC apply uses) can reach a downstream definition in
    /// exactly this state, so the gate is not just theoretical. Refusing to
    /// resume until the definition reaches `Live` closes the window instead
    /// of racing it.
    DefinitionNotLive { transform: String },
    /// A failure from [`crate::intake::publication`]'s backfill-marker
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
            | ApplyError::HopBoundExceeded { .. }
            | ApplyError::ReverseTriggerNotResolvable { .. } => ErrorCode::Internal,
            ApplyError::SourceTableDropped { .. } => ErrorCode::NotFound,
            ApplyError::ColumnNotPaused { .. } => ErrorCode::NotFound,
            // The definition's persisted status conflicts with what
            // `resume_column` was asked to do, the same category
            // `ValidationError::DuplicateRelationshipName` and
            // `StagingError::ProducerAlreadyRunning` use for "existing state
            // blocks this request" rather than "the request itself is
            // malformed" (-> Validation) or "nothing by that name exists"
            // (-> NotFound).
            ApplyError::DefinitionNotLive { .. } => ErrorCode::Conflict,
            ApplyError::Intake(err) => err.code(),
            ApplyError::TransformNotFound { .. } => ErrorCode::NotFound,
            ApplyError::TransformNotPaused { .. } => ErrorCode::Conflict,
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
            ApplyError::Intake(err) => write!(f, "backfill marker error: {err}"),
            ApplyError::TransformNotFound { transform } => {
                write!(f, "no transform named '{transform}' is registered")
            }
            ApplyError::TransformNotPaused { transform } => write!(
                f,
                "'{transform}' is not currently paused; resuming it re-runs its full \
                 backfill, which is only valid from `paused` or `quarantined`"
            ),
            ApplyError::ReverseTriggerNotResolvable { from_table } => write!(
                f,
                "cannot resolve a whole-keyspace reverse trigger against live full row images \
                 for from-table '{from_table}': a TRUNCATE carries no image, so the reverse \
                 delta/fallback path has no per-row old/new parent to diff against; this \
                 combination is unreachable from any current call site and indicates a newly \
                 added caller that must implement the whole-keyspace arm for real"
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
            ApplyError::ClaimLost
            | ApplyError::VersionFenceMiss { .. }
            | ApplyError::HopBoundExceeded { .. }
            | ApplyError::SourceTableDropped { .. }
            | ApplyError::ColumnNotPaused { .. }
            | ApplyError::DefinitionNotLive { .. }
            | ApplyError::TransformNotFound { .. }
            | ApplyError::TransformNotPaused { .. }
            | ApplyError::ReverseTriggerNotResolvable { .. } => None,
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

/// The catalog's lookup key for a folded record's `src_table`: everything
/// after the last `.`, if any.
///
/// A definition's `def.source` is always a *bare* table name — even once
/// issue #76 taught the grammar's `TRANSFORM ... FROM <table>` clause an
/// explicit `schema.table` spelling, `def.source` itself still only ever
/// holds the bare table part (see `defs::ast::TransformDef`'s own doc
/// comment for why; `defs::parser`'s grammar and `defs/mod.rs`'s own tests
/// cover both the bare and explicitly-qualified parses). CDC intake's
/// own producer, though, always stages changes under the qualified
/// `"schema.table"` shape `intake::publication::qualify` builds, which
/// [`FoldedChange::src_table`] inherits directly from the ring. This is the
/// one seam that reconciles the two conventions: strip a schema prefix
/// before ever asking the catalog about a folded record's source.
///
/// Note this produces a *bare* key even though, as of issue #72,
/// `transform_definitions.source_table`/`source_table_versions.source_table`
/// themselves now persist the fully-qualified form — those columns' own
/// read sites (e.g. [`crate::defs::source_table_version`]) match against
/// their bare table-name suffix precisely so this function's output, and
/// every internal key this whole apply path builds from it (`by_source`,
/// `ApplyPlan::versions`, etc.), can stay unchanged rather than needing this
/// hot path to thread real schema identity through. See
/// [`crate::defs::source_table_version`]'s doc comment for the full
/// bare-vs-qualified rationale — including why issue #73 (which also
/// persists `transform_definitions.target_table` qualified) does *not*
/// retire this stripping: a target table's own downstream `src_table` (the
/// `Recompute` rows this module stages) is still unqualified —
/// [`crate::defs::ddl::neighbor_table_name`] deliberately never adds a
/// schema, issue #73 or not — so stripping remains a no-op for that case,
/// exactly as before #72, rather than becoming a stable identity function
/// this call site could now skip outright. Retiring the split entirely (by
/// qualifying every emitted `src_table`, `Recompute` rows included) is issue
/// #75's emission-audit territory.
///
/// This function's output stays purely a *lookup key* (issue #76's own
/// reviewer follow-up): every catalog read below it (`source_table_version`,
/// `transforms_for_source`, `relationships_to_table`) keeps using this bare
/// form, matching the bare-suffix indexes those tables are keyed on. The
/// *physical* SQL builders that actually read a live source row
/// (`ddl::source_primary_key`, [`read_live_rows_batch`], the source string
/// embedded in an [`AggregateTargetPlan`]) use the qualified
/// `change.src_table` each bucket's own changes already carry instead — see
/// `compute`'s `by_source` loop — never this bare key, so a same-named table
/// in a different schema can't make one of those builders read the wrong
/// physical relation.
fn catalog_source_key(src_table: &str) -> &str {
    match src_table.rsplit_once('.') {
        Some((_, table)) => table,
        None => src_table,
    }
}

/// Resolves `src_table` to the fully-qualified identity
/// [`catalog::transforms_for_source`]/[`catalog::dependents_of`] now require
/// (issue #74, ADR-0007: `schema_nodes` keys on qualified identity, so a
/// bare lookup there silently finds nothing rather than erroring).
///
/// A no-op for the common case — `src_table` already contains a `.` — which
/// covers every real CDC-staged or backfill-enumerated change (issue #76
/// qualifies `change.src_table` unconditionally at the point it's staged).
/// Two different shapes of bare `src_table` reach this function, needing
/// two different resolutions — both handled by delegating to
/// [`catalog::resolve_graph_identity`] rather than this function choosing
/// between them itself:
///
/// 1. A downstream `Recompute` trigger *this apply path itself* staged for
///    a chained definition's target (`compute`'s "Downstream propagation"
///    step, `apply_and_mark_drained`), carrying the plain, bare
///    `def.def.target` as its `src_table` (qualifying every such row at the
///    point it's staged is issue #75's emission-audit territory, not this
///    one's — see `catalog_source_key`'s own doc comment on the same
///    deliberate-bare convention). This is `resolve_graph_identity`'s
///    bare-target-suffix fallback: the name can only be some other live
///    definition's own target.
/// 2. A reverse-recompute trigger for a relationship's from-side
///    (`from_side_keys`'s callers below, staging `rel.def.from_table` as
///    `src_table`) —
///    `relationship_definitions.from_table` is always bare (ADR-0007's
///    "Scope" section leaves relationship endpoints unqualified) and is a
///    genuine *source* table, never anyone's target, so the bare-target-
///    suffix fallback above would never find it. This is
///    `resolve_graph_identity`'s *first* step instead: a plain physical
///    `search_path` lookup, exactly like resolving a fresh definition's own
///    bare `FROM`.
async fn qualified_schema_node_key(pool: &Pool, src_table: &str) -> Result<String, ApplyError> {
    if src_table.contains('.') {
        return Ok(src_table.to_string());
    }
    Ok(catalog::resolve_graph_identity(pool, src_table).await?)
}

/// Decodes a staged jsonb image (bound as text — this crate has no
/// `serde_json` dependency, matching `append.rs`/`fold.rs`'s convention)
/// into a [`Row`] via `jsonb_each_text`, so the evaluator never has to
/// parse JSON itself. A JSON `null` value decodes to `None`, matching
/// `Row`'s "absent column" vs. "present but NULL" distinction the evaluator
/// depends on (`eval.rs`'s `MissingColumn` vs. plain `None` propagation).
/// The intermediate `::text` cast matters, same as `append.rs`: `$1::jsonb`
/// alone makes Postgres describe the placeholder as `jsonb`, which
/// `&str`'s `ToSql` rejects before the value is ever sent.
async fn decode_image(pool: &Pool, image_text: &str) -> Result<Row, ApplyError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "select key, value from jsonb_each_text($1::text::jsonb)",
            &[&image_text],
        )
        .await?;
    let mut row = Row::with_capacity(rows.len());
    for r in rows {
        let key: String = r.get(0);
        let value: Option<String> = r.get(1);
        row.insert(key, value);
    }
    Ok(row)
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
/// decoding costs no extra round trip either; [`decode_image`]'s per-image
/// query is only paid for images that arrive already staged (`new_image`),
/// never for a live refetch. A key absent from the returned map means its
/// row is gone (already deleted, or never existed), which [`compute`]
/// treats as a delete, matching `read_live_row`'s old `None` case exactly.
///
/// `pk` may be a composite (multi-column) primary key (issue #126): `keys`
/// are each [`ddl::pk_key_sql_expr`]'s U+001F-joined text — the same
/// [`crate::intake::extract_key`] shape every [`FoldedChange::key`] already
/// carries for a composite-PK source, whether staged by real CDC intake or
/// by this crate's own reverse-relationship path
/// ([`from_side_rows_for_trigger_txn`]/`from_side_keys`'s callers) —
/// so the two agree on one row identity regardless of which produced it.
/// The batch match itself is a keyset join, one bind-parameter array per
/// `pk` column (mirroring `staging::apply_aggregate`'s `keyset_unnest`/
/// `keyset_match`), rather than a single `= any($1)` — `pk.len() == 1`
/// degenerates to exactly that single-array-parameter shape, so the
/// single-column case (still the overwhelmingly common one) pays no extra
/// cost.
///
/// # NULL-keyed groups (issue #110)
///
/// A `NULL` component decodes off `keys` (via [`ddl::transpose_pk_keys`]) as
/// a real `Option::None`, bound as SQL `NULL` in its column's array — so a
/// plain `t.<col> = u.<c>` join condition would never match it (`NULL` is
/// never `=` anything, including another `NULL`), which is exactly how a
/// `NULL`-keyed aggregate group's live row used to be mistaken for "already
/// deleted" by every downstream consumer of this function. [`live_rows_join_cond`]
/// therefore uses `is not distinct from` — the same per-column, only-when-
/// needed choice `apply_aggregate::keyset_match` already makes — for any `pk`
/// column that carries at least one `NULL` in this batch, so that group
/// resolves to its real live row instead.
async fn read_live_rows_batch(
    pool: &Pool,
    source_table: &str,
    pk: &[PrimaryKeyColumn],
    keys: &[&str],
) -> Result<HashMap<String, Row>, ApplyError> {
    if keys.is_empty() {
        return Ok(HashMap::new());
    }
    let client = pool.get().await?;
    let columns = ddl::transpose_pk_keys(pk, source_table, keys)?;
    let pk_idents: Vec<String> = pk.iter().map(|c| quote_ident(&c.name)).collect();
    let arrays: Vec<String> = pk
        .iter()
        .enumerate()
        .map(|(i, c)| format!("${}::text[]::{}[]", i + 1, c.data_type))
        .collect();
    let u_cols: Vec<String> = (0..pk.len()).map(|i| format!("c{i}")).collect();
    let null_safe: Vec<bool> = columns
        .iter()
        .map(|c| c.iter().any(Option::is_none))
        .collect();
    let join_cond = live_rows_join_cond(&pk_idents, &u_cols, &null_safe);
    let k_expr = ddl::pk_key_sql_expr(pk, Some("t"));
    // Issue #248: an explicit per-column `jsonb_build_object`, not
    // `to_jsonb(t.*)` — see `row_as_text_jsonb_sql`'s doc comment for why.
    let row_columns = live_row_columns(&**client, source_table).await?;
    let doc_expr = row_as_text_jsonb_sql("t", &row_columns);
    let sql = format!(
        "select m.k, e.key, e.value \
         from (select {k_expr} as k, {doc_expr} as doc from {} t \
               join unnest({}) as u({}) on {join_cond}) m \
         cross join lateral jsonb_each_text(m.doc) e",
        ddl::qualified_source_table(source_table),
        arrays.join(", "),
        u_cols.join(", "),
    );
    let params: Vec<&(dyn ToSql + Sync)> =
        columns.iter().map(|c| c as &(dyn ToSql + Sync)).collect();
    let db_rows = client.query(&sql, &params).await?;
    let mut rows: HashMap<String, Row> = HashMap::new();
    for db_row in db_rows {
        let key: String = db_row.get(0);
        let field: String = db_row.get(1);
        let value: Option<String> = db_row.get(2);
        rows.entry(key).or_default().insert(field, value);
    }
    Ok(rows)
}

/// A `t.<col> <op> u.<c>` conjunction matching a refetched row's primary-key
/// columns against the keyset relation — [`read_live_rows_batch`]'s join
/// condition, factored out as its own pure, directly testable function (the
/// same convention [`key_array_filter`] and
/// `apply_aggregate::keyset_match`/`keyset_match_source` use for their own
/// per-column operator choice). `null_safe[i]` selects `is not distinct
/// from` over plain `=` for column `i`: `=` is preferred whenever no key in
/// this batch binds a `NULL` for that column (hashable/indexable, so
/// Postgres can pick a plan that uses a btree index on `t.<col>` — the same
/// tradeoff `apply_aggregate::keyset_match`'s own doc comment explains), but
/// a batch that does needs `is not distinct from` for it (issue #110):
/// plain `=` never matches a `NULL` operand, so a `NULL`-keyed group's live
/// row would otherwise be indistinguishable from "row doesn't exist" and
/// mistaken for a delete.
fn live_rows_join_cond(pk_idents: &[String], u_cols: &[String], null_safe: &[bool]) -> String {
    pk_idents
        .iter()
        .zip(u_cols)
        .enumerate()
        .map(|(i, (ident, u_col))| {
            let op = if null_safe[i] {
                "is not distinct from"
            } else {
                "="
            };
            format!("t.{ident} {op} u.{u_col}")
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
    let row = client
        .query_opt(
            "select pg_catalog.format_type(a.atttypid, a.atttypmod) \
             from pg_attribute a \
             where a.attrelid = pg_catalog.to_regclass($1) \
               and a.attname = $2 \
               and a.attnum > 0 \
               and not a.attisdropped",
            &[&table, &column],
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
/// than a caller-supplied subset. `table` may be either the bare/qualified
/// form `to_regclass` parses unquoted (e.g. `key_column_pg_type`'s own
/// `table` argument) or an already `quote_ident`-quoted `"schema"."table"`
/// string (e.g. [`ddl::qualified_relationship_projection_table`]'s output):
/// `to_regclass` parses a quoted-identifier bind parameter exactly the way
/// the SQL parser would parse the same text in a `FROM` clause, so either
/// shape resolves to the right relation.
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
            &[&table],
        )
        .await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

/// [`live_row_columns`], memoized per `table` in `cache` — the caching
/// counterpart Phase 3's reverse-trigger loop needs
/// (`apply_and_mark_drained_many`'s "3d" step, via
/// [`stage_reverse_recompute_fallback`] and its `diff_pass` closure).
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
                tbl = quote_ident(from_table),
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
                tbl = quote_ident(from_table),
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
    key_src_changed: &HashMap<String, Option<std::time::SystemTime>>,
    reverse_recomputes: &mut HashMap<(String, String), (i32, Option<std::time::SystemTime>)>,
) -> Result<(), ApplyError> {
    if key_hops.is_empty() {
        return Ok(());
    }
    let join_keys: Vec<String> = key_hops.keys().cloned().collect();
    let from_pk = ddl::source_primary_key(pool, &rel.def.from_table).await?;
    let matches = from_side_keys(
        pool,
        &rel.def.from_table,
        &from_pk,
        &rel.def.from_col,
        &ReverseTrigger::Keys(&join_keys),
    )
    .await?;
    for (from_key, join_text) in matches {
        // `Keys` always reports which key matched — see `from_side_keys`'s
        // own doc comment.
        let join_text =
            join_text.expect("ReverseTrigger::Keys always reports the matched join value");
        let hop = key_hops.get(&join_text).copied().unwrap_or(0) + 1;
        let src_changed = key_src_changed.get(&join_text).copied().flatten();
        reverse_recomputes
            .entry((rel.def.from_table.clone(), from_key))
            .and_modify(|(h, sc)| {
                *h = (*h).max(hop);
                *sc = earliest_src_changed(*sc, src_changed);
            })
            .or_insert((hop, src_changed));
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
/// "decide in Phase 2, apply in Phase 3" split [`ApplyPlan::downstream_readers`]
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
/// `from_table` is the definition's own source table (a relationship's
/// `from_table`).
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
/// reads. `None` is the shape `quarantine::recompute_column`'s ad hoc,
/// non-transactional resume path passes, since it isn't part of the staging
/// ring's claim/fold/compute/apply pipeline this gen bump guards — that
/// caller discards the returned gen-bump map entirely, so `None` simply
/// costs it nothing beyond not bothering to compute the old-side half.
///
/// `changes`, when supplied, is the same-length, same-index slice of
/// [`FoldedChange`]s `rows`/`old_rows` were decoded from — issue #133's
/// signal, read for its `group_key` (the real, pre-fold union of touched
/// join keys; see that field's doc comment) and unioned into the same
/// gen-bump touched-key set `old_rows` widens. `None` for the same
/// `quarantine::recompute_column` caller as `old_rows`: that path has no
/// `FoldedChange`s at all (a live full-table scan, not the staging ring's
/// pipeline) and, as above, discards the gen-bump map regardless.
pub(crate) async fn build_relationship_context(
    pool: &Pool,
    from_table: &str,
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
        let Some(reldef) = catalog::relationship_by_name(pool, from_table, &rel_name).await? else {
            // Unknown relationship: leave it out and let the evaluator surface
            // `EvalError::UnknownRelationship`, the same as the pure path.
            continue;
        };
        let from_col = reldef.def.from_col.clone();
        let to_col = reldef.def.to_col.clone();
        let to_table = reldef.def.to_table.clone();

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
                let qualified_projection = projection.as_ref().map(|p| {
                    ddl::qualified_relationship_projection_table(
                        pool.target_schema(),
                        &p.projection_table,
                    )
                });

                let to_rows_by_key = match &qualified_projection {
                    Some(qualified_projection) => {
                        fetch_relationship_projection_rows(
                            pool,
                            qualified_projection,
                            &to_table,
                            &to_col,
                            &join_keys,
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
                            from_table = %from_table,
                            "to-one relationship has no settled parent projection; \
                             resolving as empty (should be unreachable — #129 creates \
                             one unconditionally)"
                        );
                        HashMap::new()
                    }
                };
                by_name.insert(
                    rel_name,
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
                            if let Some(Some(text)) = old_row.get(&from_col) {
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
// (b) — then, for every aggregate target whose fields are fully invertible
// and read only this one relationship
// ([`ReverseRelationshipShape::aggregate_shapes`]), applies a true
// subtract-old/add-new delta over the parent's from-side rows — reusing
// `apply_aggregate`'s existing per-row contribution/delta-apply machinery
// rather than reinventing it. Anything that mechanism can't cover (a 1-1
// target, a `MIN`/`MAX` field, a definition reading more than one
// relationship — see [`build_reverse_relationship_shape`]'s doc comment)
// falls back to the pre-#131 image-less `Recompute`, unchanged — the same
// fallback any of #132's four guards also uses when it rejects a record.

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
    /// The target schema `projection_table_bare` lives in — bare, for the
    /// same `information_schema.columns` introspection.
    target_schema: String,
    to_col: String,
    from_table: String,
    from_col: String,
    /// The from-table's primary key, possibly composite (issue #126) — see
    /// [`from_side_rows_for_trigger_txn`]'s doc comment for how a
    /// multi-column key's row identity is encoded/decoded.
    from_pk: Vec<PrimaryKeyColumn>,
    /// Fully-invertible, single-relationship aggregate targets reading this
    /// relationship — the issue #131 fast (true-delta) path. See
    /// [`build_reverse_relationship_shape`]'s doc comment for exactly which
    /// definitions qualify.
    aggregate_shapes: Vec<ReverseAggregateShape>,
    /// Whether at least one definition on `from_table` referencing this
    /// relationship is *not* covered by `aggregate_shapes` — a
    /// `KeySpace::OneToOne` definition, an aggregate with a
    /// `RecomputeOnly` field (`MIN`/`MAX`, or a composed expression), or an
    /// aggregate reading more than one relationship. Every touched
    /// from-side row still needs the pre-#131 image-less `Recompute`
    /// treatment when this is `true`.
    needs_recompute_fallback: bool,
}

/// One aggregate target's issue #131 fast-path shape: everything needed to
/// turn a live-enumerated from-side row into a [`apply_aggregate::GroupPlan`]
/// delta, without a live `JOIN` back to the to-side table (the parent's
/// old/new *image* already has the value; see the module doc comment above).
#[derive(Debug)]
struct ReverseAggregateShape {
    /// The target's fully-qualified identity
    /// ([`crate::defs::model::Definition::target_table`]).
    target: String,
    /// An empty-`.groups` template — cloned fresh per [`RelationshipReverseRecord`]
    /// this shape applies to (Phase 3 does not batch sibling records
    /// touching the same target together; see that step's own doc comment
    /// for why that's a documented, non-correctness-affecting
    /// simplification). Built from the *original*, unrewritten definition
    /// (`AggregateTargetPlan::new`'s usual construction) — its
    /// `field_exprs`/`count_column_names` must match what
    /// `create_aggregate_target_table` actually created, not this shape's
    /// relationship-substituted evaluation form below.
    template: AggregateTargetPlan,
    /// `def`, relationship-substituted (every `RelationshipPath { rel:
    /// <this relationship's name>, column }` rewritten to `Column(<synthetic
    /// column name>)`, per `synthetic_columns`) and then
    /// [`apply_aggregate::contribution_def`]'s `AVG`-as-`SUM` rewrite
    /// applied on top — ready to hand [`apply_aggregate::row_contribution`]
    /// directly, the same way `accumulate_changes` hands it its own
    /// per-batch rewrite.
    contribution_def: TransformDef,
    /// `def`'s source-column type map, widened with one entry per synthetic
    /// column (typed from the to-side column it stands in for).
    source_columns: HashMap<String, ValueType>,
    /// `(to_side_column, synthetic_column_name)` — how Phase 3 splices the
    /// parent's old/new image into a live from-side row before evaluating
    /// it (see [`augment_row_with_relationship_value`]).
    synthetic_columns: Vec<(String, String)>,
    /// The row-column name [`apply_aggregate::derive_group_key`] should read
    /// for each of `template`'s `GROUP BY` keys, in order — a plain key's
    /// own column name (present on the from-side row as-is), or (issue #137)
    /// a relationship-path key's synthetic column name from
    /// `synthetic_columns` (present only on an *augmented* row — see
    /// [`augment_row_with_relationship_value`]). `template.group_by` itself
    /// cannot be reused for this: it holds each key's **target** column name
    /// (`author`, say), never the synthetic name
    /// (`__trellis_rev_author`-shaped) an augmented row actually carries —
    /// reading `template.group_by` directly against an augmented row would
    /// silently find nothing for a relationship-path key and always resolve
    /// it to `NULL`.
    group_by_row_columns: Vec<String>,
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
    /// `pg_current_wal_lsn()` against the same connection). Phase 3 must not
    /// apply this record until [`StagedWatermark::get`] reports intake has
    /// staged everything committed at or before this value — see
    /// `check_reverse_guards`'s guard (a) arm and this module's "Issue #131,
    /// #132" doc section for why a lower/earlier capture is always safe (it
    /// only makes the barrier easier, never wrongly permissive) while a
    /// later one would not be.
    watermark: PgLsn,
    hop_gen: i32,
    src_changed: Option<std::time::SystemTime>,
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

/// The synthetic source-column name [`build_reverse_relationship_shape`]
/// substitutes for a `RelationshipPath { column, .. }` reference — the
/// `__trellis_`-prefixed hidden-column convention
/// [`ddl::PROJECTION_GEN_COLUMN`]'s doc comment already flags as carrying a
/// (small, accepted) collision risk with a real source column, reused here
/// for the same reason.
fn synthetic_relationship_column(column: &str) -> String {
    format!("__trellis_rev_{column}")
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

/// Rewrites every `RelationshipPath { rel: <rel_name>, column }` in `expr`
/// (recursing through `BinaryOp`/`FunctionCall`) to `Column(<synthetic
/// column name>)` per `synthetic_columns`, in place. A `RelationshipPath`
/// for a *different* relationship name is left untouched — for this
/// module's own reverse-path caller, reaching one here at all means the
/// caller already excluded this definition from the fast path (see
/// [`build_reverse_relationship_shape`]'s multi-relationship fallback), so
/// this function is never actually asked to resolve one there.
///
/// `pub(super)`: issue #136's forward aggregate delta path
/// (`apply_aggregate::build_forward_relationship_shape`) reuses this
/// directly rather than reimplementing it — this function has no
/// reverse-specific assumption baked into its own signature (it takes a
/// bare `rel_name`/`synthetic_columns` map, nothing about which direction
/// the substitution serves), unlike [`synthetic_relationship_column`]'s
/// naming convention, which the forward path deliberately does *not* reuse
/// verbatim (see that path's own `forward_relationship_synthetic_column`
/// for why: a forward plan can carry more than one relationship, unlike a
/// reverse shape).
pub(super) fn substitute_relationship_path(
    expr: &mut Expr,
    rel_name: &str,
    synthetic_columns: &HashMap<String, String>,
) {
    match expr {
        Expr::RelationshipPath { rel, column } if rel == rel_name => {
            let synthetic = synthetic_columns
                .get(column)
                .cloned()
                .unwrap_or_else(|| synthetic_relationship_column(column));
            *expr = Expr::Column(synthetic);
        }
        Expr::BinaryOp { lhs, rhs, .. } => {
            substitute_relationship_path(lhs, rel_name, synthetic_columns);
            substitute_relationship_path(rhs, rel_name, synthetic_columns);
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                substitute_relationship_path(arg, rel_name, synthetic_columns);
            }
        }
        Expr::Column(_)
        | Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::TypedLiteral { .. } => {}
        Expr::RelationshipPath { .. } => {}
    }
}

/// Builds `rel`'s [`ReverseRelationshipShape`] (issue #131) — the one-time,
/// per-relationship, per-`compute()`-call catalog resolution every parent
/// key this batch's fold touches for `rel` shares (see
/// [`RelationshipReverseRecord`]'s doc comment).
///
/// **The design fork this issue's own report needs to flag prominently**:
/// which definitions get the new true-delta fast path
/// (`ReverseRelationshipShape::aggregate_shapes`) versus the pre-#131
/// fallback (`needs_recompute_fallback`, i.e. an ordinary image-less
/// `Recompute` staged for every touched from-side row, exactly as before
/// this issue). A definition qualifies for the fast path **iff** all of:
/// 1. It's a [`KeySpace::Aggregate`] definition (a `KeySpace::OneToOne`
///    target has no additive semantics to delta at all — a from-side row's
///    own target row is just re-derived outright, which is already O(one
///    row), not the O(group) cost this epic targets — so 1-1 targets simply
///    keep going through the old mechanism, unchanged).
/// 2. Every field [`apply_aggregate::classify_fields`] classifies is
///    invertible (`Sum`/`Avg`/`Count`) — a `RecomputeOnly` field
///    (`MIN`/`MAX`, or a composed expression) has no per-row delta at all by
///    construction, so a definition with even one such field falls back
///    whole (not just that one field) to keep this issue's scope tractable;
///    a future pass could split a target's fields between the two
///    mechanisms.
/// 3. It references **exactly this one relationship** — a definition
///    reading two different to-one relationships needs the *other* one's
///    current value resolved too (a second projection read) to evaluate a
///    contribution at all, which this issue does not implement; it falls
///    back rather than silently mis-evaluating.
async fn build_reverse_relationship_shape(
    pool: &Pool,
    rel: &RelationshipDefinition,
) -> Result<ReverseRelationshipShape, ApplyError> {
    let projection = catalog::relationship_projection(pool, rel.id).await?;
    let (qualified_projection, projection_table_bare) = match projection {
        Some(p) => (
            ddl::qualified_relationship_projection_table(pool.target_schema(), &p.projection_table),
            p.projection_table,
        ),
        None => {
            tracing::error!(
                relationship = %rel.def.name,
                to_table = %rel.def.to_table,
                "to-one relationship has no settled parent projection; every \
                 reverse record for it will be treated as an ordering-check \
                 miss (should be unreachable — #129 creates one unconditionally)"
            );
            (String::new(), String::new())
        }
    };
    let target_schema = pool.target_schema().to_string();
    let from_pk = ddl::source_primary_key(pool, &rel.def.from_table).await?;
    // `transforms_for_source` matches `schema_nodes.table_name` exactly
    // (ADR-0007's fully-qualified keying) and every SQL-emitting site below
    // (`AggregateTargetPlan::source`, `from_side_rows_for_trigger_txn`'s
    // `ddl::qualified_source_table`) documents the same requirement — unlike
    // `source_primary_key` above (a `to_regclass` resolution that already
    // tolerates a bare name via `search_path`), so this shape stores the
    // qualified form of `from_table` throughout, not `rel.def.from_table`
    // verbatim (which the to-many arm elsewhere in this module can get away
    // with, since it only ever feeds `source_primary_key`/
    // `from_side_keys`).
    let qualified_from_table = qualified_schema_node_key(pool, &rel.def.from_table).await?;
    let defs = catalog::transforms_for_source(pool, &qualified_from_table).await?;

    let mut aggregate_shapes = Vec::new();
    let mut needs_recompute_fallback = false;

    for def in &defs {
        let refs = eval::relationship_references(&def.def);
        let rel_refs: Vec<&(String, String)> = refs
            .iter()
            .filter(|(name, _)| name == &rel.def.name)
            .collect();
        if rel_refs.is_empty() {
            continue;
        }
        let distinct_rels: std::collections::HashSet<&str> =
            refs.iter().map(|(name, _)| name.as_str()).collect();

        let KeySpace::Aggregate { group_by } = &def.def.key_space else {
            // KeySpace::OneToOne — design fork 1, see this function's doc
            // comment.
            needs_recompute_fallback = true;
            continue;
        };
        if distinct_rels.len() > 1 {
            // Design fork 3.
            needs_recompute_fallback = true;
            continue;
        }

        let relationships = catalog::resolve_relationships(pool, &def.def).await?;
        let substituted_exprs = crate::defs::backfill::substituted_field_exprs(&def.def)?;
        let field_plans = apply_aggregate::classify_fields(
            &def.def,
            group_by,
            &def.source_columns,
            &substituted_exprs,
            &relationships,
        )?;
        if field_plans
            .iter()
            .any(|f| f.kind == apply_aggregate::AggFieldKind::RecomputeOnly)
        {
            // Design fork 2.
            needs_recompute_fallback = true;
            continue;
        }

        // Issue #137: a `GROUP BY` key reaching here may itself be a
        // `GroupByKey::RelationshipPath` — design fork 3 above only excludes
        // more than one *distinct* relationship reference across fields and
        // `GROUP BY` together, so a relationship-path key that survived that
        // check is guaranteed to name `rel.def.name` itself (the sole
        // relationship this shape was built for), never some other
        // relationship. `relationships` (resolved for this relationship
        // only, just above) is therefore always the right map to look up a
        // relationship-path key's to-side column type in below.
        let group_by_types: Vec<ValueType> = group_by
            .iter()
            .map(|key| match key {
                GroupByKey::Column(c) => def
                    .source_columns
                    .get(c)
                    .copied()
                    .unwrap_or(ValueType::Numeric),
                GroupByKey::RelationshipPath { rel: r, column } => relationships
                    .get(r)
                    .and_then(|res| res.column_types.get(column))
                    .copied()
                    .unwrap_or(ValueType::Numeric),
            })
            .collect();
        let field_exprs: HashMap<String, Expr> = substituted_exprs
            .into_iter()
            .filter(|(name, _)| !group_by_contains(group_by, name))
            .collect();
        let template = AggregateTargetPlan::new(
            group_by,
            group_by_types,
            field_plans,
            qualified_from_table.clone(),
            def.target_table.clone(),
            field_exprs,
            // Every per-row contribution below resolves the relationship's
            // value from the parent's old/new image via a synthetic column,
            // never a live join — but `apply_aggregate::probe_group_exists`
            // (called generically by `apply_aggregate_target` for every
            // non-forced delta group, reverse-fast-path groups included)
            // still needs this join wired whenever a `GROUP BY` key itself
            // reads the relationship (issue #137): "does the source still
            // have any row for this (tag, author) tuple" is a question about
            // *all* of `post_tags`, not just the rows this one change
            // touched, so it has no synthetic-column shortcut and must join
            // back to the live to-side table — exactly the same live-join
            // existence check the forward path's `probe_group_exists` has
            // always used for a relationship-reading target, #136 never
            // touched that (it only replaced live-join *value* reads with
            // the settled projection, not the boolean existence probe).
            // Design fork 3 already ensures `rel` is the only relationship
            // this shape could possibly need, so this is always exactly one
            // join, never a second lookup.
            vec![apply_aggregate::RelJoin {
                name: rel.def.name.clone(),
                to_table: rel.def.to_table.clone(),
                to_col: rel.def.to_col.clone(),
                from_col: rel.def.from_col.clone(),
            }],
        );

        let mut synthetic_columns = Vec::new();
        let mut synthetic_map = HashMap::new();
        let mut source_columns = def.source_columns.clone();
        for (_, column) in &rel_refs {
            if synthetic_map.contains_key(column.as_str()) {
                continue;
            }
            let synthetic = synthetic_relationship_column(column);
            let value_type = relationships
                .get(&rel.def.name)
                .and_then(|r| r.column_types.get(column))
                .copied()
                .unwrap_or(ValueType::Text);
            source_columns.insert(synthetic.clone(), value_type);
            synthetic_columns.push((column.clone(), synthetic.clone()));
            synthetic_map.insert(column.clone(), synthetic);
        }
        let mut rewritten = def.def.clone();
        for field in &mut rewritten.fields {
            substitute_relationship_path(&mut field.expr, &rel.def.name, &synthetic_map);
        }
        // Issue #137 fix: a `GROUP BY` key that is itself this relationship's
        // path (e.g. `GROUP BY tag, post.author`) must be rewritten to the
        // same synthetic `Column` its field-level references above already
        // are, mirroring `apply_aggregate::build_forward_relationship_shape`'s
        // own `rewritten.key_space` rewrite. Without this,
        // `eval::evaluate_aggregate`'s `group_by` set (keyed by each key's
        // *target* column name, e.g. `author`) never matches a field whose
        // expression was just substituted to `Column("__trellis_rev_author")`
        // — any field bare-passthrough-referencing the relationship path
        // (e.g. `SELECT post.author AS author`, the exact shape
        // `validate::a_group_by_relationship_path_bare_passthrough_field_is_allowed`
        // proves is legal) would then fail with `EvalError::MissingColumn`
        // the moment the reverse fast path tried to compute its
        // contribution, aborting the whole apply.
        if let KeySpace::Aggregate { group_by } = &mut rewritten.key_space {
            for key in group_by.iter_mut() {
                if let GroupByKey::RelationshipPath {
                    rel: key_rel,
                    column,
                } = key
                    && key_rel == &rel.def.name
                    && let Some(synthetic_name) = synthetic_map.get(column)
                {
                    *key = GroupByKey::Column(synthetic_name.clone());
                }
            }
        }
        let contribution_def = apply_aggregate::contribution_def(&rewritten);
        // Issue #137: see `ReverseAggregateShape::group_by_row_columns`'s
        // own doc comment — `synthetic_map` already carries an entry for
        // every relationship-path `GROUP BY` key's column (via `rel_refs`,
        // which `eval::relationship_references` now includes group-by
        // references in), whether or not any field also reads it.
        let group_by_row_columns: Vec<String> = group_by
            .iter()
            .map(|key| match key {
                GroupByKey::Column(name) => name.clone(),
                GroupByKey::RelationshipPath { column, .. } => synthetic_map[column].clone(),
            })
            .collect();

        aggregate_shapes.push(ReverseAggregateShape {
            target: def.target_table.clone(),
            template,
            contribution_def,
            source_columns,
            synthetic_columns,
            group_by_row_columns,
        });
    }

    Ok(ReverseRelationshipShape {
        id: rel.id,
        qualified_projection,
        projection_table_bare,
        target_schema,
        to_col: rel.def.to_col.clone(),
        from_table: qualified_from_table,
        from_col: rel.def.from_col.clone(),
        from_pk,
        aggregate_shapes,
        needs_recompute_fallback,
    })
}

/// The `to_col` text value off `row`, or `None` if `row` is absent or the
/// column is (an absent column and a genuine SQL `NULL` are both "no key
/// here" for join purposes).
fn relationship_key_text(row: &Option<Row>, to_col: &str) -> Option<String> {
    row.as_ref().and_then(|r| r.get(to_col)).cloned().flatten()
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
/// **and** captures guard (a)'s `X` — `pg_current_wal_lsn()`, "the source's
/// write frontier" — in the very same statement, per the issue's own
/// requirement (`select ..., pg_current_wal_lsn() from <projection> where
/// ...`), not as a separate round trip. A plain, unlocked read taken once in
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
/// still issues a bare `select pg_current_wal_lsn()` so `watermark` is
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
                    "select {lsn_ident}, {gen_ident}, pg_current_wal_lsn() \
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
    let row = client.query_one("select pg_current_wal_lsn()", &[]).await?;
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
/// by both the reverse-delta fast path (`diff_pass`) and the reverse
/// fallback ([`stage_reverse_recompute_fallback`]).
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
/// (`apply_and_mark_drained_many`'s "3d" step, both directly from
/// [`stage_reverse_recompute_fallback`] and from the reverse-delta fast
/// path's `diff_pass` closure), and `from_table` is invariant across many
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
    /// Guard (a): intake has not yet staged everything committed at or
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
/// either its `old_image` or `new_image`, committed (`lsn`) at or before
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

/// Issue #134 review follow-up: the fast (`diff_pass`) aggregate delta
/// path's own, *additional* precondition — stricter than, and checked
/// independently of, guard (c) above (which still governs the plain
/// apply-or-defer decision, unchanged, per #132's own ablation proof that
/// its current "still undrained" shape is load-bearing for *that*
/// question).
///
/// **The hazard this closed** (confirmed by an independent review
/// reproduction against the shipped, unmodified #134 commit — real, not
/// retry-specific): `apply_aggregate::accumulate_changes`'s
/// `force_every_group` path (`rel_joins` non-empty) used to do a **live**,
/// full group recompute, via a direct SQL join back to the relationship's
/// to-side table, whenever *any* sibling from-side row's own forward CDC was
/// evaluated — completely independent of, and invisible to, this
/// relationship's settled-parent-projection/guard machinery (it never read
/// the projection, and critically it never bumped
/// [`ddl::PROJECTION_GEN_COLUMN`] the way the projection-based forward path
/// does — see [`RelationshipGenBump`]'s own doc comment). If a sibling's own
/// drain ran `force_every_group` for this same group *after* `old_row` was
/// true but *before* this reverse's `diff_pass` got to apply, the group's
/// stored value could already equal what `diff_pass` was about to *add on
/// top of* — a real double-correction, confirmed to reproduce **on a
/// genuinely first attempt** (`retry_count == 0`), not just a retried one:
/// `retry_count > 0` alone (this module's other guard-rejection-specific
/// restriction) was too narrow, since guard (c) itself can genuinely find
/// nothing in flight — the racing sibling may have already fully drained —
/// and let a first attempt straight into the same trap.
///
/// **Issue #136 (epic #127) deleted `force_every_group` outright** — an
/// ordinary sibling forward touch to a relationship-reading aggregate now
/// always resolves through the same settled-parent-projection/guard
/// machinery this function protects (`apply_aggregate::build_forward_relationship_shape`,
/// wired from this module's own `build_relationship_context` call in the
/// aggregate branch of `compute`), so the specific live-read race this
/// function was built for is no longer reachable through an ordinary CDC
/// row at all. This function's own CDC-row scan below is left unchanged
/// regardless: it has no way to know *why* a matching sibling row exists
/// (a safe post-#136 delta application, or some other cause), so it still
/// conservatively routes to the fallback on any match — safe (the fallback
/// is always correct), just more conservative than strictly necessary for
/// that one now-closed case. Tightening this to distinguish the two is a
/// possible follow-up, not attempted here (out of #136's own scope, and
/// this function still has genuine, narrower value below independent of
/// that hazard — see "What this checks" and "What it does *not* close").
///
/// **What this checks, and its own limits.** Unlike guard (c) (scoped to
/// rows still `state <> 'drained'`), this scans every physical ring row —
/// staged, in-flight, *or already drained* — matching `keys` via
/// `from_col`, with `lsn` in `(since_lsn, watermark_x]` (an *exclusive*
/// lower bound: `since_lsn` is `record.prev_lsn`, the projection's
/// already-known-good position as of Phase 2's capture — anything at or
/// before it is already accounted for; `None` means "no prior projection
/// row" — a parent INSERT — so *every* matching row counts, since there is
/// no "before" era to bound against). This catches the same-batch and
/// recently-drained-but-not-yet-retired cases — the realistic shape of
/// this hazard, and the only shape this module's own tests (this issue's
/// retry scenarios, and every existing #131/#132/#133 positive-path test)
/// can exercise without deliberately engineering ring retirement into the
/// gap.
///
/// **What it does *not* close**: a sibling whose ring evidence has already
/// been *retired* (`retire::retire_drained_segments` truncates the whole
/// physical slot once every older batch has drained) before this check
/// runs leaves no trace here to find — this function can only see what the
/// ring still holds.
///
/// **Post-#136, that residual gap is closed for the ordinary delta-path
/// case by guard (b), not by this function.** `check_reverse_guards` (guard
/// (b) in particular) always runs *before* this function is ever reached —
/// see this step's own call site — and now that the aggregate branch of
/// `compute` bumps `__trellis_gen` via `build_relationship_context` the same
/// way the `KeySpace::OneToOne` branch always did (see this issue's own
/// comment on that call site), any sibling forward delta that touched this
/// parent — retired ring evidence or not, since a generation counter, unlike
/// a ring row, is never erased by retirement — already failed guard (b) and
/// never reaches this function at all. What remains genuinely unclosed is
/// narrower and pre-existing (not introduced or widened by #136): an
/// image-less-recompute-triggered [`apply_aggregate::GroupPlan::force_full_recompute`]
/// (backfill, definition re-derive, or reverse propagation's own fallback —
/// see `apply_aggregate`'s module doc comment) still recomputes via a live
/// `JOIN` and still never bumps `gen`, so a retired *and* image-less-forced
/// sibling could still race undetected by either guard (b) or this
/// function's own ring scan. Not fixed here — the image-less/forced path is
/// explicitly out of #136's scope (see that issue), and closing this sliver
/// would need the same "bump gen from inside the bulk recompute" follow-up
/// this comment used to describe for the pre-#136 case generally.
async fn relationship_fast_path_precondition_holds(
    txn: &Transaction<'_>,
    from_table: &str,
    from_col: &str,
    keys: &[&str],
    since_lsn: Option<PgLsn>,
    watermark_x: PgLsn,
) -> Result<bool, ApplyError> {
    if keys.is_empty() {
        return Ok(true);
    }
    let arms = converge::per_ring_table(" union all ", |_slot, table| {
        format!(
            "select 1 from {table} r \
             where r.src_table = $1 \
               and r.op in ('insert', 'update', 'delete') \
               and r.lsn <= $2 \
               and ($5::pg_lsn is null or r.lsn > $5) \
               and (r.old_image ->> $3 = any($4::text[]) \
                    or r.new_image ->> $3 = any($4::text[]))"
        )
    });
    let sql = format!("select exists ({arms})");
    let row = txn
        .query_one(
            &sql,
            &[&from_table, &watermark_x, &from_col, &keys, &since_lsn],
        )
        .await?;
    let anything_found: bool = row.get(0);
    Ok(!anything_found)
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
// path, not block the forward path.** Guards (a)/(b)/(c) exist to protect
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

/// Stages the pre-#131 image-less `Recompute` fallback (issue #131's own
/// stopgap, shared since by every guard rejection before #134 and by
/// `ReverseRelationshipShape::needs_recompute_fallback`/`!fast_path_safe`
/// since) for every from-side row currently matching `old_key`/`new_key` via
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
/// `row_columns` is `shape.from_table`'s live column list, resolved once by
/// the caller via [`cached_row_columns`] — not re-introspected per call here
/// — since this function's own caller (the "3d" step's `for record in
/// &plan.relationship_reverses` loop) runs once per distinct touched parent
/// key in the batch, and many records touching one relationship all share
/// this same `from_table`. See [`from_side_rows_for_trigger_txn`]'s doc
/// comment for why that function takes the same parameter rather than
/// introspecting it itself.
#[allow(clippy::too_many_arguments)]
async fn stage_reverse_recompute_fallback(
    txn: &Transaction<'_>,
    shape: &ReverseRelationshipShape,
    old_key: &Option<String>,
    new_key: &Option<String>,
    hop_gen: i32,
    src_changed: Option<std::time::SystemTime>,
    seen_keys: &mut std::collections::HashSet<String>,
    fallback: &mut Vec<(String, String, i32, Option<std::time::SystemTime>)>,
    row_columns: &[String],
) -> Result<(), ApplyError> {
    for key in [old_key.clone(), new_key.clone()].into_iter().flatten() {
        let trigger = ReverseTrigger::Keys(std::slice::from_ref(&key));
        let from_rows = from_side_rows_for_trigger_txn(
            txn,
            &shape.from_table,
            &shape.from_col,
            &shape.from_pk,
            &trigger,
            row_columns,
        )
        .await?;
        for (from_key, _) in from_rows {
            if seen_keys.insert(from_key.clone()) {
                fallback.push((shape.from_table.clone(), from_key, hop_gen, src_changed));
            }
        }
    }
    Ok(())
}

/// Splices `parent_row`'s referenced to-side columns into a clone of
/// `from_row`, under each synthetic column name
/// [`ReverseAggregateShape::synthetic_columns`] maps it to — how a live
/// from-side row is made ready for
/// [`apply_aggregate::row_contribution`] against a
/// [`ReverseAggregateShape::contribution_def`], which reads the
/// relationship's value as an ordinary `Column` reference rather than
/// resolving a live `RelationshipPath` join. `parent_row` is `None` only
/// for the pass this apply never takes (a parent INSERT has no old-key
/// pass; a parent DELETE has no new-key pass — see this function's one call
/// site), so the synthetic column is always populated when it's actually
/// read.
fn augment_row_with_relationship_value(
    from_row: &Row,
    synthetic_columns: &[(String, String)],
    parent_row: &Option<Row>,
) -> Row {
    let mut augmented = from_row.clone();
    for (to_col, synthetic) in synthetic_columns {
        // Always insert the synthetic key, even when `parent_row` is
        // absent (this row's `from_col` doesn't match this pass' parent
        // key at all — a parent insert/delete/PK-change's "no match" side)
        // — as `None` (present, SQL `NULL`), never leaving the key out of
        // the map entirely. The evaluator's `Row` convention distinguishes
        // the two (`eval::EvalError::MissingColumn` fires only for a truly
        // *absent* key): the relationship column always exists on the
        // to-side schema, it just has no matching row for this pass, which
        // is exactly the same "resolves to `NULL`" shape a genuine SQL
        // `LEFT JOIN` no-match already produces (see `eval.rs`'s
        // `evaluate_with_relationships`/`ToOneRelationship` handling).
        let value = parent_row
            .as_ref()
            .and_then(|row| row.get(to_col))
            .cloned()
            .flatten();
        augmented.insert(synthetic.clone(), value);
    }
    augmented
}

/// The settled parent projection's current data columns (excluding the key
/// and the two bookkeeping columns) — the same `information_schema.columns`
/// introspection [`catalog::ensure_relationship_projection_in_txn`] already
/// uses, duplicated here since Phase 3 holds no `pool` (only `txn`) and that
/// function is private to `defs::catalog`.
async fn projection_data_columns(
    txn: &Transaction<'_>,
    target_schema: &str,
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
            &[&target_schema, &projection_table_bare],
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

    let Some(new_image) = new_image else {
        return Ok(());
    };
    let data_columns = projection_data_columns(
        txn,
        &shape.target_schema,
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

/// The to-side rows whose `to_col` matches any of `join_keys` (compared at
/// `to_col`'s own native type via [`key_column_pg_type`] — issue #125,
/// falling back to the old `::text` comparison if the column can't be
/// introspected), grouped by that key's `::text` (the evaluator's key
/// convention, shared with [`from_side_keys`]). A `NULL` `to_col` is
/// absent (SQL `NULL` never joins) — matching the evaluator's requirement
/// that such a to-side row carry no key. To-one relationships get exactly one
/// row per key (`to_col` is UNIQUE); to-many get the full related set.
/// Decodes each row's columns via the same in-SQL `jsonb_each_text` unnest
/// [`read_live_rows_batch`] uses.
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
    let tbl_ident = quote_ident(to_table);
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
/// to-one counterpart to [`fetch_to_side_rows`], reading `qualified_projection`
/// (already schema-qualified via [`ddl::qualified_relationship_projection_table`])
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
/// agree, and `to_table` is a plain bare/qualified table name `to_regclass`
/// resolves directly — unlike `qualified_projection`, which arrives here
/// already `quote_ident`-quoted for direct interpolation, not in the shape
/// `to_regclass` expects for *this* lookup (`to_table`/`key_col` is a
/// same-named-column shortcut, not a general rule about quoted input:
/// [`live_row_columns`], just below, binds `qualified_projection` itself as
/// a `to_regclass` parameter to read the projection's own live columns, and
/// that works fine — `to_regclass` parses an already-quoted qualified name
/// exactly like the SQL parser would parse the same text in a `FROM`
/// clause).
async fn fetch_relationship_projection_rows(
    pool: &Pool,
    qualified_projection: &str,
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
    let row_columns = live_row_columns(&**client, qualified_projection).await?;
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
            &[&table, &columns],
        )
        .await?;
    let mut types = HashMap::with_capacity(rows.len());
    for row in rows {
        let name: String = row.get(0);
        let type_oid: u32 = row.get(1);
        types.insert(name, crate::defs::pg_type::value_type_for_oid(type_oid));
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
/// `transform` fields). `src_changed` is [`FoldedChange::src_changed`]:
/// `Some` for a change that traces back to a real source commit (the
/// histogram's eventual `.observe()` value is `now - src_changed`, `now`
/// sampled fresh at flush time — see [`flush_apply_metrics`]), `None` for a
/// bare recompute trigger with no origin timestamp to measure against —
/// such a change still counts toward throughput, just not latency.
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
/// are terminal (that's [`ApplyPlan::downstream_readers`], computed once,
/// after every source's changes have been evaluated — see the end of
/// [`compute`]). Buffering here and filtering to only the terminal targets'
/// entries there reuses that one dedup'd downstream-reader lookup instead of
/// adding a second one per change; the filtered result is itself stored on
/// [`ApplyPlan`] (not flushed) for the same retry-safety reason as
/// `transform_observations`.
fn buffer_transform_apply_metrics(
    transform: &str,
    src_changed: Option<std::time::SystemTime>,
    end_to_end_origins: &mut HashMap<String, Vec<std::time::SystemTime>>,
    transform_observations: &mut Vec<(String, Option<std::time::SystemTime>)>,
) {
    if let Some(src_changed) = src_changed {
        end_to_end_origins
            .entry(transform.to_string())
            .or_default()
            .push(src_changed);
    }
    transform_observations.push((transform.to_string(), src_changed));
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
}

/// One key's deletion from a target table (the folded change had no
/// `new_image`). `src_changed` plays the same forward-carrying role as
/// [`TargetWrite::src_changed`].
#[derive(Debug, Clone)]
struct TargetDelete {
    pk_text: String,
    hop_gen: i32,
    src_changed: Option<std::time::SystemTime>,
}

/// Everything Phase 3 needs to write one target table: its primary key
/// shape (for the pre-lock/upsert/delete SQL), the calculated-field column
/// names and their inferred [`ValueType`]s (both aligned with every
/// [`TargetWrite::values`], so [`apply_target`] knows which Postgres type
/// each column casts to), and the writes and deletes this batch computed
/// for it.
#[derive(Debug, Clone)]
struct TargetPlan {
    pk: PrimaryKeyColumn,
    field_names: Vec<String>,
    field_types: Vec<ValueType>,
    writes: Vec<TargetWrite>,
    deletes: Vec<TargetDelete>,
    /// The persisted, fully-qualified `"schema.table"` identity of this
    /// target (issue #73's `Definition::target_table`, ADR-0007) —
    /// carried alongside the bare `def.def.target` this plan is keyed by
    /// (see [`ApplyPlan::targets`]'s doc comment on why the map key itself
    /// stays bare) so [`apply_target`] can bind the *right* physical table
    /// into its `INSERT`/`UPDATE`/`DELETE` SQL, rather than leaving a
    /// target explicitly qualified into a non-default schema (issue #76) to
    /// resolve against whatever `search_path` the executing session
    /// happens to carry. Mirrors [`AggregateTargetPlan::source`]/this same
    /// struct's own eventual reuse of `qualified_source`'s established
    /// pattern from #76.
    qualified_target: String,
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
    pk: PrimaryKeyColumn,
    hop_gen: i32,
    /// Same role as [`TargetPlan::qualified_target`]: the persisted,
    /// fully-qualified target identity this clear's `DELETE FROM` must bind,
    /// rather than the bare map key it's stored under.
    qualified_target: String,
    src_changed: Option<std::time::SystemTime>,
}

/// The aggregate-target counterpart to [`ClearPlan`] — see
/// [`ApplyPlan::aggregate_clears`]'s doc comment for why this carries no
/// [`PrimaryKeyColumn`] of its own (a plain full-table `DELETE`, no
/// `RETURNING`-projected key shape needed). `qualified_target` plays the
/// same role [`ClearPlan::qualified_target`]/[`TargetPlan::qualified_target`]
/// do: the persisted, fully-qualified identity the `DELETE FROM` must bind,
/// not the bare map key this is stored under.
#[derive(Debug, Clone)]
struct AggregateClearPlan {
    hop_gen: i32,
    qualified_target: String,
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
/// Widened to `pub(super)` for issue #104: `apply_aggregate`'s
/// `accumulate_changes` reuses this exact merge to fold a `FoldedChange`'s
/// `src_changed` into the touched [`apply_aggregate::GroupPlan`]'s own
/// running origin, the aggregate-path counterpart to this module's own
/// `hop_gen`-style fan-in above.
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
    use std::time::{Duration, SystemTime};
    use tokio_postgres::NoTls;
    use tokio_postgres::types::PgLsn;

    /// Issue #180's step-4 guard: a key this batch both deleted (with a
    /// captured pre-delete image) and wrote — reachable when the forward
    /// aggregate apply and a per-record reverse-relationship apply touch one
    /// group inside a single batch — must be reported as written, so the
    /// captured image never rides downstream and annihilates the write's own
    /// image-less `Recompute` in the fold.
    #[test]
    fn a_key_both_deleted_and_written_in_one_batch_counts_as_written() {
        let touched: Vec<ChangedKey> = vec![
            (
                "gone".to_string(),
                0,
                None,
                Some(r#"{"g":"gone"}"#.to_string()),
            ),
            (
                "moved".to_string(),
                0,
                None,
                Some(r#"{"g":"moved"}"#.to_string()),
            ),
            ("moved".to_string(), 0, None, None),
            ("fresh".to_string(), 0, None, None),
        ];
        let written = keys_written_without_image(&touched);
        assert!(
            written.contains("moved"),
            "a key deleted and then rewritten in the same batch must count as written"
        );
        assert!(written.contains("fresh"), "a plain write counts as written");
        assert!(
            !written.contains("gone"),
            "a key only ever deleted must keep its image-bearing propagation"
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
        let pk = crate::defs::require_single_column_pk(
            crate::defs::source_primary_key(&pool, source)
                .await
                .expect("introspect source primary key"),
            source,
        )
        .expect("single-column pk");
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

        // Stage one CDC row with a real `src_changed`, so both the
        // per-transform and end-to-end histograms have something to
        // observe, not just the throughput counter.
        client
            .execute(
                "insert into seg_0 (src_table, key, op, lsn, old_image, new_image, hop_gen, \
                 src_changed) \
                 values ($1, $2, $3, $4, $5::text::jsonb, $6::text::jsonb, 0, now())",
                &[
                    &source,
                    &"1",
                    &"insert",
                    &PgLsn::from(1u64),
                    &None::<String>,
                    &Some(r#"{"price":"10.00","tax":"1.50"}"#.to_string()),
                ],
            )
            .await
            .expect("stage cdc row");

        let mut seal_client = client;
        let seal_outcome = crate::staging::seal::seal_phase1(&mut seal_client)
            .await
            .expect("seal phase 1");
        crate::staging::seal::seal_phase2(&seal_client, seal_outcome.sealed_seg_seq)
            .await
            .expect("seal phase 2");
        let seg_seq = seal_outcome.sealed_seg_seq;

        let mut phase1_client = pool.get().await.expect("connection");
        let txn = phase1_client.transaction().await.expect("begin phase 1");
        claim::claim(&*txn, seg_seq, "worker", 1)
            .await
            .expect("claim");
        let filter = claim::owned_bucket_filter(&*txn, seg_seq, "worker")
            .await
            .expect("owned_bucket_filter");
        let folded = fold::fold(&txn, seg_seq, filter).await.expect("fold");
        txn.commit().await.expect("commit phase 1");

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
            "compute must buffer exactly one (transform, src_changed) observation: {:?}",
            plan1.transform_observations
        );
        let (observed_transform, observed_src_changed) = &plan1.transform_observations[0];
        assert_eq!(observed_transform, target);
        assert!(
            observed_src_changed.is_some(),
            "the staged change carried a real src_changed, so it must be buffered as Some"
        );
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
            Some(1),
            "exactly one throughput increment must land, even though compute() ran twice: \
             {after_flush}"
        );
        assert_eq!(
            metric_value(
                &after_flush,
                "trellis_transform_latency_seconds_count",
                target
            ),
            Some(1),
            "exactly one per-transform latency observation must land: {after_flush}"
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

    /// Issue #110 regression pin: [`live_rows_join_cond`] uses plain `=` for
    /// a column no key in the batch binds `NULL` for (preserving the
    /// pre-#110, index-friendly shape), but `is not distinct from` for a
    /// column that does — so a `NULL`-keyed group's live row is found
    /// instead of being mistaken for "already deleted."
    #[test]
    fn live_rows_join_cond_uses_is_not_distinct_from_only_for_a_null_carrying_column() {
        let idents = vec![r#""warehouse""#.to_string(), r#""sku""#.to_string()];
        let u_cols = vec!["c0".to_string(), "c1".to_string()];

        assert_eq!(
            live_rows_join_cond(&idents, &u_cols, &[false, false]),
            r#"t."warehouse" = u.c0 and t."sku" = u.c1"#,
            "no NULL anywhere in the batch: both columns keep the indexable `=`"
        );
        assert_eq!(
            live_rows_join_cond(&idents, &u_cols, &[true, false]),
            r#"t."warehouse" is not distinct from u.c0 and t."sku" = u.c1"#,
            "only the column that actually carries a NULL switches operator"
        );
        assert_eq!(
            live_rows_join_cond(&idents, &u_cols, &[true, true]),
            r#"t."warehouse" is not distinct from u.c0 and t."sku" is not distinct from u.c1"#
        );
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

    /// [`ReverseTrigger::Keys`] via [`from_side_rows_for_trigger_txn`] — the
    /// full-row, transactional shape [`stage_reverse_recompute_fallback`]
    /// and the reverse-delta fast path's `diff_pass` both drive, inside
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
}

/// Phase 2's output: an in-memory plan Phase 3 applies inside one
/// transaction, with no further catalog reads of its own beyond the version
/// fence.
///
/// `versions` is the version-fence's read set: every source table this
/// batch evaluated against, and the `source_table_versions.version` Phase 2
/// loaded for it (`None` for a source with no definitions at all — still
/// fenced, so a definition created against it mid-drain is caught too).
/// `downstream_readers` records, per target table this batch wrote to,
/// whether any definition currently reads it — decided here (a catalog
/// read, like the evaluation lookups above it) rather than in Phase 3,
/// which holds no pool connection of its own and must stay a pure,
/// txn-scoped function. See [`apply_and_mark_drained`]'s doc comment on the
/// staleness this implies and why it's an accepted tradeoff.
#[derive(Debug, Clone, Default)]
pub struct ApplyPlan {
    versions: HashMap<String, Option<i64>>,
    targets: HashMap<String, TargetPlan>,
    /// [`KeySpace::Aggregate`] targets' per-group deltas (issue #11's
    /// aggregate extension) — the same role [`ApplyPlan::targets`] plays for
    /// [`KeySpace::OneToOne`], kept as a separate map since the two key
    /// spaces' Phase 3 write shapes (`apply_target`'s ordered pre-lock CTE
    /// vs. `apply_aggregate::apply_aggregate_target`'s sequential per-group
    /// upserts) are different enough not to share one plan type.
    aggregate_targets: HashMap<String, AggregateTargetPlan>,
    downstream_readers: HashMap<String, bool>,
    /// Targets to clear in full at Phase 3, keyed by target table name —
    /// issue #60's truncate propagation. See [`ClearPlan`].
    clears: HashMap<String, ClearPlan>,
    /// The aggregate-target counterpart to [`ApplyPlan::clears`]: a
    /// truncate on an aggregate definition's source clears every group, but
    /// (unlike a 1-1 target's single-column primary key) there is no single
    /// column shape to `RETURNING`-project a physically-changed group key
    /// out of generically — so this is applied as a plain `DELETE FROM
    /// <target>` (every group atomically gone), counted toward
    /// [`ApplyOutcome::keys_deleted`], but
    /// *not* staged for downstream propagation. A documented gap, not an
    /// oversight: closing it needs composite-key downstream propagation,
    /// out of scope for this issue (see `staging::apply_aggregate`'s module
    /// doc comment for the rest of what this issue does cover).
    ///
    /// Investigated (issue #11 review, corrected during #51/#52 review):
    /// could a definition actually be *created* reading from an aggregate
    /// target today, making this skip a live correctness gap rather than a
    /// moot one? Yes, and the originally-assumed safety net does **not**
    /// reliably prevent it: `create_definition`'s own primary-key-shape gate
    /// (issue #177, in `catalog::create_definition_inner`) closes this for a
    /// `OneToOne` downstream definition — it rejects any source with more
    /// than one PK column, so a `OneToOne` read of a multi-column-`GROUP BY`
    /// aggregate target is rejected at create time; a single-column
    /// `GROUP BY` still produces a genuinely single-column aggregate-target
    /// PK, so `DdlError::CompositePrimaryKeyUnsupported` never fires for
    /// that case either. The gate is `OneToOne`-only, though: a downstream
    /// **`Aggregate`** definition (this field's own real shape, and issue
    /// #171's actual repro) needs no PK narrowing at all and is untouched by
    /// it, so a multi-column `GROUP BY` chained into another aggregate
    /// remains fully creatable and live.
    /// Previously (**[#103](https://github.com/salesforce-misc/trellis/issues/103)**
    /// for one grouping column,
    /// **[#171](https://github.com/salesforce-misc/trellis/issues/171)** for
    /// several — both before #177's gate existed, and #171's case remains
    /// live today for the `Aggregate` shape the gate doesn't cover),
    /// the encoded group-key text (`derive_group_key`'s locally-invented
    /// `"{len}:{value}"` shape, `apply_aggregate.rs`) reached a real
    /// evaluator via [`ApplyPlan::aggregate_targets`]' `written`/`deleted`
    /// downstream-propagation path (step 3b in
    /// `apply_and_mark_drained_many`) and got misread as a raw PK value —
    /// confirmed to crash for a numeric-typed single group column, plausibly
    /// silently corrupting downstream rows for a text-typed one, and failing
    /// the whole batch with `DdlError::MalformedCompositeKey` for a
    /// multi-column `GROUP BY`. Fixed: `derive_group_key` emits exactly
    /// `ddl::pk_key_sql_expr`'s own row-identity encoding at either arity
    /// (the bare value for one grouping column, the U+001F join for
    /// several), matching the aggregate target's real PK shape exactly, so a
    /// chained definition's live refetch reads the correct key. This
    /// paragraph's own "moot" claim was itself already corrected once,
    /// during #51/#52 review — left in place (rather than deleted) as the
    /// historical record of all three corrections.
    ///
    /// The *other* half of this field's gap — a truncate clear on an
    /// aggregate source not propagating downstream at all (this field's own
    /// doc comment, above) — is a separate, still-open item: #103's/#171's
    /// fixes only correct the key a chained definition's live refetch
    /// resolves against once a write/delete *does* propagate; they do not
    /// add propagation to the truncate-clear path. That path now has no
    /// encoding obstacle left (a group key at any arity is a valid composite
    /// row identity), only the missing `RETURNING`-projection of the cleared
    /// group keys noted above — still out of scope here.
    aggregate_clears: HashMap<String, AggregateClearPlan>,
    /// Issue #16: the (non-truncate) folded records whose `(src_table,
    /// key)` is already in the `poison` marker table — excluded from every
    /// map above (the fold excludes a poisoned key *globally*, not just
    /// from evaluation), and instead parked into `poison_held` by
    /// [`apply_and_mark_drained`], in the same Phase-3 transaction, before
    /// the drained mark. See `quarantine::park_batch_contribution`'s doc
    /// comment for why this must happen even when this batch didn't cause
    /// the key's eviction.
    poisoned_park: Vec<FoldedChange>,
    /// Issue #16: every non-truncate, non-poisoned `(src_table, key)` this
    /// batch computed against — cleared from `key_deaths` by
    /// [`apply_and_mark_drained`] on a successful commit, per doc 06's "a
    /// clean drain clears the counters for the keys it just applied."
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
    reverse_recomputes: Vec<(String, String, i32, Option<std::time::SystemTime>)>,
    /// Epic #49 cross-cutting review fix (issues #51/#52): every
    /// `(transform, src_changed)` observation [`buffer_transform_apply_metrics`]
    /// buffered during this `compute` call, in place of recording each one
    /// immediately — drained by [`flush_apply_metrics`] into
    /// [`crate::metrics::record_transform_latency`]/
    /// [`crate::metrics::increment_changes_applied`] only once the batch
    /// this plan belongs to actually commits, so a plan a retry discards
    /// (version-fence miss, rolled-back Phase 3 failure) never reaches the
    /// registry at all. See [`buffer_transform_apply_metrics`]'s doc comment
    /// for why eager recording here was the bug.
    transform_observations: Vec<(String, Option<std::time::SystemTime>)>,
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
    relationship_projection_clears: std::collections::HashSet<String>,
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
/// fine-grained span, one per consuming transform.
#[tracing::instrument(
    name = "staging.compute",
    skip(pool, folded),
    fields(
        folded = folded.len(),
        poisoned = tracing::field::Empty,
        sources = tracing::field::Empty,
    )
)]
pub async fn compute(pool: &Pool, folded: &[FoldedChange]) -> Result<ApplyPlan, ApplyError> {
    // Issue #16: exclude already-poisoned keys before anything else touches
    // them — the fold excludes a poisoned key globally, not just from this
    // one batch's evaluation. Truncate sentinels are never candidates: a
    // truncate is whole-keyspace, not a key quarantine can attribute
    // anything to.
    let candidates: Vec<(&str, &str)> = folded
        .iter()
        .filter(|c| !c.is_truncate && c.relationship_reverse_deferred.is_none())
        .map(|c| (c.src_table.as_str(), c.key.as_str()))
        .collect();
    let poisoned = quarantine::poisoned_keys_among(pool, &candidates).await?;
    tracing::Span::current().record("poisoned", poisoned.len());

    let mut by_source: HashMap<&str, Vec<&FoldedChange>> = HashMap::new();
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
    let mut poisoned_park: Vec<FoldedChange> = Vec::new();
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
        if poisoned.contains(&(change.src_table.clone(), change.key.clone())) {
            poisoned_park.push(change.clone());
            continue;
        }
        applied_keys.push((change.src_table.clone(), change.key.clone()));
        by_source
            .entry(catalog_source_key(&change.src_table))
            .or_default()
            .push(change);
    }
    if !poisoned_park.is_empty() {
        tracing::warn!(
            excluded = poisoned_park.len(),
            "batch excludes already-poisoned keys, parking this batch's own contribution"
        );
    }
    tracing::Span::current().record("sources", by_source.len());

    let mut versions: HashMap<String, Option<i64>> = HashMap::new();
    let mut targets: HashMap<String, TargetPlan> = HashMap::new();
    let mut aggregate_targets: HashMap<String, AggregateTargetPlan> = HashMap::new();
    // Issue #52: every `Some(src_changed)` origin timestamp
    // `buffer_transform_apply_metrics` sees below, buffered per consuming
    // target — filtered into `ApplyPlan::end_to_end_origins` only for
    // targets the `downstream_readers` computation at the end of this
    // function finds terminal (see that call site's comment). Not itself
    // part of `ApplyPlan` — only the terminal-filtered subset is.
    let mut end_to_end_origins: HashMap<String, Vec<std::time::SystemTime>> = HashMap::new();
    // Epic #49 cross-cutting review fix (issues #51/#52): every
    // `(transform, src_changed)` pair `buffer_transform_apply_metrics` below
    // would previously have recorded immediately — now buffered here and
    // carried out on `ApplyPlan`, flushed post-commit by
    // [`flush_apply_metrics`]. See `buffer_transform_apply_metrics`'s doc
    // comment for why eager recording here was the bug.
    let mut transform_observations: Vec<(String, Option<std::time::SystemTime>)> = Vec::new();
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
    let mut reverse_recomputes: HashMap<(String, String), (i32, Option<std::time::SystemTime>)> =
        HashMap::new();
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
    let mut relationship_projection_clears: std::collections::HashSet<String> =
        std::collections::HashSet::new();

    for (source_key, changes) in by_source {
        tracing::debug!(
            src_table = %source_key,
            changes = changes.len(),
            "evaluating a source table's folded changes"
        );
        let version = catalog::source_table_version(pool, source_key).await?;
        versions.insert(source_key.to_string(), version);

        // The fully-qualified source identity this batch's own CDC producer
        // staged (issue #76, ADR-0007) — `change.src_table`, not `source_key`
        // (that stays bare purely as the catalog lookup key, per
        // `catalog_source_key`'s own doc comment). Every change in this
        // bucket shares the same bare suffix by construction (`by_source`
        // grouped on it); they're expected to also share this qualified form
        // (the same physical table), so any one of them gives the right
        // answer for the physical reads below — used in place of a bare
        // `source_key` so `source_primary_key`/`read_live_rows_batch` don't
        // leave the schema to resolve against whatever `search_path` the
        // executing session happens to carry.
        let qualified_source = changes[0].src_table.as_str();

        // `source_key` alone determines the source table's primary key, not
        // the individual definition (issue #69) — introspected once per
        // source here and reused both below (every definition subscribed to
        // this source) and by the row decode below (every change, whichever
        // definition it's evaluated against). A live `42P01` here means
        // `source_key` no longer exists (issue #16's dropped-table purge,
        // not an ordinary DDL error) — see [`ApplyError::SourceTableDropped`].
        let pk = match ddl::source_primary_key(pool, qualified_source).await {
            Ok(pk) => pk,
            Err(DdlError::Db(db_err)) if quarantine::is_undefined_table(&db_err) => {
                return Err(ApplyError::SourceTableDropped {
                    source_table: source_key.to_string(),
                });
            }
            Err(DdlError::NoPrimaryKey { source_table })
                if quarantine::source_table_missing(pool, &source_table).await? =>
            {
                return Err(ApplyError::SourceTableDropped { source_table });
            }
            Err(err) => return Err(err.into()),
        };

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
        let mut rows: Vec<Option<Row>> = Vec::with_capacity(changes.len());
        let mut live_refetch_indices: Vec<usize> = Vec::new();
        for (i, change) in changes.iter().enumerate() {
            let row = match (&change.new_image, &change.old_image) {
                (Some(image_text), _) => Some(decode_image(pool, image_text).await?),
                (None, Some(_)) => None,
                (None, None) => {
                    live_refetch_indices.push(i);
                    None
                }
            };
            rows.push(row);
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

        // `qualified_source` (via `qualified_schema_node_key`), not
        // `source_key`: `schema_nodes`/`schema_edges` now key on qualified
        // identity (issue #74, ADR-0007), so `transforms_for_source` (a
        // thin `dependents_of` wrapper) needs an exact qualified match
        // here, not the bare catalog-lookup key `catalog_source_key`'s own
        // doc comment already explains stays bare for
        // `source_table_version`/`relationships_to_table` below (both still
        // bare-suffix-keyed, unaffected by #74). `qualified_source` is
        // usually already fully-qualified (real CDC/backfill), but a
        // downstream-propagation hop's `src_table` is a bare target name
        // this same apply path staged — `qualified_schema_node_key` resolves
        // that case too; see its own doc comment.
        let defs = catalog::transforms_for_source(
            pool,
            &qualified_schema_node_key(pool, qualified_source).await?,
        )
        .await?;

        // Aggregate definitions need each change's *old*-side row too (to
        // derive a grain-migrating change's old group key and its old
        // contribution — see `apply_aggregate`'s doc comment), decoded once
        // here and shared across every aggregate definition on this source,
        // same as `rows` above. Only decoded when this source actually has
        // an aggregate reader, to avoid the extra round trips for the
        // (overwhelmingly common) 1-1-only source. Issue #130 widens this
        // same gate: a 1-1 definition reading a to-one relationship also
        // needs each change's old-image join-key value, to bump `gen` for
        // the parent a re-point/delete moved *away* from (see
        // `RelationshipGenBump`'s doc comment) — sharing one decode here
        // rather than a second pass over the same images.
        let needs_old_rows = defs.iter().any(|def| {
            matches!(def.def.key_space, KeySpace::Aggregate { .. })
                || !eval::relationship_references(&def.def).is_empty()
        });
        let mut old_rows: Vec<Option<Row>> = Vec::with_capacity(changes.len());
        if needs_old_rows {
            for change in &changes {
                let old_row = match &change.old_image {
                    Some(image_text) => Some(decode_image(pool, image_text).await?),
                    None => None,
                };
                old_rows.push(old_row);
            }
        } else {
            old_rows.resize_with(changes.len(), || None);
        }

        // Reverse recompute (issue #30): this source is some relationship's
        // *to-side*. A change to a related row must re-derive every from-side
        // row whose enrichment reads it. For each relationship pointing at
        // this table, collect the join-key text of every to-side row this
        // batch touched — the related row's `to_col`. For a to-one this is a
        // PRIMARY KEY/UNIQUE column, so it rides in the default replica
        // identity of every image, including a delete's pre-image. For a
        // to-many, `to_col` is the *foreign* side (non-key), so it only rides
        // in the pre-image when the to-side carries an adequate replica
        // identity — which is exactly why issue #41 gates that at
        // `create_relationship` (define) time: `REPLICA IDENTITY FULL` or a
        // covering replica-identity index. That gate is creation-time only and
        // not re-validated per batch, so an operator who later relaxes the
        // to-side's replica identity would silently degrade reverse recompute
        // here (the `.unwrap_or(&None)` below cannot tell an absent column from
        // a genuine NULL — hence the guard must live at define time, not here).
        // Then resolve, with one live lookup, the from-side keys whose
        // `from_col` matches, and stage each as an image-less recompute at the
        // triggering change's `hop_gen + 1`.
        let inbound_rels = catalog::relationships_to_table(pool, source_key).await?;
        // Decode each change's pre-image once, reused across every inbound
        // relationship below (the join key lives in the pre-image for a
        // delete/re-parent). Skipped entirely when this table is nobody's
        // to-side, so the common no-relationship source pays nothing.
        let reverse_old_rows: Vec<Option<Row>> = if inbound_rels.is_empty() {
            Vec::new()
        } else {
            let mut decoded = Vec::with_capacity(changes.len());
            for change in &changes {
                decoded.push(match &change.old_image {
                    Some(image_text) => Some(decode_image(pool, image_text).await?),
                    None => None,
                });
            }
            decoded
        };
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
                let mut key_src_changed: HashMap<String, Option<std::time::SystemTime>> =
                    HashMap::new();
                for (i, change) in changes.iter().enumerate() {
                    let mut note = |value: &Option<String>, hop: i32| {
                        if let Some(text) = value {
                            key_hops
                                .entry(text.clone())
                                .and_modify(|h| *h = (*h).max(hop))
                                .or_insert(hop);
                            key_src_changed
                                .entry(text.clone())
                                .and_modify(|sc| {
                                    *sc = earliest_src_changed(*sc, change.src_changed)
                                })
                                .or_insert(change.src_changed);
                        }
                    };
                    if let Some(row) = &rows[i] {
                        note(row.get(&rel.def.to_col).unwrap_or(&None), change.hop_gen);
                    }
                    if let Some(old) = &reverse_old_rows[i] {
                        note(old.get(&rel.def.to_col).unwrap_or(&None), change.hop_gen);
                    }
                }
                accumulate_from_side_recomputes(
                    pool,
                    rel,
                    &key_hops,
                    &key_src_changed,
                    &mut reverse_recomputes,
                )
                .await?;
                continue;
            }

            // Issue #244: an image-less change — a bare
            // `StagedChange::Recompute` trigger — is *not* a parent state
            // transition, and must never become a reverse **delta**.
            //
            // Such a trigger asserts only "this key exists as of now" (see
            // `intake::publication::enumerate_and_append`'s doc comment); it
            // carries no images at all, and it is staged in quantity by
            // paths that have nothing to do with the parent changing: a
            // definition's ring backfill enumeration of its own source
            // table, forward propagation's chained-target hop, and every
            // reverse/TRUNCATE-clear fallback (whose from-side table is very
            // often *also* some other relationship's to-side).
            //
            // The delta path below can't represent that. It reads
            // `rows[i]`, which for an image-less trigger is `compute`'s own
            // *live re-read* of the row — so the record it would build is
            // `old_row = None`, `new_row = Some(live row)`, byte-for-byte
            // the shape of a genuine parent INSERT, and `diff_pass` would
            // dutifully add the parent's contribution to every matching
            // from-side row's group a second time, on top of the
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
            let has_image_less = changes
                .iter()
                .any(|change| change.old_image.is_none() && change.new_image.is_none());
            if has_image_less {
                let mut key_hops: HashMap<String, i32> = HashMap::new();
                let mut key_src_changed: HashMap<String, Option<std::time::SystemTime>> =
                    HashMap::new();
                for (i, change) in changes.iter().enumerate() {
                    if change.old_image.is_some() || change.new_image.is_some() {
                        continue;
                    }
                    // Only the live re-read can carry the join key here, by
                    // construction — there is no image to read one from. A
                    // re-read that came back empty (the key no longer
                    // exists) leaves nothing to resolve a from-side row
                    // through, exactly as the both-images-absent skip below
                    // always intended.
                    let Some(row) = &rows[i] else { continue };
                    let Some(Some(join_text)) = row.get(&rel.def.to_col) else {
                        continue;
                    };
                    key_hops
                        .entry(join_text.clone())
                        .and_modify(|h| *h = (*h).max(change.hop_gen))
                        .or_insert(change.hop_gen);
                    key_src_changed
                        .entry(join_text.clone())
                        .and_modify(|sc| *sc = earliest_src_changed(*sc, change.src_changed))
                        .or_insert(change.src_changed);
                }
                accumulate_from_side_recomputes(
                    pool,
                    rel,
                    &key_hops,
                    &key_src_changed,
                    &mut reverse_recomputes,
                )
                .await?;
            }

            // Issue #131: to-one. One `ReverseRelationshipShape` per
            // relationship per `compute()` call, shared (via `Arc`) by every
            // parent key this batch's fold touches for it.
            let shape = match relationship_reverse_shapes.get(&rel.id) {
                Some(shape) => Arc::clone(shape),
                None => {
                    let shape = Arc::new(build_reverse_relationship_shape(pool, rel).await?);
                    relationship_reverse_shapes.insert(rel.id, Arc::clone(&shape));
                    shape
                }
            };
            for (i, change) in changes.iter().enumerate() {
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
                let old_row = reverse_old_rows[i].clone();
                let new_row = rows[i].clone();
                let read_key = relationship_key_text(&old_row, &rel.def.to_col)
                    .or_else(|| relationship_key_text(&new_row, &rel.def.to_col));
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
                    retry_count: 0,
                });
            }
        }

        for def in &defs {
            let KeySpace::Aggregate { group_by } = &def.def.key_space else {
                let field_names: Vec<String> =
                    def.def.fields.iter().map(|f| f.name.clone()).collect();
                // A relationship-enriched field's type can't come from
                // `infer_field_types` (it rejects a `<rel>.<column>` path,
                // whose type is a *to-side* column's, unknown to the from-side
                // type map). The field's inferred type *is* its target column's
                // declared type, though (see `infer_field_types`' doc), and
                // that column already exists — introspect it. Non-relationship
                // definitions keep the pure inference, behavior-identical.
                let field_types: Vec<ValueType> =
                    if eval::relationship_references(&def.def).is_empty() {
                        // This branch runs only for relationship-free definitions
                        // (guarded above), so type inference needs no relationship
                        // metadata: an empty map (issue #40).
                        let inferred_types = validate::infer_field_types(
                            &def.def,
                            &def.source_columns,
                            &std::collections::HashMap::new(),
                        )?;
                        field_names
                            .iter()
                            .map(|name| {
                                inferred_types
                                    .get(name)
                                    .copied()
                                    .unwrap_or(ValueType::Numeric)
                            })
                            .collect()
                    } else {
                        // Broader sweep, reviewer follow-up to issue #74 (epic
                        // #78's own whole-branch review): `def.def.target` is
                        // always bare, even for a definition whose `TRANSFORM`
                        // clause explicitly spelled `schema.target` (issue #76;
                        // see `TransformDef`'s own doc comment), so binding it
                        // straight into `to_column_types`'s `to_regclass` lookup
                        // relied on the connection's pinned `search_path`
                        // (`Config::schema`/`Config::target_schema`/`"public"`)
                        // finding it — silently wrong (or simply absent) for a
                        // target explicitly qualified into a schema outside that
                        // pin. `def.target_table` is right here on the same
                        // struct, already the fully-qualified identity issue #73
                        // persisted at acceptance time — use it instead of
                        // re-deriving (or mis-deriving) the physical location
                        // from the bare AST field.
                        let target_types =
                            to_column_types(pool, &def.target_table, &field_names).await?;
                        field_names
                            .iter()
                            .map(|name| {
                                target_types
                                    .get(name)
                                    .copied()
                                    .unwrap_or(ValueType::Numeric)
                            })
                            .collect()
                    };

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
                let paused = quarantine::paused_columns_for(pool, &def.def.target).await?;
                let (field_names, field_types): (Vec<String>, Vec<ValueType>) = if paused.is_empty()
                {
                    (field_names, field_types)
                } else {
                    field_names
                        .into_iter()
                        .zip(field_types)
                        .filter(|(name, _)| !paused.contains(name))
                        .unzip()
                };

                // Issue #126: `pk` is this source's full (possibly
                // composite) primary key, shared with the `KeySpace::Aggregate`
                // branch above (which needs no single-column narrowing at
                // all). A `KeySpace::OneToOne` definition can only be created
                // against a single-column source primary key — issue #177 put
                // that same `ddl::require_single_column_pk` gate in
                // `catalog::create_definition_inner`, so it now holds for the
                // ring-path entry points (`create_definition`/
                // `create_definition_without_backfill`) too, not just
                // `install_definition` (see `quarantine.rs`'s
                // `a_composite_primary_key_source_is_rejected_at_create_time_not_quarantined_or_halted`,
                // which pins that the create-time rejection is what fires
                // now). What's left for this narrowing to catch is a source
                // whose primary key *changed* to composite after its
                // definition was accepted — still a real, typed halting
                // error, not an invariant violation.
                let target_pk = ddl::require_single_column_pk(pk.clone(), qualified_source)?;
                let plan = targets
                    .entry(def.def.target.clone())
                    .or_insert_with(|| TargetPlan {
                        pk: target_pk,
                        field_names: field_names.clone(),
                        field_types: field_types.clone(),
                        writes: Vec::new(),
                        deletes: Vec::new(),
                        // The persisted, fully-qualified identity (issue #73)
                        // — not re-derived, since `def` (this source's own
                        // catalog `Definition`) already carries it. See
                        // `TargetPlan::qualified_target`'s doc comment.
                        qualified_target: def.target_table.clone(),
                    });

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
                // `from_col` values — then threaded into every row eval below.
                // A definition with no relationship references stays on the
                // plain `eval::evaluate` path, behavior-identical to before.
                let rel_ctx = if eval::relationship_references(&def.def).is_empty() {
                    None
                } else {
                    let (ctx, gen_bumps) = build_relationship_context(
                        pool,
                        source_key,
                        &def.def,
                        &rows,
                        Some(&old_rows),
                        Some(changes.as_slice()),
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
                    Some(ctx)
                };

                // Three shapes, per fold.rs's rules: `Some(new_image)` is an
                // insert/update, evaluated straight from the staged post-image.
                // `(None, Some(old_image))` is a genuine CDC delete — the fold's
                // first image-bearing row's pre-image survived, only the last
                // one's post-image didn't. `(None, None)` is everything else
                // image-less: a bare recompute trigger (reverse propagation,
                // definition re-derive, backfill), or — coincidentally the same
                // shape — a key inserted and deleted within one batch. Neither
                // carries an image to evaluate, so both re-read the *current*
                // row from `source_key` live: present means write with the live
                // image; absent (already gone, or never existed) means delete.
                // This is why `eval.rs`'s "no database access here" is scoped to
                // the evaluator itself, not this module. Decoded/re-read once
                // per change above, shared across every definition on this
                // source (issue #69) — not redone here per definition.
                for (change, row) in changes.iter().zip(rows.iter()) {
                    match row {
                        Some(row) => {
                            // `def.source_columns` is the same type map this
                            // definition was validated against at creation time
                            // (#63's write-path gap: persisted alongside the
                            // definition — see `catalog::create_definition` —
                            // rather than defaulting every column to Numeric).
                            let mut evaluated = match &rel_ctx {
                                Some(ctx) => eval::evaluate_with_relationships_excluding(
                                    &def.def,
                                    row,
                                    &def.source_columns,
                                    ctx,
                                    &mut regex_cache,
                                    &paused,
                                )?,
                                None => eval::evaluate_excluding(
                                    &def.def,
                                    row,
                                    &def.source_columns,
                                    &mut regex_cache,
                                    &paused,
                                )?,
                            };
                            let values: Vec<Option<String>> = field_names
                                .iter()
                                .map(|name| match evaluated.remove(name) {
                                    Some(Some(value)) => Some(value.to_string()),
                                    Some(None) | None => None,
                                })
                                .collect();
                            plan.writes.push(TargetWrite {
                                pk_text: change.key.clone(),
                                values,
                                hop_gen: change.hop_gen,
                                src_changed: change.src_changed,
                            });
                        }
                        None => {
                            plan.deletes.push(TargetDelete {
                                pk_text: change.key.clone(),
                                hop_gen: change.hop_gen,
                                src_changed: change.src_changed,
                            });
                        }
                    }
                    buffer_transform_apply_metrics(
                        &def.def.target,
                        change.src_changed,
                        &mut end_to_end_origins,
                        &mut transform_observations,
                    );
                }
                continue;
            };

            // Aggregate dispatch (issue #11): fold this source's changes
            // into per-group deltas on `def.def.target`'s aggregate plan,
            // via `apply_aggregate` rather than duplicating its logic here.
            //
            // Substitute cross-field-alias references (e.g. `double_total =
            // total + total` where `total` is itself a field) once up front,
            // so classification and the plan's rendered `field_exprs` share
            // one substitution pass — see
            // `defs::backfill::substituted_field_exprs`'s doc comment for why
            // the raw, un-substituted `Expr` can't be rendered as SQL.
            let substituted_exprs = crate::defs::backfill::substituted_field_exprs(&def.def)?;
            // Issue #94: a to-one relationship path an aggregate field folds
            // (`SUM(post.word_count)`) needs its relationship's endpoints (to
            // build the recompute's LEFT JOIN) and its to-side column's type
            // (to type the target column). Issue #137: a `GROUP BY` key can
            // read a relationship too, typed the same way. Both come from the
            // same catalog resolution `defs::catalog` validates against; a
            // relationship-free aggregate resolves to an empty map and costs
            // one cheap no-op.
            let relationships = catalog::resolve_relationships(pool, &def.def).await?;
            let group_by_types: Vec<ValueType> = group_by
                .iter()
                .map(|key| match key {
                    GroupByKey::Column(c) => def
                        .source_columns
                        .get(c)
                        .copied()
                        .unwrap_or(ValueType::Numeric),
                    GroupByKey::RelationshipPath { rel, column } => relationships
                        .get(rel)
                        .and_then(|r| r.column_types.get(column))
                        .copied()
                        .unwrap_or(ValueType::Numeric),
                })
                .collect();
            let field_plans = apply_aggregate::classify_fields(
                &def.def,
                group_by,
                &def.source_columns,
                &substituted_exprs,
                &relationships,
            )?;
            let mut rel_joins: Vec<apply_aggregate::RelJoin> = Vec::new();
            for rel_name in relationships.keys() {
                // Endpoints (`from_col` especially) come from the stored
                // relationship row; `ResolvedRelationship` carries only the
                // to-side, since that's all the validator needs.
                if let Some(reldef) =
                    catalog::relationship_by_name(pool, &def.def.source, rel_name).await?
                {
                    // Mirrors `defs::backfill::resolve_to_one_joins`'s guard:
                    // the validator makes a to-many path in an aggregate
                    // unreachable today, but this loop has no other cardinality
                    // check of its own, and a silent to-many LEFT JOIN here
                    // would fan out source rows and inflate every SUM instead
                    // of failing loudly like the direct-build path does.
                    if reldef.cardinality != RelationshipCardinality::ToOne {
                        return Err(crate::defs::backfill::BackfillError::Unsupported(
                            "an aggregate over a to-many relationship".to_string(),
                        )
                        .into());
                    }
                    rel_joins.push(apply_aggregate::RelJoin {
                        name: rel_name.clone(),
                        to_table: reldef.def.to_table,
                        to_col: reldef.def.to_col,
                        from_col: reldef.def.from_col,
                    });
                }
            }
            rel_joins.sort_by(|a, b| a.name.cmp(&b.name));
            let field_exprs: HashMap<String, crate::defs::ast::Expr> = substituted_exprs
                .into_iter()
                .filter(|(name, _)| !group_by_contains(group_by, name))
                .collect();
            let target_plan = aggregate_targets
                .entry(def.def.target.clone())
                .or_insert_with(|| {
                    AggregateTargetPlan::new(
                        group_by,
                        group_by_types,
                        field_plans,
                        qualified_source.to_string(),
                        def.target_table.clone(),
                        field_exprs,
                        rel_joins,
                    )
                });

            // Issue #136: a relationship-reading aggregate's ordinary
            // (non-image-less) per-row delta now resolves its `<rel>.<column>`
            // reads the same way a `KeySpace::OneToOne` target already does —
            // against the settled parent projection, via
            // `build_relationship_context` — rather than the old
            // `force_every_group` full live-join recompute. Gated on
            // `eval::relationship_references`, exactly like the `OneToOne`
            // branch above, so a relationship-free aggregate (the
            // overwhelmingly common case) costs nothing extra.
            //
            // This also closes a latent gap: before this issue,
            // `accumulate_changes` never called `build_relationship_context`
            // for an aggregate definition at all (it had no need to — the
            // forced-recompute path read the live to-side table directly,
            // never the projection), so a to-one relationship consumed
            // *only* by an invertible aggregate never got its projection's
            // `__trellis_gen` bumped by any forward apply, even though issue
            // #131's reverse fast path (`build_reverse_relationship_shape`)
            // has been eligible for exactly that shape (a `KeySpace::Aggregate`
            // definition with only invertible fields reading one
            // relationship) since #131 shipped — guard (b) could not have
            // detected a race for such a relationship. Wiring this the same
            // way the `OneToOne` branch already does closes that gap for
            // every relationship an aggregate reads, not just this issue's
            // own new delta path.
            let rel_ctx = if eval::relationship_references(&def.def).is_empty() {
                None
            } else {
                let (ctx, gen_bumps) = build_relationship_context(
                    pool,
                    source_key,
                    &def.def,
                    &rows,
                    Some(&old_rows),
                    Some(changes.as_slice()),
                )
                .await?;
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
                Some(ctx)
            };

            let mut regex_cache = eval::RegexCache::new();
            apply_aggregate::accumulate_changes(
                target_plan,
                &def.def,
                &changes,
                &rows,
                &old_rows,
                &def.source_columns,
                &mut regex_cache,
                rel_ctx.as_ref(),
            )?;
            for change in &changes {
                buffer_transform_apply_metrics(
                    &def.def.target,
                    change.src_changed,
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
    for change in &relationship_reverse_deferrals {
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
                let shape = Arc::new(build_reverse_relationship_shape(pool, &rel).await?);
                relationship_reverse_shapes.insert(rel_id, Arc::clone(&shape));
                shape
            }
        };
        let old_row = match &change.old_image {
            Some(text) => Some(decode_image(pool, text).await?),
            None => None,
        };
        let new_row = match &change.new_image {
            Some(text) => Some(decode_image(pool, text).await?),
            None => None,
        };
        if old_row.is_none() && new_row.is_none() {
            // Should be unreachable: a deferred row is only ever staged
            // from a `RelationshipReverseRecord` that already had at least
            // one image (Phase 3's own staging site can only reach the
            // guard-rejection branch for a record that had one — see
            // `check_reverse_guards`'s "both keys `None`" short-circuit,
            // which always passes and never reaches that branch at all).
            // Defensively skip rather than build a meaningless record.
            continue;
        }
        let read_key = relationship_key_text(&old_row, &shape.to_col)
            .or_else(|| relationship_key_text(&new_row, &shape.to_col));
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
            retry_count: change.retry_count,
        });
    }

    // Truncate clears (issue #60): for each truncated src_table, resolve its
    // targets via the catalog and record a full clear for each — the same
    // "resolve targets from the catalog" step the by-source loop above runs
    // per key, just once per truncated source instead of once per key.
    let mut clears: HashMap<String, ClearPlan> = HashMap::new();
    let mut aggregate_clears: HashMap<String, AggregateClearPlan> = HashMap::new();
    for change in &truncated {
        let source_key = catalog_source_key(&change.src_table);
        // Fence this source too, even though nothing evaluated against it —
        // a definition change against a truncated source, landing mid-drain,
        // must trip Phase 3's version fence exactly like it would for a
        // source this batch actually evaluated `f()` against.
        let version = catalog::source_table_version(pool, source_key).await?;
        versions.entry(source_key.to_string()).or_insert(version);

        let pk = match ddl::source_primary_key(pool, &change.src_table).await {
            Ok(pk) => pk,
            Err(DdlError::Db(db_err)) if quarantine::is_undefined_table(&db_err) => {
                return Err(ApplyError::SourceTableDropped {
                    source_table: source_key.to_string(),
                });
            }
            Err(DdlError::NoPrimaryKey { source_table })
                if quarantine::source_table_missing(pool, &source_table).await? =>
            {
                return Err(ApplyError::SourceTableDropped { source_table });
            }
            Err(err) => return Err(err.into()),
        };
        // `&change.src_table` (qualified), not `source_key` (bare) — see
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
            match &def.def.key_space {
                KeySpace::Aggregate { .. } => {
                    aggregate_clears
                        .entry(def.def.target.clone())
                        .and_modify(|existing| {
                            existing.hop_gen = existing.hop_gen.max(change.hop_gen)
                        })
                        .or_insert(AggregateClearPlan {
                            hop_gen: change.hop_gen,
                            qualified_target: def.target_table.clone(),
                        });
                }
                KeySpace::OneToOne => {
                    // Issue #126: same narrowing, and the same "can never
                    // actually fail here" reasoning, as the by-source loop's
                    // identical `TargetPlan` construction above.
                    let target_pk = ddl::require_single_column_pk(pk.clone(), &change.src_table)?;
                    clears
                        .entry(def.def.target.clone())
                        .and_modify(|existing| {
                            existing.hop_gen = existing.hop_gen.max(change.hop_gen);
                            existing.src_changed =
                                earliest_src_changed(existing.src_changed, change.src_changed);
                        })
                        .or_insert(ClearPlan {
                            pk: target_pk,
                            hop_gen: change.hop_gen,
                            qualified_target: def.target_table.clone(),
                            src_changed: change.src_changed,
                        });
                }
            }
            // A TRUNCATE is a genuine applied change to every direct
            // downstream target, same as a row-driven change — recorded
            // once per def per truncated source, mirroring the row-driven
            // by_source loop above (issue #51/ADR-0009 decision 5).
            buffer_transform_apply_metrics(
                &def.def.target,
                change.src_changed,
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
        // as an image-less recompute — see `ReverseTrigger::WholeKeyspace`'s
        // doc comment for why "every non-NULL join column", not a specific
        // value list, is the right query for a TRUNCATE. Pushed into the
        // same `reverse_recomputes`
        // accumulator the row-driven path uses, so it's deduped the same way
        // (issue #79) and drained through the same image-less `Recompute`
        // pipeline below — no separate emission path needed.
        let inbound_rels = catalog::relationships_to_table(pool, source_key).await?;
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
                relationship_projection_clears.insert(
                    ddl::qualified_relationship_projection_table(
                        pool.target_schema(),
                        &projection.projection_table,
                    ),
                );
            }
            let from_pk = ddl::source_primary_key(pool, &rel.def.from_table).await?;
            let from_keys = from_side_keys(
                pool,
                &rel.def.from_table,
                &from_pk,
                &rel.def.from_col,
                &ReverseTrigger::WholeKeyspace,
            )
            .await?;
            let hop = change.hop_gen + 1;
            for (from_key, _) in from_keys {
                reverse_recomputes
                    .entry((rel.def.from_table.clone(), from_key))
                    .and_modify(|(h, sc)| {
                        *h = (*h).max(hop);
                        *sc = earliest_src_changed(*sc, change.src_changed);
                    })
                    .or_insert((hop, change.src_changed));
            }
        }
    }

    let mut downstream_readers = HashMap::new();
    let mut all_targets: std::collections::HashSet<&String> = targets.keys().collect();
    all_targets.extend(clears.keys());
    all_targets.extend(aggregate_targets.keys());
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
        let has_downstream =
            !catalog::transforms_for_source(pool, &qualified_schema_node_key(pool, target).await?)
                .await?
                .is_empty();
        downstream_readers.insert(target.clone(), has_downstream);
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

    let reverse_recomputes: Vec<(String, String, i32, Option<std::time::SystemTime>)> =
        reverse_recomputes
            .into_iter()
            .map(|((from_table, from_key), (hop, src_changed))| {
                (from_table, from_key, hop, src_changed)
            })
            .collect();

    Ok(ApplyPlan {
        versions,
        targets,
        aggregate_targets,
        downstream_readers,
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
    for (transform, src_changed) in &plan.transform_observations {
        if let Some(src_changed) = src_changed {
            let latency = std::time::SystemTime::now()
                .duration_since(*src_changed)
                .unwrap_or(std::time::Duration::ZERO);
            crate::metrics::record_transform_latency(transform, latency);
        }
        crate::metrics::increment_changes_applied(transform);
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
/// value [`apply_target`] must treat `key` as, via [`ddl::split_pk_key`]
/// (issue #205).
///
/// `pk` is always exactly one column here: a [`KeySpace::OneToOne`] target's
/// own primary key is narrowed to a single column at definition time
/// (`catalog::create_definition_inner`'s issue #177 gate, backed by
/// [`ddl::require_single_column_pk`]), so [`TargetPlan::pk`] is never
/// composite — unlike [`ddl::split_pk_key`]'s general (possibly
/// multi-column) contract, there is no arity to worry about here.
///
/// For the overwhelmingly common case — [`PrimaryKeyColumn::nullable`] is
/// `false`, true of every genuine, never-NULL primary key (an intake source
/// table, or any other real `PRIMARY KEY`) — this is a byte-identical no-op:
/// [`ddl::split_pk_key`] returns a not-null column's single part unchanged
/// (see that function's doc comment), so `key` itself comes back out,
/// `Cow::Borrowed`. It only differs for a [`KeySpace::OneToOne`] definition
/// chained directly off an aggregate target's own (nullable) grouping-column
/// PK: `key` may then carry issue #110's `NULL_KEY_SENTINEL`/escape
/// treatment, which this undoes — `None` means a genuine NULL-keyed group.
///
/// A `None` result can never be stored as this target's own primary-key
/// value: [`ddl::create_target_table`] always declares it a real `primary
/// key` column, which Postgres makes `NOT NULL` unconditionally, regardless
/// of whether the *source* column this target's key was narrowed from is
/// itself nullable. [`apply_target`]'s callers treat `None` as "no
/// representable row" and skip the key entirely — the same outcome
/// `defs::backfill::discover_pk_ranges`'s ordered `(lo, hi]` PK-range walk
/// already, structurally, produces for a NULL-keyed source row: `max()`
/// ignores `NULL`, and every range's `<=`/`>` bound is `NULL` (unknown) for
/// a `NULL` operand, so such a row is never selected by any chunk's `WHERE`
/// and a full backfill never attempts to insert it either. Skipping here
/// keeps live CDC apply's answer — "this group has no row in the target" —
/// consistent with backfill's, rather than attempting an insert Postgres's
/// own `NOT NULL` constraint would reject anyway (a hard per-transaction
/// error, not a silent one, but one this target shape can never avoid by
/// definition, so there is nothing more useful decoding to `NULL` could do
/// here than recognizing exactly this and omitting the row).
fn decode_target_pk_text<'a>(
    pk: &PrimaryKeyColumn,
    target: &str,
    key: &'a str,
) -> Result<Option<Cow<'a, str>>, ApplyError> {
    Ok(ddl::split_pk_key(std::slice::from_ref(pk), target, key)?
        .into_iter()
        .next()
        .flatten())
}

/// One physically-touched target key, as [`apply_and_mark_drained_many`]'s
/// `changed` accumulator and downstream-propagation step track it: the key
/// text, the `hop_gen` it carries forward, (issues #51/#52's multi-hop gap)
/// the `src_changed` origin it carries forward — `None` for an aggregate
/// target's group key (see the 3b step's doc comment) or any other touched
/// key with no traceable origin — and (issues #180/#196) the key's
/// pre-delete image, `Some` only when this entry is a genuine deletion whose
/// prior row state was captured at delete time (an extinct aggregate
/// group's `AggregateApplyResult::deleted` entry, or a deleted 1-1 target
/// row's own `apply_target`-captured entry — 3b's and step 3's doc comments
/// respectively), `None` for every written key and for a deletion no
/// producer captures an image for yet (the truncate-clear case — see step
/// 4's own doc comment on why that one stays image-less). Step 4 reads this
/// to decide whether a downstream `Recompute` can stay image-less (safe
/// whenever a live refetch would find the *right* row — true for every
/// write, and true for a delete only once a downstream chain's own live
/// refetch is known to correctly see "gone") or must become an
/// image-bearing delete instead, so a chained aggregate can subtract the
/// extinct row's last-known contribution rather than silently dropping the
/// change (issue #180, widened to the 1-1 target case by issue #196).
type ChangedKey = (String, i32, Option<std::time::SystemTime>, Option<String>);

/// One row [`apply_target`]'s delete statement actually removed: its key,
/// paired with the pre-delete image captured by that statement's own
/// `RETURNING ...` (issue #196) — an explicit per-column
/// `jsonb_build_object(..., <col>::text, ...)::text`, not `to_jsonb(t.*)::text`
/// (issue #248: see `row_as_text_jsonb_sql`'s doc comment for why) — the
/// 1-1-target counterpart to `apply_aggregate::AggregateApplyResult::deleted`'s
/// tuple.
type TargetDeletedKey = (String, String);

/// Every key in one target's [`ChangedKey`] accumulator that this batch
/// *wrote* (no captured pre-delete image), as a lookup set — the guard
/// [`apply_and_mark_drained_many`]'s step 4 checks before it lets a
/// captured image ride downstream as a real delete. See that call site's own
/// comment for why a key that is both deleted and written inside one batch
/// must propagate image-less.
fn keys_written_without_image(touched: &[ChangedKey]) -> std::collections::HashSet<&str> {
    touched
        .iter()
        .filter(|(_, _, _, old_image)| old_image.is_none())
        .map(|(key, _, _, _)| key.as_str())
        .collect()
}

/// Runs one target table's ordered pre-lock, then its no-op-suppressed
/// upsert and delete, returning the keys Postgres actually wrote to vs.
/// deleted (as opposed to every key this batch merely *proposed* — the
/// no-op-suppression `WHERE ... IS DISTINCT FROM ...` guard can mean a
/// proposed write physically changes nothing). Issue #196: each deleted key
/// is paired with its pre-delete image (`RETURNING ...`, an explicit
/// per-column `jsonb_build_object` per issue #248 — see [`TargetDeletedKey`]'s
/// doc comment — the same shape `apply_aggregate::delete_group_row`'s issue
/// #180 fix captures for an extinct aggregate group), so `apply_and_mark_drained_many`
/// can stage a real image-bearing delete for a deleted 1-1 target row
/// instead of an image-less `Recompute` — see [`ChangedKey`]'s doc comment.
///
/// The pre-lock takes every key this call touches (write or delete) `FOR
/// UPDATE`, ordered ascending, in one round trip — the deadlock-avoidance
/// convention doc 05 calls for between concurrent workers writing
/// overlapping target rows. It binds the whole key set as a single `text[]`
/// parameter, so — unlike the upsert below — its size never approaches the
/// bind-parameter cap regardless of batch size. Because this transaction
/// already holds every lock it needs before the upsert/delete run, chunking
/// those into multiple statements below doesn't reopen the ordering gap the
/// pre-lock exists to close: two transactions racing on overlapping keys
/// still each take every lock, in the same ascending order, before either
/// writes anything.
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
    skip(txn, target, plan),
    fields(
        transform = %target,
        proposed_writes = plan.writes.len(),
        proposed_deletes = plan.deletes.len(),
        written = tracing::field::Empty,
        deleted = tracing::field::Empty,
    )
)]
async fn apply_target(
    txn: &Transaction<'_>,
    target: &str,
    plan: &TargetPlan,
) -> Result<(Vec<String>, Vec<TargetDeletedKey>), ApplyError> {
    if plan.writes.is_empty() && plan.deletes.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let pk_ident = quote_ident(&plan.pk.name);
    let pk_cast = plan.pk.data_type.as_str();
    // `plan.qualified_target` (issue #73's persisted identity), not a bare
    // `quote_ident(target)` — a target explicitly qualified into a
    // non-default schema (issue #76) isn't necessarily on this connection's
    // pinned `search_path`. See `TargetPlan::qualified_target`'s doc comment
    // and `ddl::qualified_target_table_ident`'s.
    let target_ident = ddl::qualified_target_table_ident(&plan.qualified_target);
    let field_idents: Vec<String> = plan.field_names.iter().map(|n| quote_ident(n)).collect();

    // Issue #248: an explicit per-column `jsonb_build_object`, not
    // `to_jsonb(t.*)`, for the delete `RETURNING` below — see
    // `row_as_text_jsonb_sql`'s doc comment for why. This target's full
    // column set is exactly `pk` plus `field_names` (`ddl::create_target_table`'s
    // own DDL never declares any other column), so — unlike the read paths
    // above, which read tables this function doesn't control the shape of —
    // no live `pg_catalog` introspection is needed here.
    let old_image_columns: Vec<String> = std::iter::once(plan.pk.name.clone())
        .chain(plan.field_names.iter().cloned())
        .collect();
    let old_image_expr = row_as_text_jsonb_sql("t", &old_image_columns);

    // Issue #205: `write.pk_text`/`delete.pk_text` is this target's shared
    // key-contract text, not necessarily a raw PK value yet — decode each
    // one through `decode_target_pk_text` before treating it as a literal PK
    // value (or a lock/match key) anywhere below. See that function's doc
    // comment: `None` (a genuine NULL-keyed group, only reachable for a
    // `KeySpace::OneToOne` definition chained off a nullable aggregate
    // grouping key) can never be this target's own stored PK value, so such
    // a key is simply dropped from every step below, rather than bound as a
    // literal `NULL_KEY_SENTINEL`/escaped string (the corruption this issue
    // closes) or as a literal SQL `NULL` (which this target's own `NOT NULL`
    // primary-key column would just as reliably reject).
    let decoded_writes: Vec<Option<Cow<'_, str>>> = plan
        .writes
        .iter()
        .map(|w| decode_target_pk_text(&plan.pk, target, &w.pk_text))
        .collect::<Result<_, _>>()?;
    let decoded_deletes: Vec<Option<Cow<'_, str>>> = plan
        .deletes
        .iter()
        .map(|d| decode_target_pk_text(&plan.pk, target, &d.pk_text))
        .collect::<Result<_, _>>()?;

    let mut lock_keys: Vec<&str> = decoded_writes
        .iter()
        .chain(decoded_deletes.iter())
        .filter_map(|k| k.as_deref())
        .collect();
    lock_keys.sort_unstable();
    lock_keys.dedup();

    txn.query(
        &format!(
            "select {pk_ident} from {target_ident} \
             where {pk_ident} = any($1::text[]::{pk_cast}[]) \
             order by {pk_ident} for update"
        ),
        &[&lock_keys],
    )
    .await?;

    let field_pg_types: Vec<&str> = plan
        .field_types
        .iter()
        .map(|t| ddl::pg_type_name(*t))
        .collect();

    let col_list = std::iter::once(pk_ident.clone())
        .chain(field_idents.iter().cloned())
        .collect::<Vec<_>>()
        .join(", ");

    // Only a write whose key actually decoded to a representable (non-NULL)
    // PK value is a candidate for insertion — see `decode_target_pk_text`'s
    // doc comment on why a NULL-decoded write has no row to write at all.
    let writable: Vec<(&TargetWrite, &str)> = plan
        .writes
        .iter()
        .zip(decoded_writes.iter())
        .filter_map(|(w, k)| k.as_deref().map(|k| (w, k)))
        .collect();

    let mut written = Vec::new();
    if !writable.is_empty() {
        let cols_per_row = 1 + plan.field_names.len();
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
            format!("on conflict ({pk_ident}) do nothing")
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
                "on conflict ({pk_ident}) do update set {set_list} \
                 where ({target_cols}) is distinct from ({excluded_cols})"
            )
        };

        for chunk in writable.chunks(rows_per_chunk) {
            let mut rows_sql = Vec::with_capacity(chunk.len());
            let mut params: Vec<&(dyn ToSql + Sync)> =
                Vec::with_capacity(chunk.len() * cols_per_row);
            for (i, (write, pk_text)) in chunk.iter().enumerate() {
                let base = i * cols_per_row;
                let mut row_parts = vec![format!("${}::text::{pk_cast}", base + 1)];
                params.push(pk_text);
                for (j, pg_type) in field_pg_types.iter().enumerate() {
                    row_parts.push(format!("${}::text::{pg_type}", base + 2 + j));
                    params.push(&write.values[j]);
                }
                rows_sql.push(format!("({})", row_parts.join(", ")));
            }

            let sql = format!(
                "insert into {target_ident} ({col_list}) \
                 select * from (values {}) as v({col_list}) \
                 {on_conflict} \
                 returning {pk_ident}::text as pk",
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
    // decoded keys (issue #205) — the same values actually bound as this
    // target's real PK, and so the same values `delete_keys` below matches
    // against.
    let write_keys: std::collections::HashSet<&str> = writable.iter().map(|(_, k)| *k).collect();

    let mut deleted = Vec::new();
    // A `None`-decoded delete has no matching write (a NULL-keyed group
    // never reaches `writable` either) and no representable row to delete —
    // see `decode_target_pk_text`'s doc comment — so it's dropped here the
    // same way a `None`-decoded write is dropped above.
    let delete_keys: Vec<&str> = decoded_deletes
        .iter()
        .filter_map(|k| k.as_deref())
        .filter(|k| !write_keys.contains(k))
        .collect();
    if !delete_keys.is_empty() {
        // Issue #196: `as t` + `old_image_expr` (an explicit per-column
        // `jsonb_build_object`, issue #248 — not `to_jsonb(t.*)`, see
        // `row_as_text_jsonb_sql`'s doc comment) captures each deleted
        // row's exact pre-delete state, the same `apply_aggregate`'s
        // `delete_group_row` does for an extinct aggregate group (issue
        // #180) — see this function's own doc comment and `ChangedKey`'s for
        // why a 1-1 target's delete needed this same treatment. `pk_ident`
        // stays unqualified (no `t.` prefix) since `t` is the sole table in
        // scope, exactly like `delete_group_row`'s own `where_sql`.
        let rows = txn
            .query(
                &format!(
                    "delete from {target_ident} as t \
                     where {pk_ident} = any($1::text[]::{pk_cast}[]) \
                     returning {pk_ident}::text as pk, {old_image_expr}::text as old_image"
                ),
                &[&delete_keys],
            )
            .await?;
        deleted.extend(
            rows.into_iter()
                .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1))),
        );
    }

    let span = tracing::Span::current();
    span.record("written", written.len());
    span.record("deleted", deleted.len());
    Ok((written, deleted))
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
///    with `bucket_count = 1` (`seal::seal_phase1`) and is drained under
///    `next_claimable_segment`'s barrier (no predecessor or successor
///    segment concurrently draining the same target), so within this one
///    transaction "clear, then write" is exactly what makes a same-batch
///    post-truncate insert (already computed into `plan.targets` by
///    [`compute`]) survive, while anything the truncate is meant to erase
///    does not.
/// 3. **The ordered pre-lock + upsert/delete**, per target table, via
///    [`apply_target`], immediately followed by the **relationship
///    settled-parent projection gen bump** (issue #130, epic #127): every
///    to-one relationship parent this batch's relationship resolution
///    touched (`plan.relationship_gen_bumps`) gets its projection's
///    `__trellis_gen` bumped by 1, in this same transaction — see that
///    step's own inline comment for the exact semantics and the #133 gap it
///    documents.
/// 4. **Downstream propagation**: for every physically-changed key (write
///    or delete — no-op-suppressed writes don't count) in a target table at
///    least one definition currently reads, stages a `Recompute` row at
///    `hop_gen + 1`, enforcing [`MAX_HOP_GEN`] first. Whether a target has
///    downstream readers was decided back in Phase 2
///    ([`ApplyPlan::downstream_readers`]), not re-checked here: Phase 3
///    holds no pool connection, only `txn`, and re-deriving "does anything
///    read this table" is a catalog read like the ones Phase 2 already did
///    for evaluation. A definition created between Phase 2 and this commit
///    that starts reading a target for the first time is not missed
///    forever — definition creation is responsible for backfilling its own
///    new consumer against current target state, a separate concern from
///    this batch's propagation.
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
/// tree's apply phase — parent of every [`apply_target`]/
/// [`apply_aggregate::apply_aggregate_target`] span this call makes (one per
/// consuming transform), since each of those runs inside this async fn's own
/// `#[tracing::instrument]`-created span.
#[tracing::instrument(
    name = "staging.apply_and_mark_drained",
    skip(txn, plan, wake_channel, watermark),
    fields(
        segments = seg_seqs.len(),
        targets = plan.targets.len(),
        aggregate_targets = plan.aggregate_targets.len(),
        keys_written = tracing::field::Empty,
        keys_deleted = tracing::field::Empty,
    )
)]
pub async fn apply_and_mark_drained_many(
    txn: &Transaction<'_>,
    seg_seqs: &[i64],
    claimed_by: &str,
    plan: &ApplyPlan,
    wake_channel: &str,
    watermark: &StagedWatermark,
) -> Result<ManyApplyOutcome, ApplyError> {
    // 1. Version fence. `source_key` is bare (see `catalog_source_key`'s doc
    // comment); `source_table_versions.source_table` is qualified as of
    // issue #72, so this matches against its bare table-name suffix, same
    // as `defs::source_table_version`'s own read — see that function's doc
    // comment for why issue #73 doesn't retire this (short version:
    // `source_key` still traces back to `ddl::neighbor_table_name`, which
    // stays bare regardless; only issue #75's emission audit would let this
    // go back to an exact match).
    for (source_key, loaded_version) in &plan.versions {
        let row = txn
            .query_opt(
                "select version from source_table_versions \
                 where split_part(source_table, '.', 2) = $1 for share",
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
    quarantine::park_batch_contribution(txn, seg_seqs[0], &plan.poisoned_park).await?;

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
    // `changed` accumulates rather than overwrites per target (`extend`,
    // not `insert`): a target can appear in both `plan.clears` and
    // `plan.targets` in the same batch — a truncate clear followed by a
    // same-batch post-truncate write to the same target — and both halves'
    // physically-touched keys must propagate downstream. The third tuple
    // element (see [`ChangedKey`]) is `src_changed` (issues #51/#52's
    // multi-hop gap), carried into the `Recompute` row step 4 stages for
    // this key, so a downstream hop reached purely through automatic
    // propagation still traces back to a real origin.
    let mut changed: HashMap<&str, Vec<ChangedKey>> = HashMap::new();

    // 2. Truncate clears, before this target's own upsert/delete below —
    // see this function's doc comment on why "clear, then write" is safe
    // here specifically (single-bucket batch, barrier-drained).
    for (target, clear) in &plan.clears {
        let pk_ident = quote_ident(&clear.pk.name);
        let target_ident = ddl::qualified_target_table_ident(&clear.qualified_target);
        let cleared: Vec<String> = txn
            .query(
                &format!("delete from {target_ident} returning {pk_ident}::text as pk"),
                &[],
            )
            .await?
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        keys_deleted += cleared.len();
        if cleared.is_empty() {
            continue;
        }
        // A truncate clear's own downstream propagation stays image-less
        // (`None`, the 4th [`ChangedKey`] field) — that gap is #98/#165/#168's
        // tracked territory, not issue #180's (which only threads an image
        // through the aggregate-group-extinction case below).
        let touched: Vec<ChangedKey> = cleared
            .into_iter()
            .map(|k| (k, clear.hop_gen, clear.src_changed, None))
            .collect();
        changed.entry(target.as_str()).or_default().extend(touched);
    }

    // 2b. Aggregate truncate clears — see [`ApplyPlan::aggregate_clears`]'s
    // doc comment on why these are a plain full-table delete with no
    // downstream propagation, unlike every other clear/write/delete this
    // function tracks via `changed`.
    for clear in plan.aggregate_clears.values() {
        let target_ident = ddl::qualified_target_table_ident(&clear.qualified_target);
        let cleared = txn
            .execute(&format!("delete from {target_ident}"), &[])
            .await?;
        keys_deleted += cleared as usize;
    }

    // 2c. Issue #168: settled parent projection clears — a `TRUNCATE` on a
    // to-one relationship's to-side table empties that relationship's
    // projection too, same "whole table" shape as 2b just above (and for
    // the same reason: nothing here can enumerate which specific keys the
    // truncate removed, and every row this projection held for this
    // relationship just vanished along with it). No downstream propagation
    // of its own, same as 2b — the from-side recompute this same truncate
    // stages via `ApplyPlan::reverse_recomputes` is what actually reaches a
    // definition; the projection is only ever read by
    // `build_relationship_context`/the reverse-guard machinery, never a
    // definition's own downstream consumer.
    for qualified_projection in &plan.relationship_projection_clears {
        txn.execute(&format!("delete from {qualified_projection}"), &[])
            .await?;
    }

    // 3. Ordered pre-lock + upsert/delete, per target table.
    for (target, target_plan) in &plan.targets {
        let (written, deleted) = apply_target(txn, target, target_plan).await?;
        keys_written += written.len();
        keys_deleted += deleted.len();

        if written.is_empty() && deleted.is_empty() {
            continue;
        }

        // Issue #205: `written`/`deleted` are keyed by `apply_target`'s
        // *decoded* PK text (the value actually stored/matched, per
        // `decode_target_pk_text`), which for a target chained off a
        // nullable grouping key can differ from `target_plan.writes`/
        // `.deletes`' own `pk_text` (the shared key-contract's still-encoded
        // form). Re-decode here too, so this lookup is keyed the same way —
        // for every not-null-PK target (the overwhelming majority) decoding
        // is a no-op and this is byte-identical to a plain `pk_text` key, as
        // before. A `None` decode is skipped: `apply_target` never returns
        // such a key in `written`/`deleted` (see that function's own doc
        // comment), so it would never be looked up anyway.
        // Keyed by `Cow`, not `String`, so the no-op decode path stays
        // allocation-free the way the pre-#205 `&str` keys were: a not-null
        // PK column's decode hands back a `Cow::Borrowed` straight out of
        // `pk_text` (which outlives this loop), and cloning that for the
        // second map is a pointer copy, not a heap copy. `Cow<'_, str>:
        // Borrow<str>` and hashes as its `str`, so the `get(key.as_str())`
        // lookups below need no wrapping and match a `Cow::Owned` key (a
        // genuinely decoded one) just the same.
        let mut hop_gen_of: HashMap<Cow<'_, str>, i32> = HashMap::new();
        let mut src_changed_of: HashMap<Cow<'_, str>, Option<std::time::SystemTime>> =
            HashMap::new();
        for w in &target_plan.writes {
            if let Some(decoded) = decode_target_pk_text(&target_plan.pk, target, &w.pk_text)? {
                hop_gen_of.insert(decoded.clone(), w.hop_gen);
                src_changed_of.insert(decoded, w.src_changed);
            }
        }
        for d in &target_plan.deletes {
            if let Some(decoded) = decode_target_pk_text(&target_plan.pk, target, &d.pk_text)? {
                hop_gen_of.insert(decoded.clone(), d.hop_gen);
                src_changed_of.insert(decoded, d.src_changed);
            }
        }

        // Issue #196: a deleted 1-1 target row's downstream propagation used
        // to stay image-less (`None`) here — the same "same failure family,
        // different producer" gap issue #180 fixed for extinct aggregate
        // groups, left unthreaded for this producer at the time. `deleted`
        // now carries each row's pre-delete image straight from
        // `apply_target`'s own `RETURNING` (an explicit per-column
        // `jsonb_build_object`, issue #248), so it stages
        // the same way 3b's extinct-group `deleted` entries do below —
        // `Some(old_image)`, letting a chained downstream aggregate subtract
        // this row's last-known contribution (`accumulate_changes`'s
        // `(Some(old_row), None)` branch) instead of a live refetch finding
        // nothing and silently dropping the change. Step 4's same-batch
        // write-vs-delete guard (issue #180 hardening, `e28a89c`) already
        // applies here for free: it reads generically off `changed`'s
        // per-target `touched` vector, regardless of which step populated
        // it, so this needs no guard logic of its own — see that guard's own
        // comment at the step 4 call site.
        let written_touched = written.into_iter().map(|key| {
            let hop_gen = hop_gen_of.get(key.as_str()).copied().unwrap_or(0);
            let src_changed = src_changed_of.get(key.as_str()).copied().flatten();
            (key, hop_gen, src_changed, None)
        });
        let deleted_touched = deleted.into_iter().map(|(key, old_image)| {
            let hop_gen = hop_gen_of.get(key.as_str()).copied().unwrap_or(0);
            let src_changed = src_changed_of.get(key.as_str()).copied().flatten();
            (key, hop_gen, src_changed, Some(old_image))
        });
        let touched: Vec<ChangedKey> = written_touched.chain(deleted_touched).collect();
        changed.entry(target.as_str()).or_default().extend(touched);
    }

    // 3b. Aggregate targets: same ordered-write step as 3, above, for
    // [`KeySpace::Aggregate`] definitions — see `apply_aggregate`'s doc
    // comment for the per-group delta/probe logic itself. Written/deleted
    // groups fold into the same `changed` accounting as the 1-1 case, so
    // downstream propagation below needs no branching of its own. This
    // stages Recompute rows keyed by `apply_aggregate::derive_group_key`'s
    // key, same as any 1-1 target — see [`ApplyPlan::aggregate_clears`]'s
    // doc comment, corrected during #51/#52's review: chaining a definition
    // onto an aggregate target *is* live and reachable, at any `GROUP BY`
    // arity. That key used to be a locally-invented, length-prefixed
    // encoding, which a chained definition's live refetch
    // (`read_live_rows_batch`, above) would then try to bind as the target's
    // real primary key — corrupting or crashing that refetch for a
    // single-column `GROUP BY`
    // ([#103](https://github.com/salesforce-misc/trellis/issues/103)) and
    // failing it outright with `DdlError::MalformedCompositeKey` for a
    // multi-column one
    // ([#171](https://github.com/salesforce-misc/trellis/issues/171)). Both
    // now fixed: `derive_group_key` emits exactly `ddl::pk_key_sql_expr`'s
    // own identity encoding at either arity (the bare value for one column,
    // the U+001F join for several), matching the target's real PK shape.
    // `src_changed` (issue #104, follow-up to #51/#52 once #103 made
    // aggregate-target chaining live rather than moot) is threaded from
    // each touched [`apply_aggregate::GroupPlan`]'s own `src_changed` —
    // folded there by `accumulate_changes` via the same
    // [`earliest_src_changed`] fan-in tie-break this module's 1-1 path
    // uses, so a `Recompute` staged for a transform chained off an
    // aggregate target now carries a real origin instead of always
    // reading `None`.
    //
    // Issue #180: `result.deleted` additionally carries each extinct group's
    // pre-delete image (`AggregateApplyResult::deleted`'s own doc comment) —
    // threaded through as this `ChangedKey`'s 4th field so step 4 below can
    // stage a real image-bearing delete instead of an image-less `Recompute`
    // for exactly this case, closing the gap
    // `defs_aggregate_chained_composite_group_key.rs`'s
    // `a_live_insert_and_an_extinct_composite_group_both_propagate_downstream`
    // (formerly `..._hits_the_image_less_gap`) now pins as fixed.
    for (target, agg_plan) in &plan.aggregate_targets {
        // `&agg_plan.target` (issue #73's persisted identity), not the bare
        // `target` map key — see `AggregateTargetPlan::target`'s doc
        // comment. `target` itself stays bare here purely as the
        // `changed`/`downstream_readers` bookkeeping key below.
        let result =
            apply_aggregate::apply_aggregate_target(txn, &agg_plan.target, agg_plan).await?;
        keys_written += result.written.len();
        keys_deleted += result.deleted.len();

        if result.written.is_empty() && result.deleted.is_empty() {
            continue;
        }
        changed.entry(target.as_str()).or_default().extend(
            result
                .written
                .into_iter()
                .map(|(key, hop_gen, src_changed)| (key, hop_gen, src_changed, None))
                .chain(result.deleted),
        );
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

    // 3d. Issue #131, epic #127: to-one relationship reverse-delta apply —
    // see `RelationshipReverseRecord`'s doc comment for the mechanism and
    // `ReverseRelationshipShape`'s for the fast-path/fallback split this
    // step dispatches on.
    //
    // Not batched across sibling records touching the same aggregate
    // target within this one drain (each record calls
    // `apply_aggregate::apply_aggregate_target` on its own, immediately) —
    // a documented, non-correctness-affecting simplification flagged for
    // whoever picks up the next perf pass: the writes are genuinely
    // additive, so N sequential per-record applies to the same group
    // produce the same final value as one batched apply with the merged
    // deltas, just as N SQL round trips instead of one. Every record in
    // `plan.relationship_reverses` is already the whole batch's fold
    // collapsed to one record per parent key (see that field's own doc
    // comment), so this only matters when *two different* parent keys in
    // one drain happen to feed the same target (e.g. an aggregate grouped
    // by a column unrelated to the relationship).
    let mut relationship_reverse_fallback: Vec<(
        String,
        String,
        i32,
        Option<std::time::SystemTime>,
    )> = Vec::new();
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
    for record in &plan.relationship_reverses {
        let shape = &record.shape;
        let old_key = relationship_key_text(&record.old_row, &shape.to_col);
        let new_key = relationship_key_text(&record.new_row, &shape.to_col);
        // Resolved once per record from the batch-wide cache above — every
        // `from_side_rows_for_trigger_txn` call this record makes (via
        // `diff_pass` below and/or `stage_reverse_recompute_fallback`)
        // reuses this same slice.
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
                    shape,
                    &old_key,
                    &new_key,
                    record.hop_gen + 1,
                    record.src_changed,
                    &mut seen_keys,
                    &mut relationship_reverse_fallback,
                    &row_columns,
                )
                .await?;
                apply_projection_advance(
                    txn,
                    shape,
                    &old_key,
                    &new_key,
                    record.new_image.as_deref(),
                    record.lsn,
                )
                .await?;
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
                relationship_id: shape.id,
                retry_count: record.retry_count + 1,
            });
            continue;
        }

        // Fast path: a true delta for every fully-invertible,
        // single-relationship aggregate target.
        //
        // **Not** an unconditional subtract-old/add-new over two
        // *independent* row sets — a parent-only change never moves a
        // from-side row into or out of its aggregate group (the row's own
        // `GROUP BY` columns never change), so every affected row is
        // diffed in place: its contribution *under the old parent state*
        // vs. *under the new one*, per field, via
        // `apply_aggregate::diff_contributions` (the same per-field
        // cancellation `accumulate_changes`'s own in-place-update branch
        // uses) — otherwise a field the relationship doesn't even read
        // (e.g. a plain `COUNT(*)`) would wrongly gain a net delta every
        // time its group is touched here. The common case (an ordinary
        // parent attribute update; `old_key == new_key`) enumerates the
        // row set once. `old_key != new_key` (a parent insert, delete, or
        // the rare case of the parent's own key value itself changing)
        // enumerates each side separately, diffing against "no relationship
        // match" (`parent = None`) for whichever side a row's own set
        // doesn't carry.
        //
        // Issue #134 correctness fork, found while building this issue's
        // own test coverage, and sharpened by review follow-up (an
        // independent reproduction proved a first-attempt, `retry_count ==
        // 0` record vulnerable too — see
        // `relationship_fast_path_precondition_holds`'s own doc comment for
        // the full hazard and the residual gap this still leaves open):
        // `diff_pass` assumes every from-side row currently matching this
        // key had its prior contribution computed under `old_parent`'s
        // value and needs correcting to `new_parent`'s — true only if
        // nothing else touched the group in between. `retry_count == 0` is
        // kept as a cheap, always-correct pre-filter (a record that has
        // already been deferred once is inherently more exposed and this
        // avoids the extra query for the common non-retried case with no
        // loss of safety), `&&`ed with a real, general check —
        // [`relationship_fast_path_precondition_holds`] — for whether any
        // sibling from-side change (staged, in-flight, *or already
        // drained*) could have raced this record's own old/new window via
        // `force_every_group`. Either one failing routes to the fallback
        // below (identical treatment to `needs_recompute_fallback`), which
        // is immune to this hazard: it stages an image-less `Recompute`,
        // which (for this same definition) goes through the identical
        // `force_every_group` live recompute, so it is idempotent no
        // matter how many times the group has already been touched.
        let mut fast_path_keys: Vec<&str> = Vec::with_capacity(2);
        if let Some(k) = old_key.as_deref() {
            fast_path_keys.push(k);
        }
        if let Some(k) = new_key.as_deref()
            && Some(k) != old_key.as_deref()
        {
            fast_path_keys.push(k);
        }
        let fast_path_safe = record.retry_count == 0
            && relationship_fast_path_precondition_holds(
                txn,
                &shape.from_table,
                &shape.from_col,
                &fast_path_keys,
                record.prev_lsn,
                record.watermark,
            )
            .await?;
        for agg_shape in shape.aggregate_shapes.iter().filter(|_| fast_path_safe) {
            let mut target_plan = agg_shape.template.clone();
            let mut regex_cache = eval::RegexCache::new();

            // Issue #137: a `GROUP BY` key can itself read this relationship
            // (`GROUP BY tag, post.author`), in which case a from-side row's
            // *group* — not just its contribution — can move as a pure side
            // effect of the to-side row's own change, even though the
            // from-side row itself never changed. `old_augmented`/
            // `new_augmented` resolve the relationship's value from the old
            // and new parent images respectively (never a live read), so the
            // old and new group keys can differ here exactly the way an
            // ordinary same-row CDC `UPDATE` can move a row between groups
            // in `accumulate_changes`'s own grain-migration branch — this
            // mirrors that branch's split (`sub_contributions` from the old
            // group, `add_contributions` to the new one) rather than
            // `diff_contributions`'s single-group assumption, whenever the
            // two keys disagree. For a plain-column-only `GROUP BY` (the
            // overwhelmingly common case), `old_group_key` and
            // `new_group_key` are always equal (neither depends on the
            // augmented/synthetic columns at all), so this takes the
            // `diff_contributions` branch exactly as before issue #137.
            let diff_pass = async |txn: &Transaction<'_>,
                                   target_plan: &mut AggregateTargetPlan,
                                   regex_cache: &mut eval::RegexCache,
                                   pass_key: &str,
                                   old_parent: &Option<Row>,
                                   new_parent: &Option<Row>|
                   -> Result<(), ApplyError> {
                let pass_key = pass_key.to_string();
                let trigger = ReverseTrigger::Keys(std::slice::from_ref(&pass_key));
                let from_rows = from_side_rows_for_trigger_txn(
                    txn,
                    &shape.from_table,
                    &shape.from_col,
                    &shape.from_pk,
                    &trigger,
                    &row_columns,
                )
                .await?;
                for (_, from_row) in from_rows {
                    let old_augmented = augment_row_with_relationship_value(
                        &from_row,
                        &agg_shape.synthetic_columns,
                        old_parent,
                    );
                    let new_augmented = augment_row_with_relationship_value(
                        &from_row,
                        &agg_shape.synthetic_columns,
                        new_parent,
                    );
                    let (old_values, old_group_key) = apply_aggregate::derive_group_key(
                        &old_augmented,
                        &agg_shape.group_by_row_columns,
                    );
                    let (new_values, new_group_key) = apply_aggregate::derive_group_key(
                        &new_augmented,
                        &agg_shape.group_by_row_columns,
                    );
                    let old_contrib = apply_aggregate::row_contribution(
                        &agg_shape.contribution_def,
                        &old_augmented,
                        &agg_shape.source_columns,
                        regex_cache,
                    )?;
                    let new_contrib = apply_aggregate::row_contribution(
                        &agg_shape.contribution_def,
                        &new_augmented,
                        &agg_shape.source_columns,
                        regex_cache,
                    )?;
                    if old_group_key == new_group_key {
                        let group = target_plan
                            .groups
                            .entry(new_group_key)
                            .or_insert_with(|| apply_aggregate::GroupPlan::new(new_values));
                        group.hop_gen = group.hop_gen.max(record.hop_gen);
                        group.src_changed =
                            earliest_src_changed(group.src_changed, record.src_changed);
                        apply_aggregate::diff_contributions(
                            &target_plan.fields,
                            group,
                            &old_contrib,
                            &new_contrib,
                        );
                    } else {
                        let old_group = target_plan
                            .groups
                            .entry(old_group_key)
                            .or_insert_with(|| apply_aggregate::GroupPlan::new(old_values));
                        old_group.hop_gen = old_group.hop_gen.max(record.hop_gen);
                        old_group.src_changed =
                            earliest_src_changed(old_group.src_changed, record.src_changed);
                        apply_aggregate::sub_contributions(
                            &target_plan.fields,
                            old_group,
                            &old_contrib,
                        );

                        let new_group = target_plan
                            .groups
                            .entry(new_group_key)
                            .or_insert_with(|| apply_aggregate::GroupPlan::new(new_values));
                        new_group.hop_gen = new_group.hop_gen.max(record.hop_gen);
                        new_group.src_changed =
                            earliest_src_changed(new_group.src_changed, record.src_changed);
                        apply_aggregate::add_contributions(
                            &target_plan.fields,
                            new_group,
                            &new_contrib,
                        );
                    }
                }
                Ok(())
            };

            if old_key == new_key {
                if let Some(key) = &old_key {
                    diff_pass(
                        txn,
                        &mut target_plan,
                        &mut regex_cache,
                        key,
                        &record.old_row,
                        &record.new_row,
                    )
                    .await?;
                }
            } else {
                if let Some(key) = &old_key {
                    diff_pass(
                        txn,
                        &mut target_plan,
                        &mut regex_cache,
                        key,
                        &record.old_row,
                        &None,
                    )
                    .await?;
                }
                if let Some(key) = &new_key {
                    diff_pass(
                        txn,
                        &mut target_plan,
                        &mut regex_cache,
                        key,
                        &None,
                        &record.new_row,
                    )
                    .await?;
                }
            }

            if target_plan.groups.is_empty() {
                continue;
            }
            let result =
                apply_aggregate::apply_aggregate_target(txn, &agg_shape.target, &target_plan)
                    .await?;
            keys_written += result.written.len();
            keys_deleted += result.deleted.len();
            if !result.written.is_empty() || !result.deleted.is_empty() {
                // Issue #180: same image-threading as the forward path's 3b
                // step above — `result.deleted`'s pre-delete image lets step
                // 4 stage a real image-bearing delete for an extinct group
                // reached through the reverse-relationship fast path too.
                // Issue #104: `result`'s own `src_changed` (folded into each
                // touched `GroupPlan` above from this single `record`'s own
                // `src_changed` — the diff_pass merges above) carries the
                // same origin `record.src_changed` would, so no separate
                // substitution is needed here, unlike before this fix, when
                // `AggregateApplyResult`'s written/deleted shape carried no
                // origin at all.
                changed
                    .entry(agg_shape.target.as_str())
                    .or_default()
                    .extend(
                        result
                            .written
                            .into_iter()
                            .map(|(key, hop_gen, src_changed)| (key, hop_gen, src_changed, None))
                            .chain(result.deleted),
                    );
            }
        }

        // Fallback: anything the fast path doesn't cover for this
        // relationship (a 1-1 target, a `RecomputeOnly` field, a
        // multi-relationship aggregate — see
        // `ReverseRelationshipShape::needs_recompute_fallback`'s doc
        // comment) still needs the pre-#131 treatment for every touched
        // from-side row. Issue #134: also runs whenever `!fast_path_safe`
        // (a retry, or `relationship_fast_path_precondition_holds` found a
        // racing sibling), covering `aggregate_shapes`' own targets too —
        // see the fast-path loop's own comment just above for why an
        // unsafe record skips that loop entirely rather than only skipping
        // it for definitions this flag already names.
        if shape.needs_recompute_fallback || !fast_path_safe {
            let mut seen_keys: std::collections::HashSet<String> = std::collections::HashSet::new();
            stage_reverse_recompute_fallback(
                txn,
                shape,
                &old_key,
                &new_key,
                record.hop_gen + 1,
                record.src_changed,
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
        apply_projection_advance(
            txn,
            shape,
            &old_key,
            &new_key,
            record.new_image.as_deref(),
            record.lsn,
        )
        .await?;
    }

    // 4. Downstream propagation, with the hop bound checked before staging
    // anything.
    let mut recompute_changes = Vec::new();
    let mut hop_bound_tables = Vec::new();
    let mut worst_hop_gen = 0;
    for (target, touched) in &changed {
        if !plan
            .downstream_readers
            .get(*target)
            .copied()
            .unwrap_or(false)
        {
            continue;
        }
        // Issue #180 hardening: one batch can physically touch the same
        // target key more than once. The forward aggregate step (3b) and
        // *each* per-record reverse-relationship fast-path apply (3d) extend
        // this very same vector, so a group whose last from-side row moves
        // away under one reverse record and whose first from-side row
        // arrives under another is deleted by one apply and rewritten by
        // the next, inside this one batch. Staging both an image-bearing
        // delete and an image-less `Recompute` for such a key would be
        // strictly worse than staging neither: [`fold::fold`]'s
        // arg-extremes only ever consider image-bearing rows, so the
        // delete's `old_image` (and its absent `new_image`) wins *both*
        // halves and the recompute is annihilated — a chained downstream
        // aggregate would then subtract a still-live group's contribution
        // and never learn its new value. A key this batch also wrote
        // therefore gives up its image and stays an ordinary `Recompute`,
        // whose downstream live refetch finds the surviving row and
        // re-derives the group correctly — exactly the pre-#180 behaviour,
        // which was only ever wrong for a key that really is gone.
        //
        // Issue #196 reuses this exact guard for a deleted 1-1 target row's
        // own image-bearing entry (step 3), with no changes needed here: the
        // guard reads generically off `touched`, whichever step(s)
        // contributed to it. Unlike the aggregate case above, step 3 cannot
        // actually produce a key with *both* a written and a deleted entry
        // in the first place — `apply_target` resolves that conflict itself
        // before it ever reaches the database (see its own "never delete a
        // key this same call just wrote" comment), and it is the only
        // producer of a 1-1 target's `touched` entries besides the
        // truncate-clear step (2, always image-less already) — but the
        // guard still applies uniformly rather than needing a carve-out, so
        // a future second producer of 1-1 target deletes/writes (there is
        // none today) inherits the same safety automatically.
        let rewritten = keys_written_without_image(touched);
        for (key, hop_gen, src_changed, deleted_old_image) in touched {
            let next_hop = hop_gen + 1;
            if next_hop > MAX_HOP_GEN {
                hop_bound_tables.push(target.to_string());
                worst_hop_gen = worst_hop_gen.max(next_hop);
                continue;
            }
            // Issues #180/#196: a deletion whose pre-delete image was
            // captured (an extinct aggregate group's `deleted` entry, or a
            // deleted 1-1 target row's own captured entry — see
            // [`ChangedKey`]'s doc comment) stages as a real image-bearing
            // delete instead of an image-less `Recompute`, so
            // a chained downstream aggregate can subtract the extinct row's
            // last-known contribution (`accumulate_changes`'s `(Some(old_row),
            // None)` branch) rather than have its live refetch find nothing
            // and silently drop the change (the module doc comment's "A
            // known gap: image-less changes"). An ordinary write still stays
            // image-less — a downstream live refetch always finds the
            // *right* current row for those, `NULL`-keyed groups included
            // (issue #110 closed #195's gap: `derive_group_key`/
            // `read_live_rows_batch` now resolve a `NULL` group key by its
            // own real identity instead of mistaking it for "no row").
            //
            // `lsn: None`, like every other row this step stages: a
            // propagated hop has no source LSN of its own. Two fold-side
            // predicates read `lsn` and were written when "no `lsn`" implied
            // "no images" — both stay sound for this row, but only by
            // argument, so re-check them if either changes: [`fold::fold`]'s
            // truncate-void filter (`(t.lsn, t.change_id) > (f.lsn,
            // f.change_id)` is `NULL`, so a truncate on the target never
            // voids this delete — harmless, since subtracting a group that a
            // truncate also erased reaches the same answer), and
            // [`from_side_change_in_flight`]'s `r.lsn <= $2` (this row is
            // invisible to that in-flight probe, exactly as its pre-#180
            // `Recompute` was).
            match deleted_old_image {
                Some(old_image) if !rewritten.contains(key.as_str()) => {
                    recompute_changes.push(StagedChange::Cdc {
                        src_table: target.to_string(),
                        key: key.clone(),
                        op: append::CdcOp::Delete,
                        lsn: None,
                        old_image: Some(old_image.clone()),
                        new_image: None,
                        origin_lsn: None,
                        src_changed: *src_changed,
                        hop_gen: next_hop,
                        group_key: None,
                    });
                }
                _ => {
                    recompute_changes.push(StagedChange::Recompute {
                        src_table: target.to_string(),
                        key: key.clone(),
                        hop_gen: next_hop,
                        group_key: None,
                        src_changed: *src_changed,
                    });
                }
            }
        }
    }

    // Reverse recompute (issue #30): from-side rows a changed related row must
    // re-derive, resolved in Phase 2 and staged here as ordinary image-less
    // recomputes — the same shape and same hop bound forward propagation uses,
    // just keyed by the from-side table/PK rather than a touched target key.
    for (from_table, key, hop_gen, src_changed) in &plan.reverse_recomputes {
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
        });
    }

    // Issue #131: the same image-less recompute staging, for step 3d's own
    // two fallback cases — the ordering-check-miss stopgap, and definitions
    // `ReverseRelationshipShape::needs_recompute_fallback` excludes from the
    // fast path.
    for (from_table, key, hop_gen, src_changed) in &relationship_reverse_fallback {
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
        });
    }

    if !hop_bound_tables.is_empty() {
        hop_bound_tables.sort();
        hop_bound_tables.dedup();
        tracing::error!(
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
    let mut segments_drained = Vec::with_capacity(seg_seqs.len());
    for &seg_seq in seg_seqs {
        let bucket_count: i16 = txn
            .query_one(
                "select bucket_count from segments where seg_seq = $1",
                &[&seg_seq],
            )
            .await?
            .get(0);

        let claimed_buckets: Vec<i16> = txn
            .query(
                "delete from seg_claims where seg_seq = $1 and claimed_by = $2 returning bucket",
                &[&seg_seq, &claimed_by],
            )
            .await?
            .into_iter()
            .map(|row| row.get(0))
            .collect();

        if claimed_buckets.is_empty() {
            tracing::warn!(
                seg_seq,
                claimed_by = %claimed_by,
                "claim was gone by completion time; nothing applied twice, but its buckets \
                 must be reclaimed by whoever holds them now"
            );
            return Err(ApplyError::ClaimLost);
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
        let batch_drained = matches!(
            completed.map(|row| row.get::<_, String>(0)),
            Some(state) if state == "drained"
        );
        segments_drained.push((seg_seq, batch_drained));
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

    let span = tracing::Span::current();
    span.record("keys_written", keys_written);
    span.record("keys_deleted", keys_deleted);
    Ok(ManyApplyOutcome {
        keys_written,
        keys_deleted,
        segments_drained,
        deferral_counts,
        fairness_escalations,
    })
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
}

// ---------------------------------------------------------------------
// Orchestrator
// ---------------------------------------------------------------------

/// The most drain attempts [`drain_once`] retries before giving up and
/// surfacing the last error — a bound on a fence-miss/serialization-failure
/// loop that never resolves (e.g. a source under constant, colliding
/// definition churn), rather than retrying forever.
const MAX_APPLY_ATTEMPTS: u32 = 5;

/// Runs one full drain attempt against `seg_seq`: Phase 1 (claim + fold, in
/// one short transaction), Phase 2 (compute), and Phase 3 (apply ∪
/// mark-drained), retrying Phase 2+3 on a version-fence miss or a
/// serialization failure/deadlock — the design doc's "reload, recompute,
/// retry" loop — using [`FenceMissBackoff`] between attempts.
///
/// Returns `Ok(None)` if this call's claim won (and already owned) nothing
/// — the buckets were all already claimed by someone else — without
/// folding or computing anything. Otherwise returns the winning attempt's
/// [`ApplyOutcome`].
///
/// Issue #56/ADR-0009 decision 3: the outermost span in the propagation
/// tree's apply phase — parent, across however many retries this call
/// takes, of every [`compute`]/[`apply_and_mark_drained`] span (and, through
/// those, every per-transform [`apply_target`] span) a winning attempt
/// makes. `attempt` is recorded once per loop iteration, so its final
/// exported value is however many attempts this call actually took, not
/// just the first.
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
    let mut folded = {
        let mut client = pool.get().await?;
        let txn = client.transaction().await?;
        claim::claim(&*txn, seg_seq, claimed_by, live_workers).await?;
        let filter = claim::owned_bucket_filter(&*txn, seg_seq, claimed_by).await?;
        if filter.is_empty() {
            txn.commit().await?;
            return Ok(None);
        }
        let folded = fold::fold(&txn, seg_seq, filter).await?;
        txn.commit().await?;
        folded
    };

    let mut backoff = FenceMissBackoff::new();
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        tracing::Span::current().record("attempt", attempt);
        let plan = match compute(pool, &folded).await {
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
                    seg_seq,
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
                if let Some(retry_folded) = classify_and_retry(
                    pool,
                    seg_seq,
                    claimed_by,
                    wake_channel,
                    &folded,
                    attempt,
                    &mut backoff,
                    err,
                )
                .await?
                {
                    folded = retry_folded;
                }
                continue;
            }
        };

        let mut client = pool.get().await?;
        let txn = client.transaction().await?;
        match apply_and_mark_drained(&txn, seg_seq, claimed_by, &plan, wake_channel, watermark)
            .await
        {
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
                backoff.reset();
                return Ok(Some(outcome));
            }
            Err(err) => {
                let _ = txn.rollback().await;
                if let Some(retry_folded) = classify_and_retry(
                    pool,
                    seg_seq,
                    claimed_by,
                    wake_channel,
                    &folded,
                    attempt,
                    &mut backoff,
                    err,
                )
                .await?
                {
                    folded = retry_folded;
                }
            }
        }
    }
}

/// The most sealed segments [`drain_many`] will coalesce into a single
/// compute-and-apply pass. A burst of incremental writes seals a new
/// segment roughly every 300ms (`ClientOptions::maintenance_interval`'s
/// default); this bounds how much of that backlog one drain call takes on
/// at once, so a very long burst still drains in several coalesced calls
/// rather than one unbounded one holding a single transaction (and its
/// locks) open over an ever-growing plan.
pub const MAX_COALESCE_SEGMENTS: usize = 32;

/// [`drain_once`] generalized over more than one sealed segment (issue #63
/// Milestone 2): claims and folds every segment in `seg_seqs` in one short
/// transaction (Phase 1), merges their folded changes into one
/// [`fold::merge_folded_changes`] list, then runs Phase 2 (compute) and
/// Phase 3 (apply ∪ mark-drained, via [`apply_and_mark_drained_many`])
/// exactly *once* over the merged list — collapsing what would have been
/// one full compute-and-apply pass per segment (each with its own version
/// fence read, its own ordered pre-lock/upsert, its own forced-group
/// bulk-recompute for any [`crate::defs::ast::KeySpace::Aggregate`] target,
/// and its own downstream-propagation staging) into one such pass for the
/// whole batch.
///
/// `seg_seqs` should come from [`next_claimable_segments`], which already
/// enforces the invariant this function relies on but does not itself
/// re-check: never mix a truncate-bearing segment with any other (a
/// truncate is drained alone — see that function's own doc comment on the
/// barrier). `seg_seqs` need not be claimable in full — a segment every one
/// of whose buckets a peer already holds simply contributes nothing and is
/// dropped before Phase 2 runs (mirroring [`drain_once`]'s `filter.is_empty()`
/// short-circuit, just per-segment instead of for the one segment it has).
///
/// Returns `Ok(None)` if this call's claims won nothing at all across every
/// segment in `seg_seqs` (every bucket of every one of them was already
/// claimed by a peer). Otherwise returns the winning attempt's
/// [`ManyApplyOutcome`], covering only the segments this call actually
/// claimed at least one bucket from — never a segment it claimed nothing
/// on, which [`apply_and_mark_drained_many`]'s completion step would
/// otherwise misreport as [`ApplyError::ClaimLost`].
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
pub async fn drain_many(
    pool: &Pool,
    seg_seqs: &[i64],
    claimed_by: &str,
    live_workers: i64,
    wake_channel: &str,
    watermark: &StagedWatermark,
) -> Result<Option<ManyApplyOutcome>, ApplyError> {
    if seg_seqs.is_empty() {
        return Ok(None);
    }

    let (mut folded, owned_segments) = {
        let mut client = pool.get().await?;
        let txn = client.transaction().await?;
        let mut per_segment: Vec<Vec<FoldedChange>> = Vec::with_capacity(seg_seqs.len());
        let mut owned: Vec<i64> = Vec::with_capacity(seg_seqs.len());
        for &seg_seq in seg_seqs {
            claim::claim(&*txn, seg_seq, claimed_by, live_workers).await?;
            let filter = claim::owned_bucket_filter(&*txn, seg_seq, claimed_by).await?;
            if filter.is_empty() {
                continue;
            }
            owned.push(seg_seq);
            per_segment.push(fold::fold(&txn, seg_seq, filter).await?);
        }
        txn.commit().await?;
        if owned.is_empty() {
            return Ok(None);
        }
        (fold::merge_folded_changes(per_segment), owned)
    };

    // Every retry-classification helper below (`classify_and_retry`,
    // `isolate_and_evict`) takes one representative `seg_seq` purely as
    // audit/probe bookkeeping (which batch's contribution a parked poison
    // row names; which real claim a rollback-only probe transaction's
    // completion step exercises) — never as something correctness depends
    // on picking exactly right among several equally-valid coalesced
    // segments. The lowest of this call's owned segments is as good a
    // representative as any; see `apply_and_mark_drained_many`'s doc
    // comment on the same choice for `poisoned_park`.
    let representative_seg_seq = owned_segments[0];

    let mut backoff = FenceMissBackoff::new();
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        tracing::Span::current().record("attempt", attempt);
        let plan = match compute(pool, &folded).await {
            Ok(plan) => plan,
            Err(ApplyError::SourceTableDropped { source_table }) => {
                tracing::warn!(
                    source_table = %source_table,
                    "source table no longer exists; purging its staged rows and retrying \
                     without it"
                );
                quarantine::purge_dropped_table(pool, &source_table).await?;
                folded.retain(|c| c.src_table != source_table);
                attempt -= 1;
                continue;
            }
            Err(err) => {
                if let Some(retry_folded) = classify_and_retry(
                    pool,
                    representative_seg_seq,
                    claimed_by,
                    wake_channel,
                    &folded,
                    attempt,
                    &mut backoff,
                    err,
                )
                .await?
                {
                    folded = retry_folded;
                }
                continue;
            }
        };

        let mut client = pool.get().await?;
        let txn = client.transaction().await?;
        match apply_and_mark_drained_many(
            &txn,
            &owned_segments,
            claimed_by,
            &plan,
            wake_channel,
            watermark,
        )
        .await
        {
            Ok(outcome) => {
                txn.commit().await?;
                // Epic #49 cross-cutting review fix (issues #51/#52): see
                // `drain_once`'s matching call — flush only now that this
                // attempt's (possibly multi-segment) transaction has
                // actually committed.
                flush_apply_metrics(&plan);
                // Issue #134/#135 review follow-up: see `drain_once`'s
                // matching call.
                flush_relationship_reverse_deferral_metrics(&outcome.deferral_counts);
                // Issue #135: same post-commit-only contract, for the
                // fairness-escalation count this attempt's own Phase 3 pass
                // discovered.
                flush_relationship_reverse_fairness_escalation_metric(outcome.fairness_escalations);
                backoff.reset();
                return Ok(Some(outcome));
            }
            Err(err) => {
                let _ = txn.rollback().await;
                if let Some(retry_folded) = classify_and_retry(
                    pool,
                    representative_seg_seq,
                    claimed_by,
                    wake_channel,
                    &folded,
                    attempt,
                    &mut backoff,
                    err,
                )
                .await?
                {
                    folded = retry_folded;
                }
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
/// Returns `Ok(Some(retry_folded))` if isolation evicted at least one key —
/// the caller must retry with `folded` replaced by `retry_folded`.
/// `Ok(None)` means "retry with `folded` unchanged" (a version fence
/// miss, a transient failure, or an isolate attempt that evicted nothing).
/// `Err(_)` propagates `err` (or a probe's own halting error) unmodified,
/// once retries are exhausted or the failure must never be retried at all.
#[allow(clippy::too_many_arguments)]
async fn classify_and_retry(
    pool: &Pool,
    seg_seq: i64,
    claimed_by: &str,
    wake_channel: &str,
    folded: &[FoldedChange],
    attempt: u32,
    backoff: &mut FenceMissBackoff,
    err: ApplyError,
) -> Result<Option<Vec<FoldedChange>>, ApplyError> {
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
            Ok(None)
        }
        // Transient (lock contention, serialization failure, dropped
        // connection, statement timeout): retry, charge nothing, no backoff
        // — `FenceMissBackoff`'s escalating schedule is reserved for
        // consecutive fence misses specifically (doc 06).
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
            tracing::debug!(seg_seq, attempt, error = %err, "transient apply failure; retrying");
            Ok(None)
        }
        // Halting schema diagnosis: never quarantine, propagate loudly
        // after recording the stop metric.
        quarantine::FailureClass::Halting => {
            tracing::error!(
                seg_seq,
                error = %err,
                "halting failure classification; never quarantined, propagating loudly"
            );
            quarantine::record_halting_stop(pool, &err.to_string()).await?;
            Err(err)
        }
        // Everything else: isolate each folded record alone to attribute
        // the failure to specific key(s), evicting any past the death
        // threshold and retrying without them. If nothing reproduces alone,
        // the error is surfaced, not blamed.
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
            match quarantine::isolate_and_evict(
                pool,
                seg_seq,
                claimed_by,
                wake_channel,
                folded,
                quarantine::DEFAULT_DEATH_THRESHOLD,
            )
            .await?
            {
                Some(retry_folded) => {
                    tracing::warn!(
                        seg_seq,
                        remaining = retry_folded.len(),
                        "isolated and evicted at least one poisoned key; retrying without it"
                    );
                    Ok(Some(retry_folded))
                }
                None => {
                    tracing::debug!(
                        seg_seq,
                        error = %err,
                        "isolation reproduced nothing; surfacing the original failure"
                    );
                    Err(err)
                }
            }
        }
    }
}

/// The next batch a free worker should pick up: the lowest-`seg_seq`
/// segment that is `'sealed'` or `'draining'` and not yet fully drained
/// (`drained_mask` short of `(1 << bucket_count) - 1`). Ordered ascending
/// so batches drain roughly in creation order, though nothing here enforces
/// that strictly — a worker could still be mid-drain on an earlier segment
/// while this returns a later one.
///
/// The one exception is the truncate barrier (issue #60): a truncate is
/// whole-keyspace, but drains are per-bucket, parallel, and — per the
/// paragraph above — explicitly *not* ordered, so a truncate is a
/// two-directional drain barrier. Predecessors must drain first (else an
/// earlier batch's insert would apply after the truncate and wrongly
/// survive); successors must not drain first (else a later batch's
/// post-truncate insert would be wiped when the truncate's clear runs). Let
/// `B` be the lowest `seg_seq` among undrained truncate-bearing segments
/// (`segments.has_truncate`, set at seal time — see `seal::seal_phase1`);
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

/// [`next_claimable_segment`] generalized to return up to `max_batch`
/// claimable segments at once (issue #63 Milestone 2), for [`drain_many`] to
/// coalesce — the batch a burst of quickly-sealing segments needs so each
/// one doesn't pay its own full compute-and-apply pass.
///
/// Runs the exact same barrier-respecting query [`next_claimable_segment`]
/// does (see its doc comment for the truncate barrier `B`), just without
/// `next_claimable_segment`'s `limit 1`. The only additional rule this adds
/// is the one [`drain_many`]'s doc comment calls out as its caller-side
/// invariant: **a truncate-bearing segment is never coalesced with another
/// segment.** Because `B` is by definition the *lowest* seg_seq among
/// undrained truncate-bearing segments and this query never returns
/// anything past `B`, the only truncate-bearing segment that can ever
/// appear in the result set is `B` itself, and — being the barrier's own
/// upper bound — it is always the *last* (highest-`seg_seq`) row, never the
/// first. So: walk the ascending rows, taking ordinary (non-truncate)
/// segments into the batch; the moment a truncate-bearing row is reached,
/// stop — returning it alone if the batch collected so far is otherwise
/// empty (it's the lowest claimable segment, so it must be handed out on
/// its own), or returning what's already been collected without it
/// otherwise (it'll be handed out alone on some future call, once nothing
/// ordinary remains ahead of it).
pub async fn next_claimable_segments(
    client: &impl GenericClient,
    max_batch: usize,
) -> Result<Vec<i64>, ApplyError> {
    if max_batch == 0 {
        return Ok(Vec::new());
    }
    let limit = max_batch as i64;
    let rows = client
        .query(
            "select seg_seq, has_truncate from segments \
             where state in ('sealed', 'draining') \
               and drained_mask <> ((1::bigint << bucket_count) - 1) \
               and seg_seq <= coalesce( \
                   (select min(seg_seq) from segments \
                    where has_truncate \
                      and drained_mask <> ((1::bigint << bucket_count) - 1)), \
                   seg_seq \
               ) \
             order by seg_seq asc \
             limit $1",
            &[&limit],
        )
        .await?;

    let mut batch = Vec::with_capacity(rows.len());
    for row in rows {
        let seg_seq: i64 = row.get(0);
        let has_truncate: bool = row.get(1);
        if has_truncate {
            if batch.is_empty() {
                batch.push(seg_seq);
            }
            break;
        }
        batch.push(seg_seq);
    }
    Ok(batch)
}
