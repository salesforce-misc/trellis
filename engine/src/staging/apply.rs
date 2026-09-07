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
//! up next" query a real drain loop (not assembled here — see doc 04's
//! [`super::liveness::claim_unless_paused`]) would call before it.

use std::collections::HashMap;
use std::fmt;

use tokio_postgres::types::ToSql;
use tokio_postgres::{GenericClient, Transaction};

use crate::defs::ast::{KeySpace, ValueType};
use crate::defs::catalog::{self, CatalogError};
use crate::defs::ddl::{self, DdlError, PrimaryKeyColumn};
use crate::defs::eval::{self, EvalError, Row};
use crate::defs::validate::{self, ValidationError};
use crate::pool::{Pool, quote_ident};

use super::append::{self, StagedChange};
use super::apply_aggregate::{self, AggregateTargetPlan};
use super::claim;
use super::error::StagingError;
use super::fold::{self, FoldedChange};
use super::liveness::FenceMissBackoff;
use super::quarantine;

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
    SourceTableDropped {
        source_table: String,
        source_relation_oid: Option<u32>,
    },
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
            ApplyError::SourceTableDropped { source_table, .. } => write!(
                f,
                "source table '{source_table}' no longer exists; purging its staged rows"
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
            ApplyError::Db(err) => Some(err),
            ApplyError::Pool(err) => Some(err),
            ApplyError::ClaimLost
            | ApplyError::VersionFenceMiss { .. }
            | ApplyError::HopBoundExceeded { .. }
            | ApplyError::SourceTableDropped { .. } => None,
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
// Source identity and qualification
// ---------------------------------------------------------------------

/// The legacy catalog lookup key for a folded record's `src_table`: everything
/// after the last `.`, if any.
///
/// Definitions are stored — and their versions keyed — by the *unqualified*
/// table name a `TRANSFORM ... FROM <table>` clause names (see
/// `defs::parser`'s grammar; `defs/mod.rs`'s own doctest parses `FROM
/// orders` to `def.source == "orders"`) — never schema-qualified. CDC
/// intake's own producer, though, always stages changes under the
/// qualified `"schema.table"` shape `intake::publication::qualify` builds,
/// which [`FoldedChange::src_table`] inherits directly from the ring. This
/// is the one seam that reconciles the two conventions for legacy rows. OID
/// bearing records instead use [`SourceKey::Oid`] throughout. A
/// target table's own downstream `src_table` (the `Recompute` rows this
/// module stages) is already unqualified —
/// [`crate::defs::ddl::neighbor_table_name`] never adds a schema — so this
/// is a no-op there.
fn catalog_source_key(src_table: &str) -> &str {
    match src_table.rsplit_once('.') {
        Some((_, table)) => table,
        None => src_table,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum SourceKey {
    Oid(u32),
    Legacy(String),
}

fn source_oid(key: &SourceKey) -> Option<u32> {
    match key {
        SourceKey::Oid(oid) => Some(*oid),
        SourceKey::Legacy(_) => None,
    }
}

fn source_key(change: &FoldedChange) -> SourceKey {
    match change.source_relation_oid {
        Some(oid) => SourceKey::Oid(oid),
        None => SourceKey::Legacy(catalog_source_key(&change.src_table).to_string()),
    }
}

fn qualified_source_ident(schema: &str, name: &str) -> String {
    // Quote components independently: quoting `schema.name` as one
    // identifier would target a literal relation name containing a dot.
    format!("{}.{}", quote_ident(schema), quote_ident(name))
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

/// Projects the physical names in a current source image back onto the
/// immutable logical names in one definition's DSL. This must be per
/// definition: different definitions may have been created on opposite sides
/// of a column rename while sharing the same staged source image.
fn remap_bound_row(row: &Row, current_names: &HashMap<String, String>) -> Row {
    let mut remapped = row.clone();
    for (logical_name, current_name) in current_names {
        if let Some(value) = row.get(current_name) {
            remapped.insert(logical_name.clone(), value.clone());
        }
    }
    remapped
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
async fn read_live_rows_batch(
    pool: &Pool,
    source_ident: &str,
    pk: &PrimaryKeyColumn,
    keys: &[&str],
) -> Result<HashMap<String, Row>, ApplyError> {
    if keys.is_empty() {
        return Ok(HashMap::new());
    }
    let client = pool.get().await?;
    let pk_ident = quote_ident(&pk.name);
    let sql = format!(
        "select m.k, e.key, e.value \
         from (select {pk_ident}::text as k, to_jsonb(t.*) as doc from {} t \
               where {pk_ident} = any($1::text[]::{}[])) m \
         cross join lateral jsonb_each_text(m.doc) e",
        source_ident, pk.data_type,
    );
    let db_rows = client.query(&sql, &[&keys]).await?;
    let mut rows: HashMap<String, Row> = HashMap::new();
    for db_row in db_rows {
        let key: String = db_row.get(0);
        let field: String = db_row.get(1);
        let value: Option<String> = db_row.get(2);
        rows.entry(key).or_default().insert(field, value);
    }
    Ok(rows)
}

// ---------------------------------------------------------------------
// Phase 2: compute
// ---------------------------------------------------------------------

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
#[derive(Debug, Clone)]
struct TargetWrite {
    pk_text: String,
    values: Vec<Option<String>>,
    hop_gen: i32,
}

/// One key's deletion from a target table (the folded change had no
/// `new_image`).
#[derive(Debug, Clone)]
struct TargetDelete {
    pk_text: String,
    hop_gen: i32,
}

/// Everything Phase 3 needs to write one target table: its primary key
/// shape (for the pre-lock/upsert/delete SQL), the calculated-field column
/// names and their inferred [`ValueType`]s (both aligned with every
/// [`TargetWrite::values`], so [`apply_target`] knows which Postgres type
/// each column casts to), and the writes and deletes this batch computed
/// for it.
#[derive(Debug, Clone)]
struct TargetPlan {
    target_ident: String,
    pk: PrimaryKeyColumn,
    field_names: Vec<String>,
    field_types: Vec<ValueType>,
    writes: Vec<TargetWrite>,
    deletes: Vec<TargetDelete>,
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
#[derive(Debug, Clone)]
struct ClearPlan {
    target_ident: String,
    pk: PrimaryKeyColumn,
    hop_gen: i32,
}

async fn target_ident_for_definition(
    pool: &Pool,
    def: &crate::defs::Definition,
) -> Result<String, ApplyError> {
    let Some(oid) = def.target_relation_oid else {
        return Ok(quote_ident(&def.def.target));
    };
    let relation = catalog::source_relation_by_oid(pool, oid)
        .await?
        .ok_or_else(|| CatalogError::TargetRelationNotFound {
            target: def.def.target.clone(),
        })?;
    Ok(qualified_source_ident(&relation.schema, &relation.name))
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
    versions: HashMap<SourceKey, VersionFence>,
    targets: HashMap<String, TargetPlan>,
    /// [`KeySpace::Aggregate`] targets' per-group deltas (issue #11's
    /// aggregate extension) — the same role [`ApplyPlan::targets`] plays for
    /// [`KeySpace::OneToOne`], kept as a separate map since the two key
    /// spaces' Phase 3 write shapes (`apply_target`'s ordered pre-lock CTE
    /// vs. `apply_aggregate::apply_aggregate_target`'s sequential per-group
    /// upserts) are different enough not to share one plan type.
    aggregate_targets: HashMap<String, AggregateTargetPlan>,
    target_idents: HashMap<String, String>,
    downstream_readers: HashMap<String, bool>,
    downstream_source_oids: HashMap<String, Option<u32>>,
    /// Targets to clear in full at Phase 3, keyed by target table name —
    /// issue #60's truncate propagation. See [`ClearPlan`].
    clears: HashMap<String, ClearPlan>,
    /// The aggregate-target counterpart to [`ApplyPlan::clears`]: a
    /// truncate on an aggregate definition's source clears every group, but
    /// (unlike a 1-1 target's single-column primary key) there is no single
    /// column shape to `RETURNING`-project a physically-changed group key
    /// out of generically, and no downstream reader can consume an
    /// aggregate target's composite key as a 1-1 source today regardless —
    /// so this is applied as a plain `DELETE FROM <target>` (every group
    /// atomically gone), counted toward [`ApplyOutcome::keys_deleted`], but
    /// *not* staged for downstream propagation. A documented gap, not an
    /// oversight: closing it needs composite-key downstream propagation,
    /// out of scope for this issue (see `staging::apply_aggregate`'s module
    /// doc comment for the rest of what this issue does cover).
    ///
    /// Investigated (issue #11 review): could a definition actually be
    /// *created* reading from an aggregate target today, making this skip a
    /// live correctness gap rather than a moot one? Yes — `defs::validate`/
    /// `create_definition` impose no primary-key-shape check at
    /// definition-creation time, so nothing stops such a definition from
    /// being saved. But `compute()`'s Phase 2 unconditionally calls
    /// `ddl::source_primary_key` for every distinct source table a batch's
    /// folded changes touch, *before* any per-definition dispatch — so the
    /// very first drain attempt against that source fails loudly with
    /// `DdlError::CompositePrimaryKeyUnsupported` (surfaced as
    /// [`ApplyError::Ddl`]), before the encoded composite group-key text
    /// could ever be misread as a single-column key. The ordinary
    /// aggregate-write path below (the "3b" step) stages downstream
    /// Recompute rows keyed the same encoded way for exactly the same
    /// reason: both paths are consistent in outcome (fail loud, never
    /// silently misuse the key) regardless of which one a batch takes, so
    /// this skip is not a live gap today.
    aggregate_clears: HashMap<String, i32>,
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
    applied_keys: Vec<(String, Option<u32>, String)>,
}

/// The version Phase 2 observed plus a current, user-facing source name. The
/// catalog key may be an OID, but a fence miss should still identify a table.
#[derive(Debug, Clone)]
struct VersionFence {
    version: Option<i64>,
    source_table: String,
}

/// Phase 2 (design doc: "no transaction, no locks"): evaluates every
/// folded change's `f()` against the transform currently reading its
/// source table, grouped by OID when present (and unqualified `src_table`
/// only for legacy rows) so each source's
/// catalog version is loaded — and fenced against — exactly once.
///
/// Reloads the catalog fresh on every call, including retries: this is
/// what makes [`drain_once`]'s retry-on-fence-miss loop "reload, recompute"
/// rather than needing any separate invalidation path.
pub async fn compute(pool: &Pool, folded: &[FoldedChange]) -> Result<ApplyPlan, ApplyError> {
    // Issue #16: exclude already-poisoned keys before anything else touches
    // them — the fold excludes a poisoned key globally, not just from this
    // one batch's evaluation. Truncate sentinels are never candidates: a
    // truncate is whole-keyspace, not a key quarantine can attribute
    // anything to.
    let candidates: Vec<&FoldedChange> = folded.iter().filter(|c| !c.is_truncate).collect();
    let poisoned = quarantine::poisoned_keys_among(pool, &candidates).await?;

    let mut by_source: HashMap<SourceKey, Vec<&FoldedChange>> = HashMap::new();
    // Truncate sentinels (issue #60) never enter the keyed by-source
    // evaluation loop below — they carry no key of their own (see
    // `append::TRUNCATE_SENTINEL_KEY`) and produce no write/delete;
    // they're handled separately, right after that loop.
    let mut truncated: Vec<&FoldedChange> = Vec::new();
    let mut poisoned_park: Vec<FoldedChange> = Vec::new();
    let mut applied_keys: Vec<(String, Option<u32>, String)> = Vec::new();
    for change in folded {
        if change.is_truncate {
            truncated.push(change);
            continue;
        }
        if poisoned.contains(&(
            change.src_table.clone(),
            change.source_relation_oid,
            change.key.clone(),
        )) {
            poisoned_park.push(change.clone());
            continue;
        }
        applied_keys.push((
            change.src_table.clone(),
            change.source_relation_oid,
            change.key.clone(),
        ));
        by_source
            .entry(source_key(change))
            .or_default()
            .push(change);
    }

    let mut versions: HashMap<SourceKey, VersionFence> = HashMap::new();
    let mut targets: HashMap<String, TargetPlan> = HashMap::new();
    let mut aggregate_targets: HashMap<String, AggregateTargetPlan> = HashMap::new();
    let mut target_idents: HashMap<String, String> = HashMap::new();

    for (source_key, changes) in by_source {
        let (version, source_table, source_ident, defs) = match &source_key {
            SourceKey::Oid(oid) => {
                let source = catalog::source_relation_by_oid(pool, *oid)
                    .await?
                    .ok_or_else(|| ApplyError::SourceTableDropped {
                        source_table: changes[0].src_table.clone(),
                        source_relation_oid: Some(*oid),
                    })?;
                (
                    catalog::source_table_version_by_oid(pool, *oid).await?,
                    source.qualified(),
                    qualified_source_ident(&source.schema, &source.name),
                    catalog::transforms_for_source_oid(pool, *oid).await?,
                )
            }
            SourceKey::Legacy(name) => (
                catalog::source_table_version(pool, name).await?,
                changes[0].src_table.clone(),
                quote_ident(name),
                catalog::transforms_for_source(pool, name).await?,
            ),
        };
        versions.insert(
            source_key.clone(),
            VersionFence {
                version,
                source_table,
            },
        );

        // `source_key` alone determines the source table's primary key, not
        // the individual definition (issue #69) — introspected once per
        // source here and reused both below (every definition subscribed to
        // this source) and by the row decode below (every change, whichever
        // definition it's evaluated against). A live `42P01` here means
        // `source_key` no longer exists (issue #16's dropped-table purge,
        // not an ordinary DDL error) — see [`ApplyError::SourceTableDropped`].
        let pk = match ddl::source_primary_key(pool, &source_ident).await {
            Ok(pk) => pk,
            Err(DdlError::Db(db_err)) if quarantine::is_undefined_table(&db_err) => {
                return Err(ApplyError::SourceTableDropped {
                    source_table: changes[0].src_table.clone(),
                    source_relation_oid: source_oid(&source_key),
                });
            }
            Err(DdlError::NoPrimaryKey { source_table })
                if quarantine::source_table_missing(pool, &source_table).await? =>
            {
                return Err(ApplyError::SourceTableDropped {
                    source_table: changes[0].src_table.clone(),
                    source_relation_oid: source_oid(&source_key),
                });
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
            let mut live_rows = read_live_rows_batch(pool, &source_ident, &pk, &live_keys).await?;
            for &i in &live_refetch_indices {
                rows[i] = live_rows.remove(changes[i].key.as_str());
            }
        }

        // Aggregate definitions need each change's *old*-side row too (to
        // derive a grain-migrating change's old group key and its old
        // contribution — see `apply_aggregate`'s doc comment), decoded once
        // here and shared across every aggregate definition on this source,
        // same as `rows` above. Only decoded when this source actually has
        // an aggregate reader, to avoid the extra round trips for the
        // (overwhelmingly common) 1-1-only source.
        let needs_old_rows = defs
            .iter()
            .any(|def| matches!(def.def.key_space, KeySpace::Aggregate { .. }));
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

        for def in &defs {
            let current_names = catalog::source_column_current_names(pool, def).await?;
            let target_ident = target_ident_for_definition(pool, def).await?;
            target_idents.insert(def.def.target.clone(), target_ident.clone());
            let KeySpace::Aggregate { group_by } = &def.def.key_space else {
                let field_names: Vec<String> =
                    def.def.fields.iter().map(|f| f.name.clone()).collect();
                let inferred_types = validate::infer_field_types(&def.def, &def.source_columns)?;
                let field_types: Vec<ValueType> = field_names
                    .iter()
                    .map(|name| {
                        inferred_types
                            .get(name)
                            .copied()
                            .unwrap_or(ValueType::Numeric)
                    })
                    .collect();
                let plan = targets
                    .entry(def.def.target.clone())
                    .or_insert_with(|| TargetPlan {
                        target_ident: target_ident.clone(),
                        pk: pk.clone(),
                        field_names: field_names.clone(),
                        field_types: field_types.clone(),
                        writes: Vec::new(),
                        deletes: Vec::new(),
                    });

                // Reused across every change below (issue #68): `regexp_count`'s
                // pattern is a validated string literal, so its compiled `Regex`
                // is the same for every row this definition evaluates, and
                // recompiling it per row would be wasted work at realistic row
                // volumes.
                let mut regex_cache = eval::RegexCache::new();
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
                            let row = remap_bound_row(row, &current_names);
                            let mut evaluated = eval::evaluate(
                                &def.def,
                                &row,
                                &def.source_columns,
                                &mut regex_cache,
                            )?;
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
                            });
                        }
                        None => {
                            plan.deletes.push(TargetDelete {
                                pk_text: change.key.clone(),
                                hop_gen: change.hop_gen,
                            });
                        }
                    }
                }
                continue;
            };

            // Aggregate dispatch (issue #11): fold this source's changes
            // into per-group deltas on `def.def.target`'s aggregate plan,
            // via `apply_aggregate` rather than duplicating its logic here.
            let group_by_types: Vec<ValueType> = group_by
                .iter()
                .map(|c| {
                    def.source_columns
                        .get(c)
                        .copied()
                        .unwrap_or(ValueType::Numeric)
                })
                .collect();
            let field_plans =
                apply_aggregate::classify_fields(&def.def, group_by, &def.source_columns)?;
            let field_exprs: HashMap<String, crate::defs::ast::Expr> = def
                .def
                .fields
                .iter()
                .filter(|f| !group_by.contains(&f.name))
                .map(|f| (f.name.clone(), f.expr.clone()))
                .collect();
            let target_plan = aggregate_targets
                .entry(def.def.target.clone())
                .or_insert_with(|| {
                    AggregateTargetPlan::new(
                        group_by.clone(),
                        group_by_types,
                        field_plans,
                        source_ident.clone(),
                        field_exprs,
                    )
                });

            let mut regex_cache = eval::RegexCache::new();
            let remapped_rows: Vec<Option<Row>> = rows
                .iter()
                .map(|row| row.as_ref().map(|row| remap_bound_row(row, &current_names)))
                .collect();
            let remapped_old_rows: Vec<Option<Row>> = old_rows
                .iter()
                .map(|row| row.as_ref().map(|row| remap_bound_row(row, &current_names)))
                .collect();
            apply_aggregate::accumulate_changes(
                target_plan,
                &def.def,
                &changes,
                &remapped_rows,
                &remapped_old_rows,
                &def.source_columns,
                &mut regex_cache,
            )?;
        }
    }

    // Truncate clears (issue #60): for each truncated src_table, resolve its
    // targets via the catalog and record a full clear for each — the same
    // "resolve targets from the catalog" step the by-source loop above runs
    // per key, just once per truncated source instead of once per key.
    let mut clears: HashMap<String, ClearPlan> = HashMap::new();
    let mut aggregate_clears: HashMap<String, i32> = HashMap::new();
    for change in &truncated {
        let source_key = source_key(change);
        // Fence this source too, even though nothing evaluated against it —
        // a definition change against a truncated source, landing mid-drain,
        // must trip Phase 3's version fence exactly like it would for a
        // source this batch actually evaluated `f()` against.
        let (version, source_table, source_ident, defs) = match &source_key {
            SourceKey::Oid(oid) => {
                let source = catalog::source_relation_by_oid(pool, *oid)
                    .await?
                    .ok_or_else(|| ApplyError::SourceTableDropped {
                        source_table: change.src_table.clone(),
                        source_relation_oid: Some(*oid),
                    })?;
                (
                    catalog::source_table_version_by_oid(pool, *oid).await?,
                    source.qualified(),
                    qualified_source_ident(&source.schema, &source.name),
                    catalog::transforms_for_source_oid(pool, *oid).await?,
                )
            }
            SourceKey::Legacy(name) => (
                catalog::source_table_version(pool, name).await?,
                change.src_table.clone(),
                quote_ident(name),
                catalog::transforms_for_source(pool, name).await?,
            ),
        };
        versions.entry(source_key.clone()).or_insert(VersionFence {
            version,
            source_table,
        });

        let pk = match ddl::source_primary_key(pool, &source_ident).await {
            Ok(pk) => pk,
            Err(DdlError::Db(db_err)) if quarantine::is_undefined_table(&db_err) => {
                return Err(ApplyError::SourceTableDropped {
                    source_table: change.src_table.clone(),
                    source_relation_oid: source_oid(&source_key),
                });
            }
            Err(DdlError::NoPrimaryKey { source_table })
                if quarantine::source_table_missing(pool, &source_table).await? =>
            {
                return Err(ApplyError::SourceTableDropped {
                    source_table: change.src_table.clone(),
                    source_relation_oid: source_oid(&source_key),
                });
            }
            Err(err) => return Err(err.into()),
        };
        for def in &defs {
            let target_ident = target_ident_for_definition(pool, def).await?;
            target_idents.insert(def.def.target.clone(), target_ident.clone());
            match &def.def.key_space {
                KeySpace::Aggregate { .. } => {
                    aggregate_clears
                        .entry(def.def.target.clone())
                        .and_modify(|hop_gen| *hop_gen = (*hop_gen).max(change.hop_gen))
                        .or_insert(change.hop_gen);
                }
                KeySpace::OneToOne => {
                    clears
                        .entry(def.def.target.clone())
                        .and_modify(|existing| {
                            existing.hop_gen = existing.hop_gen.max(change.hop_gen)
                        })
                        .or_insert(ClearPlan {
                            target_ident,
                            pk: pk.clone(),
                            hop_gen: change.hop_gen,
                        });
                }
            }
        }
    }

    let mut downstream_readers = HashMap::new();
    let mut downstream_source_oids = HashMap::new();
    let mut all_targets: std::collections::HashSet<&String> = targets.keys().collect();
    all_targets.extend(clears.keys());
    all_targets.extend(aggregate_targets.keys());
    all_targets.extend(aggregate_clears.keys());
    for target in all_targets {
        let source_oid = catalog::target_relation_oid(pool, target).await?;
        let has_downstream = match source_oid {
            Some(oid) => !catalog::transforms_for_source_oid(pool, oid)
                .await?
                .is_empty(),
            None => !catalog::transforms_for_source(pool, target)
                .await?
                .is_empty(),
        };
        downstream_readers.insert(target.clone(), has_downstream);
        downstream_source_oids.insert(target.clone(), source_oid);
    }

    Ok(ApplyPlan {
        versions,
        targets,
        aggregate_targets,
        target_idents,
        downstream_readers,
        downstream_source_oids,
        clears,
        aggregate_clears,
        poisoned_park,
        applied_keys,
    })
}

// ---------------------------------------------------------------------
// Phase 3: apply ∪ mark-drained
// ---------------------------------------------------------------------

/// Runs one target table's ordered pre-lock, no-op-suppressed upsert, and
/// delete as a single statement, returning the keys Postgres actually wrote
/// to vs. deleted (as opposed to every key this batch merely *proposed* —
/// the no-op-suppression `WHERE ... IS DISTINCT FROM ...` guard can mean a
/// proposed write physically changes nothing).
///
/// The `locked` CTE takes every key this call touches (write or delete)
/// `FOR UPDATE`, ordered ascending — the deadlock-avoidance convention doc
/// 05 calls for between concurrent workers writing overlapping target rows
/// — and is referenced from both `upserted`/`deleted` via a non-correlated
/// `(select count(*) from locked) >= 0` guard, mirroring `claim.rs`'s
/// `flip_guard` exactly: an unreferenced data-modifying CTE is silently
/// planned away by Postgres, so both branches must force `locked` to run
/// even when their own row set is empty.
async fn apply_target(
    txn: &Transaction<'_>,
    target: &str,
    plan: &TargetPlan,
) -> Result<(Vec<String>, Vec<String>), ApplyError> {
    if plan.writes.is_empty() && plan.deletes.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let pk_ident = quote_ident(&plan.pk.name);
    let pk_cast = plan.pk.data_type.as_str();
    let target_ident = target;
    let field_idents: Vec<String> = plan.field_names.iter().map(|n| quote_ident(n)).collect();

    let mut lock_keys: Vec<&str> = plan
        .writes
        .iter()
        .map(|w| w.pk_text.as_str())
        .chain(plan.deletes.iter().map(|d| d.pk_text.as_str()))
        .collect();
    lock_keys.sort_unstable();
    lock_keys.dedup();

    let mut sql = format!(
        "with locked as ( \
             select {pk_ident} from {target_ident} \
             where {pk_ident} = any($1::text[]::{pk_cast}[]) \
             order by {pk_ident} for update \
         )"
    );

    let mut params: Vec<&(dyn ToSql + Sync)> = vec![&lock_keys];
    let mut next_param = 2usize;

    // Owned text renderings of every write row's values, kept alive for the
    // whole function so `params` can borrow into them.
    let write_pk_texts: Vec<&str> = plan.writes.iter().map(|w| w.pk_text.as_str()).collect();
    let write_field_texts: Vec<Vec<Option<String>>> =
        plan.writes.iter().map(|w| w.values.clone()).collect();

    let field_pg_types: Vec<&str> = plan
        .field_types
        .iter()
        .map(|t| match t {
            ValueType::Numeric => "numeric",
            ValueType::Text => "text",
            ValueType::Boolean => "boolean",
            ValueType::Uuid => "uuid",
        })
        .collect();

    let col_list = std::iter::once(pk_ident.clone())
        .chain(field_idents.iter().cloned())
        .collect::<Vec<_>>()
        .join(", ");

    let has_writes = !plan.writes.is_empty();
    if has_writes {
        let cols_per_row = 1 + plan.field_names.len();
        let mut rows_sql = Vec::with_capacity(plan.writes.len());
        for i in 0..plan.writes.len() {
            let base = next_param + i * cols_per_row;
            let mut row_parts = vec![format!("${base}::text::{pk_cast}")];
            for (j, pg_type) in field_pg_types.iter().enumerate() {
                row_parts.push(format!("${}::text::{pg_type}", base + 1 + j));
            }
            rows_sql.push(format!("({})", row_parts.join(", ")));
        }
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

        sql.push_str(&format!(
            ", upserted as ( \
                 insert into {target_ident} ({col_list}) \
                 select * from (values {}) as v({col_list}) \
                 where (select count(*) from locked) >= 0 \
                 on conflict ({pk_ident}) do update set {set_list} \
                 where ({target_cols}) is distinct from ({excluded_cols}) \
                 returning {pk_ident}::text as pk \
             )",
            rows_sql.join(", "),
        ));

        for (pk_text, field_texts) in write_pk_texts.iter().zip(write_field_texts.iter()) {
            params.push(pk_text);
            for v in field_texts {
                params.push(v);
            }
        }
        next_param += plan.writes.len() * cols_per_row;
    }

    let delete_keys: Vec<&str> = plan.deletes.iter().map(|d| d.pk_text.as_str()).collect();
    let has_deletes = !plan.deletes.is_empty();
    if has_deletes {
        sql.push_str(&format!(
            ", deleted as ( \
                 delete from {target_ident} \
                 where {pk_ident} = any(${next_param}::text[]::{pk_cast}[]) \
                   and (select count(*) from locked) >= 0 \
                 returning {pk_ident}::text as pk \
             )"
        ));
        params.push(&delete_keys);
    }

    let mut selects = Vec::new();
    if has_writes {
        selects.push("select 'w' as kind, pk from upserted".to_string());
    }
    if has_deletes {
        selects.push("select 'd' as kind, pk from deleted".to_string());
    }
    sql.push(' ');
    sql.push_str(&selects.join(" union all "));

    let rows = txn.query(&sql, &params).await?;
    let mut written = Vec::new();
    let mut deleted = Vec::new();
    for row in rows {
        let kind: &str = row.get(0);
        let pk: String = row.get(1);
        if kind == "w" {
            written.push(pk);
        } else {
            deleted.push(pk);
        }
    }
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
///    [`apply_target`].
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
pub async fn apply_and_mark_drained(
    txn: &Transaction<'_>,
    seg_seq: i64,
    claimed_by: &str,
    plan: &ApplyPlan,
    wake_channel: &str,
) -> Result<ApplyOutcome, ApplyError> {
    // 1. Version fence.
    for (source_key, loaded) in &plan.versions {
        let (sql, param): (&str, &(dyn ToSql + Sync)) = match source_key {
            SourceKey::Oid(oid) => (
                "select version from source_table_versions \
                 where source_relation_oid = $1::oid for share",
                oid,
            ),
            SourceKey::Legacy(name) => (
                "select version from source_table_versions where source_table = $1 for share",
                name,
            ),
        };
        let row = txn.query_opt(sql, &[param]).await?;
        let current: Option<i64> = row.map(|r| r.get(0));
        if current != loaded.version {
            return Err(ApplyError::VersionFenceMiss {
                src_table: loaded.source_table.clone(),
            });
        }
    }

    // 1b. Issue #16: park this batch's own folded contribution for every
    // already-poisoned key it's excluding, before the drained mark below —
    // "parked work is the source of truth" means every excluding batch must
    // do this itself, in the same transaction, not just the batch that
    // caused the eviction. See `quarantine::park_batch_contribution`'s doc
    // comment for why this runs unconditionally rather than only on the
    // batch that tripped the threshold.
    quarantine::park_batch_contribution(txn, seg_seq, &plan.poisoned_park).await?;

    let mut keys_written = 0usize;
    let mut keys_deleted = 0usize;
    // `changed` accumulates rather than overwrites per target (`extend`,
    // not `insert`): a target can appear in both `plan.clears` and
    // `plan.targets` in the same batch — a truncate clear followed by a
    // same-batch post-truncate write to the same target — and both halves'
    // physically-touched keys must propagate downstream.
    let mut changed: HashMap<&str, Vec<(String, i32)>> = HashMap::new();

    // 2. Truncate clears, before this target's own upsert/delete below —
    // see this function's doc comment on why "clear, then write" is safe
    // here specifically (single-bucket batch, barrier-drained).
    for (target, clear) in &plan.clears {
        let pk_ident = quote_ident(&clear.pk.name);
        let target_ident = &clear.target_ident;
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
        let touched: Vec<(String, i32)> = cleared.into_iter().map(|k| (k, clear.hop_gen)).collect();
        changed.entry(target.as_str()).or_default().extend(touched);
    }

    // 2b. Aggregate truncate clears — see [`ApplyPlan::aggregate_clears`]'s
    // doc comment on why these are a plain full-table delete with no
    // downstream propagation, unlike every other clear/write/delete this
    // function tracks via `changed`.
    for target in plan.aggregate_clears.keys() {
        let target_ident = plan
            .target_idents
            .get(target)
            .map(String::as_str)
            .unwrap_or(target);
        let cleared = txn
            .execute(&format!("delete from {target_ident}"), &[])
            .await?;
        keys_deleted += cleared as usize;
    }

    // 3. Ordered pre-lock + upsert/delete, per target table.
    for (target, target_plan) in &plan.targets {
        let (written, deleted) = apply_target(txn, &target_plan.target_ident, target_plan).await?;
        keys_written += written.len();
        keys_deleted += deleted.len();

        if written.is_empty() && deleted.is_empty() {
            continue;
        }

        let mut hop_gen_of: HashMap<&str, i32> = HashMap::new();
        for w in &target_plan.writes {
            hop_gen_of.insert(w.pk_text.as_str(), w.hop_gen);
        }
        for d in &target_plan.deletes {
            hop_gen_of.insert(d.pk_text.as_str(), d.hop_gen);
        }

        let touched: Vec<(String, i32)> = written
            .into_iter()
            .chain(deleted)
            .map(|key| {
                let hop_gen = hop_gen_of.get(key.as_str()).copied().unwrap_or(0);
                (key, hop_gen)
            })
            .collect();
        changed.entry(target.as_str()).or_default().extend(touched);
    }

    // 3b. Aggregate targets: same ordered-write step as 3, above, for
    // [`KeySpace::Aggregate`] definitions — see `apply_aggregate`'s doc
    // comment for the per-group delta/probe logic itself. Written/deleted
    // groups fold into the same `changed` accounting as the 1-1 case, so
    // downstream propagation below needs no branching of its own. This
    // stages Recompute rows keyed by the encoded composite group key, same
    // as any 1-1 target — see [`ApplyPlan::aggregate_clears`]'s doc comment
    // for why that is not a live misuse risk today: no definition reading
    // from an aggregate target can actually survive its first drain attempt.
    for (target, agg_plan) in &plan.aggregate_targets {
        let target_ident = plan
            .target_idents
            .get(target)
            .map(String::as_str)
            .unwrap_or(target);
        let result = apply_aggregate::apply_aggregate_target(txn, target_ident, agg_plan).await?;
        keys_written += result.written.len();
        keys_deleted += result.deleted.len();

        if result.written.is_empty() && result.deleted.is_empty() {
            continue;
        }
        changed
            .entry(target.as_str())
            .or_default()
            .extend(result.written.into_iter().chain(result.deleted));
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
        for (key, hop_gen) in touched {
            let next_hop = hop_gen + 1;
            if next_hop > MAX_HOP_GEN {
                hop_bound_tables.push(target.to_string());
                worst_hop_gen = worst_hop_gen.max(next_hop);
                continue;
            }
            recompute_changes.push(StagedChange::Recompute {
                src_table: target.to_string(),
                source_relation_oid: plan.downstream_source_oids.get(*target).copied().flatten(),
                key: key.clone(),
                hop_gen: next_hop,
                group_key: None,
            });
        }
    }

    if !hop_bound_tables.is_empty() {
        hop_bound_tables.sort();
        hop_bound_tables.dedup();
        return Err(ApplyError::HopBoundExceeded {
            hop_gen: worst_hop_gen,
            tables: hop_bound_tables,
        });
    }

    append::append(txn, &recompute_changes).await?;

    // 4b. Issue #16: a clean drain clears the death counters for every key
    // it just applied (not the poisoned ones it parked above) — doc 06's
    // "clean drain clears counters for keys it applied."
    quarantine::clear_key_deaths(txn, &plan.applied_keys).await?;

    // 5. Completion: release this claim and mark its buckets drained, in
    // one statement.
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

    // 6. Wake anything awaiting convergence.
    txn.execute("select pg_notify($1, '')", &[&wake_channel])
        .await?;

    Ok(ApplyOutcome {
        keys_written,
        keys_deleted,
        batch_drained,
    })
}

/// What one successful [`apply_and_mark_drained`] call did: how many target
/// rows it physically wrote/deleted (no-op-suppressed writes excluded), and
/// whether this call's completion flipped the segment to `'drained'`
/// (`false` if other buckets are still outstanding).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApplyOutcome {
    pub keys_written: usize,
    pub keys_deleted: usize,
    pub batch_drained: bool,
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
pub async fn drain_once(
    pool: &Pool,
    seg_seq: i64,
    claimed_by: &str,
    live_workers: i64,
    wake_channel: &str,
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
        let plan = match compute(pool, &folded).await {
            Ok(plan) => plan,
            Err(ApplyError::SourceTableDropped {
                source_table,
                source_relation_oid,
            }) => {
                // Issue #16's one sanctioned exception to immutability: the
                // table this batch's folded rows name is gone, not any one
                // row's fault, so no retry or per-key quarantine resolves
                // it. Purge every ring/quarantine row naming it and retry
                // with it excluded. Not counted against
                // `MAX_APPLY_ATTEMPTS` — this corrects `folded` itself
                // rather than retrying the same input.
                quarantine::purge_dropped_table(pool, &source_table, source_relation_oid).await?;
                folded.retain(|change| match source_relation_oid {
                    Some(oid) => change.source_relation_oid != Some(oid),
                    None => change.src_table != source_table,
                });
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
        match apply_and_mark_drained(&txn, seg_seq, claimed_by, &plan, wake_channel).await {
            Ok(outcome) => {
                txn.commit().await?;
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

/// Classifies `err` (per [`quarantine::classify`]) and either retries or
/// propagates, shared by both [`drain_once`]'s Phase 2 and Phase 3 failure
/// arms so a bad key is attributed identically regardless of which phase
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
                return Err(err);
            }
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
                return Err(err);
            }
            Ok(None)
        }
        // Halting schema diagnosis: never quarantine, propagate loudly
        // after recording the stop metric.
        quarantine::FailureClass::Halting => {
            quarantine::record_halting_stop(pool, &err.to_string()).await?;
            Err(err)
        }
        // Everything else: isolate each folded record alone to attribute
        // the failure to specific key(s), evicting any past the death
        // threshold and retrying without them. If nothing reproduces alone,
        // the error is surfaced, not blamed.
        quarantine::FailureClass::Isolate => {
            if attempt >= MAX_APPLY_ATTEMPTS {
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
                Some(retry_folded) => Ok(Some(retry_folded)),
                None => Err(err),
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
pub async fn next_claimable_segment(
    client: &impl GenericClient,
) -> Result<Option<i64>, ApplyError> {
    let row = client
        .query_opt(
            "select seg_seq from segments \
             where state in ('sealed', 'draining') \
               and drained_mask <> ((1::bigint << bucket_count) - 1) \
               and seg_seq <= coalesce( \
                   (select min(seg_seq) from segments \
                    where has_truncate \
                      and drained_mask <> ((1::bigint << bucket_count) - 1)), \
                   seg_seq \
               ) \
             order by seg_seq asc \
             limit 1",
            &[],
        )
        .await?;
    Ok(row.map(|r| r.get(0)))
}
