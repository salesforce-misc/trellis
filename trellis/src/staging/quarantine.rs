//! Quarantine (issue #16, stage 06's other half): isolate, evict, park,
//! release. See docs/staging-and-claiming/06-cleanup-and-reclaim.md's
//! "Quarantine" section (the design this module implements) and
//! docs/decisions/0003-quarantine-storage-and-api.md (storage shape,
//! settled, including its "Retry policy" section). [`DEFAULT_DEATH_THRESHOLD`]
//! is this module's answer for the fuse threshold; issue #862 tracks making it
//! configurable.
//!
//! **Failure classification** ([`classify`]) is the entry point [`super::apply::drain_once`]
//! consults on every Phase-3 failure — see
//! docs/staging-and-claiming/05-apply-and-exactly-once-deltas.md's
//! "Failure classification" table, which this module's [`FailureClass`]
//! mirrors one-for-one except for the "ordering artefact" row: see
//! [`classify`]'s doc comment for why that row folds into [`FailureClass::Transient`]
//! here rather than getting its own variant.
//!
//! **Isolation and eviction** ([`isolate_and_evict`]) is what turns "a
//! non-transient failure happened" into "this specific key is the cause, in
//! this definition's apply" — never the other way around. Whole-key poison
//! is per transform (#799): an evicted key is left out of the apply of the
//! definition it fails in, and every other definition reading it keeps
//! applying it. **Parking** ([`park_batch_contribution`]) is what keeps a
//! poisoned key's *later* changes held for that definition while it's
//! excluded; it is called from [`super::apply::compute`]/
//! [`super::apply::apply_and_mark_drained`], not from this module's own
//! callers, because it must run inside the same Phase-3 transaction as the
//! rest of the batch's apply. **Release** ([`release_key`]) is the
//! operator-driven undo.
//!
//! **The one sanctioned exception to immutability** ([`purge_dropped_table`])
//! lives here too: a staged row naming a table Postgres no longer has can
//! never apply, and unlike every other failure class, retrying or
//! quarantining it individually cannot help — the fix is schema-shaped, not
//! key-shaped.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::ops::Range;
use std::time::SystemTime;

use tokio_postgres::types::PgLsn;
use tokio_postgres::{GenericClient, Transaction};

use crate::defs::ast::{KeySpace, TransformDef};
use crate::defs::catalog;
use crate::defs::ddl::{self, DdlError};
use crate::defs::eval::AliasReaders;
use crate::defs::model::TransformStatus;
use crate::locks::{ColumnPauseLock, ColumnPauseOp};
use crate::pool::Pool;

use super::append::{self, RING_SIZE, StagedChange, ring_table_name};
use super::apply::{self, ApplyError, DirectFocus};
use super::fold::FoldedChange;
use super::watermark::StagedWatermark;

/// The fuse threshold ADR-0003 left open, decided here: a key evicts once
/// [`record_key_death`] returns a count at or past this many. `0` disables
/// eviction entirely (see [`isolate_and_evict`]) — an operator who has set
/// this to `0` has chosen to let a poisoning key wedge the instance (doc 06,
/// condition 4: an undrainable batch below the retirement boundary wedges
/// every candidate), rather than have this module evict silently. Not yet
/// wired to any per-instance or per-transform setting (`TrellisOptions` has
/// none for it), so every call site uses this constant directly; issue #862
/// tracks making it configurable.
pub const DEFAULT_DEATH_THRESHOLD: i32 = 5;

/// The column-fuse's threshold (`docs/decisions/0003-quarantine-storage-and-api.md`'s
/// amendment, "Undecided" -> now decided): a sibling of
/// [`DEFAULT_DEATH_THRESHOLD`], same value, named separately because the two
/// fuses count different things and are meant to stay independently
/// tunable if either is ever wired to real config — [`DEFAULT_DEATH_THRESHOLD`]
/// counts *repeated attempts against one key*; this counts *distinct
/// poisoned rows for one `(transform, column)` pair* (see
/// `column_failures`' migration comment). Fixed count, not a percentage —
/// same reasoning [`DEFAULT_DEATH_THRESHOLD`] documents, not yet configurable
/// per transform/column.
pub const DEFAULT_COLUMN_DEATH_THRESHOLD: i32 = DEFAULT_DEATH_THRESHOLD;

/// The whole-transform fuse's threshold (`docs/decisions/0003-quarantine-storage-and-api.md`'s
/// "The original transform-wide fuse still exists as a coarser, separate
/// tier"): another sibling of [`DEFAULT_DEATH_THRESHOLD`] and
/// [`DEFAULT_COLUMN_DEATH_THRESHOLD`], same value, named separately for the
/// same independent-tunability reason those two document. This one counts
/// *distinct keys poisoned for one definition* (#799) — see
/// `quarantine_if_crossed` — rather than repeated attempts
/// against one key or distinct poisoned rows for one `(transform, column)`
/// pair.
pub const DEFAULT_TRANSFORM_DEATH_THRESHOLD: i32 = DEFAULT_DEATH_THRESHOLD;

// ---------------------------------------------------------------------
// Failure classification
// ---------------------------------------------------------------------

/// Which of doc 05's failure-classification rows an [`ApplyError`] falls
/// into, decided by [`classify`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    /// Retry; charge nothing to any key.
    Transient,
    /// Reload the schema and retry; back off on consecutive misses only.
    VersionFenceMiss,
    /// Never quarantine. The drain pauses every definition the failure
    /// reaches, and the rest of the batch commits (`staging::halt`, #663).
    Halting,
    /// Everything else: isolate before blaming (see [`isolate_and_evict`]).
    Isolate,
}

/// Classifies `err` per doc 05's failure-classification table.
///
/// **Design decision on "ordering artefact"** (doc 05: "a delta guard
/// tripped while a lower-numbered batch is still outstanding" — self-heals,
/// charge only once every predecessor has drained): no concrete mechanism in
/// this codebase produces that shape today. [`super::apply::apply_target`]'s
/// ordered pre-lock takes every lock this batch needs inside one statement,
/// so it cannot itself observe "a predecessor is still outstanding" as a
/// distinct error — the only failure a lock conflict *can* surface here is a
/// plain serialization failure or deadlock, already [`FailureClass::Transient`].
/// Rather than invent a fifth class with no real trigger, this row folds
/// into [`FailureClass::Transient`]: both share the exact treatment doc 05
/// specifies for ordering artefacts ("charge nothing"), and both are, by
/// construction, retried by [`super::apply::drain_once`]'s reload-recompute
/// loop — which is itself what "once every predecessor has drained" reduces
/// to when there is no separate signal to wait on.
///
/// **The innermost [`ApplyError`] decides (issue #670).** An `ApplyError`
/// can nest another on its [`std::error::Error::source`] chain, as
/// `ApplyError::Backfill(BackfillError::Propagation(Box<ApplyError>))` does
/// when a backfill's downstream propagation fails. The wrapper says only
/// where the failure surfaced; the innermost says what it is, so a
/// structural `Ddl(NoPrimaryKey)` is [`FailureClass::Halting`] whether it
/// arrives bare or wrapped.
pub fn classify(err: &ApplyError) -> FailureClass {
    match innermost_apply_error(err) {
        ApplyError::VersionFenceMiss { .. } => FailureClass::VersionFenceMiss,
        ApplyError::HopBoundExceeded { .. } | ApplyError::AggregateOffLedger { .. } => {
            FailureClass::Halting
        }
        // A truncate the drain can't raise a floor for (#774) is no key's
        // fault: every page holding it reproduces it.
        ApplyError::TruncateWithoutLsn { .. } => FailureClass::Halting,
        // A spilled accumulator staged in memory (#926) is a programming
        // error, no key's fault either. It names no table, so the halt
        // pauses nothing: the page retries once and surfaces it as a drain
        // holdup, charging no key, where isolating it could evict one.
        ApplyError::SpilledMutationsNotFlushed => FailureClass::Halting,
        // Both of these mean "this definition can never work against this
        // source's real schema" — a structural, schema-shape diagnosis
        // exactly like the hop bound, not a per-row data problem. By the
        // time either reaches here, `compute()` has already ruled out "the
        // table is simply gone" (`drain_once` special-cases
        // `ApplyError::SourceTableDropped` before classification ever runs)
        // — what's left is a real primary key shape (no primary key at all)
        // or type `ddl::source_primary_key` cannot use (issue #107), which
        // every key touching that source reproduces identically alone.
        // Isolating it would charge, and eventually evict, every such key
        // one at a time for a failure none of them individually caused. A
        // composite (multi-column) primary key used to be a third such
        // structural rejection here too, before issue #121 taught every 1-1
        // consumer to key on the source's full primary key rather than
        // narrowing it to one column.
        ApplyError::Ddl(DdlError::NoPrimaryKey { .. })
        | ApplyError::Ddl(DdlError::UnsupportedPrimaryKeyType { .. }) => FailureClass::Halting,
        // Issue #766: Postgres refused the drain's role a read or write, for
        // row-level security (every Trellis session runs with `row_security
        // = off`, so a statement the policies would filter raises instead)
        // or for a missing privilege. Either says something about the role
        // and a table, never about a row: every key on that table reproduces
        // it, so isolating would charge, and eventually evict, each of them
        // for nothing. Halt the definitions it reaches instead
        // (`staging::halt`).
        _ if is_insufficient_privilege(err) => FailureClass::Halting,
        _ if is_transient(err) => FailureClass::Transient,
        _ => FailureClass::Isolate,
    }
}

/// The last [`ApplyError`] on `err`'s [`std::error::Error::source`] chain,
/// `err` itself if it nests none. See [`classify`].
pub(super) fn innermost_apply_error(err: &ApplyError) -> &ApplyError {
    let mut innermost = err;
    let mut link = std::error::Error::source(err);
    while let Some(err) = link {
        if let Some(apply) = err.downcast_ref::<ApplyError>() {
            innermost = apply;
        }
        link = err.source();
    }
    innermost
}

/// Whether `err` is a lost claim, bare or wrapped in another [`ApplyError`]:
/// decided by the innermost one, as [`classify`] decides the class. A lost
/// claim classifies [`FailureClass::Isolate`], so both callers that must
/// never isolate one (`apply::classify_and_retry` and [`isolate_and_evict`]'s
/// probe loop) check this first. If they matched only a bare `ClaimLost`, a
/// wrapped one would be isolated and every key in the page charged a death
/// for it. (Nothing in the drain wraps a `ClaimLost` today.)
pub(super) fn is_claim_lost(err: &ApplyError) -> bool {
    matches!(innermost_apply_error(err), ApplyError::ClaimLost)
}

/// Whether `err` is a transient Postgres or pool failure, whichever
/// [`ApplyError`] variant wraps it (issue #653). The drain reaches the same
/// deadlock or dropped connection through `ApplyError::Db`, through a nested
/// module's error (`StagingError::Db` from `append::append`,
/// `CatalogError::Db`, `DdlError::Db`, ...), and through a pool checkout
/// (`ApplyError::Pool`), so this walks the [`std::error::Error::source`]
/// chain rather than matching one variant. The first
/// [`tokio_postgres::Error`] on the chain decides: by its SQLSTATE
/// ([`is_transient_sqlstate`]), or as a lost connection however it was
/// reported. A dropped connection reaches the caller either with no SQLSTATE
/// (the socket closed first) or as the server's own `FATAL` (`08xxx`,
/// `57P01` from `pg_terminate_backend`, `57P02`, `57P03`, `57P05`), whichever
/// arrives first, and [`crate::error_code::classify_pg_error`] already maps
/// both to [`crate::error_code::ErrorCode::Connectivity`]. A pool timeout
/// (waiting for a free connection, creating one, or recycling one) is
/// transient too, since it is load or a briefly unreachable server, not
/// anything a record did. So is [`ApplyError::LedgerEntryCollected`], a
/// ledger entry lock that lost a key to the tombstone GC (#712), whose
/// retry finds the key gone and inserts it afresh.
fn is_transient(err: &ApplyError) -> bool {
    is_transient_error(err)
}

/// [`is_transient`] over any error's [`std::error::Error::source`] chain, for
/// a caller whose error isn't an [`ApplyError`]: a backfill chunk's
/// (`defs::chunk_queue::fail_chunk`, #616) is classified by the same rule.
pub(crate) fn is_transient_error(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut link: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(err) = link {
        if let Some(ApplyError::LedgerEntryCollected { .. }) = err.downcast_ref::<ApplyError>() {
            return true;
        }
        if let Some(pg) = err.downcast_ref::<tokio_postgres::Error>() {
            return is_transient_sqlstate(pg.code())
                || crate::error_code::classify_pg_error(pg)
                    == crate::error_code::ErrorCode::Connectivity;
        }
        if let Some(deadpool_postgres::PoolError::Timeout(_)) =
            err.downcast_ref::<deadpool_postgres::PoolError>()
        {
            return true;
        }
        link = err.source();
    }
    false
}

/// Whether the first [`tokio_postgres::Error`] on `err`'s
/// [`std::error::Error::source`] chain is `42501` (`insufficient_privilege`):
/// row-level security that applies to a session running with `row_security
/// = off` ("query would be affected by row-level security policy for table
/// ..."), or a plain "permission denied". See [`classify`].
pub(crate) fn is_insufficient_privilege(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut link: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(err) = link {
        if let Some(pg) = err.downcast_ref::<tokio_postgres::Error>() {
            return pg.code() == Some(&tokio_postgres::error::SqlState::INSUFFICIENT_PRIVILEGE);
        }
        link = err.source();
    }
    false
}

/// The SQLSTATE of the first [`tokio_postgres::Error`] on `err`'s
/// [`std::error::Error::source`] chain, if it has one: what `poison.sqlstate`
/// records for a held key (#824), so [`release_retyped_keys`] can tell a
/// value that didn't fit a column (`22001`, `22003`) from any other failure.
pub(crate) fn sqlstate_of(err: &(dyn std::error::Error + 'static)) -> Option<String> {
    let mut link: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(err) = link {
        if let Some(pg) = err.downcast_ref::<tokio_postgres::Error>() {
            return pg.code().map(|code| code.code().to_string());
        }
        link = err.source();
    }
    None
}

/// The transient SQLSTATEs doc 05 names (`40001`/`40P01`, lock-not-available,
/// statement timeout) plus a dropped connection — [`tokio_postgres::Error::code`]
/// is `None` for a connection-level failure (never reached the server to get
/// a SQLSTATE at all), which is exactly the "dropped connection" case doc 05
/// lists alongside the coded ones.
///
/// Plus `53300` (too many connections, issue #670): the server refused a new
/// connection because `max_connections`, or a role's or database's
/// connection limit, is full. It arrives on a pool checkout
/// (`PoolError::Backend`) or an unpooled page session's connect, and says
/// only that the server is busy, nothing about any record.
fn is_transient_sqlstate(code: Option<&tokio_postgres::error::SqlState>) -> bool {
    use tokio_postgres::error::SqlState;
    match code {
        Some(code) => {
            *code == SqlState::T_R_SERIALIZATION_FAILURE
                || *code == SqlState::T_R_DEADLOCK_DETECTED
                || *code == SqlState::LOCK_NOT_AVAILABLE
                || *code == SqlState::QUERY_CANCELED
                || *code == SqlState::TOO_MANY_CONNECTIONS
        }
        None => true,
    }
}

/// Whether `err` is Postgres's "the table named in this query doesn't
/// exist" (`42P01`) — the live-truth signal [`super::apply::compute`] uses
/// to raise [`ApplyError::SourceTableDropped`] instead of a generic
/// database error, so [`super::apply::drain_once`] can route it to
/// [`purge_dropped_table`] rather than the ordinary isolate/evict path.
pub(super) fn is_undefined_table(err: &tokio_postgres::Error) -> bool {
    err.code() == Some(&tokio_postgres::error::SqlState::UNDEFINED_TABLE)
}

/// Whether `source_table` no longer exists, checked directly via
/// `to_regclass` — the live-truth signal [`super::apply::compute`] uses
/// alongside [`is_undefined_table`]. `pg_catalog.to_regclass` returns `NULL`
/// for a name that resolves to nothing rather than raising `42P01`, which is
/// exactly what makes `ddl::source_primary_key`'s own query come back with
/// zero rows (and thus `DdlError::NoPrimaryKey`) for a dropped table — the
/// same shape a real table that genuinely lacks a primary key produces. This
/// is what tells the two apart before either is trusted.
pub(super) async fn source_table_missing(
    pool: &Pool,
    source_table: &str,
) -> Result<bool, ApplyError> {
    let client = pool.get().await?;
    let row = client
        .query_one(
            "select pg_catalog.to_regclass($1) is null",
            &[&ddl::regclass_arg(source_table)],
        )
        .await?;
    Ok(row.get(0))
}

// ---------------------------------------------------------------------
// The poison marker
// ---------------------------------------------------------------------

/// Which definitions each of `candidates` (a folded batch's non-truncate
/// `(src_table, key)` pairs) is poisoned for, per the `poison` marker table:
/// the read [`super::apply::compute`] runs before evaluating anything. Whole-key
/// poison is per transform (#799): a poisoned key is left out of the apply of
/// each definition it's poisoned for, and every other reader applies it as
/// usual. A pair poisoned for no definition is absent from the map.
///
/// **`candidates` must already carry the canonical `src_table` identity**
/// (issue #283) — `poison` is keyed on it, so a raw bare spelling matches
/// nothing here and the key it names is re-evaluated (and re-poisoned) despite
/// already being evicted. [`super::apply::compute`], the only caller, resolves
/// each distinct source table through [`CanonicalSrcTables`] before building
/// this list, and looks the returned map up with the same canonical pairs.
pub(super) async fn poisoned_keys_among(
    pool: &Pool,
    candidates: &[(&str, &str)],
) -> Result<HashMap<(String, String), HashSet<i64>>, ApplyError> {
    let mut poisoned: HashMap<(String, String), HashSet<i64>> = HashMap::new();
    if candidates.is_empty() {
        return Ok(poisoned);
    }
    let client = pool.get().await?;
    let src_tables: Vec<&str> = candidates.iter().map(|(t, _)| *t).collect();
    let keys: Vec<&str> = candidates.iter().map(|(_, k)| *k).collect();
    let rows = client
        .query(
            "select distinct p.src_table, p.key, p.transform_id from poison p \
             join unnest($1::text[], $2::text[]) as u(src_table, key) \
               on p.src_table = u.src_table and p.key = u.key",
            &[&src_tables, &keys],
        )
        .await?;
    for row in rows {
        poisoned
            .entry((row.get(0), row.get(1)))
            .or_default()
            .insert(row.get(2));
    }
    Ok(poisoned)
}

/// What a definition's parked changes for one held key tell [`release_key`]:
/// the fields of one `poison_held` row it stages the key's `Recompute` from
/// (#803). A definition holds one row per `(src_table, key)`, and each park
/// merges into it ([`HeldKey::merge`]) rather than adding a row per segment,
/// so a key held for good costs one row however often it changes.
///
/// The park's SQL ([`MERGE_HELD_ROW`]) merges the same way, field for field.
/// The row also carries two fields this leaves out: the greatest parked
/// `lsn`, which only the release's to-one projection rewrite reads (#754),
/// and the join values the parked changes' raw new images and pre-images
/// held (`join_values`), which that rewrite reads and
/// [`HeldKey::release`] stages beside the `Recompute` (#944).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HeldKey {
    /// The spelling the earliest parked change was held under, which the
    /// `Recompute` is staged under.
    pub(crate) src_table: String,
    /// The segment of the earliest parked change.
    pub(crate) seg_seq: i64,
    /// The earliest parked change's pre-image (a recompute's prior-image
    /// hint): the state readers last saw.
    pub(crate) prior_image: Option<String>,
    /// The earliest origin, or `None` (unknown) once any parked change's is
    /// unknown ([`super::fold::earliest_origin`]).
    pub(crate) origin_lsn: Option<PgLsn>,
    /// The earliest known source-change time. `Some` exactly when some parked
    /// change was a source change, so it also decides the `Recompute`'s
    /// `hop_gen`.
    pub(crate) src_changed: Option<SystemTime>,
    /// The deepest parked `hop_gen`.
    pub(crate) hop_gen: i32,
    /// The union of the parked `group_key`s, sorted and deduplicated, as
    /// the fold's own cross-segment merge keeps it.
    pub(crate) group_key: Vec<String>,
}

impl HeldKey {
    /// One parked change, before any merge.
    pub(crate) fn parked(
        src_table: &str,
        seg_seq: i64,
        prior_image: Option<String>,
        origin_lsn: Option<PgLsn>,
        src_changed: Option<SystemTime>,
        hop_gen: i32,
        group_key: Option<Vec<String>>,
    ) -> Self {
        let mut group_key = group_key.unwrap_or_default();
        group_key.sort_unstable();
        group_key.dedup();
        HeldKey {
            src_table: src_table.to_string(),
            seg_seq,
            prior_image,
            origin_lsn,
            src_changed,
            hop_gen,
            group_key,
        }
    }

    /// Merges a later-parked change (`other`) into this held row: the earlier
    /// segment's image and spelling, the earliest origin (unknown if either
    /// is) and source-change time, the deepest `hop_gen`, the union of
    /// `group_key`s. On a segment tie the row already held wins, as the
    /// park's `on conflict` does. Every other field is order-free, so a key's
    /// row is the same whatever order its parks commit in.
    pub(crate) fn merge(self, other: HeldKey) -> HeldKey {
        let (first, second) = if other.seg_seq < self.seg_seq {
            (other, self)
        } else {
            (self, other)
        };
        let mut group_key = first.group_key;
        group_key.extend(second.group_key);
        group_key.sort_unstable();
        group_key.dedup();
        HeldKey {
            src_table: first.src_table,
            seg_seq: first.seg_seq,
            prior_image: first.prior_image,
            origin_lsn: super::fold::earliest_origin(first.origin_lsn, second.origin_lsn),
            src_changed: super::apply::earliest_src_changed(first.src_changed, second.src_changed),
            hop_gen: first.hop_gen.max(second.hop_gen),
            group_key,
        }
    }

    /// The image-less `Recompute` a release stages for `key` from this row:
    /// see [`release_key`]'s doc comment for what each field carries.
    pub(crate) fn recompute(self, key: &str) -> StagedChange {
        let hop_gen = self.recompute_hop_gen();
        StagedChange::Recompute {
            src_table: self.src_table,
            key: key.to_string(),
            hop_gen,
            group_key: (!self.group_key.is_empty()).then_some(self.group_key),
            src_changed: self.src_changed,
            prior_image: self.prior_image,
            origin_lsn: self.origin_lsn,
        }
    }

    /// Everything a release stages for `key` from this row: its
    /// [`HeldKey::recompute`], then one [`StagedChange::ReleasedJoinValue`]
    /// per element of the row's `join_values` (#944), with the
    /// `Recompute`'s provenance, so they fold into it.
    ///
    /// The `Recompute`'s reverse path re-derives the from-side rows of its
    /// prior image's join values and its live row's. Batches drain out of
    /// order, so a parked change can have held a value between the two that
    /// a batch drained unheld wrote into from-side rows (the projection
    /// rewrite, `apply::release_to_one_projections`, fixes the to-one
    /// projection row, but not those rows). The join values name it, and
    /// the fold's `to_col_values` hands it to the reverse path, which
    /// re-derives those rows on a later page under that page's fences: the
    /// release takes no lock for them.
    pub(crate) fn release(self, key: &str, join_values: BTreeSet<String>) -> Vec<StagedChange> {
        let carried = join_values
            .into_iter()
            .map(|image| StagedChange::ReleasedJoinValue {
                src_table: self.src_table.clone(),
                key: key.to_string(),
                image,
                hop_gen: self.recompute_hop_gen(),
                src_changed: self.src_changed,
                origin_lsn: self.origin_lsn,
            })
            .collect::<Vec<_>>();
        let mut staged = vec![self.recompute(key)];
        staged.extend(carried);
        staged
    }

    /// The deepest parked `hop_gen`, or 0 if any parked change was a source
    /// change.
    fn recompute_hop_gen(&self) -> i32 {
        if self.src_changed.is_some() {
            0
        } else {
            self.hop_gen
        }
    }
}

/// The `on conflict` clause every park into `poison_held` ends with: merges
/// the parked change (`excluded`) into the key's held row (`h`), as
/// [`HeldKey::merge`] does, plus the two fields it leaves out: the union of
/// `join_values` and the greatest `lsn`. Every right-hand side reads `h` as it was before the
/// update, so `old_image` compares the old `seg_seq`.
const MERGE_HELD_ROW: &str = "on conflict (transform_id, src_table, key) do update set \
         seg_seq = least(h.seg_seq, excluded.seg_seq), \
         old_image = case when excluded.seg_seq < h.seg_seq \
                          then excluded.old_image else h.old_image end, \
         origin_lsn = case when h.origin_lsn is null or excluded.origin_lsn is null \
                           then null else least(h.origin_lsn, excluded.origin_lsn) end, \
         src_changed = least(h.src_changed, excluded.src_changed), \
         hop_gen = greatest(h.hop_gen, excluded.hop_gen), \
         group_key = case when h.group_key is null and excluded.group_key is null then null \
                          else array(select distinct g collate \"C\" \
                                     from unnest(coalesce(h.group_key, '{}') \
                                                 || coalesce(excluded.group_key, '{}')) g \
                                     order by 1) end, \
         join_values = array(select distinct j \
                             from unnest(h.join_values || excluded.join_values) j), \
         lsn = greatest(h.lsn, excluded.lsn)";

/// Parks `changes` — a batch's own folded contribution for keys already
/// poisoned for a definition, each paired with that definition's id — into
/// `poison_held`. A definition holds one row per key (#803): the first park
/// inserts it, and every later one merges into it ([`MERGE_HELD_ROW`],
/// [`HeldKey::merge`]), keeping what [`release_key`] stages the key's
/// `Recompute` from. Must run **inside the same Phase-3 transaction**
/// as the rest of the batch's apply, before the drained mark — see the
/// module doc comment's "Parked work... is the source of truth" and doc 06's
/// matching section: this is what keeps a held key's band blocked for as
/// long as the definition holds it.
///
/// **Lock order (ADR-0002 I5).** The definitions' rows are locked first,
/// in id order, then each held row in `(transform_id, src_table, key)`
/// order, so two pages parking overlapping keys queue on their first shared
/// row rather than deadlock. Two pages parking the same key contend for its
/// one row, and the second merges into what the first committed.
///
/// **Parks only while the definition still holds the key.** `changes` were
/// chosen from `poison` when the page was computed, outside this transaction.
/// A resume (rule 5 of #799) or a release since then deleted the key's
/// `poison` and `poison_held` rows, and a row parked after that would be held
/// for a key nothing holds: no release would ever name it, and it would block
/// every watermark token from then on (`converge`'s condition 4). The resume
/// or release re-derives the key from its live row, so the skipped change
/// is covered. The definitions' rows are locked `for key share` first, the
/// lock the insert's foreign key takes anyway: a resume holds its definition's
/// row `for update` while it deletes, so a page that parks for it either
/// parks before the resume deletes (and the resume deletes its rows too) or
/// waits for the resume to commit and then finds the key no longer held. A
/// release ([`release_key`]) bumps the version fence of the key's table
/// first, which waits for this page to commit, since the page holds that
/// fence `for share` until then; the release then reads the parked row and
/// releases it too. A page that read the fence before the release and
/// reaches it after misses it and computes again.
pub(super) async fn park_batch_contribution(
    txn: &Transaction<'_>,
    seg_seq: i64,
    changes: &[(i64, FoldedChange)],
) -> Result<(), ApplyError> {
    if changes.is_empty() {
        return Ok(());
    }
    let mut ids: Vec<i64> = changes.iter().map(|(id, _)| *id).collect();
    ids.sort_unstable();
    ids.dedup();
    txn.execute(
        "select 1 from transform_definitions where id = any($1) order by id for key share",
        &[&ids],
    )
    .await?;
    let mut ordered: Vec<&(i64, FoldedChange)> = changes.iter().collect();
    ordered.sort_by(|(a_id, a), (b_id, b)| {
        (a_id, &a.src_table, &a.key).cmp(&(b_id, &b.src_table, &b.key))
    });
    let statement = format!(
        "insert into poison_held as h \
             (transform_id, src_table, key, seg_seq, old_image, origin_lsn, src_changed, \
              hop_gen, group_key, join_values, lsn) \
         select $1::bigint, $2::text, $3::text, $4::bigint, $5::text::jsonb, $6::pg_lsn, \
                $7::timestamptz, $8::integer, $9::text[], \
                array(select distinct j from ( \
                          select jsonb_build_object(c, v) as j \
                          from unnest($10::text[], $11::text[]) u(c, v) \
                          union all \
                          select jsonb_build_object(r.to_col, $5::text::jsonb ->> r.to_col) \
                          from relationship_definitions r \
                          where r.to_schema || '.' || r.to_table = $2 \
                            and $5::text::jsonb ->> r.to_col is not null) named), \
                $12::pg_lsn \
         where exists (select 1 from poison p \
                       where p.transform_id = $1 and p.src_table = $2 and p.key = $3) \
         {MERGE_HELD_ROW}"
    );
    for (transform_id, change) in ordered {
        // Issue #315: a recompute's prior-image hint rides in `old_image`,
        // exactly as it does in the ring, so `release_key` hands it on.
        let old_image = if change.old_image.is_none() && change.new_image.is_none() {
            &change.prior_image
        } else {
            &change.old_image
        };
        // The key's join values: every one its raw rows' new images held
        // (`to_col_values`), and its pre-image's. Batches drain out of order
        // (doc 04), so the change before this one may have been applied, not
        // parked, and only this pre-image names the value it left: the held
        // row keeps the earliest park's pre-image alone, and a release that
        // missed the value would leave its to-one projection row behind.
        let (columns, values): (Vec<&str>, Vec<&str>) = change
            .to_col_values
            .iter()
            .map(|(column, value)| (column.as_str(), value.as_str()))
            .unzip();
        txn.execute(
            &statement,
            &[
                transform_id,
                &change.src_table,
                &change.key,
                &seg_seq,
                old_image,
                &change.origin_lsn,
                &change.src_changed,
                &change.hop_gen,
                &change.group_key,
                &columns,
                &values,
                &change.lsn,
            ],
        )
        .await?;
    }
    Ok(())
}

/// Clears `key_deaths` for every `(src_table, key)` pair in `keys` — called
/// from inside a successful apply's own transaction, per doc 06: "a clean
/// drain clears the counters for the keys it just applied, so a transient
/// death does not accumulate toward a false eviction."
///
/// Per transform (#799), a definition the key is poisoned for skipped it, so
/// its counter is kept: only the definitions the key isn't poisoned for
/// applied it.
///
/// **`keys` must already carry the canonical `src_table` identity** (issue
/// #283), for the same reason [`poisoned_keys_among`]'s candidates must:
/// [`record_key_death`] writes under it, so clearing by a raw bare spelling
/// would delete nothing and leave a transient death accumulating toward a false
/// eviction forever. `ApplyPlan::applied_keys` — this function's only source of
/// `keys` — is built canonically in [`super::apply::compute`] for that reason.
pub(super) async fn clear_key_deaths(
    txn: &Transaction<'_>,
    keys: &[(String, String)],
) -> Result<(), ApplyError> {
    if keys.is_empty() {
        return Ok(());
    }
    let src_tables: Vec<&str> = keys.iter().map(|(t, _)| t.as_str()).collect();
    let ks: Vec<&str> = keys.iter().map(|(_, k)| k.as_str()).collect();
    txn.execute(
        "delete from key_deaths d \
         using unnest($1::text[], $2::text[]) as u(src_table, key) \
         where d.src_table = u.src_table and d.key = u.key \
           and not exists (select 1 from poison p \
                           where p.transform_id = d.transform_id \
                             and p.src_table = d.src_table and p.key = d.key)",
        &[&src_tables, &ks],
    )
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------
// Isolate before blaming
// ---------------------------------------------------------------------

/// Increments `key_deaths` for `transform_id`'s `(src_table, key)` and
/// returns the new count — an upsert, since the counter's row may not exist
/// yet for a key's first attributed failure. Per transform (#799): each
/// definition a key fails for counts its own deaths toward its own eviction.
///
/// `src_table` is the canonical identity, not the ring spelling (issue #283):
/// two spellings of one logical source used to maintain two independent
/// row-level counters for the same physical row, so the row-level fuse took up
/// to twice as many real failures to fire.
async fn record_key_death(
    client: &impl GenericClient,
    transform_id: i64,
    src_table: &str,
    key: &str,
    last_error: &str,
) -> Result<i32, ApplyError> {
    let row = client
        .query_one(
            "insert into key_deaths \
                 (transform_id, src_table, key, deaths, last_error, last_death_at) \
             values ($1, $2, $3, 1, $4, now()) \
             on conflict (transform_id, src_table, key) do update set \
                 deaths = key_deaths.deaths + 1, \
                 last_error = excluded.last_error, \
                 last_death_at = now() \
             returning deaths",
            &[&transform_id, &src_table, &key, &last_error],
        )
        .await?;
    Ok(row.get(0))
}

/// One key [`isolate_and_evict`]'s probe loop reproduced an
/// [`FailureClass::Isolate`] failure for, attributed to one definition
/// (#799), carrying **both** spellings of its source table (issue #283)
/// because the two halves of that function need different ones: quarantine's
/// own counter/marker tables are keyed on `canonical_src_table`, while
/// matching the key back to the ring row it came from (its parked
/// contribution) has to use `raw_src_table`, the spelling `folded` actually
/// holds.
#[derive(Clone)]
struct PoisonedProbe {
    /// The ring row's own `src_table`, verbatim.
    raw_src_table: String,
    /// [`qualified_src_table`] of the above — quarantine's canonical key.
    canonical_src_table: String,
    key: String,
    /// The definition whose apply the failure is attributed to.
    culprit: Culprit,
    /// The SQLSTATE of the record's failure alone ([`sqlstate_of`]), which
    /// the key's `poison` row keeps (#824).
    sqlstate: Option<String>,
}

/// A definition [`attribute`] found a pinned key's failure in: the one its
/// key is charged and, past the threshold, poisoned for (#799).
#[derive(Clone)]
struct Culprit {
    transform_id: i64,
    /// Its bare target, for logs.
    target: String,
    /// Its `fuse_rearmed_at` when attribution read it, before probing it. A
    /// resume stamps a new one and deletes the definition's poison, so an
    /// eviction that finds it changed would poison the rebuilt definition
    /// for a failure of the one before it, and is skipped ([`evict_for`]).
    epoch: Option<SystemTime>,
    last_error: String,
}

/// Marks `transform_id`'s `(src_table, key)` poisoned (idempotent — a
/// re-eviction after release refreshes the marker rather than erroring) and
/// parks this batch's own folded contribution for it, in one transaction —
/// the eviction act itself. `contribution` is the one [`FoldedChange`] this
/// batch folded for the key (if any); a key can in principle cross the
/// threshold in a batch that folded no record for it at all only via a
/// race with a concurrent isolate elsewhere, which is not a live path here
/// since eviction always runs against the same batch that just isolated the
/// key.
async fn evict_key(
    txn: &Transaction<'_>,
    seg_seq: i64,
    probe: &PoisonedProbe,
    contribution: Option<&FoldedChange>,
) -> Result<(), ApplyError> {
    let culprit = &probe.culprit;
    tracing::warn!(
        transform = %culprit.target,
        src_table = %probe.canonical_src_table,
        key = %probe.key,
        last_error = %culprit.last_error,
        "evicting a key to the poison table for one definition; it crossed the row-level \
         death threshold, and every other definition reading it keeps applying it"
    );
    txn.execute(
        "insert into poison (transform_id, src_table, key, last_error, sqlstate) \
         values ($1, $2, $3, $4, $5) \
         on conflict (transform_id, src_table, key) do update set \
             last_error = excluded.last_error, sqlstate = excluded.sqlstate, \
             poisoned_at = now()",
        &[
            &culprit.transform_id,
            &probe.canonical_src_table,
            &probe.key,
            &culprit.last_error,
            &probe.sqlstate,
        ],
    )
    .await?;
    if let Some(change) = contribution {
        // Issue #283: `poison_held` is keyed on the same canonical identity as
        // the `poison` row just written, so the parked contribution is restaged
        // under `src_table` rather than the ring row's own spelling. Without
        // this, a bare-staged ring row's held work would be invisible to
        // `release_key`'s canonical delete — orphaned parked work that
        // `converge` gates on forever. Replaying it later under the qualified
        // name is also what issue #267 made the ring invariant anyway.
        let mut parked = change.clone();
        parked.src_table = probe.canonical_src_table.clone();
        park_batch_contribution(txn, seg_seq, &[(culprit.transform_id, parked)]).await?;
    }
    Ok(())
}

/// A key [`isolate_and_evict`] reproduced a failure for and charged a death,
/// but that is still below the row-level death threshold — so it stays in the
/// batch, un-evicted. Carried out of [`isolate_and_evict`] so the caller can
/// name it in its log (issue #614): without it, "isolation pinned this key,
/// which is `deaths` of `threshold` from eviction" looked identical to
/// "isolation reproduced nothing".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChargedKey {
    /// The bare target of the definition the death was charged to (#799).
    pub transform: String,
    /// Canonical identity (issue #283) — the one the `key_deaths` row is under.
    pub src_table: String,
    pub key: String,
    /// The key's death count *after* this charge.
    pub deaths: i32,
}

/// What [`isolate_and_evict`] concluded about a failed batch.
#[derive(Debug)]
pub enum IsolationOutcome {
    /// `threshold == 0`: the row-level fuse is disabled (per
    /// [`DEFAULT_DEATH_THRESHOLD`]'s doc comment), so nothing was probed or
    /// charged.
    FuseDisabled,
    /// No single key reproduced an isolate-eligible failure on its own that
    /// could be attributed to a definition (#799): the failure is not
    /// attributable to a key, so nothing was charged.
    NothingReproduced,
    /// Isolation ran [`MAX_ISOLATION_PROBES`] probes without pinning the
    /// failure on any key, and stopped with parts of the batch unprobed (issue
    /// #655). Nothing was charged. Distinct from [`Self::NothingReproduced`],
    /// which means every part of the batch that could hold a failing key was
    /// probed.
    ProbeLimitReached { probes: usize },
    /// Isolation stopped after [`MAX_CONSECUTIVE_TRANSIENT_PROBES`] probes in
    /// a row hit a transient error, without pinning the failure on any key
    /// (issue #670). Nothing was charged. A lock or deadlock storm says
    /// nothing about the batch's records, and each probe in it can wait out a
    /// whole `lock_timeout`, so isolation stops rather than spending the rest
    /// of [`MAX_ISOLATION_PROBES`] on it; the page is retried on a later
    /// drain, once the storm may have passed.
    TransientStorm { probes: usize },
    /// At least one key reproduced the failure alone and was charged a death,
    /// but none reached the threshold, so nothing was evicted. Every entry in
    /// `charged` is below the threshold.
    ChargedBelowThreshold { charged: Vec<ChargedKey> },
    /// At least one key crossed the threshold and was poisoned for the
    /// definition it fails in: [`super::apply::drain_once`] recomputes the
    /// batch, which now leaves the key out of that definition's apply only
    /// (#799), and reapplies it. `evicted` counts the `(definition, key)`
    /// pairs poisoned. `charged` lists any *other* keys that reproduced the
    /// failure in the same probe pass but stayed below the threshold.
    Evicted {
        evicted: usize,
        charged: Vec<ChargedKey>,
    },
}

/// How many [`ChargedKey`]s [`describe_charged_keys`] spells out before
/// summarizing the rest as a count: a batch where every key fails would
/// otherwise put the whole batch into one log line.
const DESCRIBED_CHARGED_KEYS: usize = 5;

/// Renders `charged` for a log field, e.g.
/// `gizmo_view: public.gizmos key=2 (1/5 deaths), gizmo_view: public.gizmos key=7 (3/5 deaths)`,
/// capped at [`DESCRIBED_CHARGED_KEYS`] entries plus an "and N more" tail.
pub(crate) fn describe_charged_keys(charged: &[ChargedKey], threshold: i32) -> String {
    let mut out = charged
        .iter()
        .take(DESCRIBED_CHARGED_KEYS)
        .map(|c| {
            format!(
                "{}: {} key={} ({}/{} deaths)",
                c.transform, c.src_table, c.key, c.deaths, threshold
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    if charged.len() > DESCRIBED_CHARGED_KEYS {
        out.push_str(&format!(
            ", and {} more",
            charged.len() - DESCRIBED_CHARGED_KEYS
        ));
    }
    out
}

/// Whether `deaths` charged to `probe`'s key poison it, given the drain's
/// row-level `threshold`: the one place that decision is made. Both the
/// partition of a page's charges ([`partition_by_threshold`]) and the
/// eviction's re-check under the definition's row lock
/// ([`still_charged_to_threshold`], #880) ask it, so a key whose threshold
/// isn't the global one (#861's retried key, poisoned on its first failure)
/// changes here and nowhere else.
fn crosses_threshold(_probe: &PoisonedProbe, deaths: i32, threshold: i32) -> bool {
    deaths >= threshold
}

/// Whether `probe`'s key is still charged to its threshold, read under the
/// definition's row lock ([`evict_for`]) and so after every release that
/// committed before it (#880). A [`release_key`] deletes the key's
/// `key_deaths` row, so an eviction whose isolation charged the key before
/// the release finds the row gone, or counting only the deaths charged after
/// it, and poisons nothing: the operator's release stands. A key that fails
/// again after its release is charged from 1 again and crosses the threshold
/// again on its own deaths. A clean apply that clears the count
/// ([`clear_key_deaths`]) in the same window skips the key the same way, as
/// a cleared count means. The same canonical `src_table` as the charge and
/// the release (#283).
async fn still_charged_to_threshold(
    txn: &Transaction<'_>,
    probe: &PoisonedProbe,
    threshold: i32,
) -> Result<bool, ApplyError> {
    let deaths: Option<i32> = txn
        .query_opt(
            "select deaths from key_deaths \
             where transform_id = $1 and src_table = $2 and key = $3",
            &[
                &probe.culprit.transform_id,
                &probe.canonical_src_table,
                &probe.key,
            ],
        )
        .await?
        .map(|row| row.get(0));
    let charged = deaths.is_some_and(|deaths| crosses_threshold(probe, deaths, threshold));
    if !charged {
        tracing::debug!(
            transform = %probe.culprit.target,
            src_table = %probe.canonical_src_table,
            key = %probe.key,
            deaths,
            "not poisoning a key whose death count a release or a clean apply reset \
             after isolation charged it"
        );
    }
    Ok(charged)
}

/// Splits each charged probe (paired with its post-charge death count) into
/// "evict now" ([`crosses_threshold`]) and "charged, still below threshold".
fn partition_by_threshold(
    charged: Vec<(PoisonedProbe, i32)>,
    threshold: i32,
) -> (Vec<PoisonedProbe>, Vec<ChargedKey>) {
    let mut evict_now = Vec::new();
    let mut below = Vec::new();
    for (probe, deaths) in charged {
        if crosses_threshold(&probe, deaths, threshold) {
            evict_now.push(probe);
        } else {
            below.push(ChargedKey {
                transform: probe.culprit.target,
                src_table: probe.canonical_src_table,
                key: probe.key,
                deaths,
            });
        }
    }
    (evict_now, below)
}

/// The most probes one [`isolate_and_evict`] call runs (issue #655).
///
/// Bisection finds one failing key in about `2 * log2(n)` probes, at most 34 for a
/// page at the default `drain_batch_cap` of 100,000 records; this cap leaves
/// room for about seven such keys in a page that size, and for many more in a
/// smaller one. It bounds the cases where bisection alone is not cheap:
/// most of a page failing on its own (up to `2n` probes to find them all),
/// a run of probes that each hit a transient error, or a dead end's
/// single-record probes (see [`Bisector`]). Hitting it charges
/// whatever keys were already pinned; the keys it didn't reach are probed
/// again on the batch's next failed drain.
pub const MAX_ISOLATION_PROBES: usize = 256;

/// How many probes in a row may hit a transient error before one
/// [`isolate_and_evict`] call stops (issue #670), ending
/// [`IsolationOutcome::TransientStorm`] unless it already pinned a key.
///
/// A probe that hits a lock timeout has waited a whole
/// [`crate::locks::LOCK_TIMEOUT`] (30 s) first. Before this limit, a lock
/// storm could run all [`MAX_ISOLATION_PROBES`] probes into it, about 2 hours
/// on one page. Three bounds it at 90 seconds, the same three lock
/// timeouts a page's own lock-timeout retries get (`apply`'s
/// `LOCK_RETRY_BUDGET`), after which the drain surfaces the failure and the
/// page is retried later. A deadlock or serialization failure fails fast, so
/// the bound only matters for lock waits. Three in a row is also rare
/// enough outside a storm not to cut a healthy isolation short: an isolated
/// transient is followed by a clean or failing probe that resets the count.
///
/// One record whose every run hits a transient error (a target row held
/// locked by a long transaction) also stops the search, since bisection
/// probes that record's shrinking runs one after another. That costs nothing:
/// the page's own apply waits on the same record, so the page can't commit
/// until the lock goes, however much of it isolation evicted.
pub const MAX_CONSECUTIVE_TRANSIENT_PROBES: usize = 3;

/// What one isolation probe of a run of records showed (issue #655).
#[derive(Debug)]
enum ProbeVerdict<E> {
    /// The run computed and applied cleanly (and was rolled back).
    Clean,
    /// The run failed with a [`FailureClass::Isolate`] error, carried here:
    /// something in it fails, alone or together with others in the run.
    Failed(E),
    /// The run failed in a way that says nothing about its records: a
    /// version fence miss. For a run of several records that means "look
    /// closer", the same as [`Self::Failed`]; for a single record it means
    /// "not reproduced", as it always has.
    Unknown,
    /// The run hit a transient error: handled exactly as [`Self::Unknown`],
    /// except that [`MAX_CONSECUTIVE_TRANSIENT_PROBES`] of these in a row stop
    /// the search (issue #670).
    Transient,
}

/// What a [`Bisector`] found.
#[derive(Debug)]
struct Bisection<E> {
    /// Each single-record run that failed on its own, as its index and its
    /// error, in index order.
    failing: Vec<(usize, E)>,
    /// How many probes ran.
    probes: usize,
    /// Whether it stopped at the probe limit with runs still unprobed.
    exhausted: bool,
    /// Whether it stopped after [`MAX_CONSECUTIVE_TRANSIENT_PROBES`] probes
    /// in a row hit a transient error, with runs still unprobed.
    transient_storm: bool,
}

/// One entry on a [`Bisector`]'s stack of work still to do.
#[derive(Debug)]
enum Pending {
    /// Probe `run`. `half_of` is the index in [`Bisector::splits`] of the
    /// failing run it is a half of, or `None` for a run split because it
    /// hit a transient error, or for a lone record.
    Run {
        run: Range<usize>,
        half_of: Option<usize>,
    },
    /// Probe each record of `region` alone: the fallback for a dead end (see
    /// [`Bisector`]). `step` counts the records already handed out; they run
    /// from [`Bisector::dead_end_offset`] (modulo the region's length) up,
    /// wrapping around to the region's start.
    Singles { region: Range<usize>, step: usize },
}

/// A failing run that [`Bisector`] split, and how many of its two halves
/// have come back clean.
#[derive(Debug)]
struct Split {
    run: Range<usize>,
    clean_halves: u8,
}

/// Adaptive group testing over records `0..n` (issue #655): probes the two
/// halves of the batch, and splits and probes again only the halves that fail,
/// down to single records. A single record that fails is reported in
/// [`Bisection::failing`]; nothing larger ever is, so the caller can only
/// blame a record that was shown to fail on its own.
///
/// The caller drives it: [`Self::next_run`] names the next run of records to
/// probe, and [`Self::record`] takes what that probe showed. The batch as a
/// whole is never probed: it already failed, which is why this runs. At most
/// `max_probes` runs are handed out; past that, [`Bisection::exhausted`] is
/// set.
///
/// With `k` records that fail alone, this takes at most about
/// `2 * k * log2(n)` probes, two per halving on each failing record's path,
/// against the `n` of probing each record alone. The probes cover `2n`
/// records in total for one failing record (each halving covers half of what
/// the one before it did, two runs at a time).
///
/// **Dead ends.** A failing run whose halves both come back clean fails only
/// through records on both sides of the split. That is a failure that only
/// appears in combination, or a record that fails alone masked by a
/// batch-mate in its own half: an aggregate group's sum, say, where one
/// record's delta alone crosses a check and a batch-mate's negative delta
/// cancels it. Bisection alone would miss that record on every drain, where
/// probing each record alone found it (review of #655). So at a dead end each
/// record of both halves is probed alone, within `max_probes`. A dead end
/// high in a large batch has more records than the probe limit leaves room
/// for, so those probes start at `dead_end_offset` into the dead end and wrap
/// around: [`isolate_and_evict`] picks the offset at random on each call, so
/// across repeated drains of a wedged page every record is eventually probed
/// alone (in about `len / max_probes` drains), where a fixed start would miss
/// a masked record past the window on every drain. Everything else runs
/// lowest index first, and [`Bisection::failing`] is in index order however
/// the records were reached.
struct Bisector<E> {
    /// Work still to do. A stack, so the search is depth first and lowest
    /// index first: a failing record is pinned (and so charged) before the
    /// probe limit can stop the search, and in the batch's own order.
    pending: Vec<Pending>,
    /// Every failing run split so far, for detecting dead ends.
    splits: Vec<Split>,
    /// The `half_of` of the run [`Self::next_run`] handed out last.
    current_half_of: Option<usize>,
    /// Where a dead end's single-record probes start, modulo its length.
    dead_end_offset: usize,
    max_probes: usize,
    /// How many of the latest probes in a row were
    /// [`ProbeVerdict::Transient`].
    consecutive_transient: usize,
    found: Bisection<E>,
}

impl<E> Bisector<E> {
    fn new(n: usize, max_probes: usize, dead_end_offset: usize) -> Self {
        let mut bisector = Bisector {
            pending: Vec::new(),
            splits: Vec::new(),
            current_half_of: None,
            dead_end_offset,
            max_probes,
            consecutive_transient: 0,
            found: Bisection {
                failing: Vec::new(),
                probes: 0,
                exhausted: false,
                transient_storm: false,
            },
        };
        match n {
            0 => {}
            1 => bisector.pending.push(Pending::Run {
                run: 0..1,
                half_of: None,
            }),
            // The whole batch already failed with an isolate-class error.
            _ => bisector.split(0..n, true),
        }
        bisector
    }

    /// Pushes `run`'s two halves, the lower on top so it is probed first.
    /// `failed` records `run` as a failing split, whose halves both coming
    /// back clean is a dead end; a run that only hit a transient error is not.
    fn split(&mut self, run: Range<usize>, failed: bool) {
        let mid = run.start + run.len() / 2;
        let half_of = failed.then(|| {
            self.splits.push(Split {
                run: run.clone(),
                clean_halves: 0,
            });
            self.splits.len() - 1
        });
        self.pending.push(Pending::Run {
            run: mid..run.end,
            half_of,
        });
        self.pending.push(Pending::Run {
            run: run.start..mid,
            half_of,
        });
    }

    /// The next run to probe, counted as a probe, or `None` once the search
    /// is done, has hit the probe limit, or has seen
    /// [`MAX_CONSECUTIVE_TRANSIENT_PROBES`] transient probes in a row.
    fn next_run(&mut self) -> Option<Range<usize>> {
        if self.pending.is_empty() {
            return None;
        }
        if self.consecutive_transient >= MAX_CONSECUTIVE_TRANSIENT_PROBES {
            self.found.transient_storm = true;
            return None;
        }
        if self.found.probes == self.max_probes {
            self.found.exhausted = true;
            return None;
        }
        self.found.probes += 1;
        match self.pending.pop()? {
            Pending::Run { run, half_of } => {
                self.current_half_of = half_of;
                Some(run)
            }
            Pending::Singles { region, step } => {
                self.current_half_of = None;
                let len = region.len();
                let index = region.start + (self.dead_end_offset % len + step) % len;
                if step + 1 < len {
                    self.pending.push(Pending::Singles {
                        region,
                        step: step + 1,
                    });
                }
                Some(index..index + 1)
            }
        }
    }

    /// What probing `run` (the last [`Self::next_run`]) showed.
    fn record(&mut self, run: Range<usize>, verdict: ProbeVerdict<E>) {
        let half_of = self.current_half_of.take();
        if matches!(verdict, ProbeVerdict::Transient) {
            self.consecutive_transient += 1;
        } else {
            self.consecutive_transient = 0;
        }
        match verdict {
            ProbeVerdict::Clean => {
                if let Some(index) = half_of {
                    let split = &mut self.splits[index];
                    split.clean_halves += 1;
                    if split.clean_halves == 2 {
                        let dead_end = split.run.clone();
                        self.probe_halves_alone(dead_end);
                    }
                }
            }
            ProbeVerdict::Failed(err) if run.len() == 1 => {
                self.found.failing.push((run.start, err))
            }
            ProbeVerdict::Unknown | ProbeVerdict::Transient if run.len() == 1 => {}
            ProbeVerdict::Failed(_) => self.split(run, true),
            ProbeVerdict::Unknown | ProbeVerdict::Transient => self.split(run, false),
        }
    }

    /// A dead end at `run`: queues each record of its halves to be probed
    /// alone, skipping a half that is one record (already probed alone).
    fn probe_halves_alone(&mut self, run: Range<usize>) {
        let mid = run.start + run.len() / 2;
        let region = match run.len() {
            // Both halves are single records.
            0..=2 => return,
            // The lower half is a single record.
            3 => mid..run.end,
            _ => run,
        };
        self.pending.push(Pending::Singles { region, step: 0 });
    }

    fn finish(mut self) -> Bisection<E> {
        // A dead end's single-record probes can wrap around; everything else
        // already finds records in index order.
        self.found.failing.sort_by_key(|(index, _)| *index);
        self.found
    }
}

/// A fresh random [`Bisector::dead_end_offset`] for one isolation call, so a
/// dead end larger than the probe limit is probed from a different place on
/// each drain. No crate dependency: [`RandomState`] is randomly keyed per
/// instance.
///
/// [`RandomState`]: std::collections::hash_map::RandomState
fn random_dead_end_offset() -> usize {
    use std::hash::BuildHasher;
    std::collections::hash_map::RandomState::new().hash_one(std::time::SystemTime::now()) as usize
}

/// Computes and applies `records` inside a transaction that always rolls
/// back (so it never commits the drained mark), checking its deferred
/// constraints before the rollback as COMMIT would (#856), and returns the
/// error it failed with, if any. `Err` only for failing to check out a connection or
/// open the transaction. `focus`, and `at`'s `skip_frozen`, are
/// [`apply::compute_page`]'s.
async fn probe_records(
    pool: &Pool,
    at: ProbeSite<'_>,
    records: &[FoldedChange],
    focus: Option<&apply::ProbeFocus<'_>>,
) -> Result<Option<ApplyError>, ApplyError> {
    let plan = match apply::compute_page(pool, records, focus, at.skip_frozen).await {
        Ok(plan) => plan,
        Err(err) => return Ok(Some(err)),
    };
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    // This probe applies-then-rolls-back purely to classify a poisoned
    // key's failure in isolation — it never commits, so a guard (a)
    // rejection here would only ever muddy the diagnosis of an
    // unrelated failure, never protect real state. `saturated()`
    // (issue #132) makes guard (a) a no-op for this probe, matching how
    // every other guard here is unaffected too: a guard rejection is an
    // `Ok` outcome (the fallback-Recompute path), never the
    // `ApplyError` this probe is specifically trying to reproduce.
    let outcome = apply::apply_and_mark_drained(
        &txn,
        at.seg_seq,
        at.claimed_by,
        &plan,
        at.wake_channel,
        &StagedWatermark::saturated(),
    )
    .await;
    // #856: a probe never commits, so a deferred constraint or constraint
    // trigger an application put on a target would never fire in it, and a
    // page failing at COMMIT would reproduce on no key. Checking every
    // deferred constraint here, after the apply, as COMMIT would, reproduces
    // it per key. Constraints that aren't deferred were already checked as
    // each statement ran, and none of Trellis's own tables declares a
    // deferrable one, so this changes nothing else.
    let outcome = match outcome {
        Ok(_) => txn
            .batch_execute("set constraints all immediate")
            .await
            .map_err(ApplyError::from),
        Err(err) => Err(err),
    };
    let _ = txn.rollback().await;
    Ok(outcome.err())
}

/// Attributes an isolate-eligible failure in `folded` to the specific key(s)
/// that fail on their own — doc 06's "Isolate before blaming" — and charges
/// each one death. Every probe computes and applies a run of `folded`'s
/// records inside a transaction that always rolls back (skipping the drained
/// mark).
///
/// **Bisection, not one probe per record (issue #655).** Probing each record
/// alone cost one compute and one rollback-only apply per record: minutes for
/// a few thousand records, hours at the default `drain_batch_cap`, all while
/// the worker held the page's buckets. [`Bisector`] probes the batch's two
/// halves and splits only the halves that fail, so one failing key costs
/// about `2 * log2(n)` probes, over `2n` records in total. A probe of many
/// records costs about what the page's own compute and apply do, so the
/// whole isolation costs a few page applies plus a few dozen fixed probe
/// overheads, against `n` fixed overheads before. Only a single record that
/// fails alone is ever charged, exactly as before; bisection only decides
/// which single records get probed. [`MAX_ISOLATION_PROBES`] caps the total.
///
/// **Dead ends.** Bisection relies on a run holding a record that fails
/// alone failing too, and a batch-mate can mask it: in an aggregate group,
/// one record's delta alone can cross a check that another record's negative
/// delta in the same half cancels. Then some failing run has two clean
/// halves, which is also what a failure that only appears in combination
/// (two records fine alone, failing together) looks like. Bisection can't
/// tell the two apart, and descending no further would miss the masked key
/// on every drain, where probing each record alone charged it and so
/// eventually evicted it. So at a dead end [`Bisector`] probes each record of
/// both halves alone, within [`MAX_ISOLATION_PROBES`]. A dead end deep in the
/// search is small and costs a few probes. One high in a large page spends
/// the rest of the limit on single records (cheap probes) and ends
/// `ProbeLimitReached` unless a key was pinned. Those probes start at a
/// random offset into the dead end on each call and wrap around, so a masked
/// key beyond one call's reach is found on a later drain: after about
/// `len / MAX_ISOLATION_PROBES` drains on average, each bounded by the limit,
/// where probing each record alone took one unbounded drain. A
/// combination-only failure charges nothing either way, as before.
///
/// **A transient error or version fence miss inside a probe** says nothing
/// about the probed records. For a run of several records it is treated like
/// a failure, so bisection looks inside it rather than skipping a half that
/// may hold a failing key; for a single record it is not a reproduction and
/// charges nothing, as before. Probing smaller runs is also what gives a probe
/// that hit lock contention or a statement timeout its best chance of an
/// answer. A database that keeps failing transiently stops the search after
/// [`MAX_CONSECUTIVE_TRANSIENT_PROBES`] such probes in a row (issue #670),
/// or propagates as `Err` once checking out a connection fails.
///
/// **Logged at start and end (issue #670)**, with the batch's record count
/// and, at the end, the probes used, the outcome, how many keys it pinned,
/// whether the search ran to completion, and the time it took. Isolation
/// holds the page's buckets for as long as it runs, so its cost is worth
/// seeing on every call, not only when it fails.
///
/// `folded`'s truncates and deferred relationship reverses are never probed.
///
/// **Charged to a definition, not to the key (#799).** A record that fails
/// alone is probed again to find which definition's apply, or which
/// relationship's reverse work, it fails in (`attribute`), and its death,
/// its eviction and the whole-transform fuse are those definitions' alone. A
/// record that fails only when several definitions apply it together is
/// charged to the ones in every failing combination, and to nobody when no
/// one is (two separate failing pairs, say).
///
/// Returns (see [`IsolationOutcome`]'s variants):
/// - `Ok(FuseDisabled)` if `threshold == 0`, without probing anything.
/// - `Ok(NothingReproduced)` if no single key reproduced an isolate-eligible
///   failure (the error is surfaced, not blamed —
///   [`super::apply::drain_once`] returns the *original* failure it already
///   holds, unmodified).
/// - `Ok(ProbeLimitReached { .. })` if the probe limit stopped isolation
///   before it pinned any key; handled as `NothingReproduced` is.
/// - `Ok(TransientStorm { .. })` if [`MAX_CONSECUTIVE_TRANSIENT_PROBES`]
///   probes in a row hit a transient error before it pinned any key.
/// - `Ok(ChargedBelowThreshold { .. })` if keys reproduced and were charged
///   but none reached `threshold`; the caller surfaces the original failure,
///   exactly as for `NothingReproduced`, but can say which keys it pinned.
/// - `Ok(Evicted { .. })` if at least one key crossed the death threshold and
///   was poisoned for the definition it fails in.
/// - `Err(_)` if a probe itself hit a [`FailureClass::Halting`] error: this
///   propagates immediately, unattributed to any key, per doc 06's "What
///   must never be quarantined" — discovered during isolation is no
///   different from discovered on the whole batch, so the caller halts on it
///   the same way (`staging::halt`, #663). Likewise
///   `Err(ApplyError::ClaimLost)` if a probe found the claim gone (issue
///   #620), with nothing charged.
///
/// **Accepted trade-off, not a bug — one charge per drain call**: when no key
/// reaches `threshold`, `apply::classify_and_retry` surfaces the
/// original failure, which ends that drain call. So a key that fails
/// deterministically, alone in its batch, is charged exactly once per drain
/// call and is evicted on the `threshold`-th *separate* drain cycle — each
/// cycle re-reads the same immutable batch, re-fails, and re-isolates. (A key
/// that stays below the threshold while another key in the same probe pass is
/// evicted *is* charged again within the call, since the retry without the
/// evicted key re-fails on it.) The alternative — retrying within the call
/// until the key is evicted — would evict faster, but would spend the call's
/// whole `MAX_APPLY_ATTEMPTS` budget (also 5) on one key; eviction after a
/// few drain cycles (about a second at the default) is the accepted cost.
/// Either way a key's death count stays what doc 06 means by it: how many
/// real, observed attempts have failed for it, cleared by any clean drain
/// that applies it.
///
/// **`skip_frozen` (issue #766)** computes every probe as
/// `apply::compute_page` does with it: a drain that Postgres refused a read
/// or write probes the page as its own retries compute it, skipping the
/// tables no unfrozen definition reads. Without that, a probe would read the
/// refused table again and halt on its `42501`, and a key failing elsewhere
/// in the page would go uncharged on every drain.
pub async fn isolate_and_evict(
    pool: &Pool,
    seg_seq: i64,
    claimed_by: &str,
    wake_channel: &str,
    folded: &[FoldedChange],
    threshold: i32,
    skip_frozen: bool,
) -> Result<IsolationOutcome, ApplyError> {
    if threshold == 0 {
        return Ok(IsolationOutcome::FuseDisabled);
    }
    let started = std::time::Instant::now();
    tracing::info!(
        seg_seq,
        records = folded.len(),
        "isolating a failed batch: probing its records for the keys that fail alone"
    );
    let mut stats = IsolationStats::default();
    let at = ProbeSite {
        seg_seq,
        claimed_by,
        wake_channel,
        skip_frozen,
    };
    let result = isolate_and_evict_probing(pool, at, folded, threshold, &mut stats).await;
    log_isolation_finished(seg_seq, folded.len(), &stats, &result, started.elapsed());
    result
}

/// What one [`isolate_and_evict`] call's probing did, for its end-of-call
/// log line: filled in as it goes, so a call that stops on an error still
/// reports the probes it spent.
#[derive(Debug, Default)]
struct IsolationStats {
    probes: usize,
    /// Why the search stopped short, or `None` if it ran to completion.
    stopped: Option<IsolationStop>,
}

/// Why a [`Bisector`] search stopped with runs still unprobed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IsolationStop {
    ProbeLimit,
    TransientStorm,
}

impl IsolationOutcome {
    /// This outcome's name, for a log field.
    fn label(&self) -> &'static str {
        match self {
            IsolationOutcome::FuseDisabled => "FuseDisabled",
            IsolationOutcome::NothingReproduced => "NothingReproduced",
            IsolationOutcome::ProbeLimitReached { .. } => "ProbeLimitReached",
            IsolationOutcome::TransientStorm { .. } => "TransientStorm",
            IsolationOutcome::ChargedBelowThreshold { .. } => "ChargedBelowThreshold",
            IsolationOutcome::Evicted { .. } => "Evicted",
        }
    }
}

/// [`isolate_and_evict`]'s end-of-call log line (issue #670). `records` is
/// the batch's record count; `pinned` counts the keys it charged, evicted
/// or not. `info`, or `warn` when the search stopped short or on an error:
/// either leaves part of the batch unprobed.
fn log_isolation_finished(
    seg_seq: i64,
    records: usize,
    stats: &IsolationStats,
    result: &Result<IsolationOutcome, ApplyError>,
    elapsed: std::time::Duration,
) {
    let elapsed_ms = elapsed.as_millis() as u64;
    let probes = stats.probes;
    let search = match stats.stopped {
        None => "complete",
        Some(IsolationStop::ProbeLimit) => "stopped at the probe limit",
        Some(IsolationStop::TransientStorm) => "stopped on consecutive transient errors",
    };
    match result {
        Ok(outcome) => {
            let pinned = match outcome {
                IsolationOutcome::ChargedBelowThreshold { charged } => charged.len(),
                IsolationOutcome::Evicted { evicted, charged } => evicted + charged.len(),
                _ => 0,
            };
            let outcome = outcome.label();
            if stats.stopped.is_some() {
                tracing::warn!(
                    seg_seq,
                    records,
                    probes,
                    outcome,
                    pinned,
                    search,
                    elapsed_ms,
                    "isolation finished without probing the whole batch; keys it did not reach \
                     are not charged this drain"
                );
            } else {
                tracing::info!(
                    seg_seq,
                    records,
                    probes,
                    outcome,
                    pinned,
                    search,
                    elapsed_ms,
                    "isolation finished"
                );
            }
        }
        Err(err) => tracing::warn!(
            seg_seq,
            records,
            probes,
            outcome = "error",
            search,
            elapsed_ms,
            error = %err,
            "isolation stopped on an error"
        ),
    }
}

/// [`isolate_and_evict`] past its fuse check and start-of-call log.
async fn isolate_and_evict_probing(
    pool: &Pool,
    at: ProbeSite<'_>,
    folded: &[FoldedChange],
    threshold: i32,
    stats: &mut IsolationStats,
) -> Result<IsolationOutcome, ApplyError> {
    let seg_seq = at.seg_seq;
    // Issue #283: every counter/marker write below lands under the *canonical*
    // (qualified, where resolvable) identity of the ring row's `src_table`,
    // never the raw spelling — resolved once per distinct source table here and
    // threaded through `attribute_column_failure`/`record_key_death`/
    // `evict_key` alike. The raw spelling is still what this function
    // *matches ring rows on* (`contribution` below): that compares against
    // `folded`'s own strings, which are the ring's, not quarantine's.
    let mut canonical_srcs = CanonicalSrcTables::default();

    // Issue #134/#135 review follow-up: a `rel_reverse_deferred` row
    // must never be probed/poisoned/parked here, for the same reason
    // `park_batch_contribution`/`poisoned_park` already exclude it at
    // the `compute()` level (that module's own comment) — `poison_held`
    // has no columns for `relationship_id`/`retry_count`, so parking one
    // would drop them entirely, and — worse — `release_key` would later
    // stage a `Recompute` against this op's synthetic sentinel
    // `src_table` (`apply::relationship_reverse_deferred_src_table`),
    // which is not a real table at all. Skipping it here is the loud,
    // safe failure mode doc 06 asks for: if nothing else in this batch
    // reproduces the error in isolation, `poisoned` stays empty and the
    // caller (`classify_and_retry`'s `Isolate` arm) surfaces the
    // original failure rather than silently corrupting quarantine
    // state. A complete fix — genuinely quarantine-safe deferred
    // reverses — is real and larger than this follow-up; tracked
    // separately rather than attempted here.
    let probeable = |c: &FoldedChange| !c.is_truncate && c.relationship_reverse_deferred.is_none();
    let candidates: Cow<'_, [FoldedChange]> = if folded.iter().all(probeable) {
        Cow::Borrowed(folded)
    } else {
        Cow::Owned(folded.iter().filter(|c| probeable(c)).cloned().collect())
    };

    let mut bisector = Bisector::new(
        candidates.len(),
        MAX_ISOLATION_PROBES,
        random_dead_end_offset(),
    );
    while let Some(run) = bisector.next_run() {
        stats.probes += 1;
        let records = &candidates[run.clone()];
        let Some(err) = probe_records(pool, at, records, None).await? else {
            bisector.record(run, ProbeVerdict::Clean);
            continue;
        };
        // Issue #620: a probe that finds its claim gone reproduced the lost
        // claim, not this run's failure, and every later probe would
        // reproduce it too, charging every key in the page a death. Stop and
        // surface it, as `apply::classify_and_retry` does for a page's own
        // `ClaimLost`: whoever holds the buckets now re-drains the page, and
        // a genuinely failing key is charged then.
        if is_claim_lost(&err) {
            return Err(err);
        }
        let verdict = match classify(&err) {
            // Unattributed to any key: `apply::classify_and_retry` halts
            // on it as on the page's own (#663).
            FailureClass::Halting => return Err(err),
            FailureClass::Isolate => {
                if let [change] = records {
                    // ADR-0003's amendment, layered alongside (not instead
                    // of) the row-level charge below: a probe failure that's
                    // specifically an evaluator error names the calculated
                    // field it broke on
                    // ([`crate::defs::eval::EvalError::field`]), which this
                    // attributes to a `(transform, column)` pair and charges
                    // toward that pair's own, independent fuse. See
                    // [`attribute_column_failure`]'s doc comment for why this
                    // never changes what gets returned from *this* function
                    // — the row-level fuse below is completely unmodified by
                    // this call. Also see that same doc comment's "Ambiguous
                    // match -> no attribution, deliberately": if the failing
                    // field name matches more than one sibling transform on
                    // this source, this call intentionally attributes nothing
                    // rather than guess, and the row-level fuse below is
                    // exactly what still protects against the failure going
                    // otherwise unhandled. Only a single record's failure is
                    // attributed, since a run's names no one key; and it is
                    // attributed as each key is pinned, so a column fuse that
                    // trips mid-isolation pauses the column for the probes
                    // after it.
                    let canonical = canonical_srcs.get(pool, &change.src_table).await?;
                    attribute_column_failure(pool, &canonical, &change.key, &err).await?;
                }
                ProbeVerdict::Failed(err)
            }
            FailureClass::Transient => ProbeVerdict::Transient,
            FailureClass::VersionFenceMiss => ProbeVerdict::Unknown,
        };
        bisector.record(run, verdict);
    }
    let bisection = bisector.finish();
    stats.stopped = if bisection.transient_storm {
        Some(IsolationStop::TransientStorm)
    } else if bisection.exhausted {
        Some(IsolationStop::ProbeLimit)
    } else {
        None
    };

    // #799: a record that fails alone is charged to the definition(s) it
    // fails in, never to the key across every reader of its table.
    let mut poisoned: Vec<PoisonedProbe> = Vec::with_capacity(bisection.failing.len());
    for (index, err) in bisection.failing {
        let change = &candidates[index];
        let canonical = canonical_srcs.get(pool, &change.src_table).await?;
        let culprits = attribute(pool, at, change, &canonical, &err, stats).await?;
        if culprits.is_empty() {
            tracing::warn!(
                seg_seq,
                src_table = %canonical,
                key = %change.key,
                error = %err,
                "a key fails alone, but not in any one definition's apply; charging nothing"
            );
        }
        for culprit in culprits {
            poisoned.push(PoisonedProbe {
                raw_src_table: change.src_table.clone(),
                canonical_src_table: canonical.clone(),
                key: change.key.clone(),
                culprit,
                sqlstate: sqlstate_of(&err),
            });
        }
    }

    if poisoned.is_empty() {
        return Ok(match stats.stopped {
            Some(IsolationStop::TransientStorm) => IsolationOutcome::TransientStorm {
                probes: bisection.probes,
            },
            Some(IsolationStop::ProbeLimit) => IsolationOutcome::ProbeLimitReached {
                probes: bisection.probes,
            },
            None => IsolationOutcome::NothingReproduced,
        });
    }

    let mut charged: Vec<(PoisonedProbe, i32)> = Vec::with_capacity(poisoned.len());
    {
        let client = pool.get().await?;
        for probe in poisoned {
            let deaths = record_key_death(
                &**client,
                probe.culprit.transform_id,
                &probe.canonical_src_table,
                &probe.key,
                &probe.culprit.last_error,
            )
            .await?;
            charged.push((probe, deaths));
        }
    }
    let (evict_now, charged) = partition_by_threshold(charged, threshold);

    if evict_now.is_empty() {
        return Ok(IsolationOutcome::ChargedBelowThreshold { charged });
    }

    // One definition at a time, in id order, so two concurrent evictions
    // spanning the same definitions take their fuse gates (issue #159,
    // `take_fuse_gate`) in one order. Each definition's gate is taken before
    // its definition row and its `poison` rows, so no transaction holding one
    // of those waits on a gate another transaction holding it waits on.
    let mut by_transform: std::collections::BTreeMap<i64, Vec<&PoisonedProbe>> =
        std::collections::BTreeMap::new();
    for probe in &evict_now {
        by_transform
            .entry(probe.culprit.transform_id)
            .or_default()
            .push(probe);
    }
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    let mut evicted = 0;
    for (transform_id, mut probes) in by_transform {
        // Test-only pause point (#880). See `super::interleave`.
        #[cfg(any(test, feature = "test-util"))]
        super::interleave::pause_at(
            &*txn,
            super::interleave::PausePoint::BeforeEvictionLocks,
            &probes[0].culprit.target,
        )
        .await?;
        if !evict_for(&txn, transform_id, probes[0].culprit.epoch).await? {
            continue;
        }
        // Its keys in key order, the order a page parks its held rows in
        // (`park_batch_contribution`), so the two never wait on each other's
        // rows in opposite orders (ADR-0002 I5).
        probes.sort_by(|a, b| {
            (&a.canonical_src_table, &a.key).cmp(&(&b.canonical_src_table, &b.key))
        });
        for probe in probes {
            let contribution = folded.iter().find(|c| {
                !c.is_truncate && c.src_table == probe.raw_src_table && c.key == probe.key
            });
            // #880: a release that committed after isolation charged this
            // key, before `evict_for` locked the definition, deleted the
            // charge. Read it now, under the lock, or the poison written
            // below would undo the release.
            if !still_charged_to_threshold(&txn, probe, threshold).await? {
                continue;
            }
            evict_key(&txn, seg_seq, probe, contribution).await?;
            evicted += 1;
        }
        quarantine_if_crossed(&txn, transform_id, &not_frozen_sql()).await?;
    }
    txn.commit().await?;

    if evicted == 0 {
        // Nothing was poisoned: every culprit's definition was frozen or
        // resumed while isolation probed it, or its key's count was deleted
        // (a release) or cleared (a clean apply) before the eviction locked
        // the definition. The retry recomputes without the frozen or resumed
        // definitions, and with the released keys, which an operator
        // releases once their cause is fixed, so the original failure is
        // likely gone; if not, the next isolation charges it afresh.
        return Ok(IsolationOutcome::ChargedBelowThreshold { charged });
    }
    Ok(IsolationOutcome::Evicted { evicted, charged })
}

/// Where [`isolate_and_evict`]'s probes run: the page's segment and claim,
/// and whether they skip the tables no unfrozen definition reads
/// (`skip_frozen`, issue #766).
#[derive(Clone, Copy)]
struct ProbeSite<'a> {
    seg_seq: i64,
    claimed_by: &'a str,
    wake_channel: &'a str,
    skip_frozen: bool,
}

/// The definitions a record that fails alone (`change`, of canonical source
/// `src_table`, with error `err`) fails in (#799), each probed on its own:
///
/// 1. With every definition that applies the record directly left out (the
///    readers of its source that aren't already poisoned for its key), what
///    is left is the reverse work done for the definitions reading the table
///    through a relationship (the to-side's reverse recomputes and settled
///    projection). If that fails, the failure is in that work, and it is
///    charged to the readers of the relationships whose work it is in
///    ([`attribute_relationships`]). A relationship's share of the work is
///    skipped only once every one of its readers holds the key
///    ([`super::apply::compute`]).
/// 2. Otherwise, with one direct reader, it's that one.
/// 3. Otherwise each direct reader is probed alone, the others left out, and
///    every one that fails is charged.
/// 4. If none fails alone, the record fails only when several of them apply
///    it together. With two, it's both of them. With three or more, each is
///    left out in turn, the others applying the record, and every one whose
///    absence makes the failure go away is charged: it is in every failing
///    combination. One that is in none of them (two separate failing pairs,
///    say) leaves nobody charged, as bisection charges nobody for a failure
///    only a combination of records reproduces, and the page is a drain
///    holdup ([`super::holdup`]).
///
/// A probe that hits a transient error or a version fence miss charges
/// nothing for what it probed, and the combinations step 4 infers from step
/// 3's probes need every one of them settled. One that hits a halting error
/// or a lost claim propagates, as bisection's own do. Each probe counts
/// toward [`MAX_ISOLATION_PROBES`]. At the limit, the definitions the probes
/// had pinned are charged and nobody else is on this drain, except that step
/// 1 also charges the readers of every relationship it hadn't probed yet.
///
/// Every probe runs on the failure path only: a page that applies runs none.
async fn attribute(
    pool: &Pool,
    at: ProbeSite<'_>,
    change: &FoldedChange,
    src_table: &str,
    err: &ApplyError,
    stats: &mut IsolationStats,
) -> Result<Vec<Culprit>, ApplyError> {
    let readers = catalog::transforms_for_source(pool, src_table).await?;
    let (poisoned, epochs) = {
        let client = pool.get().await?;
        let poisoned: HashSet<i64> = client
            .query(
                "select transform_id from poison where src_table = $1 and key = $2",
                &[&src_table, &change.key],
            )
            .await?
            .iter()
            .map(|row| row.get(0))
            .collect();
        let epochs: HashMap<i64, Option<SystemTime>> = client
            .query("select id, fuse_rearmed_at from transform_definitions", &[])
            .await?
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
        (poisoned, epochs)
    };
    let culprit = |id: i64, target: &str, last_error: String| Culprit {
        transform_id: id,
        target: target.to_string(),
        epoch: epochs.get(&id).copied().flatten(),
        last_error,
    };
    let direct: Vec<&crate::defs::model::Definition> = readers
        .iter()
        .filter(|def| !poisoned.contains(&def.id))
        .collect();
    let probe = Probe {
        at,
        change,
        src_table,
    };

    // 1. The relationship readers' share alone. A relationship none of whose
    // readers is left without the key holds no share left to fail: its work
    // is left out once every reader of it holds the key.
    let inbound: Vec<(i64, Vec<(i64, String)>)> =
        apply::relationship_readers_by_rel(pool, src_table)
            .await?
            .into_iter()
            .map(|(rel, readers)| {
                let readers: Vec<(i64, String)> = readers
                    .into_iter()
                    .filter(|(id, _)| !poisoned.contains(id))
                    .collect();
                (rel, readers)
            })
            .filter(|(_, readers)| !readers.is_empty())
            .collect();
    if !inbound.is_empty() {
        match probe.run(pool, DirectFocus::None, None, stats).await? {
            Probed::Unsettled | Probed::OutOfProbes => return Ok(Vec::new()),
            Probed::Failed(shared_err) => {
                let charged =
                    attribute_relationships(pool, &probe, &inbound, &shared_err, stats).await?;
                return Ok(charged
                    .iter()
                    .map(|(id, (target, last_error))| culprit(*id, target, last_error.clone()))
                    .collect());
            }
            Probed::Clean => {}
        }
    }

    // 2. The one direct reader.
    match direct.as_slice() {
        [] => return Ok(Vec::new()),
        [def] => return Ok(vec![culprit(def.id, &def.def.target, err.to_string())]),
        _ => {}
    }

    // 3. Each direct reader alone.
    let mut culprits = Vec::new();
    let mut settled = true;
    for def in &direct {
        match probe
            .run(pool, DirectFocus::Only(def.id), None, stats)
            .await?
        {
            Probed::Failed(def_err) => {
                culprits.push(culprit(def.id, &def.def.target, def_err.to_string()));
            }
            Probed::Clean => {}
            Probed::Unsettled | Probed::OutOfProbes => settled = false,
        }
    }
    if !culprits.is_empty() || !settled {
        return Ok(culprits);
    }

    // 4. Only together. With two, leaving either out is the other's probe
    // alone, which step 3 ran: it's both.
    if direct.len() == 2 {
        return Ok(direct
            .iter()
            .map(|def| culprit(def.id, &def.def.target, err.to_string()))
            .collect());
    }
    for def in &direct {
        match probe
            .run(pool, DirectFocus::AllBut(def.id), None, stats)
            .await?
        {
            Probed::Clean => culprits.push(culprit(def.id, &def.def.target, err.to_string())),
            Probed::Failed(_) | Probed::Unsettled => {}
            Probed::OutOfProbes => break,
        }
    }
    Ok(culprits)
}

/// [`attribute`]'s step 1 past its combined probe, which failed with
/// `shared_err`: the readers to charge, by id, each with its bare target and
/// the error it is charged with. `inbound` is each relationship to the
/// record's table that has a reader left without the key, with those
/// readers, in relationship id order.
///
/// With one such relationship, the failure is in its work. With several,
/// each is probed alone, the record left out of every other relationship's
/// reverse work (#822), and the readers of each one that fails alone are
/// charged, once each however many of them they read through. If none fails
/// alone, the failure is in their work together, or in the work of a
/// relationship no reader is left without the key, and every reader is
/// charged. At the probe limit, every relationship not yet probed is charged
/// as well. A probe that settles nothing charges nothing for its
/// relationship, and if no other one failed alone, nobody is charged.
async fn attribute_relationships(
    pool: &Pool,
    probe: &Probe<'_>,
    inbound: &[(i64, Vec<(i64, String)>)],
    shared_err: &ApplyError,
    stats: &mut IsolationStats,
) -> Result<BTreeMap<i64, (String, String)>, ApplyError> {
    if let [(_, readers)] = inbound {
        return Ok(readers
            .iter()
            .map(|(id, target)| (*id, (target.clone(), shared_err.to_string())))
            .collect());
    }
    let mut probed = Vec::with_capacity(inbound.len());
    for (rel, _) in inbound {
        // Past the limit, each one is `OutOfProbes` without probing.
        probed.push(
            probe
                .run(pool, DirectFocus::None, Some(*rel), stats)
                .await?,
        );
    }
    Ok(relationship_charges(inbound, probed, shared_err))
}

/// [`attribute_relationships`]' charges from what each of `inbound`'s
/// relationships' probes settled (`probed`, in the same order).
fn relationship_charges(
    inbound: &[(i64, Vec<(i64, String)>)],
    probed: Vec<Probed>,
    shared_err: &ApplyError,
) -> BTreeMap<i64, (String, String)> {
    let mut charged: BTreeMap<i64, (String, String)> = BTreeMap::new();
    let charge = |charged: &mut BTreeMap<i64, (String, String)>,
                  readers: &[(i64, String)],
                  last_error: &ApplyError| {
        for (id, target) in readers {
            charged
                .entry(*id)
                .or_insert_with(|| (target.clone(), last_error.to_string()));
        }
    };
    let mut unsettled = false;
    for ((_, readers), probed) in inbound.iter().zip(probed) {
        match probed {
            Probed::Failed(rel_err) => charge(&mut charged, readers, &rel_err),
            Probed::Clean => {}
            Probed::Unsettled => unsettled = true,
            Probed::OutOfProbes => charge(&mut charged, readers, shared_err),
        }
    }
    if charged.is_empty() && !unsettled {
        for (_, readers) in inbound {
            charge(&mut charged, readers, shared_err);
        }
    }
    charged
}

/// The record one of [`attribute`]'s probes applies, and where.
struct Probe<'a> {
    at: ProbeSite<'a>,
    change: &'a FoldedChange,
    /// The record's canonical source.
    src_table: &'a str,
}

/// What one of [`attribute`]'s probes settled.
enum Probed {
    /// The record applied.
    Clean,
    /// It failed with an [`FailureClass::Isolate`] error.
    Failed(ApplyError),
    /// It hit a transient error or a version fence miss, which says nothing
    /// about the work it probed.
    Unsettled,
    /// [`MAX_ISOLATION_PROBES`] were spent before it ran.
    OutOfProbes,
}

impl Probe<'_> {
    /// Applies the record with only the work `direct` and `only_rel` keep
    /// ([`apply::ProbeFocus`]). A halting error or a lost claim propagates.
    async fn run(
        &self,
        pool: &Pool,
        direct: DirectFocus,
        only_rel: Option<i64>,
        stats: &mut IsolationStats,
    ) -> Result<Probed, ApplyError> {
        if stats.probes >= MAX_ISOLATION_PROBES {
            stats.stopped.get_or_insert(IsolationStop::ProbeLimit);
            return Ok(Probed::OutOfProbes);
        }
        stats.probes += 1;
        let focus = apply::ProbeFocus {
            src_table: self.src_table,
            key: &self.change.key,
            direct,
            only_rel,
        };
        let records = std::slice::from_ref(self.change);
        let Some(err) = probe_records(pool, self.at, records, Some(&focus)).await? else {
            return Ok(Probed::Clean);
        };
        if is_claim_lost(&err) {
            return Err(err);
        }
        Ok(match classify(&err) {
            FailureClass::Halting => return Err(err),
            FailureClass::Isolate => Probed::Failed(err),
            FailureClass::Transient | FailureClass::VersionFenceMiss => Probed::Unsettled,
        })
    }
}

/// Takes `transform_id`'s fuse gate and its definition row, and says whether
/// it may still be poisoned (#799): it isn't frozen, and no resume has
/// stamped a new `fuse_rearmed_at` since attribution read `epoch`. A resume
/// deletes the definition's poison and rebuilds it, so a key its old self
/// failed on must not land after the resume and hold a key of the rebuilt
/// one. The row lock (`for no key update`, the lock the fuse's own status
/// write takes) holds off a resume until this transaction ends, and the gate
/// comes first, as the fuse has always taken it (issue #159).
async fn evict_for(
    txn: &Transaction<'_>,
    transform_id: i64,
    epoch: Option<SystemTime>,
) -> Result<bool, ApplyError> {
    take_fuse_gate(txn, transform_id).await?;
    let current = txn
        .query_opt(
            "select 1 from transform_definitions \
             where id = $1 and fuse_rearmed_at is not distinct from $2 \
               and status = any($3) \
             for no key update",
            &[&transform_id, &epoch, &TransformStatus::dispatchable()],
        )
        .await?;
    if current.is_none() {
        tracing::debug!(
            transform_id,
            "not poisoning a key for a definition frozen or resumed while isolation probed it"
        );
    }
    Ok(current.is_some())
}

/// Resolves a ring row's raw `src_table` to the fully-qualified identity
/// [`catalog::transforms_for_source`]/[`catalog::dependents_of`] require
/// (issue #74, ADR-0007: `schema_nodes` keys on qualified identity, so a bare
/// lookup there silently finds *nothing* rather than erroring) — this
/// module's counterpart to `super::apply`'s own `qualified_schema_node_key`,
/// which every `apply.rs` call site already goes through.
///
/// Issue #281: both of this module's `transforms_for_source` call sites
/// ([`trip_transform_fuse_if_crossed`] and [`attribute_column_failure`]) used
/// to pass the raw `src_table` straight through, so for any bare spelling the
/// whole-transform fuse crossed its threshold and quarantined nothing, and
/// column failures went unattributed — both entirely silently, since an
/// unqualified argument is an empty result set, not an error. Issue #267
/// stopped `apply.rs` *emitting* bare `src_table` going forward, but durable
/// pre-#267 ring rows and the crate's own integration fixtures that stage
/// bare `src_table` by hand still reach here.
///
/// **Unresolvable names fall through to the raw spelling rather than
/// erroring**, which is the one deliberate difference from
/// `apply::qualified_schema_node_key` (that one propagates
/// [`catalog::CatalogError::SourceTableNotFound`] to its caller; this one
/// instead mirrors `catalog::resolve_relationship_endpoint`, which tolerates
/// exactly that variant and hands back the name unchanged). Every
/// caller here is *diagnosing* an already-failed batch: turning a name this
/// module cannot resolve into a brand-new `ApplyError` would replace the
/// original failure being quarantined with a confusing secondary one, and
/// could abort the eviction transaction that is the whole point of the call.
/// Two real shapes hit that branch — `apply::relationship_reverse_deferred_src_table`'s
/// U+001F-prefixed synthetic sentinel (neither bare nor qualified, and not a
/// physical table at all; [`isolate_and_evict`] already skips those rows
/// before either call site, so this is belt-and-braces), and a durable ring
/// row naming a source table that has since been dropped. Both previously
/// reached `transforms_for_source` and found nothing; both still do, which is
/// exactly the pre-existing behavior for the cases where nothing *can* be
/// found.
///
/// **Issue #283: this is also the canonical key every quarantine counter and
/// marker table is now written and read under** — `poison`, `poison_held`,
/// `key_deaths`, `column_failures` and `transform_fuse_gate`. Before that, all
/// five stored whatever spelling the ring row happened to carry, so one logical
/// source staged both bare and qualified accumulated two independent sets of
/// quarantine state that never combined: two half-threshold fuse budgets that
/// never tripped, two row-level death counters for one physical row, a fold
/// exclusion that missed a key poisoned under the other spelling, and a
/// per-spelling (so non-serializing) `take_fuse_gate` lock row. Every write and
/// every read now goes through this one resolution, and
/// `V33__quarantine_canonical_src_table.sql` folded the pre-existing bare rows
/// into their qualified counterpart. The unresolvable spellings above are the
/// deliberate exception: they keep their raw key, which is self-consistent
/// (nothing else can resolve them either) and exactly the pre-#283 behaviour.
pub(super) async fn qualified_src_table(
    pool: &Pool,
    src_table: &str,
) -> Result<String, ApplyError> {
    if src_table.contains('.') {
        return Ok(src_table.to_string());
    }
    match catalog::resolve_graph_identity(pool, src_table).await {
        Ok(qualified) => Ok(qualified),
        Err(catalog::CatalogError::SourceTableNotFound(_)) => Ok(src_table.to_string()),
        Err(err) => Err(err.into()),
    }
}

/// A memo over [`qualified_src_table`] for the two callers that resolve a whole
/// folded batch's worth of `src_table`s at once ([`isolate_and_evict`] and
/// [`super::apply::compute`], issue #283): a batch routinely carries many
/// changes per source table, and the resolution is a pure function of the
/// catalog for the duration of one batch.
///
/// Cheap by construction in the common case: an already-qualified spelling —
/// which, since issue #267, is every `src_table` `apply.rs` emits — short
/// circuits inside [`qualified_src_table`] without touching the database at
/// all, so this only ever spends a round trip on the durable bare rows and
/// hand-staged fixtures that issue #281's doc comment enumerates, once each.
#[derive(Default)]
pub(super) struct CanonicalSrcTables {
    cache: HashMap<String, String>,
}

impl CanonicalSrcTables {
    /// The canonical (qualified, where resolvable) identity for `src_table`,
    /// resolving and memoizing it on first sight.
    pub(super) async fn get(&mut self, pool: &Pool, src_table: &str) -> Result<String, ApplyError> {
        if let Some(canonical) = self.cache.get(src_table) {
            return Ok(canonical.clone());
        }
        let canonical = qualified_src_table(pool, src_table).await?;
        self.cache.insert(src_table.to_string(), canonical.clone());
        Ok(canonical)
    }

    /// The already-resolved canonical identity for `src_table`, or `None` if
    /// [`Self::get`] was never called for it — the borrow-free lookup
    /// [`super::apply::compute`] uses once it has pre-resolved every source
    /// table in a batch, so its per-change hot loop takes no `&mut self` and no
    /// `.await`.
    pub(super) fn canonical(&self, src_table: &str) -> Option<&str> {
        self.cache.get(src_table).map(String::as_str)
    }
}

// ---------------------------------------------------------------------
// Whole-transform fuse (ADR-0003's original, coarser tier — issue #105)
// ---------------------------------------------------------------------
//
// ADR-0003: "if a failure isn't attributable to one column (e.g. a
// key-shape/DDL failure that dooms every column's write for that row
// alike), it trips the whole transform to quarantined exactly as before."
// Every key [`isolate_and_evict`] evicts is exactly that: a failure that
// survived a probe run *alone* and still wasn't attributable to a single
// calculated field (whether or not [`attribute_column_failure`] separately
// also charged a column fuse alongside it — the two tiers are independent,
// see that function's own doc comment). Whole-key poison is per transform
// (#799), so the `poison` rows of one definition *are* the distinct-evicted-key
// count its fuse needs — no counter table of its own, unlike
// [`key_deaths`]/`column_deaths`'s incrementally-maintained counters, and a
// definition is charged for its own evictions only, never for a sibling's on
// the same source. A resume deletes the definition's rows
// ([`resume_transform`]), which is what re-arms its fuse.
//
// What `poison` cannot supply on its own is *serialization* between two
// concurrent evictions for the same definition (issue #159): each counts
// inside its own transaction and cannot see the other's uncommitted insert,
// so two workers landing the 4th and 5th eviction at once both counted 4 and
// neither tripped. That is what `transform_fuse_gate`/[`take_fuse_gate`] adds
// — one row lock per definition, in exactly the shape [`record_key_death`]
// already uses for `key_deaths`, taken before any counting happens.

/// Takes `transform_id`'s whole-transform-fuse gate on `txn` and holds it
/// until that transaction ends: the serialization point issue #159 was
/// missing. Every eviction transaction that is about to ask "has this
/// definition crossed [`DEFAULT_TRANSFORM_DEATH_THRESHOLD`]?" passes through
/// this one row first, so two of them for the same definition can never both
/// answer from a snapshot taken before the other's `poison` insert landed.
///
/// An `INSERT ... ON CONFLICT DO UPDATE`, not a `SELECT ... FOR UPDATE`, for
/// the same reason [`record_key_death`]/`charge_column_failure` use that
/// shape for `key_deaths`/`column_deaths`: the gate row may not exist yet
/// (first-ever eviction for this definition), and `FOR UPDATE` over zero rows
/// locks nothing at all — two concurrent first evictions would each sail
/// straight through. `ON CONFLICT` covers both halves: Postgres's speculative
/// insertion makes the losing *inserter* wait on the winner's transaction,
/// and `DO UPDATE` (not `DO NOTHING`, which takes no row lock once the row
/// exists) makes every later caller wait on the row lock.
///
/// Deliberately no `RETURNING` and no maintained count: the threshold
/// decision stays a `count(*)` over the definition's `poison` rows.
/// `V30__transform_fuse_gate.sql` has the full rationale.
async fn take_fuse_gate(txn: &Transaction<'_>, transform_id: i64) -> Result<(), ApplyError> {
    txn.execute(
        "insert into transform_fuse_gate (transform_id, checks, last_checked_at) \
         values ($1, 1, now()) \
         on conflict (transform_id) do update set \
             checks = transform_fuse_gate.checks + 1, \
             last_checked_at = now()",
        &[&transform_id],
    )
    .await?;
    Ok(())
}

/// Checks whether `transform_id`'s whole-transform fuse has crossed
/// [`DEFAULT_TRANSFORM_DEATH_THRESHOLD`] distinct keys poisoned for it, and
/// quarantines it if so, in `txn` — the transaction that just poisoned its
/// key(s), so the count sees them before they commit.
///
/// **Concurrent evictions for one definition are serialized first** (issue
/// #159), by [`take_fuse_gate`], before the count runs. Reading `poison` from
/// `txn` is what makes this call see *its own* new rows, but it is also
/// exactly what made it blind to a *sibling* transaction's concurrent,
/// not-yet-committed ones: two workers each poisoning a key (say the 4th and
/// 5th) each counted 4, neither crossed the threshold-of-5, and the transform
/// stayed live past its fuse point until some later, unrelated eviction
/// happened to re-run the check. The gate's row lock forces those two into an
/// order; the second one through does not begin counting until the first has
/// committed, and — READ COMMITTED, one fresh snapshot per statement — its
/// count then includes the sibling's rows. Idempotent: the gate is a row lock
/// the transaction already holds when [`isolate_and_evict`] took it first.
///
/// Never relabels a freeze: a definition already frozen
/// ([`TransformStatus::Quarantined`] or [`TransformStatus::Paused`]) is left
/// alone (no write, no log line), including when the freeze landed after the
/// eviction read its candidates (issue #338, see [`quarantine_if_crossed`]).
///
/// `pub` rather than module-private only so the issue-#159 regression test
/// (`tests/quarantine.rs`) can drive two genuinely overlapping eviction
/// transactions through it directly: the race lives in the window between one
/// transaction's `poison` insert and its commit, which two full
/// `drain_once`/`isolate_and_evict` pipelines cannot be made to interleave
/// deterministically from the outside. [`isolate_and_evict`] takes the same
/// two steps itself, with the definition row's staleness check between them
/// (`evict_for`).
#[cfg(any(test, feature = "internals"))]
pub async fn trip_transform_fuse_if_crossed(
    txn: &Transaction<'_>,
    transform_id: i64,
) -> Result<(), ApplyError> {
    take_fuse_gate(txn, transform_id).await?;
    quarantine_if_crossed(txn, transform_id, &not_frozen_sql()).await?;
    Ok(())
}

/// A SQL predicate over a definition's row `t`: it isn't frozen. The states
/// a drain's eviction may quarantine a definition from: an applying one,
/// and one building (a relationship reader can be charged for the reverse
/// work of a to-side key before it goes live, #799).
fn not_frozen_sql() -> String {
    let frozen = [TransformStatus::Paused, TransformStatus::Quarantined]
        .map(|status| format!("'{}'", status.as_str()))
        .join(", ");
    format!("t.status not in ({frozen})")
}

/// The `poison_held.seg_seq` a build's parked re-derive ([`evict_build_key`])
/// is held under. No batch has it, and it sorts after every one, so a batch's
/// parked change merged into the same held row ([`HeldKey::merge`]) keeps its
/// pre-image there: [`release_key`] hands on the earliest parked change's
/// prior image, the image readers last saw, which the build's re-derive
/// doesn't know.
pub(crate) const BUILD_PARK_SEG_SEQ: i64 = i64::MAX;

/// Quarantines `key` of `src_table` (canonical) for the building definition
/// `transform_id`, because a backfill chunk narrowed its failure,
/// `last_error` with SQLSTATE `sqlstate`, to that key alone (`defs::chunk_queue::fail_chunk`, #616),
/// in the caller's transaction. It is the chunk-shaped counterpart of a
/// drain's eviction ([`isolate_and_evict`]):
///
/// - a `poison` row, so the build's chunks leave the key out of their writes
///   and, once the definition applies, the drain leaves it out of the
///   definition's apply;
/// - a death in `key_deaths`, with the error;
/// - a parked re-derive in `poison_held`, so [`release_key`] re-stages the
///   key as a re-derive once its cause is fixed. It carries the WAL insert
///   position as its origin, so, like a batch's parked change, it holds
///   every watermark token taken from then on until the release drains
///   (`converge`'s condition 4).
///
/// The key is evicted on its first failure, where a drain's needs
/// [`DEFAULT_DEATH_THRESHOLD`]: the chunk has already reproduced the failure
/// alone, on the key's current row. The whole-transform fuse is checked
/// once the caller has committed, by [`trip_build_fuse`].
pub(crate) async fn evict_build_key(
    txn: &Transaction<'_>,
    transform_id: i64,
    src_table: &str,
    key: &str,
    last_error: &str,
    sqlstate: Option<&str>,
) -> Result<(), ApplyError> {
    record_key_death(txn, transform_id, src_table, key, last_error).await?;
    txn.execute(
        "insert into poison (transform_id, src_table, key, last_error, sqlstate) \
         values ($1, $2, $3, $4, $5) \
         on conflict (transform_id, src_table, key) do update set \
             last_error = excluded.last_error, sqlstate = excluded.sqlstate, \
             poisoned_at = now()",
        &[&transform_id, &src_table, &key, &last_error, &sqlstate],
    )
    .await?;
    txn.execute(
        &format!(
            "insert into poison_held as h (transform_id, src_table, key, seg_seq, origin_lsn) \
             values ($1, $2, $3, $4, pg_current_wal_insert_lsn()) \
             {MERGE_HELD_ROW}"
        ),
        &[&transform_id, &src_table, &key, &BUILD_PARK_SEG_SEQ],
    )
    .await?;
    Ok(())
}

/// Checks the building definition `definition_id`'s whole-transform fuse
/// after a build quarantined one of its keys ([`evict_build_key`]), in a
/// transaction of its own. A build whose chunks fail on
/// [`DEFAULT_TRANSFORM_DEATH_THRESHOLD`] keys is quarantined like a live
/// definition whose drain does, so a failure that only looks like a data
/// error (it narrows to whichever key a chunk tries alone) stops after that
/// many keys. Only the building definition's own count is checked (#799):
/// its keys are poisoned for it alone. Returns whether it was quarantined.
///
/// Separate from the eviction's transaction so the fuse's gate is never taken
/// while the chunk queue holds the definition row, which the drain's fuse
/// takes after the gate.
pub(crate) async fn trip_build_fuse(pool: &Pool, definition_id: i64) -> Result<bool, ApplyError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    take_fuse_gate(&txn, definition_id).await?;
    let tripped = quarantine_if_crossed(&txn, definition_id, "t.status = 'backfilling'").await?;
    txn.commit().await?;
    Ok(tripped)
}

/// Quarantines definition `id` if the keys poisoned for it have crossed
/// [`DEFAULT_TRANSFORM_DEATH_THRESHOLD`]. Returns whether it did.
///
/// The caller nominated `id` from an earlier, unlocked read, so by the time
/// this runs the row may have moved on: an operator may have paused it, or
/// paused and resumed it (issue #338). The verdict is one conditional
/// `UPDATE` that re-checks, against the row as it is when the write happens,
/// both that the definition is still in a state the fuse may freeze and that
/// its count crosses the threshold. Under READ COMMITTED a row changed by a
/// transaction that commits while this `UPDATE` waits on it is re-evaluated
/// against the committed version, so a pause that lands mid-statement is seen
/// too. A paused definition therefore stays `paused`, the way
/// [`crate::defs::lifecycle::pause_transform`] leaves a quarantined one
/// `quarantined`: both triggers are the same freeze, and the label records
/// which one fired first. A resume deletes the definition's `poison` rows, so
/// a resumed definition's count starts again from zero (#799).
///
/// `may_freeze` is a SQL predicate over the definition's row `t` saying
/// which states it may be frozen from: any that isn't frozen
/// ([`not_frozen_sql`]) for a drain's eviction, `backfilling` for a build's
/// ([`trip_build_fuse`]).
async fn quarantine_if_crossed(
    txn: &Transaction<'_>,
    id: i64,
    may_freeze: &str,
) -> Result<bool, ApplyError> {
    const COUNT: &str = "(select count(*) from poison p where p.transform_id = t.id)";
    let tripped = txn
        .query_opt(
            &format!(
                "update transform_definitions t set status = $1 \
                 where t.id = $2 and {may_freeze} and {COUNT} >= $3 \
                 returning split_part(t.target_table, '.', 2), {COUNT}"
            ),
            &[
                &TransformStatus::Quarantined.as_str(),
                &id,
                &(DEFAULT_TRANSFORM_DEATH_THRESHOLD as i64),
            ],
        )
        .await?;
    let Some(row) = tripped else {
        return Ok(false);
    };
    let target: String = row.get(0);
    let poisoned_count: i64 = row.get(1);
    tracing::warn!(
        transform = %target,
        poisoned_count,
        "whole-transform fuse tripped; quarantining"
    );
    Ok(true)
}

// ---------------------------------------------------------------------
// Column-level fuse (docs/decisions/0003-quarantine-storage-and-api.md's
// 2026-09-12 amendment)
// ---------------------------------------------------------------------
//
// Everything below is a second, independent fuse tier, finer-grained than
// the row-level one above: it trips per `(transform, column)` instead of
// per key, so one broken calculated-field formula doesn't force every other
// healthy column on the same transform into quarantine. It never changes
// [`isolate_and_evict`]'s own return value or the row-level fuse's
// behavior — [`attribute_column_failure`] is pure side-effecting bookkeeping
// called *alongside* the existing per-key charge, and a definition with
// nothing currently paused pays only one extra small indexed lookup per
// batch (`paused_columns_for`, called from [`super::apply::compute`]).
//
// **Scope, stated up front**: this tier only ever activates for
// [`crate::defs::ast::KeySpace::OneToOne`] definitions (plain or
// relationship-enriched). [`crate::defs::ast::KeySpace::Aggregate`] fields
// are never attributed here and can never be paused by the automatic fuse —
// the aggregate ledger (`staging::ledger`) has no notion of "skip this one
// column and keep accumulating the others," and inventing one is a materially bigger
// project than this amendment. An aggregate transform's overall lifecycle
// status (the existing whole-transform fuse) is completely unaffected by
// this scope cut.

/// Attributes `err` to a `(transform, column)` pair and charges it toward
/// that pair's fuse, if `err` is specifically
/// [`crate::defs::eval::EvalError`] (a calculated-field failure — the only
/// kind [`crate::defs::eval::EvalError::field`] can name) coming from a
/// [`crate::defs::ast::KeySpace::OneToOne`] definition. Anything else (a DDL/
/// key-shape failure, a plain database error) is left entirely to the
/// existing row-level fuse — this function is a no-op for it.
///
/// Attribution is by field-name match against every definition
/// [`crate::defs::catalog::transforms_for_source`] returns for `src_table`
/// — [`super::apply::compute`] evaluates one definition's fields inside one
/// `eval::evaluate*` call and doesn't itself thread transform identity
/// through [`crate::defs::eval::EvalError`] (which would mean widening that
/// error type's public shape, and every existing construction site/test of
/// it, just for this one caller).
///
/// **Ambiguous match -> no attribution, deliberately.** If more than one
/// sibling `KeySpace::OneToOne` definition on `src_table` declares a field
/// named [`crate::defs::eval::EvalError::field`], there is no reliable way
/// to tell here which one actually produced `err` (only one may even be the
/// one that's broken). Guessing — e.g. "lowest id wins" — would be a
/// *deterministic misattribution*: every failure would land on the same
/// (possibly perfectly healthy) column every time, potentially freezing it
/// while the actually-broken sibling never accumulates a `column_status`
/// entry at all. That's worse than doing nothing, so this falls through to
/// the existing whole-row/transform-wide fuse (the row-level charge
/// [`isolate_and_evict`] already runs alongside this call, unaffected by
/// this function's return value either way) instead of attempting a fancier
/// disambiguation heuristic.
async fn attribute_column_failure(
    pool: &Pool,
    src_table: &str,
    key: &str,
    err: &ApplyError,
) -> Result<(), ApplyError> {
    let ApplyError::Eval(eval_err) = err else {
        return Ok(());
    };
    let field = eval_err.field();

    // Issue #281: qualify first — a bare `src_table` handed straight to
    // `transforms_for_source` returns an empty candidate set, so every column
    // failure on it fell through unattributed. See [`qualified_src_table`].
    let qualified = qualified_src_table(pool, src_table).await?;
    let candidates = catalog::transforms_for_source(pool, &qualified).await?;
    let mut matches = candidates.into_iter().filter(|def| {
        matches!(def.def.key_space, KeySpace::OneToOne)
            && def.def.fields.iter().any(|f| f.name == field)
    });
    let Some(def) = matches.next() else {
        return Ok(());
    };
    // See this function's doc comment ("Ambiguous match -> no attribution,
    // deliberately"): a second sibling definition matching the same field
    // name means attribution would be a guess, not a fact, so fall back to
    // the pre-existing row-level fuse rather than risk freezing the wrong
    // column.
    if matches.next().is_some() {
        return Ok(());
    }
    let transform = def.def.target;

    // Issue #283: `column_failures` is keyed on the canonical identity, not the
    // raw ring spelling — its primary key `(transform_table, column_name,
    // src_table, key)` is what makes `column_deaths` count *distinct* failing
    // rows, and a split key let one stubborn row charge that counter twice
    // (the one place the dual spelling over-counted rather than under-counted).
    charge_column_failure(pool, &transform, field, &qualified, key, &err.to_string()).await
}

/// Every column [`super::apply::compute`] must currently exclude from
/// evaluation/writes for `transform_table` — the read
/// [`super::apply::compute`] does once per definition per batch to implement
/// "freeze at the last successfully computed value" (decision: never null a
/// paused column out, never keep reattempting its already-fused formula).
/// Empty (the overwhelmingly common case) for a transform with nothing
/// paused.
///
/// Closed over `def`'s alias readers ([`AliasReaders::close`], issue #748):
/// a field reading a paused field by alias is held out too, so it keeps its
/// value rather than being evaluated over the paused field's absence and
/// written NULL. Its `column_status` row ([`cascade_pause`]) says the same
/// for `status`; the closure here also covers a reader whose row hasn't
/// committed yet ([`pause_column`] writes the paused field's row before its
/// cascade runs).
///
/// `defs::backfill`'s durable chunk-queue write path
/// (`write_one_to_one_range`/`backfill_relationship_one_to_one`) needs this
/// same exclusion for the same reason (a (re-)executed backfill chunk must
/// not overwrite a column live CDC has since paused) but runs its own copy
/// of the identical query (`defs::backfill::paused_columns_for`) rather than
/// calling this function — `defs` sits below `staging` in this crate's
/// layering (`staging::apply` already depends on `defs::backfill`, so the
/// reverse dependency would be circular) and that call site already holds a
/// plain `&Client` rather than a `&Pool`.
pub(super) async fn paused_columns_for(
    pool: &Pool,
    def: &TransformDef,
) -> Result<HashSet<String>, ApplyError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "select column_name from column_status where transform_table = $1",
            &[&def.target],
        )
        .await?;
    let mut paused: HashSet<String> = rows.into_iter().map(|row| row.get(0)).collect();
    if !paused.is_empty() {
        AliasReaders::of(def).close(&mut paused);
    }
    Ok(paused)
}

/// Records one distinct poisoned row's failure for `(transform, column)` and
/// charges its counter — but only if `(src_table, key)` hasn't already been
/// recorded for this exact pair (`column_failures`' primary key makes the
/// insert idempotent). This is what makes the column fuse count *distinct
/// poisoned rows*, not raw retry attempts: a single stubborn key retried
/// across several `drain_once` attempts (the row-level fuse's own bread and
/// butter — see `quarantine.rs`'s existing
/// `repeated_real_failures_cross_the_eviction_threshold_and_the_batch_still_drains`
/// test) charges this counter exactly once, no matter how many times it's
/// probed, so it can never cross this fuse's threshold alone — only a real
/// *breadth* of distinct failing rows can. Trips the fuse
/// ([`trip_column_fuse`]) once the count reaches
/// [`DEFAULT_COLUMN_DEATH_THRESHOLD`], and again on a row charged already
/// while the count is still there, since only a trip that failed leaves it
/// there.
async fn charge_column_failure(
    pool: &Pool,
    transform: &str,
    column: &str,
    src_table: &str,
    key: &str,
    error: &str,
) -> Result<(), ApplyError> {
    let client = pool.get().await?;
    let inserted = client
        .execute(
            "insert into column_failures (transform_table, column_name, src_table, key, error) \
             values ($1, $2, $3, $4, $5) \
             on conflict (transform_table, column_name, src_table, key) do nothing",
            &[&transform, &column, &src_table, &key, &error],
        )
        .await?;
    if inserted == 0 {
        // This row was charged already. If its charge reached the threshold
        // and the trip then failed, the count is still at or past it: the
        // trip resets it in the transaction that writes the pause, and that
        // transaction's first lock is a fence bump that can wait out the
        // lock timeout behind a writer in flight (issue #903). Trip now,
        // rather than wait for another row's failure.
        let deaths: Option<i32> = client
            .query_opt(
                "select deaths from column_deaths \
                 where transform_table = $1 and column_name = $2",
                &[&transform, &column],
            )
            .await?
            .map(|row| row.get(0));
        if deaths.is_some_and(|deaths| deaths >= DEFAULT_COLUMN_DEATH_THRESHOLD) {
            trip_column_fuse(pool, transform, column, error).await?;
        }
        return Ok(());
    }

    let row = client
        .query_one(
            "insert into column_deaths (transform_table, column_name, deaths, last_error, last_death_at) \
             values ($1, $2, 1, $3, now()) \
             on conflict (transform_table, column_name) do update set \
                 deaths = column_deaths.deaths + 1, \
                 last_error = excluded.last_error, \
                 last_death_at = now() \
             returning deaths",
            &[&transform, &column, &error],
        )
        .await?;
    let deaths: i32 = row.get(0);
    if deaths >= DEFAULT_COLUMN_DEATH_THRESHOLD {
        trip_column_fuse(pool, transform, column, error).await?;
    }
    Ok(())
}

/// Trips the column fuse for `(transform, column)`: pauses it
/// (`column_status`, `local_fuse = true` — this pair's *own* fuse tripped,
/// as opposed to a pause it only inherited via [`cascade_pause`]), resets
/// its counter, and cascades the pause to every dependent reader
/// (decision #5).
///
/// Deliberately does **not** clear `column_failures` here (unlike the
/// row-level fuse's `key_deaths`, which the counter reset above otherwise
/// mirrors): those rows are [`Trellis::sample_quarantined`]'s data source
/// for a `transform.column` target, and clearing them at the exact moment
/// the fuse trips would erase the evidence right when a caller most wants to
/// see it (diagnosing *why* it paused). Nothing needs them cleared to stay
/// correct either — once paused, [`super::apply::compute`] excludes this
/// column from evaluation entirely, so no *new* failure can be attributed to
/// it while it stays paused. [`resume_column`] is what actually clears
/// `column_failures`, once the column is live again and eligible to start
/// accumulating a fresh set.
async fn trip_column_fuse(
    pool: &Pool,
    transform: &str,
    column: &str,
    last_error: &str,
) -> Result<(), ApplyError> {
    tracing::warn!(
        transform = %transform,
        column = %column,
        last_error = %last_error,
        "column fuse tripped; pausing it and cascading the pause to its dependents"
    );
    let fence = pause_fence(pool, transform).await?;
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    bump_pause_fence(&*txn, fence.as_deref()).await?;
    crate::locks::lock_column_pauses(&*txn, ColumnPauseLock::Exclusive, ColumnPauseOp::Fuse)
        .await?;
    let mark: String = txn
        .query_one(
            "insert into column_status \
                 (transform_table, column_name, paused_at, last_error, local_fuse, cascade_pending) \
             values ($1, $2, now(), $3, true, true) \
             on conflict (transform_table, column_name) do update set \
                 local_fuse = true, last_error = excluded.last_error, cascade_pending = true \
             returning xmin::text",
            &[&transform, &column, &last_error],
        )
        .await?
        .get(0);
    txn.execute(
        "delete from column_deaths where transform_table = $1 and column_name = $2",
        &[&transform, &column],
    )
    .await?;
    txn.commit().await?;

    cascade_pause(pool, transform, column, &mark, None).await
}

/// The source whose version fence a `column_status` pause of `transform`
/// bumps ([`bump_pause_fence`]): its definition's, when it is a 1-1
/// definition, since only a 1-1 plan holds a paused column out of its writes
/// (`super::apply::compute`). `None` for an aggregate's column, or a target
/// no definition has.
async fn pause_fence(pool: &Pool, transform: &str) -> Result<Option<String>, ApplyError> {
    Ok(catalog::definition_by_target(pool, transform)
        .await?
        .filter(|def| matches!(def.def.key_space, KeySpace::OneToOne))
        .map(|def| def.source_table))
}

/// Bumps `source`'s version fence (`super::build::bump_version_fence`), when
/// there is one, in the transaction that writes a `column_status` pause:
/// [`pause_column`], [`trip_column_fuse`] and each pair [`cascade_pause`]
/// reaches (issue #903). The pause holds the column out of every write from
/// its commit on, not just out of the writers that start after it:
///
/// - A page reads the paused columns in Phase 2, after it reads the fence,
///   and Phase 3 holds the fence `for share` to its commit. One that read
///   the column as live and reaches the fence after the pause commits
///   misses it and computes again, with the column left out. One that
///   holds the fence already commits before the pause does, since the bump
///   waits for it.
/// - A 1-1 build chunk takes the fence `for share` first thing in its
///   transaction (`super::build::run_rederive`), so it either commits before
///   the pause or finds the fence moved and plans again.
///
/// So `PAUSE` returns with no write of the column still to land. The bump is
/// the transaction's first lock, as for a column resume (issue #744): it
/// waits for the writers in flight, and holding anything they might wait on
/// meanwhile could close a cycle.
async fn bump_pause_fence(
    txn: &impl GenericClient,
    source: Option<&str>,
) -> Result<(), ApplyError> {
    if let Some(source) = source {
        super::build::bump_version_fence(txn, source).await?;
    }
    Ok(())
}

/// Pauses every direct and transitive dependent of `(transform, column)`
/// (decision #5: a transform reading a paused column's output must also
/// pause, rather than silently consume a frozen/stale value with no
/// signal), including a field of the same definition that reads it by
/// alias (issue #748) — a breadth-first walk over
/// [`crate::defs::catalog::column_dependents`], recorded into
/// `column_pause_cascades` so [`resume_column`] can later tell a purely
/// cascaded pause apart from one with its own independent (`local_fuse`)
/// reason to stay paused. Iterative, not recursive: `docs/transforms.md`'s
/// "Chaining and cycle detection" guarantees the underlying dependency graph
/// is acyclic, so a queue-based walk always terminates, without needing
/// `async fn` self-recursion's `Box::pin` boilerplate.
///
/// Each dependent's edge and row commit together, fenced against its
/// definition's writers in flight ([`bump_pause_fence`]), so a pair can wait
/// out the lock timeout and fail the walk part-way (issue #912). The walk is
/// idempotent, so running it again finishes the job:
///
/// - It walks every dependent's dependents, whether this walk paused it or
///   found it paused already, once each (a visited set). A dependent already
///   paused (by an earlier cascade, or its own `local_fuse`) still gets this
///   walk's edge recorded, so un-cascading `transform`/`column` later can't
///   resume it out from under a *different* still-live reason.
/// - A pair whose edge and row are both there already is skipped without a
///   transaction: a retry doesn't bump the fences of the pairs it already
///   reached, nor wait on their writers again.
/// - A pair commits only while its upstream column is still paused, read
///   under the column-pause lock, taken after the fence bump, so a walk
///   racing that column's [`resume_column`] can't pause a reader the resume
///   already released. The walk goes no further down that branch.
/// - The same read, under the same lock, checks the reader's definition
///   still has the field and the field still reads the upstream column, as
///   the walk listed it before any lock and an edit may have committed
///   since (issue #955). A pair that fails it is skipped the same way.
///
/// A pause records the walk as owed (`column_status.cascade_pending`) in
/// the transaction that writes its own row, and the walk clears it once it
/// has reached every dependent. [`complete_pause_cascades`] finishes the
/// walk of any pause still marked.
///
/// `mark` is the root row's `xmin` as the walk's caller read it: the
/// pause's own commit, or [`complete_pause_cascades`]'s read of the marked
/// row. The walk clears the mark only while the row still has that `xmin`.
/// A pause of the same column that commits while this walk runs writes the
/// row again, so its mark outlives this walk and stays until a walk started
/// after it finishes; a walk that started earlier may have read the graph
/// before a reader only the later one reaches. A row written for any other
/// reason meanwhile keeps its mark too, and the next capture pass walks it
/// again, which costs a read per pair.
///
/// `lock_timeout`, when set, caps each pair's lock waits below the
/// session's ([`complete_pause_cascades`] runs on the staging worker's
/// maintenance loop, which nothing else seals for).
async fn cascade_pause(
    pool: &Pool,
    transform: &str,
    column: &str,
    mark: &str,
    lock_timeout: Option<std::time::Duration>,
) -> Result<(), ApplyError> {
    let mut visited: HashSet<(String, String)> = HashSet::new();
    let mut queue: VecDeque<(String, String)> = VecDeque::new();
    visited.insert((transform.to_string(), column.to_string()));
    queue.push_back((transform.to_string(), column.to_string()));

    while let Some((upstream_transform, upstream_column)) = queue.pop_front() {
        let deps = catalog::column_dependents(pool, &upstream_transform, &upstream_column).await?;
        // Test-only pause point (#955). See `super::interleave`. The walk
        // holds nothing here, and nothing but the pair's own lock waits
        // keeps an edit of a reader it listed from committing.
        #[cfg(any(test, feature = "test-util"))]
        if !deps.is_empty() {
            // In a transaction of its own: the hook lifts the session's
            // lock timeout for the pause with `set_config(..., true)`.
            let mut client = pool.get().await?;
            let txn = client.transaction().await?;
            super::interleave::pause_at(
                &*txn,
                super::interleave::PausePoint::AfterCascadeDependentsRead,
                &upstream_transform,
            )
            .await?;
            txn.commit().await?;
        }
        for (downstream_transform, downstream_column) in deps {
            if !cascaded_already(
                pool,
                (&downstream_transform, &downstream_column),
                (&upstream_transform, &upstream_column),
            )
            .await?
                && !pause_dependent(
                    pool,
                    (&downstream_transform, &downstream_column),
                    (&upstream_transform, &upstream_column),
                    lock_timeout,
                )
                .await?
            {
                // The upstream column was resumed under this walk.
                continue;
            }
            let pair = (downstream_transform, downstream_column);
            if visited.insert(pair.clone()) {
                queue.push_back(pair);
            }
        }
    }

    pool.get()
        .await?
        .execute(
            "update column_status set cascade_pending = false \
             where transform_table = $1 and column_name = $2 and cascade_pending \
               and xmin::text = $3",
            &[&transform, &column, &mark],
        )
        .await?;
    Ok(())
}

/// Whether [`cascade_pause`] reached `downstream` from `upstream` already:
/// its edge and its row commit together, so both being there means the
/// pair's transaction committed and nothing has resumed it since.
async fn cascaded_already(
    pool: &Pool,
    (downstream_transform, downstream_column): (&str, &str),
    (upstream_transform, upstream_column): (&str, &str),
) -> Result<bool, ApplyError> {
    Ok(pool
        .get()
        .await?
        .query_opt(
            "select 1 from column_pause_cascades e \
             join column_status s \
               on s.transform_table = e.downstream_transform \
              and s.column_name = e.downstream_column \
             where e.downstream_transform = $1 and e.downstream_column = $2 \
               and e.upstream_transform = $3 and e.upstream_column = $4",
            &[
                &downstream_transform,
                &downstream_column,
                &upstream_transform,
                &upstream_column,
            ],
        )
        .await?
        .is_some())
}

/// One pair of [`cascade_pause`]: records the edge from `upstream` and
/// pauses `downstream`, in a transaction whose first lock bumps
/// `downstream`'s definition's source fence ([`bump_pause_fence`]), so no
/// transaction holds two sources' fences. It then takes the column-pause lock
/// exclusive ([`crate::locks::lock_column_pauses`]), as every write of a
/// pause does, so a definition being defined or edited to read
/// `downstream`'s column either reads this pause or commits before the walk
/// goes on to read the graph (issue #914). False, with nothing written,
/// when `upstream` is no longer paused: the read of its row is made under
/// the lock, so a [`resume_column`] deleting it has committed already, and
/// one that comes later waits for this commit and then deletes the edge with
/// the rest. Also false when `downstream` no longer reads `upstream`
/// ([`catalog::reads_column`]): the walk listed it before it took any lock,
/// and an `ALTER TRANSFORM` of its definition may have dropped the field or
/// its read since (issue #955).
async fn pause_dependent(
    pool: &Pool,
    (downstream_transform, downstream_column): (&str, &str),
    (upstream_transform, upstream_column): (&str, &str),
    lock_timeout: Option<std::time::Duration>,
) -> Result<bool, ApplyError> {
    let fence = pause_fence(pool, downstream_transform).await?;
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    if let Some(timeout) = lock_timeout {
        crate::locks::set_local_lock_timeout(&*txn, timeout).await?;
    }
    bump_pause_fence(&*txn, fence.as_deref()).await?;
    // Test-only pause point (#912). See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        &*txn,
        super::interleave::PausePoint::AfterCascadeFenceBump,
        downstream_transform,
    )
    .await?;
    crate::locks::lock_column_pauses(&*txn, ColumnPauseLock::Exclusive, ColumnPauseOp::Cascade)
        .await?;
    let upstream_paused = txn
        .query_opt(
            "select 1 from column_status \
             where transform_table = $1 and column_name = $2",
            &[&upstream_transform, &upstream_column],
        )
        .await?
        .is_some();
    // The walk listed this reader before it took any lock, so an edit of
    // the reader's definition may have committed since (#955). Every edit
    // holds the lock exclusive, so what is committed now stays so until this
    // pair commits.
    if !upstream_paused
        || !catalog::reads_column(
            &*txn,
            (downstream_transform, downstream_column),
            (upstream_transform, upstream_column),
        )
        .await?
    {
        txn.rollback().await?;
        return Ok(false);
    }
    txn.execute(
        "insert into column_pause_cascades \
             (downstream_transform, downstream_column, upstream_transform, upstream_column) \
         values ($1, $2, $3, $4) \
         on conflict do nothing",
        &[
            &downstream_transform,
            &downstream_column,
            &upstream_transform,
            &upstream_column,
        ],
    )
    .await?;
    let newly_paused = txn
        .execute(
            "insert into column_status (transform_table, column_name, paused_at, last_error, local_fuse) \
             values ($1, $2, now(), $3, false) \
             on conflict (transform_table, column_name) do nothing",
            &[
                &downstream_transform,
                &downstream_column,
                &cascade_reason(upstream_transform, upstream_column),
            ],
        )
        .await?;
    txn.commit().await?;
    if newly_paused > 0 {
        tracing::warn!(
            transform = %downstream_transform,
            column = %downstream_column,
            upstream_transform = %upstream_transform,
            upstream_column = %upstream_column,
            "column paused via cascade from an upstream pause"
        );
    }
    Ok(true)
}

/// Finishes the cascade of every column pause still marked as owing one
/// (`column_status.cascade_pending`, issue #912). A pause writes its own row
/// and the mark in one transaction, then walks its dependents
/// ([`cascade_pause`]), each pair in a transaction of its own whose fence
/// bump can wait out the lock timeout behind a writer in flight. A walk that
/// fails part-way, or whose process dies, leaves the mark, and a reader of
/// the paused column unpaused (decision #5). An operator's `PAUSE` returns
/// the error and can be re-run, but a column fuse trip has no caller to
/// retry it, and its column, now paused, stops failing, so no new charge
/// trips it again. An `ALTER TRANSFORM` that pauses a field at birth
/// (issue #915) walks it once it commits ([`cascade_edit_pauses`]), but the
/// edit has already committed, so a walk that fails there leaves its mark
/// to this.
///
/// The staging worker's capture pass calls this each pass, as it releases
/// the keys an in-place re-type held. That pass runs on the maintenance
/// loop, which nothing else seals for, so each pair's lock waits here are
/// capped at [`CASCADE_COMPLETION_LOCK_TIMEOUT`] rather than the session's
/// 30 s: a writer that still holds a reader's fence fails that pause's walk
/// after a second, and the walk is tried again next pass. A walk that
/// fails is logged, and the rest still run; its mark stays until a walk
/// finishes. Returns how many walks finished. Only reading the marks
/// fails the call.
pub async fn complete_pause_cascades(pool: &Pool) -> Result<usize, ApplyError> {
    walk_marked_pauses(pool, None, Some(CASCADE_COMPLETION_LOCK_TIMEOUT)).await
}

/// Walks the cascade of each of `transform`'s column pauses marked as owing
/// one (`column_status.cascade_pending`), right after an `ALTER TRANSFORM`
/// of `transform` that marked them commits (issue #915), as [`pause_column`]
/// walks its own pause right after its commit. The edit's own transaction
/// can't run the walk: it holds its definition's source fence, and each
/// reader's pair bumps that reader's own ([`pause_dependent`]). So the walk
/// runs here, with nothing else held, and its readers stop applying the
/// edited field's frozen value by the time the edit returns.
///
/// The edit has committed by then, so it shouldn't fail for its walk: a walk
/// that fails is logged and keeps its mark, which the capture pass's
/// [`complete_pause_cascades`] finishes. Each pair waits for its locks as
/// long as the session's lock timeout, as a `PAUSE`'s walk does. Returns how
/// many walks finished. Only reading the marks fails the call.
pub async fn cascade_edit_pauses(pool: &Pool, transform: &str) -> Result<usize, ApplyError> {
    walk_marked_pauses(pool, Some(transform), None).await
}

/// Walks ([`cascade_pause`]) each column pause marked as owing its cascade,
/// of `transform` when given and of every target otherwise, in
/// `(transform, column)` order, with each pair's lock waits capped at
/// `lock_timeout` when given. Logs a walk that fails and goes on to the
/// next. Returns how many walks finished.
async fn walk_marked_pauses(
    pool: &Pool,
    transform: Option<&str>,
    lock_timeout: Option<std::time::Duration>,
) -> Result<usize, ApplyError> {
    let pending: Vec<(String, String, String)> = pool
        .get()
        .await?
        .query(
            "select transform_table, column_name, xmin::text from column_status \
             where cascade_pending and ($1::text is null or transform_table = $1) \
             order by transform_table, column_name",
            &[&transform],
        )
        .await?
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect();
    let mut finished = 0;
    for (transform, column, mark) in &pending {
        tracing::info!(
            transform = %transform,
            column = %column,
            "walking a column pause's owed cascade"
        );
        match cascade_pause(pool, transform, column, mark, lock_timeout).await {
            Ok(()) => finished += 1,
            Err(err) => tracing::warn!(
                transform = %transform,
                column = %column,
                error = %err,
                "couldn't finish a column pause's cascade; the capture pass retries it"
            ),
        }
    }
    Ok(finished)
}

/// How long each pair of a walk [`complete_pause_cascades`] runs waits for a
/// lock (its fence bump, behind the writers in flight) before the walk gives
/// up until the next pass. The same order as `RETYPE_LOCK_TIMEOUT`, the
/// staging worker's other wait on a writer: long enough for a page or a
/// build chunk to finish, short enough that a writer holding a fence for
/// the whole session lock timeout can't stall sealing for as long.
pub const CASCADE_COMPLETION_LOCK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

/// Pauses one calculated column deliberately — the operator-driven trigger
/// for the same column-pause state [`trip_column_fuse`] reaches automatically,
/// exposed through the grammar as `PAUSE TRANSFORM <target>.<column>` (issue
/// #227; issue #228, decision 2).
///
/// ADR-0014's pause is "one state, two triggers", and that is as true at
/// column granularity as at whole-transform granularity: this writes the same
/// `column_status` row the fuse writes and runs the same [`cascade_pause`]
/// over dependent readers, so a paused column freezes at its current value, is
/// skipped by [`super::apply::compute`]'s evaluation, and is recovered by the
/// same [`resume_column`] (which re-derives it across every existing row).
/// Nothing here is a second freezing mechanism.
///
/// **Frozen from the moment it returns** (issue #903). The transaction that
/// writes the column's row bumps its definition's source's version fence
/// first, and so does each pair [`cascade_pause`] reaches
/// ([`bump_pause_fence`]). A page or build chunk that read the column as
/// live before the pause either commits before the pause does, or finds the
/// fence moved and plans again with the column left out. So no write of the
/// column lands after the call returns, and the call waits for the writers
/// in flight, as a column resume does.
///
/// **A reader defined after it** (issue #914). Define and `ALTER TRANSFORM`
/// pause a field that reads a column paused when they run, with the row and
/// edge [`cascade_pause`] would write. The transaction that writes the
/// column's row takes the column-pause lock exclusive after its fence bump
/// ([`crate::locks::lock_column_pauses`]); a define reading paused columns
/// holds it shared to its commit. So a define either reads this pause, or
/// commits before it and is in the graph [`cascade_pause`] walks. An edit
/// that pauses a field which already has readers takes it exclusive too,
/// and marks the field as owing its cascade (issue #915), which it walks
/// once it commits ([`cascade_edit_pauses`]).
///
/// **`local_fuse` is set even though no fuse tripped.** That column records
/// "this pair has a reason of its own to stay paused", as opposed to a pause
/// merely inherited via [`cascade_pause`] — which is exactly true of an
/// operator pause, and is what stops [`resume_column`] on some *upstream*
/// column from un-pausing this one out from under the operator who paused it.
/// The trade-off: `column_status.last_error` is left null, so a reader
/// ([`crate::Trellis::quarantine_status`]) sees this the same way it sees a
/// cascaded pause — paused, with no error of its own. That matches the
/// whole-transform pause, which likewise records the frozen state without
/// recording who asked for it.
///
/// Idempotent, per ADR-0014: pausing an already-paused column (by an earlier
/// pause, by its own fuse, or purely by cascade) succeeds, upgrading a
/// cascade-only pause to one with its own reason and re-walking the cascade
/// (itself idempotent). Pause runs on Trellis's own connections rather than
/// inside a caller's migration transaction, so a replayed migration has to be
/// safe to re-run.
///
/// Whether `transform` and `column` actually exist is checked by the caller
/// ([`crate::Trellis::apply`]), which has the catalog reads and the
/// [`crate::TrellisError`] variants for it — `column_status` has no foreign
/// key onto either (see `V22__column_status_drops_target_table_fkey.sql`), so
/// this function would otherwise happily park a row naming nothing.
#[tracing::instrument(
    name = "quarantine.pause_column",
    skip(pool),
    fields(transform = %transform, column = %column)
)]
pub async fn pause_column(pool: &Pool, transform: &str, column: &str) -> Result<(), ApplyError> {
    let mark = {
        let fence = pause_fence(pool, transform).await?;
        let mut client = pool.get().await?;
        let txn = client.transaction().await?;
        bump_pause_fence(&*txn, fence.as_deref()).await?;
        crate::locks::lock_column_pauses(&*txn, ColumnPauseLock::Exclusive, ColumnPauseOp::Pause)
            .await?;
        let mark: String = txn
            .query_one(
                "insert into column_status \
                     (transform_table, column_name, paused_at, local_fuse, cascade_pending) \
                 values ($1, $2, now(), true, true) \
                 on conflict (transform_table, column_name) do update set \
                     local_fuse = true, cascade_pending = true \
                 returning xmin::text",
                &[&transform, &column],
            )
            .await?
            .get(0);
        txn.commit().await?;
        mark
    };
    tracing::info!(
        transform = %transform,
        column = %column,
        "column paused by request; cascading the pause to its dependents"
    );
    cascade_pause(pool, transform, column, &mark, None).await
}

/// Resumes a paused column: clears its counter and its `column_status` row
/// and outgoing cascade edges, starts a field build that rewrites its value
/// across every existing row (#625 F8b), then un-cascades every dependent
/// this pause reached — but only a dependent with *no other* remaining
/// reason to stay paused (no `local_fuse` of its own, and no other live
/// `column_pause_cascades` edge into it — decision: careful not to un-pause
/// a dependent that has its own independent reason to stay paused). Returns
/// every `(transform, column)` pair actually resumed, `transform`/`column`
/// itself first, in the order resumed.
///
/// **Sibling readers** (issue #748). A field of the same definition that
/// reads a resumed field by alias was paused with it ([`cascade_pause`]).
/// One with no other reason to stay paused is released in the same
/// transaction, and the field build covers every such reader, so it is
/// rebuilt from the resumed field rather than left frozen at its value from
/// before the pause. One that also reads a field still paused stays paused
/// (its other cascade edge), and the build's chunks leave it out
/// ([`paused_columns_for`]'s closure) until that field's resume.
///
/// **The field build** (`super::build::start_field_build`). Each 1-1 pair's
/// unpause and its field build's registration commit together: the
/// definition moves `live -> backfilling` (one already under a Re-derive
/// build keeps building) and the drain workers rewrite the column in the
/// background, so the call returns at once (#666) and the definition reads
/// `backfilling` until the column is rebuilt. The column applies from that
/// commit, as a new definition does under the Re-derive build (#625 B1):
/// every page after it writes the column from its change's image, and each
/// chunk rewrites it under the keys' entry lock, so a row changed while the
/// build runs needs no catch-up. An aggregate's column is never held out of
/// Apply (only a 1-1 plan drops paused columns, `apply::compute`), so its
/// resume only unpauses it.
///
/// Errors with [`ApplyError::ColumnNotPaused`] if `(transform, column)`
/// isn't currently paused — resuming a live column is caller error, not a
/// silent no-op — and with [`ApplyError::ColumnAwaitingCapture`] if it is
/// an `ALTER TRANSFORM` field still awaiting its capture widen (#687). A
/// dependent in that state reached through the cascade stays paused too,
/// and so do its own readers, until its field build's start releases it and
/// un-cascades from it (issue #918, `super::build::release_awaiting_capture`).
/// A field that reads another field of its definition still paused, by
/// alias, stays paused (issue #748): the resume drops only its own reason
/// (`local_fuse`), leaves the cascade's, and resumes nothing. The resume of
/// the field it reads releases and rebuilds it.
///
/// Errors with [`ApplyError::DefinitionNotLive`] — with no side effects at
/// all, checked before any of this function's deletes run — if `transform`
/// doesn't apply (`super::build::takes_field_build`, a field build's
/// precondition: `live`, `catching_up`, or under a Re-derive build). A
/// definition still behind an old-path build is the concrete case: its rows
/// are still being written by a build that skips the paused column. The same
/// check applies per-pair inside the cascade queue below, and a downstream
/// pair is not an abort of the whole call — by the time it is reached, any
/// upstream pairs earlier in the queue have already been fully resumed and
/// committed, so there is nothing left to roll back. A pair whose definition
/// has not started building (`waiting_to_backfill`) is released by deleting
/// its row: its build reads the paused set when its chunks run, so it writes
/// the column without a field build (issue #916). One whose definition is
/// mid-build on the old path, or frozen, can't be released, and the walk has
/// deleted its edge: it is held as a pause of its own with a reason that says
/// so ([`hold_orphaned_pause`]), for its `RESUME` once the definition
/// applies, and logged.
#[tracing::instrument(
    name = "quarantine.resume_column",
    skip(pool),
    fields(transform = %transform, column = %column, resumed = tracing::field::Empty)
)]
pub async fn resume_column(
    pool: &Pool,
    transform: &str,
    column: &str,
) -> Result<Vec<(String, String)>, ApplyError> {
    {
        let client = pool.get().await?;
        let awaiting_capture: Option<bool> = client
            .query_opt(
                "select awaiting_capture from column_status \
                 where transform_table = $1 and column_name = $2",
                &[&transform, &column],
            )
            .await?
            .map(|row| row.get(0));
        match awaiting_capture {
            None => {
                return Err(ApplyError::ColumnNotPaused {
                    transform: transform.to_string(),
                    column: column.to_string(),
                });
            }
            // #687: only the widened capture's catch-up may unpause it.
            Some(true) => {
                return Err(ApplyError::ColumnAwaitingCapture {
                    transform: transform.to_string(),
                    column: column.to_string(),
                });
            }
            Some(false) => {}
        }

        // Gate before any mutation: resuming is all-or-nothing, so a
        // blocked resume must leave `column_deaths`/`column_failures`/
        // `column_status` untouched, not partially cleaned up. A missing
        // definition (a dangling `column_status` row with nothing left in
        // the catalog) isn't this function's problem to police — fall
        // through and let the loop below's own lookup handle it the way it
        // already does.
        if let Some(def) = catalog::definition_by_target(pool, transform).await? {
            let build: Option<String> = client
                .query_one(
                    "select build from transform_definitions where id = $1",
                    &[&def.id],
                )
                .await?
                .get(0);
            if !super::build::takes_field_build(def.status, build.as_deref()) {
                return Err(ApplyError::DefinitionNotLive {
                    transform: transform.to_string(),
                });
            }
            // #708: the field rebuilds from the live schema, so it resumes
            // only while define would accept its definition.
            refuse_unless_valid(pool, transform, &def).await?;
        }

        client
            .execute(
                "delete from column_deaths where transform_table = $1 and column_name = $2",
                &[&transform, &column],
            )
            .await?;
        client
            .execute(
                "delete from column_failures where transform_table = $1 and column_name = $2",
                &[&transform, &column],
            )
            .await?;

        // Whether it stays paused with a sibling, read and acted on under
        // the column-pause lock (#922): a resume of that sibling running at
        // the same time reads this column's `local_fuse` to decide whether
        // to release it. Without the lock, this could read the sibling
        // paused, that resume read `local_fuse` still set and keep this
        // column paused, and this then clear it: a pause with no reason
        // left, which no resume of the sibling would release. It changes
        // no paused state, so it takes no fence.
        let mut locked = pool.get().await?;
        let txn = locked.transaction().await?;
        crate::locks::lock_column_pauses(&*txn, ColumnPauseLock::Exclusive, ColumnPauseOp::Resume)
            .await?;
        if reads_paused_sibling(&*txn, pool, transform, column).await? {
            // Test-only pause point (#922). See `super::interleave`.
            #[cfg(any(test, feature = "test-util"))]
            super::interleave::pause_at(
                &*txn,
                super::interleave::PausePoint::BeforeSiblingHeldResume,
                transform,
            )
            .await?;
            txn.execute(
                "update column_status set local_fuse = false \
                 where transform_table = $1 and column_name = $2",
                &[&transform, &column],
            )
            .await?;
            txn.commit().await?;
            tracing::info!(
                transform = %transform,
                column = %column,
                "column reads a field still paused; it stays paused with it"
            );
            return Ok(Vec::new());
        }
        txn.rollback().await?;
    }

    let queue = VecDeque::from([(transform.to_string(), column.to_string())]);
    let resumed = resume_pairs(pool, transform, queue).await?;

    tracing::Span::current().record("resumed", resumed.len());
    tracing::info!(resumed = ?resumed, "resumed paused column(s)");
    Ok(resumed)
}

/// The reason (`column_status.last_error`) a column paused by the cascade
/// from `upstream_transform.upstream_column` gives.
pub(crate) fn cascade_reason(upstream_transform: &str, upstream_column: &str) -> String {
    format!("paused because upstream column '{upstream_transform}.{upstream_column}' is paused")
}

/// Restates the reason `transform.column` gives for its pause once one of
/// the edges into it is gone and others are left (issue #950): if its
/// `last_error` names an upstream column it no longer has an edge from, it
/// names the first one it still has. A row with a reason of its own
/// (`local_fuse`), one awaiting its capture, and one with no edge left are
/// left as they are. The caller holds the column-pause lock.
pub(crate) async fn restate_cascade_reason(
    txn: &impl GenericClient,
    transform: &str,
    column: &str,
) -> Result<(), tokio_postgres::Error> {
    let rows = txn
        .query(
            "select s.last_error, c.upstream_transform, c.upstream_column \
             from column_status s \
             join column_pause_cascades c \
               on c.downstream_transform = s.transform_table \
              and c.downstream_column = s.column_name \
             where s.transform_table = $1 and s.column_name = $2 \
               and not s.local_fuse and not s.awaiting_capture \
             order by c.upstream_transform, c.upstream_column",
            &[&transform, &column],
        )
        .await?;
    let reasons: Vec<String> = rows
        .iter()
        .map(|row| cascade_reason(row.get(1), row.get(2)))
        .collect();
    let Some(first) = reasons.first() else {
        return Ok(());
    };
    let current: Option<String> = rows[0].get(0);
    if current.is_some_and(|current| reasons.contains(&current)) {
        return Ok(());
    }
    txn.execute(
        "update column_status set last_error = $3 \
         where transform_table = $1 and column_name = $2",
        &[&transform, &column, first],
    )
    .await?;
    Ok(())
}

/// What [`uncascade`] released.
pub(crate) struct Uncascaded {
    /// The columns of the walked definition no longer paused: the seeds,
    /// and every sibling released with them.
    pub released: Vec<String>,
    /// Readers in other definitions whose last reason to stay paused went
    /// with the edges deleted. Their rows are still there, for
    /// [`resume_pairs`].
    pub dependents: Vec<(String, String)>,
}

/// Un-cascades from `seeds`, columns of `target` whose pause is gone (the
/// caller deleted their rows), in `txn`: deletes the edges that leave them,
/// and releases each reader left with no reason to stay paused (no
/// `local_fuse`, no other edge into it, and not awaiting its capture,
/// which its own field build's start releases, #687). A reader in `target`
/// is released in `txn` and walked from in turn (issue #748); one in
/// another definition is returned in [`Uncascaded::dependents`]. A reader
/// still paused through another edge has its reason restated
/// ([`restate_cascade_reason`]).
///
/// Two paths end a column's pause this way, and both must: a resume
/// ([`resume_column`]), and the release of a field that awaited its capture
/// (`super::build::release_awaiting_capture`, issue #918), which may have
/// passed a pause on to readers after the resume that took its own edge
/// away left it in place.
pub(crate) async fn uncascade(
    txn: &impl GenericClient,
    target: &str,
    seeds: Vec<String>,
) -> Result<Uncascaded, tokio_postgres::Error> {
    let mut released = seeds.clone();
    let mut frontier = seeds;
    let mut dependents = Vec::new();
    while let Some(upstream) = frontier.pop() {
        let affected = txn
            .query(
                "delete from column_pause_cascades \
                 where upstream_transform = $1 and upstream_column = $2 \
                 returning downstream_transform, downstream_column",
                &[&target, &upstream],
            )
            .await?;
        for row in affected {
            let downstream_transform: String = row.get(0);
            let downstream_column: String = row.get(1);
            let status = txn
                .query_opt(
                    "select local_fuse, awaiting_capture from column_status \
                     where transform_table = $1 and column_name = $2",
                    &[&downstream_transform, &downstream_column],
                )
                .await?;
            let Some(status) = status else {
                // Already resumed by some other path (shouldn't happen
                // within one walk, but tolerate it rather than panic).
                continue;
            };
            let (local_fuse, awaiting_capture): (bool, bool) = (status.get(0), status.get(1));
            // #687: a field awaiting its capture widen stays paused; with
            // this cascade gone, its field build's start unpauses it.
            if local_fuse || awaiting_capture {
                continue;
            }
            let remaining: i64 = txn
                .query_one(
                    "select count(*) from column_pause_cascades \
                     where downstream_transform = $1 and downstream_column = $2",
                    &[&downstream_transform, &downstream_column],
                )
                .await?
                .get(0);
            if remaining > 0 {
                // Still paused through another edge: its reason names one.
                restate_cascade_reason(txn, &downstream_transform, &downstream_column).await?;
                continue;
            }
            if downstream_transform == target {
                txn.execute(
                    "delete from column_status \
                     where transform_table = $1 and column_name = $2",
                    &[&target, &downstream_column],
                )
                .await?;
                released.push(downstream_column.clone());
                frontier.push(downstream_column);
            } else {
                dependents.push((downstream_transform, downstream_column));
            }
        }
    }
    Ok(Uncascaded {
        released,
        dependents,
    })
}

/// Resumes each pair of `queue`, and the dependents in other definitions
/// each releases, in a transaction apiece ([`resume_column`]'s walk past
/// its first column). `transform` is the definition the walk began in. A
/// pair stays paused, as the walk goes on, when its definition can't take a
/// field build (one that hasn't started building needs none, and is
/// released) or no longer validates, judged on the definition read under
/// the column-pause lock (#965); it is then held as a pause of its own
/// ([`hold_orphaned_pause`]). Returns the pairs resumed, in order.
pub(crate) async fn resume_pairs(
    pool: &Pool,
    transform: &str,
    mut queue: VecDeque<(String, String)>,
) -> Result<Vec<(String, String)>, ApplyError> {
    let mut resumed = Vec::new();
    while let Some((t, c)) = queue.pop_front() {
        let Some(def) = catalog::definition_by_target(pool, &t).await? else {
            continue;
        };
        // Test-only pause point (#965). See `super::interleave`. The walk
        // holds nothing here: an edit of `t`'s definition can commit.
        #[cfg(any(test, feature = "test-util"))]
        {
            // In a transaction of its own: the hook lifts the session's
            // lock timeout for the pause with `set_config(..., true)`.
            let mut client = pool.get().await?;
            let txn = client.transaction().await?;
            super::interleave::pause_at(
                &*txn,
                super::interleave::PausePoint::AfterResumePairDefinitionRead,
                &t,
            )
            .await?;
            txn.commit().await?;
        }
        let mut client = pool.get().await?;
        let txn = client.transaction().await?;
        // The unpause and the field build's registration commit together
        // (#625 F8b): the column applies from this commit, and the build's
        // chunks rewrite it under the keys' entry lock, so nothing needs a
        // catch-up. See this function's doc comment. The version fence
        // keeps a page that read the column paused from applying after this
        // commit (`super::build::bump_version_fence`). The bump is this
        // transaction's first lock: it waits for the pages in flight, and
        // one of them can be waiting on a transaction that wants the
        // definition row, so holding that row here would close a cycle
        // (issue #744). A rollback below takes the bump back with it.
        let one_to_one = matches!(def.def.key_space, KeySpace::OneToOne);
        if one_to_one {
            super::build::bump_version_fence(&*txn, &def.source_table).await?;
        }
        // The column-pause lock, exclusive, after the fence and before the
        // definition row, as every write of pause state takes it: this
        // deletes `t`'s rows one by one, in walk order, and a define or
        // edit reading them holds the lock too, so it reads either before
        // this starts or after it commits. See
        // `crate::locks::lock_column_pauses` for the order.
        crate::locks::lock_column_pauses(&*txn, ColumnPauseLock::Exclusive, ColumnPauseOp::Resume)
            .await?;
        let Some(row) = txn
            .query_opt(
                "select status, build from transform_definitions where id = $1 for update",
                &[&def.id],
            )
            .await?
        else {
            // Dropped (`DROP TRANSFORM`) since the lookup above: there is
            // nothing left of it to resume, and its rows went with it.
            txn.rollback().await?;
            continue;
        };
        let status_text: String = row.get(0);
        let status = TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
            panic!("transform_definitions.status held unrecognized value '{status_text}'")
        });
        let build: Option<String> = row.get(1);
        // A definition that has not started building has built nothing the
        // column's pause held out: every build of it, the Re-derive build
        // and the old path alike, reads the paused set when its chunks run,
        // after this commit. Releasing the pair is deleting its row; there
        // is no field build to start and none to wait for (issue #916).
        let unbuilt = status == TransformStatus::WaitingToBackfill;
        if !unbuilt && !super::build::takes_field_build(status, build.as_deref()) {
            // Reached via cascade (the initial pair was already gated above
            // before any side effects ran, and a definition that left the
            // state since is caught the same way): a downstream dependent
            // this pause cascaded onto (`column_dependents`, unlike the
            // applying-status-filtered lookups CDC apply uses, does not
            // require the dependent to be applying) can still be mid-build.
            // Aborting the whole call here would misrepresent what already
            // happened, since earlier pairs in this queue may already be
            // fully resumed and committed — instead this pair alone is left
            // paused, to be resumed on a later call. The walk that reached
            // it deleted its edge, so it is held as a pause of its own
            // (`hold_orphaned_pause`) rather than left with a reason that
            // no longer holds. A build that had already run its chunks
            // with the column held out can't be told to write it now, and a
            // field build waits on the definition applying.
            txn.rollback().await?;
            tracing::warn!(
                transform = %t,
                column = %c,
                status = %status_text,
                "dependent column stays paused: its definition can't take a field build"
            );
            hold_orphaned_pause(
                pool,
                &t,
                &c,
                &format!(
                    "its definition is {status_text} and can't take a field build yet; \
                     RESUME it once the definition is live"
                ),
            )
            .await?;
            continue;
        }
        // The definition as it stands under the lock and its row's: an
        // `ALTER TRANSFORM` that dropped or edited a field since the lookup
        // above has committed (it takes both), and the build below must
        // cover the fields as they are now.
        let Some(def) = catalog::definition_by_id_in(&*txn, def.id).await? else {
            txn.rollback().await?;
            continue;
        };
        if !def.def.fields.iter().any(|f| f.name == c) {
            if t != transform {
                // A reader's field the cascade reached, dropped by an edit
                // since: its pause rows went with it.
                txn.rollback().await?;
                continue;
            }
            return Err(ApplyError::ColumnNotPaused {
                transform: def.def.target.clone(),
                column: c.to_string(),
            });
        }
        // #708, #965: a dependent the cascade reaches is re-validated as the
        // resumed field's definition was at the gate in `resume_column`, on
        // the definition just read: an edit committed before the lock is
        // what the answer is about, and none can commit after it. One
        // define would refuse now stays paused, as a dependent still
        // mid-build does above. Nothing is written yet, so it rolls back
        // before it is held.
        if t != transform {
            match refuse_unless_valid_in(&txn, pool.schema(), &t, &def).await {
                Ok(()) => {}
                Err(ApplyError::ResumeRefused { reason, .. }) => {
                    txn.rollback().await?;
                    tracing::warn!(
                        transform = %t,
                        column = %c,
                        error = %reason,
                        "dependent column stays paused: its definition no longer validates"
                    );
                    hold_orphaned_pause(
                        pool,
                        &t,
                        &c,
                        &format!(
                            "its definition no longer validates ({reason}); \
                             RESUME it once the definition validates again"
                        ),
                    )
                    .await?;
                    continue;
                }
                Err(err) => return Err(err),
            }
        }

        txn.execute(
            "delete from column_status where transform_table = $1 and column_name = $2",
            &[&t, &c],
        )
        .await?;
        // Test-only pause point (#917). See `super::interleave`.
        #[cfg(any(test, feature = "test-util"))]
        super::interleave::pause_at(
            &*txn,
            super::interleave::PausePoint::AfterResumedColumnDeleted,
            &t,
        )
        .await?;
        // Un-cascade from `c`, and from each sibling this releases with it
        // (issue #748): a field of this definition that reads a released
        // one by alias, and has no other reason to stay paused, is released
        // in this same transaction and rebuilt by the same field build. A
        // dependent in another definition goes on the queue.
        let Uncascaded {
            released,
            dependents,
        } = uncascade(&*txn, &t, vec![c.clone()]).await?;
        queue.extend(dependents);
        // The build covers every field reading a released one by alias,
        // whether or not it had a cascade edge: one still held out by
        // another paused field it reads is left out of the build's chunks
        // (`paused_columns_for`), and that field's own resume builds it.
        if one_to_one && !unbuilt {
            let mut fields: HashSet<String> = released.iter().cloned().collect();
            AliasReaders::of(&def.def).close(&mut fields);
            let fields: Vec<String> = def
                .def
                .fields
                .iter()
                .filter(|f| fields.contains(&f.name))
                .map(|f| f.name.clone())
                .collect();
            super::build::start_field_build(&*txn, def.id, status, &fields).await?;
        }
        txn.commit().await?;
        resumed.extend(released.into_iter().map(|field| (t.clone(), field)));
    }

    Ok(resumed)
}

/// Holds `(transform, column)` as a pause of its own when a walk that
/// resumed its upstream column has deleted its last edge and then can't
/// release it (issue #916): its definition can't take a field build, or no
/// longer validates. Its row is there, with no `local_fuse`, no edge and not
/// awaiting a capture, and `last_error` still says it waits on the upstream,
/// which no longer holds; nothing would release it, and its definition would
/// go live with the field held out. Setting `local_fuse` makes the pause
/// its own, as an operator's, and the reason (`why`) says why and when its
/// `RESUME` is the way out: once the definition is live, or validates again.
/// The row's readers, if any, keep their edges from it.
///
/// A row that has a reason after all (another pause took it over meanwhile,
/// or it was released) is left as it is.
async fn hold_orphaned_pause(
    pool: &Pool,
    transform: &str,
    column: &str,
    why: &str,
) -> Result<(), ApplyError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    crate::locks::lock_column_pauses(&*txn, ColumnPauseLock::Exclusive, ColumnPauseOp::Resume)
        .await?;
    let held = txn
        .execute(
            "update column_status s set local_fuse = true, last_error = $3 \
             where s.transform_table = $1 and s.column_name = $2 \
               and not s.local_fuse and not s.awaiting_capture \
               and not exists ( \
                   select 1 from column_pause_cascades c \
                   where c.downstream_transform = s.transform_table \
                     and c.downstream_column = s.column_name)",
            &[
                &transform,
                &column,
                &format!("paused: the upstream column it read was resumed, but {why}"),
            ],
        )
        .await?;
    txn.commit().await?;
    if held > 0 {
        tracing::warn!(
            transform = %transform,
            column = %column,
            "column held paused on its own: its upstream was resumed and it could not be released"
        );
    }
    Ok(())
}

/// Re-runs define-time validation for `definition` against the live schema
/// ([`catalog::revalidate`], #708), on a transaction of its own that
/// changes nothing, as a field resume's gate: [`ApplyError::ResumeRefused`]
/// for `transform` while define would refuse it.
async fn refuse_unless_valid(
    pool: &Pool,
    transform: &str,
    definition: &crate::defs::model::Definition,
) -> Result<(), ApplyError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    refuse_unless_valid_in(&txn, pool.schema(), transform, definition).await
}

/// [`refuse_unless_valid`] on a transaction the caller holds, which it
/// changes nothing in.
async fn refuse_unless_valid_in(
    txn: &tokio_postgres::Transaction<'_>,
    schema: &str,
    transform: &str,
    definition: &crate::defs::model::Definition,
) -> Result<(), ApplyError> {
    match catalog::revalidate(txn, schema, definition).await {
        Ok(_) => Ok(()),
        Err(err @ (catalog::CatalogError::Db(_) | catalog::CatalogError::Pool(_))) => {
            Err(err.into())
        }
        Err(reason) => Err(ApplyError::ResumeRefused {
            transform: transform.to_string(),
            reason: Box::new(reason),
        }),
    }
}

/// Whether `column` of `transform` reads another of its fields that is
/// still paused, by alias, directly or through others (issue #748): Apply
/// holds it out with that field whatever its own row says
/// ([`paused_columns_for`]).
async fn reads_paused_sibling(
    client: &impl GenericClient,
    pool: &Pool,
    transform: &str,
    column: &str,
) -> Result<bool, ApplyError> {
    let Some(def) = catalog::definition_by_target(pool, transform).await? else {
        return Ok(false);
    };
    let mut others: HashSet<String> = client
        .query(
            "select column_name from column_status \
             where transform_table = $1 and column_name <> $2",
            &[&transform, &column],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    AliasReaders::of(&def.def).close(&mut others);
    Ok(others.contains(column))
}

/// Resumes a whole-transform-frozen definition — ADR-0003's coarser,
/// transform-wide fuse tier, distinct from [`resume_column`]'s per-column
/// tier (which additionally requires the owning definition to already be
/// `live`; a frozen definition is, by construction, never that).
///
/// **One resume for both of ADR-0014's pause triggers** (issue #142).
/// `target`'s current status must be either [`TransformStatus::Quarantined`]
/// (the poison fuse tripped it) or [`TransformStatus::Paused`] (an operator
/// froze it deliberately via `PAUSE TRANSFORM` — see [`crate::Trellis::apply`]) —
/// [`ApplyError::TransformNotPaused`] otherwise, checked before any
/// mutation, because resuming a transform that isn't frozen at all is caller
/// error, not a silent no-op, matching [`resume_column`]'s
/// [`ApplyError::ColumnNotPaused`] discipline. The recovery is identical for
/// both triggers and deliberately so (ADR-0014's "Resume reconciles with
/// source, not by catch-up"): a frozen definition's share of the change
/// stream is drained for its siblings while it's frozen and is not
/// recoverable by replay.
///
/// This function itself only schedules that reconciliation. It drops the
/// definition to [`TransformStatus::WaitingToBackfill`]. A definition the
/// Re-derive build takes (`super::build::qualifies`, #625 F3) is then
/// rebuilt by the staging worker's next pass, which starts a Re-derive build
/// over its ledger (`super::build::start_ready_builds`): it keeps the
/// ledger, the groups and any group deltas still owed to them (B4), and its
/// sweep drops the keys deleted while it was frozen. Any other definition
/// gets a fresh `pending_backfill` marker for its source table
/// ([`crate::intake::markers::park_marker`], which every marker goes
/// through). The settled projections of the to-one relationships the
/// definition reads are refreshed from their to-sides in the same
/// transaction, unless another reader that isn't frozen reads them (issue
/// #768: the drain may have skipped changes to a to-side while every reader
/// was frozen). The target is left exactly as the freeze left it until that
/// marker's discharge
/// ([`crate::intake::markers::run_pending_backfills`]) runs, which in one
/// transaction deletes every target row no current source row backs (issue
/// #330, `intake::resume_orphans`) and dispatches the rebuild by shape
/// (ADR-0016): chunks or a direct-build job that drain threads run, or, for
/// a shape the direct build can't render, an enumeration of every current
/// source row for the drain to re-derive. The discharge respects the marker's `xmin`
/// fence, never re-deriving the target without waiting out a concurrent
/// transaction that might still be pinning it (issue #55;
/// docs/observability.md's "Backfill status and the `xmin` caveat" applies
/// here exactly as it does to a fresh transform's own initial backfill:
/// resuming can sit in `waiting_to_backfill` for as long as some unrelated
/// transaction pins the cluster's `xmin`, and that is correct, not a fault).
///
/// Deletes the definition's unclaimed, undone `backfill_chunks` rows too
/// (issue #332): the fresh backfill makes them redundant, and a pause only
/// withheld them from dispatch. Chunks a worker still holds are left for that
/// worker; however it gives one up, it is discarded rather than handed out
/// again or used to complete the definition (issues #360/#397,
/// `defs::chunk_queue::finish_chunk`/`release_chunk`/`reclaim_stale_chunks`).
///
/// **The trip half of this contract** lives in [`isolate_and_evict`]'s
/// eviction (`quarantine_if_crossed`): when the count of keys poisoned for
/// a definition crosses [`DEFAULT_TRANSFORM_DEATH_THRESHOLD`], that
/// definition is quarantined (issue #105; per definition since #799).
///
/// **Releases every key the definition holds** (#799): its own `poison`,
/// `poison_held` and `key_deaths` rows are deleted in the same transaction,
/// and the fresh build re-derives every key from the source, so the parked
/// work is superseded rather than replayed. Every other definition's rows
/// are untouched: whole-key poison is per transform, so a sibling on the same
/// source keeps its own held keys. Deleting the rows is also what re-arms
/// the fuse (issue #160): the resumed definition starts from a count of zero.
/// `fuse_rearmed_at` is stamped too, as the resume's epoch: a backfill chunk
/// of the old build is stale against it (issues #360/#397), and an eviction
/// whose attribution read the old one poisons nothing (`evict_for`).
#[tracing::instrument(name = "quarantine.resume_transform", skip(pool), fields(transform = %target))]
pub async fn resume_transform(pool: &Pool, target: &str) -> Result<(), ApplyError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;

    // `target` is the bare transform name (matching `resume_column`'s own
    // `transform` parameter convention), but `transform_definitions.target_table`
    // is persisted fully-qualified (issue #73) — match on its bare suffix,
    // the same `split_part(target_table, '.', 2)` pattern
    // `TargetTableSuffixCollision`'s own check already uses, rather than
    // requiring every caller to know and pass the qualified identity.
    let not_found = || ApplyError::TransformNotFound {
        transform: target.to_string(),
    };
    let fenced_source: String = txn
        .query_opt(
            "select source_table from transform_definitions \
             where split_part(target_table, '.', 2) = $1",
            &[&target],
        )
        .await?
        .ok_or_else(not_found)?
        .get(0);
    let Some((id, status)) = lock_frozen(
        &txn,
        "split_part(target_table, '.', 2)",
        &target,
        &fenced_source,
    )
    .await?
    else {
        return Err(not_found());
    };
    if !status.is_frozen() {
        return Err(ApplyError::TransformNotPaused {
            transform: target.to_string(),
        });
    }
    match resume_locked(
        &txn,
        pool.schema(),
        target,
        id,
        &fenced_source,
        status,
        false,
    )
    .await?
    {
        ResumeStep::Resumed => {
            txn.commit().await?;
            tracing::info!(
                transform = %target,
                from = %status.as_str(),
                to = %TransformStatus::WaitingToBackfill.as_str(),
                "transform resumed; re-parked for a fresh backfill"
            );
        }
        ResumeStep::Retyping(copies) => {
            txn.commit().await?;
            tracing::info!(
                transform = %target,
                copies = ?copies,
                "transform resume accepted; the staging worker re-types its copies, then rebuilds it"
            );
        }
    }
    Ok(())
}

/// The first steps of a resume of the definition sourced from
/// `fenced_source` and found by `column = value`: bumps the source's version
/// fence, then locks the definition's row and reads its id and status.
/// `None` if it's gone, or was dropped and defined again on another source.
///
/// Issue #768: the drain skips a table whose key can't be used while every
/// definition reading it is frozen, judged under the fence of each reader's
/// source (`staging::apply::source_key_for_apply`). The bump makes a page
/// that judged this definition frozen miss its fence if it reaches it after
/// the resume commits, so it can't skip rows the rebuild needs. It is the
/// transaction's first lock (issue #744): it waits for the pages holding the
/// fence, and holding the definition row meanwhile could close a cycle
/// through one queued on a transaction that wants it.
async fn lock_frozen(
    txn: &Transaction<'_>,
    column: &str,
    value: &(dyn tokio_postgres::types::ToSql + Sync),
    fenced_source: &str,
) -> Result<Option<(i64, TransformStatus)>, ApplyError> {
    super::build::bump_version_fence(txn, fenced_source).await?;
    let row = txn
        .query_opt(
            &format!(
                "select id, source_table, status from transform_definitions \
                 where {column} = $1 for update"
            ),
            &[value],
        )
        .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let source_table: String = row.get(1);
    if source_table != fenced_source {
        // Dropped and defined again on another source between the two
        // reads: not the definition the caller resumed.
        return Ok(None);
    }
    let status_text: String = row.get(2);
    let status = TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
        panic!("transform_definitions.status held unrecognized value '{status_text}'")
    });
    Ok(Some((row.get(0), status)))
}

/// What [`resume_locked`] did.
#[derive(Debug)]
enum ResumeStep {
    /// The definition is resumed: `waiting_to_backfill`, its rebuild
    /// scheduled.
    Resumed,
    /// Some of its typed copies have to be re-typed first (their labels);
    /// it stays paused with a resume request.
    Retyping(Vec<String>),
}

/// A resume of frozen definition `id` (bare target `target`, qualified
/// source `source_table`), in `txn`, which [`lock_frozen`] has locked it
/// in. Every resume goes through here: the operator's
/// ([`resume_transform`]), the capture pass's of a definition whose upstream
/// went live ([`resume_caused_definitions`]), and the staging worker's
/// completion of one that had copies to re-type
/// ([`finish_requested_resumes`]).
///
/// 1. **It re-validates the definition against the live schema**
///    ([`catalog::revalidate`], #708, #760), before it changes anything, and
///    refuses with [`ApplyError::ResumeRefused`] while define would refuse
///    it: a key, join or `GROUP BY` column of a type or collation define
///    refuses, a relationship whose join columns no longer match (#590), a
///    redefined source key. A whole-transform resume also refuses an
///    aggregate whose group-delta table lacks columns define would create now
///    ([`catalog::check_deltas_shape`], #857); a column resume doesn't make
///    that check.
/// 2. **It compares each column Trellis created with a type from the
///    source (typed copies, and calculated, aggregate and contribution
///    columns) with the type define would give it now**
///    ([`crate::defs::copies`], #767, #824). If any differ, the
///    copies are re-typed before the rebuild, by the staging worker, under
///    `ACCESS EXCLUSIVE` (a table rewrite for `integer` to `bigint`). So the
///    operator's resume, `from_pass` false, only records a resume request
///    and replaces the definition's `capture_failure` with one saying so,
///    and returns [`ResumeStep::Retyping`]; the definition stays paused until
///    the worker has re-typed them and comes back here. The worker's own
///    call, `from_pass` true, returns that too if a copy drifted again
///    since, and its caller rolls back.
/// 3. **Otherwise it completes the resume** ([`complete_resume`]).
async fn resume_locked(
    txn: &Transaction<'_>,
    schema: &str,
    target: &str,
    id: i64,
    source_table: &str,
    status: TransformStatus,
    from_pass: bool,
) -> Result<ResumeStep, ApplyError> {
    let Some(definition) = catalog::definition_by_id_in(txn, id).await? else {
        return Err(ApplyError::TransformNotFound {
            transform: target.to_string(),
        });
    };
    let revalidated = match catalog::revalidate(txn, schema, &definition).await {
        Ok(revalidated) => revalidated,
        Err(err @ (catalog::CatalogError::Db(_) | catalog::CatalogError::Pool(_))) => {
            return Err(err.into());
        }
        Err(reason) => {
            return Err(ApplyError::ResumeRefused {
                transform: target.to_string(),
                reason: Box::new(reason),
            });
        }
    };
    match catalog::check_deltas_shape(txn, &definition, &revalidated).await {
        Ok(()) => {}
        Err(err @ (catalog::CatalogError::Db(_) | catalog::CatalogError::Pool(_))) => {
            return Err(err.into());
        }
        Err(reason) => {
            return Err(ApplyError::ResumeRefused {
                transform: target.to_string(),
                reason: Box::new(reason),
            });
        }
    }
    let rels = catalog::relationships_read_by(txn, &definition.def, source_table).await?;
    let rel_refs: Vec<&crate::defs::model::RelationshipDefinition> = rels.iter().collect();
    let copies = crate::defs::copies::typed_copies(
        txn,
        schema,
        &definition.def,
        source_table,
        &definition.target_table,
        &rel_refs,
    )
    .await?;
    let drifted: Vec<crate::defs::copies::CopyState> = crate::defs::copies::inspect(txn, copies)
        .await?
        .into_iter()
        .filter(|s| s.drifted())
        .collect();
    if !drifted.is_empty() {
        let labels: Vec<String> = drifted.iter().map(|s| s.copy.label()).collect();
        if !from_pass {
            request_retype(txn, id, source_table, &drifted).await?;
        }
        return Ok(ResumeStep::Retyping(labels));
    }
    complete_resume(txn, id, source_table, status, &definition, &revalidated).await?;
    Ok(ResumeStep::Resumed)
}

/// Records that definition `id`'s resume waits on the staging worker to
/// re-type `drifted` (`resume_requests`), and says so in its
/// `capture_failure`, which [`complete_resume`] deletes. It replaces
/// whatever reason the definition was paused for: the operator has dealt
/// with that, and the request is what holds it now.
async fn request_retype(
    txn: &Transaction<'_>,
    id: i64,
    source_table: &str,
    drifted: &[crate::defs::copies::CopyState],
) -> Result<(), ApplyError> {
    txn.execute(
        "insert into resume_requests (transform_id) values ($1) \
         on conflict (transform_id) do update set requested_at = now(), timeout_cancels = 0",
        &[&id],
    )
    .await?;
    let (columns, error) = retyping_failure(source_table, drifted.iter());
    set_capture_failure(txn, id, source_table, &columns, &error).await
}

/// The columns of `source_table` and the `capture_failure` text that report
/// a resume waiting on the staging worker to re-type `drifted`
/// ([`request_retype`]). The staging worker rewrites it on a transient
/// retry ([`record_transient_retry`]).
fn retyping_failure<'a>(
    source_table: &str,
    drifted: impl Iterator<Item = &'a crate::defs::copies::CopyState> + Clone,
) -> (Vec<String>, String) {
    let mut columns: Vec<String> = Vec::new();
    for column in drifted
        .clone()
        .flat_map(|s| s.copy.columns_of(source_table))
    {
        if !columns.iter().any(|c| c == column) {
            columns.push(column.to_string());
        }
    }
    let changes: Vec<String> = drifted
        .map(|s| {
            format!(
                "{} from {} to {}",
                s.copy.label(),
                s.copy_type.display,
                s.live_type.display
            )
        })
        .collect();
    let error = format!(
        "resuming: the staging worker is re-typing the columns Trellis created to the types \
         define would give them now ({}), then it rebuilds the definition. It stays paused \
         until then",
        changes.join(", ")
    );
    (columns, error)
}

/// Sets definition `id`'s `capture_failure` (kind `capture`), replacing any
/// it has: what a resume in progress, or one the staging worker couldn't
/// finish, reports. It drops any upstream resume recorded as the pause's
/// cause (`caused_by`, #828): the definition has been resumed, by the
/// operator or by the capture pass ([`resume_caused_definitions`]), so what
/// holds it now is its own resume.
async fn set_capture_failure(
    client: &impl GenericClient,
    id: i64,
    source_table: &str,
    columns: &[String],
    error: &str,
) -> Result<(), ApplyError> {
    client
        .execute(
            "insert into capture_failures (transform_id, source_table, columns, error, kind) \
             values ($1, $2, $3, $4, 'capture') \
             on conflict (transform_id) do update \
             set source_table = excluded.source_table, columns = excluded.columns, \
                 error = excluded.error, kind = excluded.kind, caused_by = null, \
                 detected_at = now()",
            &[&id, &source_table, &columns, &error],
        )
        .await?;
    Ok(())
}

/// The resume itself, once [`resume_locked`] has re-validated definition
/// `id` and found every copy current: see [`resume_transform`]'s doc for
/// the rebuild it schedules. It also records the live source column types
/// and key column types the rebuild builds from
/// ([`catalog::record_source_columns`],
/// [`crate::defs::key_types::record_definition`]), so the capture pass
/// compares later changes against them, and deletes any resume request.
async fn complete_resume(
    txn: &Transaction<'_>,
    id: i64,
    source_table: &str,
    status: TransformStatus,
    definition: &crate::defs::model::Definition,
    revalidated: &catalog::Revalidated,
) -> Result<(), ApplyError> {
    // `fuse_rearmed_at = now()` in the same statement, not a separate one:
    // it is this resume's epoch, part of the same atomic "this transform
    // starts over" transition as the status drop (issue #160), and what a
    // stale chunk or eviction checks against.
    //
    // `build = null` too (#625 F2): a definition paused during a Re-derive
    // build is no longer under one. The staging worker's next pass starts a
    // new one if it still qualifies (`staging::build::start_ready_builds`),
    // and otherwise an old build takes it. Every old dispatch then sees
    // `build` null, the ring enumeration's `go_live` included, so a resumed
    // definition never ends `live` still marked as building, which
    // `catalog::table_has_reader` would skip.
    txn.execute(
        "update transform_definitions \
         set status = $1, fuse_rearmed_at = now(), build = null, \
             build_marker_generation = null where id = $2",
        &[&TransformStatus::WaitingToBackfill.as_str(), &id],
    )
    .await?;
    // #708, #767: the rebuild casts each source value through its live type,
    // and the capture pass compares the key columns against what it builds
    // from.
    catalog::record_source_columns(txn, id, &revalidated.source_columns).await?;
    crate::defs::key_types::record_definition(
        txn,
        id,
        &definition.def,
        source_table,
        &catalog::rel_refs(&revalidated.relationships),
        true,
    )
    .await?;

    // Discard the definition's leftover unclaimed backfill chunks (issue
    // #332). The pause only withheld them from `claim_chunks`; unfrozen, they
    // would be handed out again and re-run work the fresh backfill parked
    // below already does. In the same transaction as the status drop, so no
    // claim can see the definition dispatchable with them still present, and
    // `claim_chunks`' `for update skip locked` never waits on these rows.
    //
    // A chunk a worker still holds is left alone, as a pause leaves it: that
    // worker may still be writing its range. The `fuse_rearmed_at` stamp
    // above marks it stale (issues #360/#397): however that worker gives it
    // up (finishing, failing or dying), the chunk queue deletes it rather
    // than completing the definition or freeing it for a rerun.
    txn.execute(
        "delete from backfill_chunks \
         where definition_id = $1 and not done and claimed_by is null",
        &[&id],
    )
    .await?;
    // Waits out any write such a chunk still has in flight, which holds the
    // chunk's row `for key share` until it commits
    // (`defs::chunk_queue::ClaimFence`, issue #434). Every write that chunk
    // makes then commits before this resume does, while the definition is
    // frozen, and the rebuild overwrites it. One that starts later sees the
    // stamp above and writes nothing, so none can land after the rebuild has
    // gone live and readers have attached to the target.
    txn.execute(
        "select 1 from backfill_chunks \
         where definition_id = $1 and not done order by id for update",
        &[&id],
    )
    .await?;

    // A definition the Re-derive build takes (#625 F3) gets no marker: the
    // staging worker's next pass starts its rebuild from `waiting_to_backfill`
    // (`super::build::start_ready_builds`), over the ledger, groups and group
    // deltas the freeze left (B4), and its sweep drops the keys deleted
    // meanwhile. A marker would have the discharge re-read the whole source
    // for nothing.
    //
    // `transform_definitions.source_table` is already the fully-qualified
    // `"schema.table"` form (issue #72) — re-resolving it via
    // `resolve_source_schema_in_txn` (bare names only) or re-`qualify`-ing it
    // would reject it outright (`DottedIdentifierComponent`).
    if !super::build::qualifies(txn, definition).await? {
        crate::intake::markers::park_marker(txn, source_table).await?;
    }
    // Issue #768: the drain skips a to-side whose key can't be used while
    // every definition reading it is frozen
    // (`staging::apply::source_key_for_apply`), so changes to a to-side this
    // definition reads may never have reached the relationship's settled
    // projection, which its rebuild's go-live catch-up and every later apply
    // read. Refreshed from the table here, in the resume, so the rebuild
    // never reads it stale. The bump in [`lock_frozen`] holds the fence the
    // skip is judged under: a page that skipped while this definition was
    // frozen has committed, its changes no longer pending to be left to it,
    // and a later one misses the fence and halts. Only the to-one
    // relationships no other reader that isn't frozen reads
    // ([`catalog::relationships_to_refresh`]), all their rows locked in one
    // statement, in `relationship_id` order, the order a page takes them in.
    let refresh =
        catalog::relationships_to_refresh(txn, &definition.def, source_table, Some(id)).await?;
    catalog::refresh_relationship_projections_by_id_in_txn(txn, &refresh).await?;
    // #799: release every key this definition holds. The fresh build below
    // re-derives each from the source, so its parked work is superseded, not
    // replayed, and the key is applied again from the build's go-live on.
    // Only this definition's rows: a sibling on the same source keeps its
    // own. Deleting them is also what gives the resumed definition a fresh
    // whole-transform fuse.
    for table in ["poison", "poison_held", "key_deaths"] {
        txn.execute(
            &format!("delete from {table} where transform_id = $1"),
            &[&id],
        )
        .await?;
    }
    // #622 C6: a transform paused by a schema change stops being reported as
    // such the moment it is resumed. Its columns count for capture again from
    // here, so the next reconcile widens the source's capture to them before
    // the discharge may dispatch the rebuild, or, if a column is still
    // missing, pauses it again with the reason
    // (`staging::schema_change::pause_readers_of_missing`). A key column
    // changed again since the re-validation above is the next pass's to
    // pause (`pause_readers_of_retyped`), against the types just recorded.
    txn.execute(
        "delete from capture_failures where transform_id = $1",
        &[&id],
    )
    .await?;
    txn.execute(
        "delete from resume_requests where transform_id = $1",
        &[&id],
    )
    .await?;
    tracing::debug!(transform_id = id, from = %status.as_str(), "resume completed");
    Ok(())
}

/// How long the staging worker waits for the lock on a table whose copies
/// it re-types before it leaves the re-type for its next pass: a resume's,
/// or one it makes in place (`staging::schema_change`, #824).
pub(crate) const RETYPE_LOCK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// The staging worker's resume of every definition a resume's re-type of
/// its upstream's target paused (#828, #970): run at the start of each
/// capture pass, before [`finish_requested_resumes`], so a re-type it
/// requests is done in the same pass. One `RESUME` of the head of a chain
/// carries the whole chain.
///
/// A definition paused with a cause (`capture_failures.caused_by`,
/// recorded by `staging::schema_change::pause_readers_of_retyped`) waits
/// until that upstream definition is `live`: its rebuild has finished
/// (#476), so the rebuild of this one reads a target that is whole, and each
/// downstream rebuild starts only after the one above it. Then it takes the
/// path an operator's `RESUME` takes ([`lock_frozen`], [`resume_locked`]),
/// in one transaction:
///
/// - **No copy drifted:** the definition is resumed, `waiting_to_backfill`.
/// - **Copies to re-type:** a resume request is recorded and the cause
///   cleared ([`request_retype`]). The [`finish_requested_resumes`] that
///   follows re-types them and completes the resume, and its
///   [`record_retype_causes`] makes the next definition down record this
///   one as its cause: the chain goes on one level per upstream rebuild.
/// - **Refused:** the schema changed after the pause and define would now
///   refuse the definition ([`ApplyError::ResumeRefused`]). Its
///   `capture_failure` takes the refusal ([`refused_resume_text`], the text
///   of a refused request), which clears the cause, so the next pass doesn't
///   try again. It stays paused. Nothing below it was paused, since its
///   target was never re-typed.
///
/// **The durable queue is `caused_by` plus `resume_requests`.** Each
/// transition is one transaction that moves a definition from the first to
/// the second (or out of both), so a crash leaves one of them and the next
/// pass goes on from it. Under the definition's lock, the step re-reads that
/// it is still frozen, still has no request and still has the same cause,
/// with the upstream still `live`: a concurrent operator `RESUME` (which
/// deletes the record) or a change of the record wins. Any other pause
/// reason replaces the record without a cause (`set_capture_failure`), so it
/// is never resumed here.
///
/// A definition's own failure is logged and doesn't stop the others. Errs
/// only when the candidates can't be read.
pub(crate) async fn resume_caused_definitions(
    client: &mut tokio_postgres::Client,
    schema: &str,
) -> Result<(), tokio_postgres::Error> {
    let candidates: Vec<(i64, i64)> = client
        .query(&caused_candidates_sql(), &[&TransformStatus::Live.as_str()])
        .await?
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    for (id, upstream) in candidates {
        if let Err(err) = resume_caused_definition(client, schema, id, upstream).await {
            tracing::warn!(
                transform_id = id,
                upstream_id = upstream,
                error = %err,
                "a definition waiting on its upstream's resume couldn't be resumed; \
                 retrying next pass"
            );
        }
    }
    Ok(())
}

/// The frozen definitions with a cause and no resume request, as `(id,
/// cause)` in id order, whose cause has the status `$1`.
fn caused_candidates_sql() -> String {
    let frozen = [TransformStatus::Paused, TransformStatus::Quarantined]
        .map(|status| format!("'{}'", status.as_str()))
        .join(", ");
    format!(
        "select d.id, f.caused_by \
         from capture_failures f \
         join transform_definitions d on d.id = f.transform_id \
         join transform_definitions u on u.id = f.caused_by \
         where d.status in ({frozen}) and u.status = $1 \
           and not exists (select 1 from resume_requests r where r.transform_id = d.id) \
         order by d.id"
    )
}

/// [`resume_caused_definitions`] for definition `id`, paused with `upstream`
/// as its cause.
async fn resume_caused_definition(
    client: &mut tokio_postgres::Client,
    schema: &str,
    id: i64,
    upstream: i64,
) -> Result<(), ApplyError> {
    let txn = client.transaction().await?;
    let row = txn
        .query_opt(
            "select source_table, split_part(target_table, '.', 2) \
             from transform_definitions where id = $1",
            &[&id],
        )
        .await?;
    let Some(row) = row else {
        return Ok(());
    };
    let source_table: String = row.get(0);
    let target: String = row.get(1);
    let Some((_, status)) = lock_frozen(&txn, "id", &id, &source_table).await? else {
        return Ok(());
    };
    // Re-read under the lock: an operator's `RESUME` that committed first
    // has deleted the record, a re-pause or a refusal has replaced it.
    let still_waiting: bool = txn
        .query_one(
            "select exists (select 1 from capture_failures f \
                            join transform_definitions u on u.id = f.caused_by \
                            where f.transform_id = $1 and f.caused_by = $2 and u.status = $3) \
                    and not exists (select 1 from resume_requests where transform_id = $1)",
            &[&id, &upstream, &TransformStatus::Live.as_str()],
        )
        .await?
        .get(0);
    if !status.is_frozen() || !still_waiting {
        return Ok(());
    }
    match resume_locked(&txn, schema, &target, id, &source_table, status, false).await {
        Ok(ResumeStep::Resumed) => {
            txn.commit().await?;
            tracing::info!(
                transform = %target,
                upstream_id = upstream,
                to = %TransformStatus::WaitingToBackfill.as_str(),
                "transform resumed once its upstream was live; re-parked for a fresh backfill"
            );
        }
        Ok(ResumeStep::Retyping(copies)) => {
            txn.commit().await?;
            tracing::info!(
                transform = %target,
                upstream_id = upstream,
                copies = ?copies,
                "transform resume requested once its upstream was live; the staging worker \
                 re-types its copies, then rebuilds it"
            );
        }
        Err(ApplyError::ResumeRefused { reason, .. }) => {
            // `resume_locked` refuses before it writes anything, so the
            // transaction, and the lock it holds, are still good for the
            // record that ends the wait.
            let columns: Vec<String> = Vec::new();
            let error = refused_resume_text(&reason);
            set_capture_failure(&txn, id, &source_table, &columns, &error).await?;
            txn.commit().await?;
            tracing::warn!(
                transform_id = id,
                upstream_id = upstream,
                "a definition waiting on its upstream's resume was refused its own: {error}"
            );
        }
        Err(err) => return Err(err),
    }
    Ok(())
}

/// The staging worker's half of every resume [`resume_transform`] left
/// waiting on re-typed copies (`resume_requests`, #767): run at the start of
/// each capture pass, before it reads the catalog, so a resume it completes
/// is `waiting_to_backfill` in that pass's snapshot.
///
/// For each request, in id order, it re-validates the definition (a schema
/// changed since the `RESUME` is caught here too), then runs
/// [`crate::defs::copies::retype_statements`], one table per transaction,
/// under a [`RETYPE_LOCK_TIMEOUT`] `lock_timeout`, and then completes the
/// resume in one transaction through [`resume_locked`], which deletes the
/// request. Each step is idempotent: a copy already re-typed isn't re-typed
/// again, so a crash anywhere leaves the request for the next pass.
///
/// - A table whose lock it can't get within the timeout is left for the next
///   pass, the request kept: re-typing takes `ACCESS EXCLUSIVE`, which every
///   reader of the target waits behind while it is queued. So is one whose
///   re-type fails any other transient way ([`is_transient_error`]: a
///   deadlock, a serialization failure, a cancelled statement, a lost
///   connection), which says nothing about whether its values convert. Each
///   such retry rewrites the `resuming: ...` text with the last error
///   ([`record_transient_retry`], #894). A statement cancelled by a
///   `statement_timeout` (57014) is also counted, and the
///   [`RETYPE_TIMEOUT_CANCEL_LIMIT`]th ends the request
///   ([`retype_cancelled_error`]): Trellis honours the operator's timeout
///   and doesn't retry a rewrite it can't finish forever.
/// - A re-validation that now fails, or a re-type that fails (a value the
///   new type can't hold, a view on the column), ends the request: the
///   definition stays paused, its `capture_failure` says why, and the next
///   `RESUME` tries again. The failing table's copies keep their types;
///   another table's, re-typed before it in its own transaction, keep their
///   new ones, which the definition, still paused, never reads, and which
///   the next resume finds current. The target keeps its rows: for values
///   that can't be converted, the message names `DROP TRANSFORM` and define
///   again as the repair ([`retype_failed_error`], #828).
/// - The transaction that re-types the definition's target also records
///   which of its columns it re-typed ([`record_retype_causes`]), so the
///   pass's pause of a definition chained off the target names this resume
///   as its cause (#828).
///
/// A request's own failure is logged and doesn't stop the others. Errs only
/// when the requests can't be read.
pub(crate) async fn finish_requested_resumes(
    client: &mut tokio_postgres::Client,
    schema: &str,
) -> Result<(), tokio_postgres::Error> {
    let ids: Vec<i64> = client
        .query(
            "select transform_id from resume_requests order by transform_id",
            &[],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    for id in ids {
        if let Err(err) = finish_requested_resume(client, schema, id).await {
            tracing::warn!(
                transform_id = id,
                error = %err,
                "a requested resume couldn't be finished; retrying next pass"
            );
        }
    }
    Ok(())
}

/// [`finish_requested_resumes`] for definition `id`.
async fn finish_requested_resume(
    client: &mut tokio_postgres::Client,
    schema: &str,
    id: i64,
) -> Result<(), ApplyError> {
    // What to re-type, read with the re-validation in one snapshot.
    let (source_table, target, statements, retrying) = {
        let txn = client.transaction().await?;
        let row = txn
            .query_opt(
                "select source_table, split_part(target_table, '.', 2), status \
                 from transform_definitions where id = $1",
                &[&id],
            )
            .await?;
        let frozen = row.as_ref().is_some_and(|row| {
            TransformStatus::from_persisted(row.get(2)).is_some_and(TransformStatus::is_frozen)
        });
        let Some(row) = row.filter(|_| frozen) else {
            // Dropped, or resumed some other way: nothing waits on this.
            txn.execute(
                "delete from resume_requests where transform_id = $1",
                &[&id],
            )
            .await?;
            txn.commit().await?;
            return Ok(());
        };
        let source_table: String = row.get(0);
        let target: String = row.get(1);
        let Some(definition) = catalog::definition_by_id_in(&txn, id).await? else {
            return Ok(());
        };
        // The group-delta shape is checked here too, before anything is
        // re-typed under `ACCESS EXCLUSIVE` that `resume_locked` would then
        // refuse.
        let checked = match catalog::revalidate(&txn, schema, &definition).await {
            Ok(revalidated) => catalog::check_deltas_shape(&txn, &definition, &revalidated).await,
            Err(err) => Err(err),
        };
        match checked {
            Ok(()) => {}
            Err(err @ (catalog::CatalogError::Db(_) | catalog::CatalogError::Pool(_))) => {
                return Err(err.into());
            }
            Err(reason) => {
                drop(txn);
                end_request(client, id, &source_table, &refused_resume_text(&reason)).await?;
                return Ok(());
            }
        }
        let rels = catalog::relationships_read_by(&txn, &definition.def, &source_table).await?;
        let rel_refs: Vec<&crate::defs::model::RelationshipDefinition> = rels.iter().collect();
        let copies = crate::defs::copies::typed_copies(
            &txn,
            schema,
            &definition.def,
            &source_table,
            &definition.target_table,
            &rel_refs,
        )
        .await?;
        let states = crate::defs::copies::inspect(&txn, copies).await?;
        let retrying = retyping_failure(&source_table, states.iter().filter(|s| s.drifted()));
        // Each table's statements, with the columns of the definition's
        // target they re-type: what a definition chained off the target
        // reads (#828).
        let mut by_table: std::collections::BTreeMap<String, Vec<crate::defs::copies::CopyState>> =
            std::collections::BTreeMap::new();
        for state in states {
            by_table
                .entry(state.copy.table.clone())
                .or_default()
                .push(state);
        }
        let mut statements: Vec<(String, Vec<String>, Vec<RetypedColumn>)> = Vec::new();
        for states in by_table.into_values() {
            let columns: Vec<RetypedColumn> = states
                .iter()
                .filter(|s| s.drifted() && s.copy.table_name == definition.target_table)
                .map(|s| RetypedColumn {
                    table: s.copy.table_name.clone(),
                    column: s.copy.column.clone(),
                    old_type: s.copy_type.display.clone(),
                    new_type: s.live_type.display.clone(),
                })
                .collect();
            for (sql, labels) in crate::defs::copies::retype_statements(&states) {
                statements.push((sql, labels, columns.clone()));
            }
        }
        txn.commit().await?;
        (source_table, target, statements, retrying)
    };

    let mut retyped: Vec<String> = Vec::new();
    for (sql, labels, columns) in statements {
        let txn = client.transaction().await?;
        crate::locks::set_local_lock_timeout(&txn, RETYPE_LOCK_TIMEOUT).await?;
        match txn.batch_execute(&sql).await {
            Ok(()) => {
                record_retype_causes(&txn, id, &columns).await?;
                txn.commit().await?;
                tracing::info!(transform_id = id, copies = ?labels, "re-typed copies for a resume");
                retyped.extend(labels);
            }
            Err(err) if is_transient_error(&err) => {
                drop(txn);
                tracing::info!(
                    transform_id = id,
                    copies = ?labels,
                    error = %err,
                    "re-typing copies for a resume failed transiently (its table's lock, say); \
                     retrying next pass"
                );
                let retry = TransientRetry {
                    target: &target,
                    labels: &labels,
                    retyped: &retyped,
                    retrying: &retrying,
                    error: &err
                        .as_db_error()
                        .map(|db| db.message().to_string())
                        .unwrap_or_else(|| err.to_string()),
                    timeout_cancel: is_timeout_cancel(err.code()),
                };
                record_transient_retry(client, id, &source_table, &retry).await?;
                return Ok(());
            }
            Err(err) => {
                txn.rollback().await?;
                end_request(
                    client,
                    id,
                    &source_table,
                    &retype_failed_error(
                        &target,
                        &labels,
                        &err.as_db_error()
                            .map(ToString::to_string)
                            .unwrap_or_else(|| err.to_string()),
                        &retyped,
                    ),
                )
                .await?;
                return Ok(());
            }
        }
    }

    let txn = client.transaction().await?;
    let Some((locked, status)) = lock_frozen(&txn, "id", &id, &source_table).await? else {
        return Ok(());
    };
    let requested: bool = txn
        .query_one(
            "select exists (select 1 from resume_requests where transform_id = $1)",
            &[&locked],
        )
        .await?
        .get(0);
    if !status.is_frozen() || !requested {
        return Ok(());
    }
    match resume_locked(&txn, schema, &target, id, &source_table, status, true).await {
        Ok(ResumeStep::Resumed) => {
            txn.commit().await?;
            tracing::info!(
                transform = %target,
                to = %TransformStatus::WaitingToBackfill.as_str(),
                "transform resumed once its copies were re-typed; re-parked for a fresh backfill"
            );
        }
        // A copy drifted again since: the next pass re-types it.
        Ok(ResumeStep::Retyping(_)) => {}
        Err(ApplyError::ResumeRefused { reason, .. }) => {
            drop(txn);
            end_request(client, id, &source_table, &refused_resume_text(&reason)).await?;
        }
        Err(err) => return Err(err),
    }
    Ok(())
}

/// What a resume the staging worker refused records for the operator
/// ([`end_request`]): define's own refusal is fixed in the schema, a refusal
/// only a resume makes ([`catalog::CatalogError::is_define_refusal`]) by
/// dropping the definition and defining it again.
fn refused_resume_text(reason: &catalog::CatalogError) -> String {
    if reason.is_define_refusal() {
        format!(
            "the resume was refused: define would refuse the definition as the schema stands \
             now: {reason}. Fix that and resume it again, or drop the definition and define it \
             again"
        )
    } else {
        format!("the resume was refused: {reason}")
    }
}

/// A column of a definition's target that its resume re-types
/// ([`finish_requested_resume`]), from `old_type` to `new_type`
/// (`format_type`): a definition chained off the target that reads it may
/// pause for it (#828).
#[derive(Debug, Clone)]
struct RetypedColumn {
    /// The target, `schema.table`.
    table: String,
    column: String,
    old_type: String,
    new_type: String,
}

/// Records, in the re-type's own transaction, that definition `id`'s resume
/// re-typed `columns` of its target (`retype_causes`), replacing an earlier
/// re-type's record of the same column. The capture pass then records `id`
/// as the cause on the pause of each definition chained off the target
/// that the re-type pauses
/// (`staging::schema_change::pause_readers_of_retyped`), and names this
/// resume in its `capture_failure`. Also deletes the records of a
/// definition that's gone. Takes no lock on any definition's row (see
/// `V76__retype_causes.sql`).
async fn record_retype_causes(
    txn: &Transaction<'_>,
    id: i64,
    columns: &[RetypedColumn],
) -> Result<(), tokio_postgres::Error> {
    if columns.is_empty() {
        return Ok(());
    }
    txn.execute(
        "delete from retype_causes r where not exists \
         (select 1 from transform_definitions d where d.id = r.transform_id)",
        &[],
    )
    .await?;
    for c in columns {
        txn.execute(
            "insert into retype_causes (table_name, column_name, transform_id, old_type, new_type) \
             values ($1, $2, $3, $4, $5) \
             on conflict (table_name, column_name) do update \
             set transform_id = excluded.transform_id, old_type = excluded.old_type, \
                 new_type = excluded.new_type, retyped_at = now()",
            &[&c.table, &c.column, &id, &c.old_type, &c.new_type],
        )
        .await?;
    }
    Ok(())
}

/// The `capture_failure` of a definition (bare target `target`) whose
/// resume couldn't re-type `labels` (`error`, Postgres's) after it re-typed
/// `retyped`. The definition stays paused and its target keeps its rows
/// (#828, decision 1): when a value can't be converted, the repair is to
/// drop the definition and define it again, which builds the target from
/// empty. Trellis doesn't empty the target itself, because the application
/// reads it.
fn retype_failed_error(target: &str, labels: &[String], error: &str, retyped: &[String]) -> String {
    // Each table is re-typed in its own transaction, so the copies of a
    // table re-typed before this one keep their new types; the next resume
    // finds them current.
    let kept = if retyped.is_empty() {
        String::new()
    } else {
        format!(" ({} were re-typed already)", retyped.join(", "))
    };
    format!(
        "the resume couldn't re-type Trellis's columns {} to the types define would give them \
         now: {error}. They keep their types{kept}, the target keeps its rows, and the definition \
         stays paused. If the cause can be removed (a view on the column, say), remove it and \
         resume the definition again. If the values can't be converted, DROP TRANSFORM {target} \
         and define it again, which builds the target from empty: Trellis doesn't empty a \
         target on its own, since the application reads it",
        labels.join(", "),
    )
}

/// How many times one resume request's re-type may be cancelled by a
/// `statement_timeout` (SQLSTATE 57014) before the request ends (#894).
/// Trellis honours the operator's `statement_timeout` and sets none of its
/// own, so a rewrite that outlasts it is cancelled and retried on the next
/// capture pass, blocking the target's readers for up to the timeout each
/// time; the request ends after this many cancellations
/// (`resume_requests.timeout_cancels`).
pub(crate) const RETYPE_TIMEOUT_CANCEL_LIMIT: i32 = 3;

/// Whether a re-type that failed with `code` was cancelled by a
/// `statement_timeout`: `QUERY_CANCELED` (57014) alone counts toward
/// [`RETYPE_TIMEOUT_CANCEL_LIMIT`]. The other transient errors keep their
/// retry on the next pass without a count: a lock timeout (`55P03`) never
/// held the lock, and a lost connection says nothing about the rewrite.
fn is_timeout_cancel(code: Option<&tokio_postgres::error::SqlState>) -> bool {
    code == Some(&tokio_postgres::error::SqlState::QUERY_CANCELED)
}

/// One transient failure of a resume's re-type of `labels`
/// ([`finish_requested_resume`]), for [`record_transient_retry`].
struct TransientRetry<'a> {
    /// The definition's bare target.
    target: &'a str,
    /// The copies the failed statement re-types.
    labels: &'a [String],
    /// The copies of earlier tables, already re-typed.
    retyped: &'a [String],
    /// The `resuming: ...` columns and text, as the request recorded them.
    retrying: &'a (Vec<String>, String),
    /// Postgres's message for the failure.
    error: &'a str,
    /// Whether it was a cancelled statement ([`is_timeout_cancel`]).
    timeout_cancel: bool,
}

/// Records a transient failure of definition `id`'s re-type (#894), in one
/// transaction locked as [`end_request`] locks: the definition's
/// `capture_failure` gets the last error, and, for a cancelled statement,
/// the attempt count, with `resume_requests.timeout_cancels` counting it. The
/// [`RETYPE_TIMEOUT_CANCEL_LIMIT`]th cancellation ends the request instead
/// ([`retype_cancelled_error`]). Does nothing if the request is gone by
/// then: a `RESUME` completed it, or the definition was dropped, and must
/// not get a stale record or a count.
async fn record_transient_retry(
    client: &mut tokio_postgres::Client,
    id: i64,
    source_table: &str,
    retry: &TransientRetry<'_>,
) -> Result<(), ApplyError> {
    let txn = client.transaction().await?;
    if lock_frozen(&txn, "id", &id, source_table).await?.is_none() {
        return Ok(());
    }
    let cancels: Option<i32> = if retry.timeout_cancel {
        txn.query_opt(
            "update resume_requests set timeout_cancels = timeout_cancels + 1 \
             where transform_id = $1 returning timeout_cancels",
            &[&id],
        )
        .await?
        .map(|row| row.get(0))
    } else {
        txn.query_opt(
            "select timeout_cancels from resume_requests where transform_id = $1",
            &[&id],
        )
        .await?
        .map(|row| row.get(0))
    };
    let Some(cancels) = cancels else {
        return Ok(());
    };
    let (columns, resuming) = retry.retrying;
    if retry.timeout_cancel && cancels >= RETYPE_TIMEOUT_CANCEL_LIMIT {
        txn.execute(
            "delete from resume_requests where transform_id = $1",
            &[&id],
        )
        .await?;
        let ended = retype_cancelled_error(retry);
        set_capture_failure(&txn, id, source_table, &[], &ended).await?;
        txn.commit().await?;
        tracing::warn!(transform_id = id, "a requested resume ended: {ended}");
        return Ok(());
    }
    let attempt = if retry.timeout_cancel {
        format!("attempt {cancels} of {RETYPE_TIMEOUT_CANCEL_LIMIT}, last error")
    } else {
        "last error".to_string()
    };
    let text = format!(
        "{resuming}. The last re-type failed ({attempt}: {}); the staging worker \
         tries again on its next pass",
        retry.error
    );
    set_capture_failure(&txn, id, source_table, columns, &text).await?;
    txn.commit().await?;
    Ok(())
}

/// The `capture_failure` of a definition whose resume's re-type of
/// `retry.labels` was cancelled [`RETYPE_TIMEOUT_CANCEL_LIMIT`] times by a
/// `statement_timeout` (#894). The definition stays paused, the copies keep
/// their types and the target its rows. Both remedies are named: raise the
/// timeout, restart Trellis and resume again, or drop the definition and
/// define it again, which isn't subject to the re-type rewrite.
///
/// The restart is part of the first remedy: a `statement_timeout` set on a
/// role or database reaches only connections opened after it, and the
/// staging worker re-types on its long-lived producer session
/// (`staging::session::ProducerSession`). A `pg_cancel_backend` of the
/// re-type is a 57014 too, so the message says "cancelled" and quotes
/// Postgres's reason for the last one.
fn retype_cancelled_error(retry: &TransientRetry<'_>) -> String {
    let kept = if retry.retyped.is_empty() {
        String::new()
    } else {
        format!(" ({} were re-typed already)", retry.retyped.join(", "))
    };
    let target = retry.target;
    format!(
        "the resume couldn't re-type Trellis's columns {} of the target table {target}: the \
         re-type was cancelled {RETYPE_TIMEOUT_CANCEL_LIMIT} times, by a statement_timeout \
         on Trellis's role or database or a cancel request (last: {}). Trellis honours the \
         timeout. They keep their types{kept}, the target keeps its rows, and the definition \
         stays paused. Raise statement_timeout for Trellis's role or database, restart \
         Trellis so its staging worker connects with the new value (a role or database \
         setting reaches only new connections), and resume the definition again, and the \
         re-type can finish. Or DROP TRANSFORM {target} and define it again, which builds the \
         target from empty and isn't subject to the re-type rewrite",
        retry.labels.join(", "),
        retry.error,
    )
}

/// Ends definition `id`'s resume request without resuming it, in a
/// transaction of its own: it stays paused, with `error` as its
/// `capture_failure`. Locked as a resume locks ([`lock_frozen`]: the fence
/// bump first, then the definition's row), so it serializes with a
/// `RESUME` or a drop of the same definition. Does nothing if the request
/// is gone by then: a `RESUME` completed it (the definition is no longer
/// paused, and must not get a record), or the definition was dropped.
async fn end_request(
    client: &mut tokio_postgres::Client,
    id: i64,
    source_table: &str,
    error: &str,
) -> Result<(), ApplyError> {
    let txn = client.transaction().await?;
    if lock_frozen(&txn, "id", &id, source_table).await?.is_none() {
        return Ok(());
    }
    let ended = txn
        .execute(
            "delete from resume_requests where transform_id = $1",
            &[&id],
        )
        .await?
        == 1;
    if !ended {
        return Ok(());
    }
    let columns: Vec<String> = Vec::new();
    set_capture_failure(&txn, id, source_table, &columns, error).await?;
    txn.commit().await?;
    tracing::warn!(transform_id = id, "a requested resume ended: {error}");
    Ok(())
}

// ---------------------------------------------------------------------
// Release
// ---------------------------------------------------------------------

/// Operator-driven release ([`crate::app::Trellis::release_key`]), one
/// transaction: stages one image-less `Recompute` of `(src_table, key)` into
/// the active batch, then deletes the key's `poison_held` row, its marker,
/// and its death counter for the definition whose bare target is `transform`
/// (#799: whole-key poison is per transform, and so is its release). Another
/// definition holding the same key keeps holding it. If the key's cause is
/// still there, the `Recompute` fails like the change before it did, and the
/// key is poisoned again once it reaches [`DEFAULT_DEATH_THRESHOLD`].
///
/// The `Recompute` reaches every reader of the key's table, not only
/// `transform`: one that doesn't hold the key re-derives it from the live
/// row, which is idempotent, and one that does parks it, as it parks every
/// change to a key it holds.
///
/// The parked changes are discarded, not replayed (#623 D3, the D split's
/// finding 7). A replayed CDC row would carry the releaser's `row_txid`, not
/// its source transaction's, and a ledger target decides whether a change is
/// already counted by that id (ADR-0002 I2), so a replay could regress an
/// entry a later Re-derive already moved past. The `Recompute` re-derives
/// the key from its current row instead, on every target that reads it. It
/// carries what the parked changes told a reader beyond the current row,
/// which every park merged into the key's one held row ([`HeldKey`], #803):
///
/// - the earliest parked change's pre-image (a recompute's own hint, or a
///   CDC change's old image) as its prior image, the state readers last saw,
///   which the relationship paths still read for the old join value (#624
///   drops it);
/// - the parked changes' earliest `origin_lsn` (unknown if any is) and
///   `src_changed`, and their deepest `hop_gen` (0 if any is a source
///   change), so the key's band stays blocked until the release drains (doc
///   06), as the replayed changes' own positions used to keep it;
/// - the union of their `group_key`s.
///
/// A key marked poisoned with nothing parked is re-derived the same way,
/// with no prior image and an unknown origin.
///
/// A `Recompute` builds no reverse record, so it never moves a to-one
/// relationship's projection of the key's table. The release writes each
/// projection row the parked changes named (the held row's pre-image and
/// `join_values`, the `to_col` values of every new image they folded and of
/// every park's pre-image), and
/// the live row names, from the live row itself in the same transaction
/// (`apply::release_to_one_projections`, issue #754), before the
/// `Recompute` re-derives the key's from-side rows from it.
///
/// The `Recompute`'s reverse path re-derives the from-side rows of its
/// prior image's join values and its live row's, and batches drain out of
/// order, so a value between them may have been read by from-side rows a
/// batch drained unheld. The release stages one
/// [`StagedChange::ReleasedJoinValue`] per `join_values` element beside the
/// `Recompute` (#944, [`HeldKey::release`]), which folds into it, so its
/// reverse path re-derives every such value's from-side rows on a later
/// page, to-one and to-many alike. That page takes their fences and locks;
/// the release takes none for them.
///
/// **Concurrency.** Three things write a definition's held keys: a drain
/// page parks a change for one ([`park_batch_contribution`]), an eviction
/// poisons one ([`isolate_and_evict`], a build chunk's [`evict_build_key`]),
/// and a resume or drop deletes them all. The release serializes with each:
///
/// - its first lock (#744) is a bump of the version fence of the key's
///   table, the canonical name the drain fences its pages on. It waits for
///   every page holding the fence, so a page that parked a change for the
///   key has committed, and the reads below see its row (ADR-0002 I1). A
///   page that read the fence before the release and applies after it
///   misses the fence and computes again, finding the key no longer held,
///   and one that read the key as held but the fence after the release parks
///   nothing ([`park_batch_contribution`] parks only while the key's
///   `poison` row is there). So no page can park a change for the key after
///   the release has deleted its rows, which would leave a held row no
///   release names, blocking every watermark token (`converge`'s condition
///   4);
/// - it then takes the definition's row `for no key update`, the lock an
///   eviction takes before it poisons a key (`evict_for`, and a build
///   chunk's `for update`), and that a resume's `for update` and a drop's
///   delete conflict with. The key's rows are read only after it: an
///   eviction that committed first is released with the rest, and a resume
///   or drop that committed first leaves nothing held, so the release
///   refuses ([`ApplyError::KeyNotHeld`]) or finds no definition
///   ([`ApplyError::TransformNotFound`]). An eviction whose isolation
///   charged the key before the release but that locks the definition after
///   it re-reads the key's death counter, which the release deleted, and
///   poisons nothing ([`still_charged_to_threshold`], #880). A key that
///   fails again is charged from 1 again.
///
/// Neither lock is held while a page waits on one of the release's, since a
/// page takes its fences before any other lock: the release takes the
/// fence, then the definition row, then the ring and the projection rows,
/// the order a page takes them in.
///
/// `src_table` may be either spelling of the table (#283), and `key` is the
/// key text [`crate::app::Trellis::sample_quarantined`] reports. Errors with
/// [`ApplyError::TransformNotFound`] for an unknown `transform`, and with
/// [`ApplyError::KeyNotHeld`], changing nothing, when the transform holds no
/// such key. A first lock that waits out the session's `lock_timeout` is
/// [`ApplyError::ReleaseLockTimeout`], also changing nothing: retryable.
/// Returns how many held rows it deleted: 1 for a key with parked changes,
/// 0 for one only marked poisoned (#803; one more for a key also held
/// under its other spelling, #283).
pub async fn release_key(
    pool: &Pool,
    transform: &str,
    src_table: &str,
    key: &str,
) -> Result<usize, ApplyError> {
    // Issue #283: quarantine's tables are keyed canonically, but this is an
    // operator entry point that may be handed either spelling of a source.
    // Matching the set of both is what keeps a release total either way — a
    // partial release would leave orphaned parked work `converge` gates on
    // forever, which is strictly worse than the extra array element. The
    // `Recompute` is staged under the first held row's own stored
    // `src_table`, so a held row keyed raw goes back onto the ring as it
    // left it.
    let names = canonical_and_raw(pool, src_table).await?;
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    // The first lock (#744): see the doc comment's "Concurrency". `names[0]`
    // is the canonical name, the one a page's fence read set keys the
    // table's changes on. A name that resolves to no table bumps a fence no
    // page reads, and the release then refuses and rolls it back.
    // A page that holds the fence past the lock timeout makes the release
    // give up (#842): name that wait, retryable, rather than return a raw
    // `55P03`. The bump never landed, and dropping `txn` rolls it back.
    if let Err(err) = super::build::bump_version_fence(&*txn, &names[0]).await {
        if crate::locks::is_lock_not_available(&err) {
            return Err(ApplyError::ReleaseLockTimeout {
                transform: transform.to_string(),
                src_table: src_table.to_string(),
                key: key.to_string(),
            });
        }
        return Err(err.into());
    }
    let transform_id: i64 = txn
        .query_opt(
            "select id from transform_definitions where split_part(target_table, '.', 2) = $1 \
             for no key update",
            &[&transform],
        )
        .await?
        .ok_or_else(|| ApplyError::TransformNotFound {
            transform: transform.to_string(),
        })?
        .get(0);

    let marked = txn
        .query(
            "delete from poison \
             where transform_id = $3 and src_table = any($1::text[]) and key = $2 \
             returning src_table",
            &[&names, &key, &transform_id],
        )
        .await?;
    let held = txn
        .query(
            "delete from poison_held \
             where transform_id = $3 and src_table = any($1::text[]) and key = $2 \
             returning src_table, seg_seq, old_image::text, origin_lsn, src_changed, hop_gen, \
                       group_key, join_values::text[], lsn",
            &[&names, &key, &transform_id],
        )
        .await?;
    if marked.is_empty() && held.is_empty() {
        // Dropping `txn` rolls the fence bump back.
        return Err(ApplyError::KeyNotHeld {
            transform: transform.to_string(),
            src_table: src_table.to_string(),
            key: key.to_string(),
        });
    }
    // One held row per spelling the key was parked under (#803), merged as
    // a park would have merged them under one.
    let merged = held
        .iter()
        .map(|row| {
            HeldKey::parked(
                row.get(0),
                row.get(1),
                row.get(2),
                row.get(3),
                row.get(4),
                row.get(5),
                row.get(6),
            )
        })
        .reduce(HeldKey::merge);
    // #944: every join value the held row names rides along with the
    // `Recompute` (`HeldKey::release`).
    let join_values: BTreeSet<String> = held
        .iter()
        .flat_map(|row| row.get::<_, Vec<String>>(7))
        .collect();
    let staged = match merged {
        Some(merged) => merged.release(key, join_values),
        None => vec![StagedChange::Recompute {
            src_table: marked[0].get(0),
            key: key.to_string(),
            hop_gen: 0,
            group_key: None,
            src_changed: None,
            prior_image: None,
            origin_lsn: None,
        }],
    };
    append::append(&txn, &staged).await?;

    txn.execute(
        "delete from key_deaths \
         where transform_id = $3 and src_table = any($1::text[]) and key = $2",
        &[&names, &key, &transform_id],
    )
    .await?;
    // Issue #754: the `Recompute` builds no reverse record, so a to-one
    // projection of this table hears about the parked changes only here.
    // A parked change's ring rows stay in its segment, at or below the held
    // row's `lsn` (its window's greatest), and an eviction parks them before
    // the page that skips them commits, so they can still be pending. The
    // release counts only pending changes above the latest parked one: the
    // parked ones are discarded here and will never write the key.
    let images: Vec<String> = held
        .iter()
        .flat_map(|row| {
            row.get::<_, Option<String>>(2)
                .into_iter()
                .chain(row.get::<_, Vec<String>>(7))
        })
        .collect();
    let parked_through: Option<PgLsn> = held
        .iter()
        .filter_map(|row| row.get::<_, Option<PgLsn>>(8))
        .max();
    apply::release_to_one_projections(pool, &txn, &names[0], key, &images, parked_through).await?;

    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        &*txn,
        super::interleave::PausePoint::BeforeReleaseCommit,
        &names[0],
    )
    .await?;
    txn.commit().await?;
    tracing::info!(
        transform,
        src_table = %names[0],
        key,
        held_rows = held.len(),
        "released a held key; a recompute re-derives it from its current row"
    );
    Ok(held.len())
}

/// Releases the keys the staging worker's in-place re-types asked for
/// (#824, `staging::schema_change`): for each `retype_releases` row, every
/// key its definition holds whose failure had the row's SQLSTATE (`22001`
/// after a `varchar` was re-typed, `22003` after a `numeric` was), each
/// through [`release_key`], so its parked work is applied again. A key held
/// for any other failure stays held, and so does one whose failure Trellis
/// recorded no SQLSTATE for.
///
/// The staging worker runs it after each capture pass, so a release the
/// re-type's transaction asked for is made even if the worker stopped in
/// between. A row is kept, and its release run again after each pass, for
/// [`RETYPE_RELEASE_WINDOW`] after the re-type asked for it: a drain that
/// reproduced a key's failure before the re-type can commit the key's
/// eviction after this read the keys held, and the next pass's release
/// finds it. A row is deleted once all its keys are released and the window
/// has passed, and only if no re-type asked again since it was read
/// (`requested_at`). A release that waits out its lock timeout
/// ([`ApplyError::ReleaseLockTimeout`]), or fails otherwise, keeps the row
/// for the next pass. A row whose definition was dropped is deleted. Errs
/// only when the rows can't be read or deleted.
pub(crate) async fn release_retyped_keys(pool: &Pool) -> Result<(), ApplyError> {
    let requests = {
        let client = pool.get().await?;
        client
            .execute(
                "delete from retype_releases r where not exists \
                 (select 1 from transform_definitions d where d.id = r.transform_id)",
                &[],
            )
            .await?;
        client
            .query(
                "select r.transform_id, r.sqlstate, r.requested_at, \
                        split_part(d.target_table, '.', 2) \
                 from retype_releases r join transform_definitions d on d.id = r.transform_id \
                 order by r.transform_id, r.sqlstate",
                &[],
            )
            .await?
    };
    for request in requests {
        let id: i64 = request.get(0);
        let sqlstate: String = request.get(1);
        let requested_at: SystemTime = request.get(2);
        let transform: String = request.get(3);
        let keys = pool
            .get()
            .await?
            .query(
                "select src_table, key from poison \
                 where transform_id = $1 and sqlstate = $2 order by src_table, key",
                &[&id, &sqlstate],
            )
            .await?;
        let mut done = true;
        let mut released = 0usize;
        for row in keys {
            let src_table: String = row.get(0);
            let key: String = row.get(1);
            match release_key(pool, &transform, &src_table, &key).await {
                Ok(_) => released += 1,
                // Released or dropped meanwhile.
                Err(ApplyError::KeyNotHeld { .. } | ApplyError::TransformNotFound { .. }) => {}
                Err(err) => {
                    done = false;
                    tracing::warn!(
                        transform = %transform,
                        src_table = %src_table,
                        key = %key,
                        error = %err,
                        "couldn't release a key held for a value its column couldn't hold before \
                         Trellis re-typed it; retrying next pass"
                    );
                }
            }
        }
        if released > 0 {
            tracing::info!(
                transform = %transform,
                sqlstate = %sqlstate,
                released,
                "released the keys held for values their columns couldn't hold before Trellis \
                 re-typed them"
            );
        }
        if done {
            pool.get()
                .await?
                .execute(
                    "delete from retype_releases \
                     where transform_id = $1 and sqlstate = $2 and requested_at = $3 \
                       and requested_at < now() - $4::float8 * interval '1 second'",
                    &[
                        &id,
                        &sqlstate,
                        &requested_at,
                        &RETYPE_RELEASE_WINDOW.as_secs_f64(),
                    ],
                )
                .await?;
        }
    }
    Ok(())
}

/// How long [`release_retyped_keys`] keeps releasing the keys an in-place
/// re-type asked for: twice the session lock timeout. An eviction that
/// reproduced a failure from before the re-type commits within it unless
/// its own lock waits, each short of the timeout (one that reaches it rolls
/// the eviction back), add up to more. A key evicted later than that stays
/// held until it is released by hand.
pub(crate) const RETYPE_RELEASE_WINDOW: std::time::Duration =
    std::time::Duration::from_secs(2 * crate::locks::LOCK_TIMEOUT.as_secs());

/// The keys one definition holds in quarantine (#759): its `poison` rows,
/// each a source key whose changes the definition's apply leaves out until
/// a release ([`release_key`]), a resume or a drop. `None` where a
/// definition holds none. `status` reports it per definition, and
/// `self_check` with every report, so a definition that reads `live` can't
/// hide a held key. The keys themselves are paged by `sample_quarantined`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldKeys {
    /// How many keys the definition holds.
    pub count: u64,
    /// The earliest `poisoned_at` among them: when the key held longest was
    /// last poisoned. A key poisoned again (after a release, or a drain that
    /// charged it again) counts from then.
    pub oldest_poisoned_at: SystemTime,
}

/// The [`HeldKeys`] of definition `transform_id`, in one read of its
/// `poison` rows (their primary key leads with the definition's id).
pub(crate) async fn held_keys(
    client: &impl GenericClient,
    transform_id: i64,
) -> Result<Option<HeldKeys>, tokio_postgres::Error> {
    let row = client
        .query_one(
            "select count(*), min(poisoned_at) from poison where transform_id = $1",
            &[&transform_id],
        )
        .await?;
    Ok(held_keys_from(row.get(0), row.get(1)))
}

/// A [`HeldKeys`] from a `count(*)` and `min(poisoned_at)` over a
/// definition's `poison` rows; `None` when there are none.
pub(crate) fn held_keys_from(count: i64, oldest: Option<SystemTime>) -> Option<HeldKeys> {
    oldest.map(|oldest_poisoned_at| HeldKeys {
        count: u64::try_from(count).unwrap_or(0),
        oldest_poisoned_at,
    })
}

// ---------------------------------------------------------------------
// The one sanctioned exception to immutability
// ---------------------------------------------------------------------

/// **The only sanctioned write to a sealed batch's rows** (doc 06). A staged
/// row naming a table Postgres no longer has can never apply — the apply
/// raises [`ApplyError::SourceTableDropped`] before writing anything, so the
/// batch can never drain and, per doc 06's condition 4, wedges the whole
/// ring below it. The escape hatch: delete `src_table`'s rows from every
/// ring table and from the quarantine track (a poisoned/held/dying key
/// naming a table that no longer exists is equally unresolvable by retry or
/// release). Invoked only from [`super::apply::drain_once`], in response to
/// a *live* `42P01` from a query against `src_table` itself — not a cached
/// or stale signal, so there is no separate "reload and check again" step
/// here: the error that triggers this call already reflects current
/// database state.
///
/// The quarantine deletes match both the canonical identity and the raw ring
/// spelling (issue #283), for the same reason [`release_key`]'s do: a leftover
/// `poison_held` row for a table that no longer exists can never be released or
/// drained, so a partial purge re-wedges exactly what this call exists to
/// unwedge. The *ring* deletes stay on the raw spelling alone — `src_table`
/// there is the string the wedged rows literally hold, which is what
/// `SourceTableDropped` reported (issue #267's final fix) and the only spelling
/// that can be in the ring for this call to have happened at all. Note that a
/// dropped table is precisely the case [`qualified_src_table`] cannot resolve,
/// so for a bare wedged row the canonical name usually *is* the raw one here,
/// and the array is what covers the case where it *is* still resolvable (the
/// table is gone but `schema_nodes`/`transform_definitions` still name it) and
/// the quarantine rows are bare while the ring rows are not.
///
/// **Known residual, deliberately not widened here:** [`canonical_and_raw`]
/// derives the canonical name *from* the given one, so it never adds the bare
/// suffix when handed an already-qualified spelling. A legacy bare quarantine
/// row for a table `V33__quarantine_canonical_src_table.sql` could not fold
/// (its bare suffix ambiguous across two schemas, or unresolvable at migration
/// time) therefore still survives a purge keyed on the qualified ring spelling —
/// exactly the pre-#283 behavior, since that delete was single-spelling too.
/// Stripping to the bare suffix here would fix that only by reintroducing the
/// ambiguity #74/ADR-0007 exists to prevent: a purge of `archive.orders` would
/// delete a bare `orders` row that may belong to `public.orders`. Tracked as its
/// own question rather than guessed at.
pub async fn purge_dropped_table(pool: &Pool, src_table: &str) -> Result<(), ApplyError> {
    let names = canonical_and_raw(pool, src_table).await?;
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    for slot in 0..RING_SIZE {
        let table = ring_table_name(slot)?;
        txn.execute(
            &format!("delete from {table} where src_table = $1"),
            &[&src_table],
        )
        .await?;
    }
    txn.execute(
        "delete from poison where src_table = any($1::text[])",
        &[&names],
    )
    .await?;
    txn.execute(
        "delete from poison_held where src_table = any($1::text[])",
        &[&names],
    )
    .await?;
    txn.execute(
        "delete from key_deaths where src_table = any($1::text[])",
        &[&names],
    )
    .await?;
    txn.commit().await?;
    Ok(())
}

/// `src_table` plus its canonical identity, deduped — the match set the two
/// whole-key/whole-source *deletes* above use instead of a single spelling
/// (issue #283). Only for delete paths that must be total: counting and
/// charging still key strictly on the canonical identity, which is the entire
/// point of that issue (two spellings, one budget), and would be re-split by
/// matching a set here.
async fn canonical_and_raw(pool: &Pool, src_table: &str) -> Result<Vec<String>, ApplyError> {
    let canonical = qualified_src_table(pool, src_table).await?;
    if canonical == src_table {
        return Ok(vec![canonical]);
    }
    Ok(vec![canonical, src_table.to_string()])
}

// ---------------------------------------------------------------------
// The halting-stop metric
// ---------------------------------------------------------------------

/// Increments `halting_stops`' single row and records `reason` — the "real
/// metric (counter + last reason)" doc 05's failure-classification table
/// calls for on the halting class, so "stopped" and "slow" are
/// distinguishable from the outside. A plain counter table, matching this
/// crate's existing convention for small operational state (`drainers`)
/// rather than a Prometheus-style dependency this crate has none of today.
///
/// Runs in the transaction that pauses the halt's closure
/// (`staging::halt::halt_closure`, #663), and only when it paused one, so
/// the count is of episodes, not of attempts.
pub async fn record_halting_stop(txn: &Transaction<'_>, reason: &str) -> Result<(), ApplyError> {
    txn.execute(
        "update halting_stops \
             set stop_count = stop_count + 1, last_reason = $1, last_stopped_at = now() \
             where id",
        &[&reason],
    )
    .await?;
    Ok(())
}

/// [`record_halting_stop`]'s counterpart read: the current stop count and
/// last reason, for an operator dashboard or health check.
#[derive(Debug, Clone)]
#[cfg(any(test, feature = "internals"))]
pub struct HaltingStopStats {
    pub stop_count: i64,
    pub last_reason: Option<String>,
    pub last_stopped_at: Option<SystemTime>,
}

#[cfg(any(test, feature = "internals"))]
pub async fn halting_stop_stats(pool: &Pool) -> Result<HaltingStopStats, ApplyError> {
    let client = pool.get().await?;
    let row = client
        .query_one(
            "select stop_count, last_reason, last_stopped_at from halting_stops where id",
            &[],
        )
        .await?;
    Ok(HaltingStopStats {
        stop_count: row.get(0),
        last_reason: row.get(1),
        last_stopped_at: row.get(2),
    })
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::defs::model::Definition;

    fn probe(src: &str, key: &str) -> PoisonedProbe {
        PoisonedProbe {
            raw_src_table: src.trim_start_matches("public.").to_string(),
            canonical_src_table: src.to_string(),
            key: key.to_string(),
            culprit: Culprit {
                transform_id: 1,
                target: "t".to_string(),
                epoch: None,
                last_error: "boom".to_string(),
            },
            sqlstate: None,
        }
    }

    fn rel_err(message: &str) -> ApplyError {
        ApplyError::Pool(crate::error::Error::Config(message.to_string()))
    }

    /// Three relationships, `r1` read by 1 and 2, `r2` by 2 and 3, `r3` by 4.
    fn three_relationships() -> Vec<(i64, Vec<(i64, String)>)> {
        let readers = |ids: &[i64]| ids.iter().map(|id| (*id, format!("t{id}"))).collect();
        vec![
            (1, readers(&[1, 2])),
            (2, readers(&[2, 3])),
            (3, readers(&[4])),
        ]
    }

    /// Each charged reader, with the error it's charged with.
    fn charges(charged: BTreeMap<i64, (String, String)>) -> Vec<(i64, String)> {
        charged
            .into_iter()
            .map(|(id, (target, error))| {
                assert_eq!(target, format!("t{id}"));
                (id, error)
            })
            .collect()
    }

    #[test]
    fn relationship_charges_charge_the_readers_of_each_relationship_that_fails_alone() {
        let shared = rel_err("shared");
        let charged = relationship_charges(
            &three_relationships(),
            vec![
                Probed::Failed(rel_err("r1")),
                Probed::Clean,
                Probed::Failed(rel_err("r3")),
            ],
            &shared,
        );
        let r1 = rel_err("r1").to_string();
        let r3 = rel_err("r3").to_string();
        // 2 reads through `r2` too, which applied, and is charged once.
        assert_eq!(charges(charged), vec![(1, r1.clone()), (2, r1), (4, r3)]);
    }

    #[test]
    fn relationship_charges_charge_every_reader_when_none_fails_alone() {
        let shared = rel_err("shared");
        let charged = relationship_charges(
            &three_relationships(),
            vec![Probed::Clean, Probed::Clean, Probed::Clean],
            &shared,
        );
        let s = shared.to_string();
        assert_eq!(
            charges(charged),
            vec![(1, s.clone()), (2, s.clone()), (3, s.clone()), (4, s)]
        );
    }

    /// The probe limit, reached after `r1`'s probe: `r2` and `r3` weren't
    /// probed, so their readers are charged with the combined probe's error,
    /// beside `r1`'s, which failed alone.
    #[test]
    fn relationship_charges_charge_every_relationship_left_unprobed_at_the_limit() {
        let shared = rel_err("shared");
        let charged = relationship_charges(
            &three_relationships(),
            vec![
                Probed::Failed(rel_err("r1")),
                Probed::OutOfProbes,
                Probed::OutOfProbes,
            ],
            &shared,
        );
        let (r1, s) = (rel_err("r1").to_string(), shared.to_string());
        assert_eq!(
            charges(charged),
            vec![(1, r1.clone()), (2, r1), (3, s.clone()), (4, s)]
        );

        // `r1` applied before the limit: only the unprobed ones are charged.
        let charged = relationship_charges(
            &three_relationships(),
            vec![Probed::Clean, Probed::OutOfProbes, Probed::OutOfProbes],
            &shared,
        );
        let s = shared.to_string();
        assert_eq!(
            charges(charged),
            vec![(2, s.clone()), (3, s.clone()), (4, s)]
        );
    }

    /// A probe that settles nothing can't rule its relationship out, so with
    /// no other failing alone, nobody is charged on this drain; one that did
    /// fail alone is still charged.
    #[test]
    fn relationship_charges_charge_nobody_for_an_unsettled_relationship() {
        let shared = rel_err("shared");
        let charged = relationship_charges(
            &three_relationships(),
            vec![Probed::Clean, Probed::Unsettled, Probed::Clean],
            &shared,
        );
        assert!(charged.is_empty(), "{charged:?}");

        let charged = relationship_charges(
            &three_relationships(),
            vec![
                Probed::Clean,
                Probed::Unsettled,
                Probed::Failed(rel_err("r3")),
            ],
            &shared,
        );
        assert_eq!(charges(charged), vec![(4, rel_err("r3").to_string())]);
    }

    /// `ceil(log2(n))`: how many halvings take `n` records down to one.
    fn halvings(n: usize) -> usize {
        (usize::BITS - (n - 1).leading_zeros()) as usize
    }

    /// Drives a [`Bisector`] over `n` records with a synthetic probe:
    /// `verdict` decides each run's outcome from its range alone.
    fn bisect_with(
        n: usize,
        max_probes: usize,
        verdict: impl Fn(&Range<usize>) -> ProbeVerdict<()>,
    ) -> Bisection<()> {
        bisect_from(n, max_probes, 0, verdict)
    }

    /// [`bisect_with`], with dead ends' single-record probes starting at
    /// `dead_end_offset`.
    fn bisect_from(
        n: usize,
        max_probes: usize,
        dead_end_offset: usize,
        verdict: impl Fn(&Range<usize>) -> ProbeVerdict<()>,
    ) -> Bisection<()> {
        let mut bisector = Bisector::new(n, max_probes, dead_end_offset);
        while let Some(run) = bisector.next_run() {
            let outcome = verdict(&run);
            bisector.record(run, outcome);
        }
        bisector.finish()
    }

    /// A run fails when it holds any of `poisoned`, each of which fails alone.
    fn fails_alone(poisoned: &[usize]) -> impl Fn(&Range<usize>) -> ProbeVerdict<()> + '_ {
        move |run| {
            if poisoned.iter().any(|p| run.contains(p)) {
                ProbeVerdict::Failed(())
            } else {
                ProbeVerdict::Clean
            }
        }
    }

    fn failing_indexes(found: &Bisection<()>) -> Vec<usize> {
        found.failing.iter().map(|(index, ())| *index).collect()
    }

    /// Issue #655: one poisoned record, the only failure in a page at the
    /// default `drain_batch_cap`, is found in two probes per halving (34),
    /// not one probe per record (100,000), wherever it sits in the page.
    #[test]
    fn bisect_finds_the_one_poisoned_record_in_two_probes_per_halving() {
        let n = 100_000;
        for p in [0, 1, 49_999, 50_000, 73_421, n - 1] {
            let found = bisect_with(n, MAX_ISOLATION_PROBES, fails_alone(&[p]));
            assert_eq!(failing_indexes(&found), vec![p]);
            assert!(
                found.probes <= 2 * halvings(n),
                "record {p}: {} probes, more than 2 per halving ({})",
                found.probes,
                2 * halvings(n)
            );
            assert!(!found.exhausted);
        }
    }

    /// Issue #655: two poisoned records are both found, in at most two probes
    /// per halving each, whether their paths split at the top or share every
    /// halving but the last.
    #[test]
    fn bisect_finds_two_poisoned_records() {
        let n = 100_000;
        for poisoned in [[17, 90_000], [500, 501], [0, n - 1]] {
            let found = bisect_with(n, MAX_ISOLATION_PROBES, fails_alone(&poisoned));
            assert_eq!(failing_indexes(&found), poisoned.to_vec());
            assert!(
                found.probes <= 2 * 2 * halvings(n),
                "{poisoned:?}: {} probes",
                found.probes
            );
            assert!(!found.exhausted);
        }
    }

    #[test]
    fn bisect_probes_a_single_record_batch_once_and_an_empty_one_never() {
        let found = bisect_with(1, MAX_ISOLATION_PROBES, fails_alone(&[0]));
        assert_eq!((failing_indexes(&found), found.probes), (vec![0], 1));
        let found = bisect_with(1, MAX_ISOLATION_PROBES, fails_alone(&[]));
        assert_eq!((failing_indexes(&found), found.probes), (vec![], 1));
        let found = bisect_with(0, MAX_ISOLATION_PROBES, fails_alone(&[]));
        assert_eq!((failing_indexes(&found), found.probes), (vec![], 0));
    }

    /// Two records that are fine alone but fail together: bisection follows
    /// the pair down until the halving that splits it, where neither half
    /// fails. That dead end probes each record of both halves alone, finds
    /// nothing to blame, and blames nothing. Deep in the search that is a few
    /// probes; at the top of a large page it is the rest of the probe limit.
    #[test]
    fn bisect_blames_nothing_for_a_failure_that_only_appears_in_combination() {
        let n = 100_000;
        let pair = |a: usize, b: usize| {
            move |run: &Range<usize>| {
                if run.contains(&a) && run.contains(&b) {
                    ProbeVerdict::Failed(())
                } else {
                    ProbeVerdict::Clean
                }
            }
        };
        // Split deep in the search: a small dead end.
        let found = bisect_with(n, MAX_ISOLATION_PROBES, pair(10, 11));
        assert!(found.failing.is_empty(), "{:?}", found.failing);
        assert!(!found.exhausted);
        assert!(found.probes <= 2 * halvings(n), "{} probes", found.probes);
        // Split by the first halving: the dead end is the whole page.
        for (a, b) in [(10, 60_000), (49_999, 50_000)] {
            let found = bisect_with(n, MAX_ISOLATION_PROBES, pair(a, b));
            assert!(found.failing.is_empty(), "({a}, {b}): {:?}", found.failing);
            assert!(found.exhausted);
            assert_eq!(found.probes, MAX_ISOLATION_PROBES);
        }
        // A two-record page: its halves are single records, already probed
        // alone, so the dead end costs nothing more.
        let found = bisect_with(2, MAX_ISOLATION_PROBES, pair(0, 1));
        assert_eq!((found.failing.len(), found.probes), (0, 2));
    }

    /// Review of #655: a dead end larger than the probe limit is probed from
    /// `dead_end_offset` and wraps around, so a masked record out of reach of
    /// one call's window is reached by a call with another offset, and random
    /// offsets reach it within a bounded number of calls.
    #[test]
    fn bisect_rotates_a_large_dead_ends_single_record_probes() {
        let n = 100_000;
        let masked = |k: usize| {
            move |run: &Range<usize>| {
                let holds = |i: usize| run.contains(&i);
                if holds(k) && (!holds(5) || holds(90_000)) {
                    ProbeVerdict::Failed(())
                } else {
                    ProbeVerdict::Clean
                }
            }
        };
        // Record 30,000 is masked by record 5 in the lower half, and 90,000
        // in the upper half completes the failure: a dead end at the top.
        // Out of reach from offset 0: the window is records 0..254.
        let found = bisect_from(n, MAX_ISOLATION_PROBES, 0, masked(30_000));
        assert!(found.failing.is_empty());
        assert!(found.exhausted);
        // An offset whose window covers it finds it.
        let found = bisect_from(n, MAX_ISOLATION_PROBES, 29_900, masked(30_000));
        assert_eq!(failing_indexes(&found), vec![30_000]);
        // The window wraps from the dead end's last record to its first.
        let found = bisect_from(n, MAX_ISOLATION_PROBES, n - 100, masked(50));
        assert_eq!(failing_indexes(&found), vec![50]);
        // The offset is taken modulo the dead end's length.
        let found = bisect_from(n, MAX_ISOLATION_PROBES, 3 * n + 29_900, masked(30_000));
        assert_eq!(failing_indexes(&found), vec![30_000]);

        // Seeded offsets (splitmix64), one per call as `isolate_and_evict`
        // draws them: the masked record is found well within the bound. Each
        // call's window covers 254 of 100,000 records, so about 394 calls are
        // expected; 4,000 is ten times that.
        let mut seed: u64 = 0x655;
        let mut next_offset = || {
            seed = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = seed;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            (z ^ (z >> 31)) as usize
        };
        let calls = (1..=4_000)
            .find(|_| {
                let found = bisect_from(n, MAX_ISOLATION_PROBES, next_offset(), masked(30_000));
                assert!(found.probes <= MAX_ISOLATION_PROBES);
                !found.failing.is_empty()
            })
            .expect("a masked record out of one window's reach is found within 4,000 calls");
        assert!(
            calls > 1,
            "the first seeded offset should not happen to cover it"
        );
    }

    /// Review of #655: a record that fails alone, masked inside its half by a
    /// batch-mate (a run fails only if it holds `k` without `m`, or `k`, `m`
    /// and `r` together, as an aggregate sum crossing a check might). The
    /// failing run whose halves both come back clean is a dead end, and
    /// probing its records alone pins `k`, as probing every record alone
    /// did before bisection.
    #[test]
    fn bisect_pins_a_record_masked_by_a_batch_mate_in_its_half() {
        let masked = |k: usize, m: usize, r: usize| {
            move |run: &Range<usize>| {
                let holds = |i: usize| run.contains(&i);
                if holds(k) && (!holds(m) || holds(r)) {
                    ProbeVerdict::Failed(())
                } else {
                    ProbeVerdict::Clean
                }
            }
        };
        // Halves {0, 1} and {2, 3} both pass: two half probes, four singles.
        let found = bisect_with(4, MAX_ISOLATION_PROBES, masked(1, 0, 3));
        assert_eq!(failing_indexes(&found), vec![1]);
        assert_eq!(found.probes, 6);
        assert!(!found.exhausted);

        // A dead end deep in a large page is small and cheap.
        let n = 100_000;
        let found = bisect_with(n, MAX_ISOLATION_PROBES, masked(1001, 1000, 1010));
        assert_eq!(failing_indexes(&found), vec![1001]);
        assert!(!found.exhausted);
        assert!(
            found.probes <= 2 * halvings(n) + 8,
            "{} probes",
            found.probes
        );

        // A dead end at the top of a large page: the single-record probes
        // reach a masked record near its start before the probe limit, and
        // the limit stops them past that.
        let found = bisect_with(n, MAX_ISOLATION_PROBES, masked(100, 5, 90_000));
        assert_eq!(failing_indexes(&found), vec![100]);
        assert!(found.exhausted);
        // Only single records are probed past the first two halves.
        let found = bisect_with(n, MAX_ISOLATION_PROBES, masked(1000, 5, 90_000));
        assert!(found.failing.is_empty());
        assert!(found.exhausted);
    }

    /// A transient error or a fence miss says nothing about the run.
    /// Bisection looks inside a run that hit one rather than skipping a half
    /// that may hold a failing record, and never blames a single record for
    /// one.
    #[test]
    fn bisect_looks_inside_a_transient_run_and_never_blames_a_record_for_it() {
        let n = 1000;
        // Record 100 makes every run holding it miss the fence; 900 fails
        // alone, and so does 150, which shares 100's half. (A record that
        // makes every run holding it hit a *transient* error stops the
        // search instead: see `bisect_stops_after_consecutive_transient_probes`.)
        let found = bisect_with(n, MAX_ISOLATION_PROBES, |run| {
            if run.contains(&900) || run.contains(&150) {
                ProbeVerdict::Failed(())
            } else if run.contains(&100) {
                ProbeVerdict::Unknown
            } else {
                ProbeVerdict::Clean
            }
        });
        assert_eq!(failing_indexes(&found), vec![150, 900]);
        assert!(!found.exhausted);
        assert!(!found.transient_storm);

        // A run that misses the fence on every probe is searched down to
        // single records, none of which is blamed. Fence misses don't count
        // toward the transient stop.
        let found = bisect_with(8, MAX_ISOLATION_PROBES, |_| ProbeVerdict::Unknown);
        assert!(found.failing.is_empty());
        assert_eq!(
            found.probes, 14,
            "every run of a full binary tree over 8 but the root"
        );
        assert!(!found.transient_storm);
    }

    /// A pool whose every checkout fails with a connection error (no
    /// SQLSTATE: transient), without any I/O until then: the socket
    /// directory doesn't exist.
    fn unreachable_pool() -> Pool {
        let config = crate::config::Config::from_dsn(
            "host=/nonexistent/trellis-issue-670 port=1 user=nobody dbname=nothing".to_string(),
        )
        .expect("valid dsn");
        Pool::new(&config).expect("a lazy pool")
    }

    fn folded_key(key: &str) -> FoldedChange {
        FoldedChange {
            src_table: "public.orders".to_string(),
            key: key.to_string(),
            new_image: Some(format!(r#"{{"id": {key}}}"#)),
            old_image: None,
            src_changed: Some(std::time::SystemTime::UNIX_EPOCH),
            origin_lsn: None,
            lsn: None,
            hop_gen: 0,
            first_seen: std::time::SystemTime::UNIX_EPOCH,
            group_key: None,
            is_truncate: false,
            relationship_reverse_deferred: None,
            retry_count: 0,
            prior_image: None,
            row_count: 1,
            has_recompute: false,
            ends_in_delete: false,
            last_change: None,
            to_col_values: Vec::new(),
        }
    }

    fn event_field<'a>(
        event: &'a crate::client::log_capture::CapturedEvent,
        name: &str,
    ) -> &'a str {
        event
            .fields
            .get(name)
            .map(|v| v.trim_matches('"'))
            .unwrap_or_else(|| panic!("no `{name}` field on {event:?}"))
    }

    /// Issue #670: isolation logs its start, with the batch's record count,
    /// and its end, with the probes it used, its outcome, the keys it
    /// pinned, whether the search completed, and how long it took. It also
    /// shows the transient stop end to end: every probe's compute fails to
    /// get a connection (transient), so isolation stops after
    /// [`MAX_CONSECUTIVE_TRANSIENT_PROBES`] probes rather than all
    /// [`MAX_ISOLATION_PROBES`], and says so.
    #[tokio::test]
    async fn isolation_logs_its_start_and_end_and_stops_on_a_transient_storm() {
        let (_guard, captured) = crate::client::log_capture::install_capture();
        let pool = unreachable_pool();
        let folded: Vec<FoldedChange> = (1..=64).map(|k| folded_key(&k.to_string())).collect();

        let outcome = isolate_and_evict(
            &pool,
            7,
            "worker-a",
            "wake",
            &folded,
            DEFAULT_DEATH_THRESHOLD,
            false,
        )
        .await
        .expect("a transient storm is an outcome, not an error");
        assert!(
            matches!(
                outcome,
                IsolationOutcome::TransientStorm {
                    probes: MAX_CONSECUTIVE_TRANSIENT_PROBES
                }
            ),
            "{outcome:?}"
        );

        let events = captured.0.lock().unwrap().clone();
        let isolation: Vec<_> = events
            .iter()
            .filter(|e| {
                e.fields
                    .get("message")
                    .is_some_and(|m| m.contains("isolat"))
            })
            .collect();
        assert_eq!(isolation.len(), 2, "{events:?}");
        let (start, end) = (isolation[0], isolation[1]);
        assert_eq!(start.level, tracing::Level::INFO);
        assert_eq!(
            event_field(start, "message"),
            "isolating a failed batch: probing its records for the keys that fail alone"
        );
        assert_eq!(event_field(start, "seg_seq"), "7");
        assert_eq!(event_field(start, "records"), "64");

        assert_eq!(end.level, tracing::Level::WARN, "the search stopped short");
        assert_eq!(event_field(end, "seg_seq"), "7");
        assert_eq!(event_field(end, "records"), "64");
        assert_eq!(
            event_field(end, "probes"),
            MAX_CONSECUTIVE_TRANSIENT_PROBES.to_string()
        );
        assert_eq!(event_field(end, "outcome"), "TransientStorm");
        assert_eq!(event_field(end, "pinned"), "0");
        assert_eq!(
            event_field(end, "search"),
            "stopped on consecutive transient errors"
        );
        event_field(end, "elapsed_ms")
            .parse::<u64>()
            .expect("elapsed_ms is a number");
    }

    /// Issue #670: the end-of-isolation line for a search that ran to
    /// completion is `info` and counts every key it pinned, evicted or only
    /// charged; one that stopped on an error is `warn` with the error.
    #[test]
    fn isolation_end_log_counts_pinned_keys_and_reports_errors() {
        let (_guard, captured) = crate::client::log_capture::install_capture();
        let complete = IsolationStats {
            probes: 20,
            stopped: None,
        };
        let charged = ChargedKey {
            transform: "order_totals".to_string(),
            src_table: "public.orders".to_string(),
            key: "3".to_string(),
            deaths: 1,
        };
        // Ten records, two evicted, one more charged below the threshold.
        let evicted = IsolationOutcome::Evicted {
            evicted: 2,
            charged: vec![charged.clone()],
        };
        log_isolation_finished(
            7,
            10,
            &complete,
            &Ok(evicted),
            std::time::Duration::from_millis(1500),
        );
        log_isolation_finished(
            7,
            10,
            &complete,
            &Ok(IsolationOutcome::ChargedBelowThreshold {
                charged: vec![charged],
            }),
            std::time::Duration::from_millis(5),
        );
        log_isolation_finished(
            7,
            10,
            &IsolationStats {
                probes: 4,
                stopped: None,
            },
            &Err(ApplyError::ClaimLost),
            std::time::Duration::from_millis(5),
        );

        let events = captured.0.lock().unwrap().clone();
        let summary: Vec<_> = events
            .iter()
            .map(|e| {
                (
                    e.level,
                    event_field(e, "outcome").to_string(),
                    e.fields.get("pinned").cloned(),
                    event_field(e, "probes").to_string(),
                )
            })
            .collect();
        assert_eq!(
            summary,
            vec![
                (
                    tracing::Level::INFO,
                    "Evicted".to_string(),
                    Some("3".to_string()),
                    "20".to_string()
                ),
                (
                    tracing::Level::INFO,
                    "ChargedBelowThreshold".to_string(),
                    Some("1".to_string()),
                    "20".to_string()
                ),
                (
                    tracing::Level::WARN,
                    "error".to_string(),
                    None,
                    "4".to_string()
                ),
            ],
            "{events:?}"
        );
        assert_eq!(event_field(&events[0], "elapsed_ms"), "1500");
        assert_eq!(event_field(&events[0], "search"), "complete");
        assert_eq!(
            event_field(&events[2], "error"),
            ApplyError::ClaimLost.to_string()
        );
    }

    /// Issue #670: a lock or deadlock storm used to split on every transient
    /// probe and run all [`MAX_ISOLATION_PROBES`], each able to wait out a
    /// whole `lock_timeout`. [`MAX_CONSECUTIVE_TRANSIENT_PROBES`] in a row
    /// stop the search, keeping what it already pinned.
    #[test]
    fn bisect_stops_after_consecutive_transient_probes() {
        // Every probe transient: it stops after the limit, pinning nothing.
        let found = bisect_with(100_000, MAX_ISOLATION_PROBES, |_| ProbeVerdict::Transient);
        assert_eq!(found.probes, MAX_CONSECUTIVE_TRANSIENT_PROBES);
        assert!(found.transient_storm);
        assert!(!found.exhausted);
        assert!(found.failing.is_empty());

        // A storm that starts once record 0 is pinned keeps it.
        let n = 1024;
        let probes = std::cell::Cell::new(0usize);
        let found = bisect_with(n, MAX_ISOLATION_PROBES, |run| {
            probes.set(probes.get() + 1);
            if probes.get() <= halvings(n) {
                fails_alone(&[0])(run)
            } else {
                ProbeVerdict::Transient
            }
        });
        assert_eq!(failing_indexes(&found), vec![0]);
        assert_eq!(found.probes, halvings(n) + MAX_CONSECUTIVE_TRANSIENT_PROBES);
        assert!(found.transient_storm);

        // One record whose every run hits a transient error (a target row
        // held locked) stops the search too: descending into it probes its
        // runs one after another. The page can't commit while that lasts
        // anyway, so a later drain loses nothing by isolating then.
        let found = bisect_with(n, MAX_ISOLATION_PROBES, |run| {
            if run.contains(&900) {
                ProbeVerdict::Failed(())
            } else if run.contains(&100) {
                ProbeVerdict::Transient
            } else {
                ProbeVerdict::Clean
            }
        });
        assert!(found.transient_storm);
        assert!(found.failing.is_empty());

        // Transients between clean or failing probes are not a storm: the
        // search runs to completion.
        let probes = std::cell::Cell::new(0usize);
        let found = bisect_with(n, MAX_ISOLATION_PROBES, |run| {
            probes.set(probes.get() + 1);
            if probes
                .get()
                .is_multiple_of(MAX_CONSECUTIVE_TRANSIENT_PROBES)
            {
                ProbeVerdict::Transient
            } else {
                fails_alone(&[700])(run)
            }
        });
        assert!(!found.transient_storm);
        assert!(!found.exhausted);
        assert!(found.probes > 2 * halvings(n), "{found:?}");
    }

    /// The probe limit stops the search, keeping what it pinned so far. Depth
    /// first, lowest index first: the lowest failing records are pinned before
    /// the limit, so repeated isolations of the same batch charge the same
    /// keys and drive them to eviction.
    #[test]
    fn bisect_stops_at_the_probe_limit_keeping_what_it_pinned() {
        let found = bisect_with(1000, 64, |_| ProbeVerdict::Failed(()));
        assert_eq!(found.probes, 64);
        assert!(found.exhausted);
        let failing = failing_indexes(&found);
        assert!(!failing.is_empty());
        assert_eq!(failing, (0..failing.len()).collect::<Vec<_>>());

        // Exactly enough probes finishes without being exhausted.
        let needed = bisect_with(1000, MAX_ISOLATION_PROBES, fails_alone(&[123])).probes;
        let found = bisect_with(1000, needed, fails_alone(&[123]));
        assert!(!found.exhausted);
        assert_eq!(failing_indexes(&found), vec![123]);
        let found = bisect_with(1000, needed - 1, fails_alone(&[123]));
        assert!(found.exhausted);
    }

    #[test]
    fn partition_by_threshold_separates_evictions_from_below_threshold_charges() {
        // Issue #614: the below-threshold keys must survive the partition as
        // named `ChargedKey`s with their death count, rather than being dropped
        // (which left the caller unable to tell them from "nothing reproduced").
        let (evict, below) = partition_by_threshold(
            vec![
                (probe("public.gizmos", "1"), 5),
                (probe("public.gizmos", "2"), 1),
                (probe("public.gizmos", "3"), 7),
                (probe("public.widgets", "4"), 4),
            ],
            5,
        );
        let evicted: Vec<&str> = evict.iter().map(|p| p.key.as_str()).collect();
        assert_eq!(evicted, ["1", "3"], "deaths >= threshold evicts");
        assert_eq!(
            evict[0].raw_src_table, "gizmos",
            "evicted probes keep the raw spelling for the retry filter"
        );
        assert_eq!(
            below,
            vec![
                ChargedKey {
                    transform: "t".to_string(),
                    src_table: "public.gizmos".to_string(),
                    key: "2".to_string(),
                    deaths: 1,
                },
                ChargedKey {
                    transform: "t".to_string(),
                    src_table: "public.widgets".to_string(),
                    key: "4".to_string(),
                    deaths: 4,
                },
            ],
            "below-threshold keys are reported under the canonical identity"
        );
    }

    #[test]
    fn describe_charged_keys_names_each_key_and_its_death_count() {
        let charged = vec![ChargedKey {
            transform: "gizmo_view".to_string(),
            src_table: "public.gizmos".to_string(),
            key: "2".to_string(),
            deaths: 1,
        }];
        assert_eq!(
            describe_charged_keys(&charged, 5),
            "gizmo_view: public.gizmos key=2 (1/5 deaths)"
        );
    }

    #[test]
    fn describe_charged_keys_caps_the_list() {
        let charged: Vec<ChargedKey> = (0..8)
            .map(|i| ChargedKey {
                transform: "v".to_string(),
                src_table: "public.t".to_string(),
                key: i.to_string(),
                deaths: 2,
            })
            .collect();
        let described = describe_charged_keys(&charged, 5);
        assert_eq!(
            described,
            "v: public.t key=0 (2/5 deaths), v: public.t key=1 (2/5 deaths), \
             v: public.t key=2 (2/5 deaths), v: public.t key=3 (2/5 deaths), \
             v: public.t key=4 (2/5 deaths), and 3 more"
        );
    }

    #[test]
    fn classify_maps_hop_bound_to_halting() {
        let err = ApplyError::HopBoundExceeded {
            hop_gen: 40,
            tables: vec!["t".to_string()],
        };
        assert_eq!(classify(&err), FailureClass::Halting);
    }

    /// Every key of an off-ledger aggregate target reproduces the failure,
    /// so isolating it would quarantine the whole target a key at a time.
    #[test]
    fn classify_maps_an_off_ledger_aggregate_to_halting() {
        let err = ApplyError::AggregateOffLedger {
            target: "t".to_string(),
        };
        assert_eq!(classify(&err), FailureClass::Halting);
    }

    #[test]
    fn classify_maps_a_truncate_without_an_lsn_to_halting() {
        let err = ApplyError::TruncateWithoutLsn {
            src_table: "public.src".to_string(),
        };
        assert_eq!(classify(&err), FailureClass::Halting);
    }

    /// Issue #926: a spilled accumulator staged in memory is a programming
    /// error no key caused, so it halts (pausing nothing, as it names no
    /// table) rather than isolate and charge a key for it.
    #[test]
    fn classify_maps_a_spilled_accumulator_staged_in_memory_to_halting() {
        assert_eq!(
            classify(&ApplyError::SpilledMutationsNotFlushed),
            FailureClass::Halting
        );
    }

    #[test]
    fn classify_maps_version_fence_miss() {
        let err = ApplyError::VersionFenceMiss {
            src_table: "orders".to_string(),
        };
        assert_eq!(classify(&err), FailureClass::VersionFenceMiss);
    }

    #[test]
    fn classify_maps_unsupported_primary_key_type_to_halting() {
        // Issue #107: a non-text-stable single-column primary key is exactly
        // as structural as the composite-key and no-key cases above, so it
        // must halt too rather than isolate-and-evict every key touching
        // that source one at a time.
        let err = ApplyError::Ddl(DdlError::UnsupportedPrimaryKeyType {
            source_table: "events".to_string(),
            column: "occurred_at".to_string(),
            pg_type: "timestamp with time zone".to_string(),
        });
        assert_eq!(classify(&err), FailureClass::Halting);
    }

    #[test]
    fn classify_maps_claim_lost_to_isolate() {
        // ClaimLost is not transient, not a fence miss, not halting — it
        // falls to the catch-all `Isolate` bucket, matching doc 05's
        // "everything else" row. (In practice `drain_once` never reaches
        // isolation for `ClaimLost` today since it isn't attributable to a
        // key, but the classifier itself has no special case for it — see
        // `isolate_and_evict`'s "no single key reproduces" fallback for how
        // an unattributable isolate-classified error still surfaces rather
        // than getting blamed on something.)
        assert_eq!(classify(&ApplyError::ClaimLost), FailureClass::Isolate);
    }

    /// A `tokio_postgres::Error` with no SQLSTATE, the shape a dropped
    /// connection takes. `tokio_postgres` has no public constructor for a
    /// connection error, so this takes the one uncoded error its public API
    /// builds without a server: a connection string that doesn't parse.
    /// These tests cover routing (whichever variant wraps the Postgres
    /// error, `classify` must reach it);
    /// [`classify_decides_wrapped_errors_by_their_real_sqlstate`] covers
    /// real coded errors and a real dropped connection against a server.
    fn uncoded_pg_error() -> tokio_postgres::Error {
        let err = "port=not-a-port"
            .parse::<tokio_postgres::Config>()
            .expect_err("an unparseable port is a config error");
        assert!(err.code().is_none(), "a config error carries no SQLSTATE");
        err
    }

    fn pool_timeout() -> crate::error::Error {
        crate::error::Error::Pool(deadpool_postgres::PoolError::Timeout(
            deadpool_postgres::TimeoutType::Wait,
        ))
    }

    #[test]
    fn classify_finds_a_transient_pg_error_whichever_variant_wraps_it() {
        // Issue #653: only `ApplyError::Db` used to be inspected, so the same
        // dropped connection surfaced through a nested module's error was
        // `Isolate`, and the drain probed the whole page one record at a time
        // for a failure no single record could reproduce.
        use crate::defs::backfill::BackfillError;
        use crate::defs::catalog::CatalogError;
        use crate::intake::IntakeError;
        use crate::staging::error::StagingError;
        let wrapped = [
            ("Db", ApplyError::Db(uncoded_pg_error())),
            (
                "Staging(Db)",
                ApplyError::Staging(StagingError::Db(uncoded_pg_error())),
            ),
            (
                "Catalog(Db)",
                ApplyError::Catalog(CatalogError::Db(uncoded_pg_error())),
            ),
            (
                "Catalog(Ddl(Db))",
                ApplyError::Catalog(CatalogError::Ddl(DdlError::Db(uncoded_pg_error()))),
            ),
            ("Ddl(Db)", ApplyError::Ddl(DdlError::Db(uncoded_pg_error()))),
            (
                "Backfill(Db)",
                ApplyError::Backfill(BackfillError::Db(uncoded_pg_error())),
            ),
            (
                "Intake(Db)",
                ApplyError::Intake(IntakeError::Db(uncoded_pg_error())),
            ),
            (
                "Pool(Connect)",
                ApplyError::Pool(crate::error::Error::Connect(uncoded_pg_error())),
            ),
            (
                "Pool(Pool(Backend))",
                ApplyError::Pool(crate::error::Error::Pool(
                    deadpool_postgres::PoolError::Backend(uncoded_pg_error()),
                )),
            ),
        ];
        for (name, err) in wrapped {
            assert_eq!(classify(&err), FailureClass::Transient, "{name}");
        }
    }

    /// #712: an entry lock that lost a key to the tombstone GC is retried
    /// uncharged, bare or wrapped, by the drain (`classify`) and by a
    /// backfill chunk (`is_transient_error`).
    #[test]
    fn a_collected_ledger_entry_is_transient() {
        use crate::defs::backfill::BackfillError;
        let collected = || ApplyError::LedgerEntryCollected {
            target: "public.agg".to_string(),
        };
        let wrapped = ApplyError::Backfill(BackfillError::Propagation(Box::new(collected())));
        assert_eq!(classify(&collected()), FailureClass::Transient);
        assert_eq!(classify(&wrapped), FailureClass::Transient);
        assert!(is_transient_error(&collected()));
        assert!(is_transient_error(&wrapped));
    }

    #[test]
    fn classify_treats_a_pool_timeout_as_transient_whichever_variant_wraps_it() {
        use crate::defs::catalog::CatalogError;
        use crate::staging::error::StagingError;
        let wrapped = [
            ("Pool", ApplyError::Pool(pool_timeout())),
            (
                "Staging(Config)",
                ApplyError::Staging(StagingError::Config(pool_timeout())),
            ),
            (
                "Catalog(Pool)",
                ApplyError::Catalog(CatalogError::Pool(pool_timeout())),
            ),
            ("Ddl(Pool)", ApplyError::Ddl(DdlError::Pool(pool_timeout()))),
        ];
        for (name, err) in wrapped {
            assert_eq!(classify(&err), FailureClass::Transient, "{name}");
        }
        for timeout in [
            deadpool_postgres::TimeoutType::Create,
            deadpool_postgres::TimeoutType::Recycle,
        ] {
            let err = ApplyError::Pool(crate::error::Error::Pool(
                deadpool_postgres::PoolError::Timeout(timeout),
            ));
            assert_eq!(classify(&err), FailureClass::Transient, "{timeout:?}");
        }
    }

    #[test]
    fn is_transient_sqlstate_matches_doc_05s_set() {
        use tokio_postgres::error::SqlState;
        for code in [
            SqlState::T_R_SERIALIZATION_FAILURE,
            SqlState::T_R_DEADLOCK_DETECTED,
            SqlState::LOCK_NOT_AVAILABLE,
            SqlState::QUERY_CANCELED,
            SqlState::TOO_MANY_CONNECTIONS,
        ] {
            assert!(is_transient_sqlstate(Some(&code)), "{}", code.code());
        }
        assert!(
            is_transient_sqlstate(None),
            "no SQLSTATE: dropped connection"
        );
        for code in [
            SqlState::UNIQUE_VIOLATION,
            SqlState::UNDEFINED_TABLE,
            SqlState::DIVISION_BY_ZERO,
        ] {
            assert!(!is_transient_sqlstate(Some(&code)), "{}", code.code());
        }
    }

    #[test]
    fn classify_keeps_non_transient_pool_failures_as_isolate() {
        // A closed pool means shutdown, and a config error never heals by
        // retrying; neither is a transient failure.
        let closed = ApplyError::Pool(crate::error::Error::Pool(
            deadpool_postgres::PoolError::Closed,
        ));
        assert_eq!(classify(&closed), FailureClass::Isolate);
        let config = ApplyError::Pool(crate::error::Error::Config("bad dsn".to_string()));
        assert_eq!(classify(&config), FailureClass::Isolate);
    }

    /// Issue #670 review: a lost claim is recognised through any wrapper, the
    /// same innermost-error rule `classify` uses, via either type that nests
    /// an `ApplyError`.
    #[test]
    fn is_claim_lost_sees_through_wrapping_apply_errors() {
        use crate::defs::backfill::BackfillError;
        use crate::intake::IntakeError;
        let backfill =
            |inner: ApplyError| ApplyError::Backfill(BackfillError::Propagation(Box::new(inner)));
        let intake =
            |inner: ApplyError| ApplyError::Intake(IntakeError::Propagation(Box::new(inner)));
        assert!(is_claim_lost(&ApplyError::ClaimLost));
        assert!(is_claim_lost(&backfill(ApplyError::ClaimLost)));
        assert!(is_claim_lost(&intake(ApplyError::ClaimLost)));
        assert!(is_claim_lost(&intake(backfill(ApplyError::ClaimLost))));
        let fence_miss = || ApplyError::VersionFenceMiss {
            src_table: "orders".to_string(),
        };
        assert!(!is_claim_lost(&fence_miss()));
        assert!(!is_claim_lost(&backfill(fence_miss())));
        assert!(!is_claim_lost(&ApplyError::Db(uncoded_pg_error())));
    }

    /// Issue #670: a structural halting diagnosis is `Halting` however deeply
    /// another `ApplyError` wraps it. A backfill's downstream propagation
    /// reports it as `Backfill(Propagation(Box<ApplyError>))`, which used to
    /// be `Isolate`: isolation would have charged, and eventually evicted,
    /// every key on the source for a failure none of them caused.
    #[test]
    fn classify_decides_a_wrapped_apply_error_by_the_innermost_one() {
        use crate::defs::backfill::BackfillError;
        let wrap =
            |inner: ApplyError| ApplyError::Backfill(BackfillError::Propagation(Box::new(inner)));
        let no_pk = || {
            ApplyError::Ddl(DdlError::NoPrimaryKey {
                source_table: "public.events".to_string(),
            })
        };
        let unsupported_pk = || {
            ApplyError::Ddl(DdlError::UnsupportedPrimaryKeyType {
                source_table: "public.events".to_string(),
                column: "occurred_at".to_string(),
                pg_type: "timestamp with time zone".to_string(),
            })
        };
        let hop_bound = || ApplyError::HopBoundExceeded {
            hop_gen: 40,
            tables: vec!["t".to_string()],
        };
        let cases = [
            (
                "Propagation(NoPrimaryKey)",
                wrap(no_pk()),
                FailureClass::Halting,
            ),
            (
                "Propagation(UnsupportedPrimaryKeyType)",
                wrap(unsupported_pk()),
                FailureClass::Halting,
            ),
            (
                "Propagation(Propagation(NoPrimaryKey))",
                wrap(wrap(no_pk())),
                FailureClass::Halting,
            ),
            (
                "Intake(Propagation(NoPrimaryKey))",
                ApplyError::Intake(crate::intake::IntakeError::Propagation(Box::new(no_pk()))),
                FailureClass::Halting,
            ),
            (
                "Propagation(HopBoundExceeded)",
                wrap(hop_bound()),
                FailureClass::Halting,
            ),
            (
                "Propagation(VersionFenceMiss)",
                wrap(ApplyError::VersionFenceMiss {
                    src_table: "orders".to_string(),
                }),
                FailureClass::VersionFenceMiss,
            ),
            (
                "Propagation(Db)",
                wrap(ApplyError::Db(uncoded_pg_error())),
                FailureClass::Transient,
            ),
            (
                "Propagation(ClaimLost)",
                wrap(ApplyError::ClaimLost),
                FailureClass::Isolate,
            ),
        ];
        for (name, err, class) in cases {
            assert_eq!(classify(&err), class, "{name}");
        }
    }

    /// Issue #670: `53300` (too many connections) is the server refusing a
    /// connection, not anything a record did. It reaches the drain as a pool
    /// checkout's `PoolError::Backend`, and used to classify `Isolate`. This
    /// takes the real error from a real checkout, against a role whose
    /// connection limit is zero (superusers are exempt from connection
    /// limits, so the test role isn't one).
    #[tokio::test]
    async fn classify_treats_too_many_connections_on_a_pool_checkout_as_transient() {
        use tokio_postgres::error::SqlState;

        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        // Roles are cluster-wide; one per database keeps tests apart.
        let role = format!("no_connections_{}", db.name().replace('-', "_"));
        let raw = connect_raw(&db).await;
        raw.batch_execute(&format!("create role {role} login connection limit 0"))
            .await
            .expect("create a role that may not connect");

        let dsn = db.dsn().replace("user=postgres", &format!("user={role}"));
        assert_ne!(dsn, db.dsn(), "the test dsn names its user");
        let config = crate::config::Config::from_dsn(dsn).expect("valid dsn");
        let pool = Pool::new(&config).expect("build a pool");
        let err = ApplyError::from(pool.get().await.expect_err("the role may not connect"));

        let pg = {
            let mut link: Option<&(dyn std::error::Error + 'static)> = Some(&err);
            loop {
                let err = link.expect("a Postgres error on the chain");
                if let Some(pg) = err.downcast_ref::<tokio_postgres::Error>() {
                    break pg;
                }
                link = err.source();
            }
        };
        assert_eq!(pg.code(), Some(&SqlState::TOO_MANY_CONNECTIONS), "{err:?}");
        assert!(
            matches!(
                &err,
                ApplyError::Pool(crate::error::Error::Pool(
                    deadpool_postgres::PoolError::Backend(_)
                ))
            ),
            "{err:?}"
        );
        assert_eq!(classify(&err), FailureClass::Transient);
    }

    /// Issue #766: a `42501`, row-level security under `row_security = off`
    /// or a plain missing privilege, is halting however it's wrapped: it is
    /// about the role and a table, so isolating it would charge every key.
    #[tokio::test]
    async fn classify_halts_on_a_refused_read_or_write() {
        use crate::defs::catalog::CatalogError;
        use tokio_postgres::error::SqlState;

        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (admin, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        admin
            .batch_execute(
                "create role refused login; \
                 create table public.policed (id int primary key); \
                 alter table public.policed enable row level security; \
                 grant select on public.policed to refused; \
                 create table public.ungranted (id int primary key);",
            )
            .await
            .expect("a role, a table its policies apply to and one it can't read");
        let dsn = db.dsn().replace("user=postgres", "user=refused");
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect as the refused role");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(crate::pool::ROW_SECURITY_OFF)
            .await
            .expect("row_security off");
        for sql in [
            "select * from public.policed",
            "select * from public.ungranted",
        ] {
            let refused = || async {
                let err = client.simple_query(sql).await.expect_err(sql);
                assert_eq!(err.code(), Some(&SqlState::INSUFFICIENT_PRIVILEGE), "{sql}");
                err
            };
            for err in [
                ApplyError::Db(refused().await),
                ApplyError::Catalog(CatalogError::Ddl(DdlError::Db(refused().await))),
            ] {
                assert_eq!(classify(&err), FailureClass::Halting, "{sql}: {err}");
            }
        }
    }

    /// The routing tests above only see uncoded errors, so they can't tell
    /// "the first Postgres error decides by its SQLSTATE" from "any Postgres
    /// error is transient". This raises real errors on a real server: a
    /// non-transient SQLSTATE must stay `Isolate` whichever variant wraps
    /// it, and a statement timeout (`57014`) and a dropped connection must be
    /// `Transient`.
    #[tokio::test]
    async fn classify_decides_wrapped_errors_by_their_real_sqlstate() {
        use crate::defs::catalog::CatalogError;
        use crate::staging::error::StagingError;
        use tokio_postgres::error::SqlState;

        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let connect = || async {
            let (client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
                .await
                .expect("connect");
            tokio::spawn(async move {
                let _ = connection.await;
            });
            client
        };
        let client = connect().await;

        let division = || async {
            let err = client
                .simple_query("select 1 / 0")
                .await
                .expect_err("division by zero");
            assert_eq!(err.code(), Some(&SqlState::DIVISION_BY_ZERO));
            err
        };
        let wrapped_division = [
            ("Db", ApplyError::Db(division().await)),
            (
                "Staging(Db)",
                ApplyError::Staging(StagingError::Db(division().await)),
            ),
            (
                "Catalog(Ddl(Db))",
                ApplyError::Catalog(CatalogError::Ddl(DdlError::Db(division().await))),
            ),
            (
                "Pool(Pool(Backend))",
                ApplyError::Pool(crate::error::Error::Pool(
                    deadpool_postgres::PoolError::Backend(division().await),
                )),
            ),
        ];
        for (name, err) in wrapped_division {
            assert_eq!(classify(&err), FailureClass::Isolate, "{name}");
        }

        let canceled = client
            .batch_execute("set statement_timeout = '10ms'; select pg_sleep(5)")
            .await
            .expect_err("statement timeout");
        assert_eq!(canceled.code(), Some(&SqlState::QUERY_CANCELED));
        let err = ApplyError::Staging(StagingError::Db(canceled));
        assert_eq!(classify(&err), FailureClass::Transient, "statement timeout");

        // A connection the server has gone away from: terminate it from a
        // second session (waiting until the backend has exited), then use it.
        let (doomed, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect the doomed session");
        let connection = tokio::spawn(connection);
        let pid: i32 = doomed
            .query_one("select pg_backend_pid()", &[])
            .await
            .expect("backend pid")
            .get(0);
        let terminated: bool = client
            .query_one("select pg_terminate_backend($1, 5000)", &[&pid])
            .await
            .expect("terminate")
            .get(0);
        assert!(terminated, "the doomed backend exits within the timeout");
        // Wait for the client to see the socket close, so the next query
        // can't race the server's own `57P01` goodbye and take it as its
        // response.
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), connection)
            .await
            .expect("the doomed connection ends once its backend exits");
        let dropped = doomed
            .simple_query("select 1")
            .await
            .expect_err("the connection is gone");
        assert!(dropped.code().is_none(), "{dropped:?}");
        let err = ApplyError::Staging(StagingError::Db(dropped));
        assert_eq!(
            classify(&err),
            FailureClass::Transient,
            "dropped connection"
        );

        // The same dropped connection can instead reach the caller as the
        // server's own `FATAL` (`57P01` admin shutdown here, what
        // `pg_terminate_backend` sends), depending on which arrives first.
        // A session terminating itself gets it deterministically, as the
        // query's own error.
        let suicidal = connect().await;
        let goodbye = suicidal
            .simple_query("select pg_terminate_backend(pg_backend_pid())")
            .await
            .expect_err("the session terminates itself");
        assert_eq!(goodbye.code(), Some(&SqlState::ADMIN_SHUTDOWN));
        let err = ApplyError::Staging(StagingError::Db(goodbye));
        assert_eq!(
            classify(&err),
            FailureClass::Transient,
            "57P01 admin shutdown"
        );
    }
    /// A raw connection onto `db` with `search_path` on trellis's schema.
    async fn connect_raw(db: &testkit::TestDatabase) -> tokio_postgres::Client {
        let (raw, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        raw.batch_execute(&format!(
            "set search_path to {}, public",
            crate::config::DEFAULT_SCHEMA
        ))
        .await
        .expect("set search_path");
        raw
    }

    /// A same-crate pool plus a raw connection onto `db`, with a live
    /// `order_totals` definition over `public.orders` and a threshold's worth
    /// of committed `poison` rows against that source: every precondition the
    /// whole-transform fuse needs to trip. Returns the pool, the raw client
    /// and the qualified source.
    async fn fuse_ready(db: &testkit::TestDatabase) -> (Pool, tokio_postgres::Client, String) {
        let config = crate::config::Config::from_dsn(db.dsn().to_string()).expect("valid schema");
        let pool = Pool::new(&config).expect("build a same-crate pool");
        let raw = connect_raw(db).await;
        raw.batch_execute(
            "create table public.orders (id bigint primary key, price numeric, tax numeric)",
        )
        .await
        .expect("seed source table");
        let columns: HashMap<String, crate::defs::ast::ValueType> = ["id", "price", "tax"]
            .iter()
            .map(|name| (name.to_string(), crate::defs::ast::ValueType::Numeric))
            .collect();
        catalog::create_definition(
            &pool,
            "TRANSFORM order_totals FROM public.orders SELECT price + tax AS total",
            &columns,
        )
        .await
        .expect("create definition");
        let src_table = "public.orders".to_string();
        for i in 0..DEFAULT_TRANSFORM_DEATH_THRESHOLD {
            raw.execute(
                "insert into poison (transform_id, src_table, key, last_error) \
                 select id, $1, $2, 'test' from transform_definitions \
                 where target_table like '%.order_totals'",
                &[&src_table, &format!("k{i}")],
            )
            .await
            .expect("insert poison marker");
        }
        (pool, raw, src_table)
    }

    async fn status_of(raw: &tokio_postgres::Client) -> String {
        raw.query_one(
            "select status from transform_definitions \
             where target_table like '%.order_totals'",
            &[],
        )
        .await
        .expect("read status")
        .get(0)
    }

    /// Runs the fuse's per-definition write for `def` (a candidate from an
    /// earlier, unlocked read) in its own transaction, as the eviction
    /// transaction would, and returns whether it quarantined.
    async fn trip_candidate(pool: &Pool, def: &Definition) -> bool {
        let mut client = pool.get().await.expect("pool connection");
        let txn = client.transaction().await.expect("begin");
        let tripped = quarantine_if_crossed(&txn, def.id, &not_frozen_sql())
            .await
            .expect("fuse write");
        txn.commit().await.expect("commit");
        tripped
    }

    /// Issue #338: the fuse reads its candidates without a lock, then writes.
    /// An operator PAUSE that commits between the two stands; the fuse must
    /// not relabel the freeze as its own. The interleaving is the fuse's two
    /// halves called in order around the pause, not a race.
    #[tokio::test]
    async fn a_pause_between_the_fuse_read_and_write_stays_paused() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, raw, src_table) = fuse_ready(&db).await;

        let candidates = catalog::transforms_for_source(&pool, &src_table)
            .await
            .expect("candidate read");
        assert_eq!(
            candidates.len(),
            1,
            "the live definition is a fuse candidate"
        );

        crate::defs::lifecycle::pause_transform(&pool, "order_totals")
            .await
            .expect("pause");

        assert!(
            !trip_candidate(&pool, &candidates[0]).await,
            "the fuse must not claim a definition an operator froze after its read"
        );
        assert_eq!(status_of(&raw).await, "paused");
    }

    /// Issue #338, the pause committing *while* the fuse's write waits on it:
    /// the pause's transaction holds the definition's row (as
    /// `pause_transform` does between its `for update` read and its commit)
    /// when the fuse's `UPDATE` reaches it. The fuse's write must re-check the
    /// row the pause committed, not the version its statement started from.
    ///
    /// The ordering is observed, not timed: the pause commits only once
    /// Postgres reports the fuse's session blocked by the pause's. That block
    /// is certain (the fuse's snapshot still sees the row `live`, so its
    /// `UPDATE` has to lock it), so the test asserts it rather than falling
    /// back to the first test's ordering; the bound only stops a regression
    /// from hanging the suite.
    #[tokio::test]
    async fn a_pause_committing_while_the_fuse_write_waits_stays_paused() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, raw, src_table) = fuse_ready(&db).await;

        let candidates = catalog::transforms_for_source(&pool, &src_table)
            .await
            .expect("candidate read");
        assert_eq!(
            candidates.len(),
            1,
            "the live definition is a fuse candidate"
        );

        let mut operator = connect_raw(&db).await;
        let pause_pid: i32 = operator
            .query_one("select pg_backend_pid()", &[])
            .await
            .expect("read the pause's backend pid")
            .get(0);
        let pause = operator.transaction().await.expect("begin pause");
        pause
            .execute(
                "update transform_definitions set status = 'paused' \
                 where target_table like '%.order_totals'",
                &[],
            )
            .await
            .expect("pause, uncommitted");

        let fuse = trip_candidate(&pool, &candidates[0]);
        let commit_pause = async {
            let mut blocked = false;
            for _ in 0..3000 {
                let waiting: i64 = raw
                    .query_one(
                        "select count(*) from pg_stat_activity \
                         where $1 = any(pg_blocking_pids(pid))",
                        &[&pause_pid],
                    )
                    .await
                    .expect("read pg_stat_activity")
                    .get(0);
                if waiting > 0 {
                    blocked = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            assert!(
                blocked,
                "the fuse's write never blocked on the uncommitted pause, so this \
                 test would not exercise the mid-statement re-check"
            );
            pause.commit().await.expect("commit pause");
        };
        let (tripped, ()) = tokio::join!(fuse, commit_pause);

        assert!(
            !tripped,
            "the fuse must not claim a definition paused while its write waited"
        );
        assert_eq!(status_of(&raw).await, "paused");
    }

    /// Issue #338's wider window: a pause *and* resume both landing between
    /// the fuse's read and its write. The resume deleted the definition's
    /// `poison` rows (#799) and handed it back for a fresh build, so the stale
    /// read's verdict no longer applies to it: the write's own count finds
    /// none of the pre-resume rows. (The pin is against a fix that re-checks
    /// the status but trusts a count taken earlier.)
    #[tokio::test]
    async fn a_pause_and_resume_between_the_fuse_read_and_write_is_not_quarantined() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, raw, src_table) = fuse_ready(&db).await;

        let candidates = catalog::transforms_for_source(&pool, &src_table)
            .await
            .expect("candidate read");
        assert_eq!(
            candidates.len(),
            1,
            "the live definition is a fuse candidate"
        );

        crate::defs::lifecycle::pause_transform(&pool, "order_totals")
            .await
            .expect("pause");
        resume_transform(&pool, "order_totals")
            .await
            .expect("resume");

        assert!(
            !trip_candidate(&pool, &candidates[0]).await,
            "the fuse must not quarantine a definition resumed after its read"
        );
        assert_eq!(status_of(&raw).await, "waiting_to_backfill");
    }

    /// The positive control for the two tests above: with nothing landing in
    /// the window, the same two halves quarantine.
    #[tokio::test]
    async fn an_undisturbed_fuse_read_and_write_quarantines() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, raw, src_table) = fuse_ready(&db).await;

        let candidates = catalog::transforms_for_source(&pool, &src_table)
            .await
            .expect("candidate read");
        assert_eq!(
            candidates.len(),
            1,
            "the live definition is a fuse candidate"
        );

        assert!(trip_candidate(&pool, &candidates[0]).await);
        assert_eq!(status_of(&raw).await, "quarantined");
    }

    // -----------------------------------------------------------------
    // #803: one held row per key
    // -----------------------------------------------------------------

    /// One park as the pre-#803 table stored it: one row per segment.
    #[derive(Debug, Clone, PartialEq)]
    struct ParkedRow {
        seg_seq: i64,
        old_image: Option<String>,
        origin_lsn: Option<PgLsn>,
        src_changed: Option<SystemTime>,
        hop_gen: i32,
        group_key: Option<Vec<String>>,
    }

    impl ParkedRow {
        fn held(&self) -> HeldKey {
            HeldKey::parked(
                "public.orders",
                self.seg_seq,
                self.old_image.clone(),
                self.origin_lsn,
                self.src_changed,
                self.hop_gen,
                self.group_key.clone(),
            )
        }
    }

    /// `Recompute`'s fields, which `StagedChange` has no `PartialEq` for.
    type RecomputeFields = (
        String,
        String,
        i32,
        Option<Vec<String>>,
        Option<SystemTime>,
        Option<String>,
        Option<PgLsn>,
    );

    fn recompute_fields(change: StagedChange) -> RecomputeFields {
        match change {
            StagedChange::Recompute {
                src_table,
                key,
                hop_gen,
                group_key,
                src_changed,
                prior_image,
                origin_lsn,
            } => (
                src_table,
                key,
                hop_gen,
                group_key,
                src_changed,
                prior_image,
                origin_lsn,
            ),
            other => panic!("expected a Recompute, got {other:?}"),
        }
    }

    /// The pre-#803 park and release, verbatim in what they computed: each
    /// park inserted a row per segment, `on conflict do nothing` (so a second
    /// park of one segment was dropped), and the release sorted the rows by
    /// `(seg_seq, held_seq)` and folded them into its `Recompute`.
    fn old_release(parks: &[ParkedRow]) -> RecomputeFields {
        let mut held: Vec<(i64, i64, &ParkedRow)> = Vec::new();
        for (held_seq, park) in parks.iter().enumerate() {
            if !held.iter().any(|(seg_seq, _, _)| *seg_seq == park.seg_seq) {
                held.push((park.seg_seq, held_seq as i64, park));
            }
        }
        held.sort_by_key(|(seg_seq, held_seq, _)| (*seg_seq, *held_seq));
        let first = held[0].2;
        let mut origin_lsn = first.origin_lsn;
        let mut src_changed: Option<SystemTime> = None;
        let mut hop_gen = 0;
        let mut source_change = false;
        let mut group_key: Vec<String> = Vec::new();
        for (_, _, row) in &held {
            origin_lsn = super::super::fold::earliest_origin(origin_lsn, row.origin_lsn);
            source_change |= row.src_changed.is_some();
            src_changed = super::super::apply::earliest_src_changed(src_changed, row.src_changed);
            hop_gen = hop_gen.max(row.hop_gen);
            for value in row.group_key.clone().unwrap_or_default() {
                if !group_key.contains(&value) {
                    group_key.push(value);
                }
            }
        }
        recompute_fields(StagedChange::Recompute {
            src_table: "public.orders".to_string(),
            key: "1".to_string(),
            hop_gen: if source_change { 0 } else { hop_gen },
            group_key: (!group_key.is_empty()).then_some(group_key),
            src_changed,
            prior_image: first.old_image.clone(),
            origin_lsn,
        })
    }

    /// The release's `Recompute` from the one merged row.
    fn new_release(parks: &[ParkedRow]) -> RecomputeFields {
        let merged = parks
            .iter()
            .map(ParkedRow::held)
            .reduce(HeldKey::merge)
            .expect("at least one park");
        recompute_fields(merged.recompute("1"))
    }

    /// `group_key` is a set: the fold's cross-segment merge keeps it sorted
    /// (`fold::merge_group_keys`), and the old release's first-seen order
    /// was only the order its rows happened to sort in.
    fn as_set(mut fields: RecomputeFields) -> RecomputeFields {
        if let Some(group_key) = fields.3.as_mut() {
            group_key.sort();
        }
        fields
    }

    fn at(secs: u64) -> Option<SystemTime> {
        Some(SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(secs))
    }

    fn lsn(position: u64) -> Option<PgLsn> {
        Some(PgLsn::from(position))
    }

    fn page(seg_seq: i64, old_image: Option<&str>, origin_lsn: Option<PgLsn>) -> ParkedRow {
        ParkedRow {
            seg_seq,
            old_image: old_image.map(str::to_string),
            origin_lsn,
            src_changed: at(seg_seq as u64 * 10),
            hop_gen: 0,
            group_key: None,
        }
    }

    /// A build chunk's parked re-derive (`evict_build_key`).
    fn build_park(origin_lsn: u64) -> ParkedRow {
        ParkedRow {
            seg_seq: BUILD_PARK_SEG_SEQ,
            old_image: None,
            origin_lsn: lsn(origin_lsn),
            src_changed: None,
            hop_gen: 0,
            group_key: None,
        }
    }

    fn keys(values: &[&str]) -> Option<Vec<String>> {
        Some(values.iter().map(|v| v.to_string()).collect())
    }

    /// #803: the release stages the same `Recompute` from the one merged row
    /// as the old release folded from a row per segment, case by case, and
    /// whichever order the parks committed in.
    #[test]
    fn a_merged_held_row_releases_the_recompute_the_per_segment_rows_did() {
        let cases: Vec<(&str, Vec<ParkedRow>)> = vec![
            (
                "one park",
                vec![page(3, Some(r#"{"id":"1","v":"a"}"#), lsn(30))],
            ),
            (
                "three segments, earliest pre-image and origin win",
                vec![
                    page(1, Some(r#"{"v":"a"}"#), lsn(10)),
                    page(2, Some(r#"{"v":"b"}"#), lsn(20)),
                    page(3, Some(r#"{"v":"c"}"#), lsn(5)),
                ],
            ),
            (
                "a null origin first stays unknown",
                vec![page(1, None, None), page(2, None, lsn(20))],
            ),
            (
                "a null origin later blanks a known one",
                vec![page(1, None, lsn(10)), page(2, None, None)],
            ),
            (
                "group keys union without duplicates",
                vec![
                    ParkedRow {
                        group_key: keys(&["b", "a"]),
                        ..page(1, None, lsn(10))
                    },
                    ParkedRow {
                        group_key: None,
                        ..page(2, None, lsn(20))
                    },
                    ParkedRow {
                        group_key: keys(&["c", "a"]),
                        ..page(3, None, lsn(30))
                    },
                ],
            ),
            (
                "hop changes only keep the deepest hop",
                vec![
                    ParkedRow {
                        src_changed: None,
                        hop_gen: 2,
                        ..page(1, None, lsn(10))
                    },
                    ParkedRow {
                        src_changed: None,
                        hop_gen: 4,
                        ..page(2, None, lsn(20))
                    },
                ],
            ),
            (
                "a source change among hop changes resets the hop",
                vec![
                    ParkedRow {
                        src_changed: None,
                        hop_gen: 3,
                        ..page(1, None, lsn(10))
                    },
                    page(2, None, lsn(20)),
                ],
            ),
            (
                "a build park after a page park keeps the page's pre-image",
                vec![page(4, Some(r#"{"v":"seen"}"#), lsn(40)), build_park(15)],
            ),
            (
                "a build park before a page park keeps the page's pre-image",
                vec![build_park(15), page(4, Some(r#"{"v":"seen"}"#), lsn(40))],
            ),
            ("a build park alone", vec![build_park(15)]),
            (
                "a segment parked twice merges once",
                vec![
                    page(2, Some(r#"{"v":"b"}"#), lsn(20)),
                    page(2, Some(r#"{"v":"b"}"#), lsn(20)),
                    page(1, Some(r#"{"v":"a"}"#), lsn(10)),
                ],
            ),
        ];
        for (name, parks) in cases {
            assert_eq!(
                new_release(&parks),
                as_set(old_release(&parks)),
                "{name}: the merged row's Recompute differs from the old fold's"
            );
            let mut reversed = parks.clone();
            reversed.reverse();
            assert_eq!(
                new_release(&reversed),
                new_release(&parks),
                "{name}: the merged row depends on the order the parks committed in"
            );
        }
    }

    /// #803's equivalence over generated park sequences: segments out of
    /// order and parked more than once, null and known origins, source and
    /// hop changes, differing group keys, and build parks among page parks.
    /// Deterministic: a fixed-seed xorshift, no proptest dependency here.
    #[test]
    fn generated_park_sequences_release_the_old_folds_recompute() {
        let mut state: u64 = 0x2545_f491_4f6c_dd1d;
        let mut next = |bound: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % bound
        };
        let images = [None, Some(r#"{"v":"a"}"#), Some(r#"{"v":"b"}"#)];
        let groups = ["p", "q", "r", "s"];
        for case in 0..5_000 {
            let segments = 1 + next(6) as usize;
            let mut distinct: Vec<ParkedRow> = Vec::new();
            for _ in 0..segments {
                let seg_seq = if next(8) == 0 {
                    BUILD_PARK_SEG_SEQ
                } else {
                    1 + next(12) as i64
                };
                if distinct.iter().any(|row| row.seg_seq == seg_seq) {
                    continue;
                }
                let row = if seg_seq == BUILD_PARK_SEG_SEQ {
                    build_park(1 + next(100))
                } else {
                    let source = next(2) == 0;
                    let group_key = match next(3) {
                        0 => None,
                        _ => Some(
                            (0..next(4))
                                .map(|_| groups[next(groups.len() as u64) as usize].to_string())
                                .collect(),
                        ),
                    };
                    ParkedRow {
                        seg_seq,
                        old_image: images[next(images.len() as u64) as usize].map(str::to_string),
                        origin_lsn: if next(5) == 0 {
                            None
                        } else {
                            lsn(1 + next(100))
                        },
                        src_changed: if source { at(1 + next(100)) } else { None },
                        hop_gen: if source { 0 } else { next(4) as i32 },
                        group_key,
                    }
                };
                distinct.push(row);
            }
            // The order the parks commit in, with some segments parked twice
            // (an eviction's park, then its page's retry parking the same
            // contribution).
            let mut parks: Vec<ParkedRow> = Vec::new();
            for row in &distinct {
                parks.push(row.clone());
                if next(4) == 0 {
                    parks.push(row.clone());
                }
            }
            for i in (1..parks.len()).rev() {
                parks.swap(i, next(i as u64 + 1) as usize);
            }
            assert_eq!(
                new_release(&parks),
                as_set(old_release(&parks)),
                "case {case}: parks {parks:?}"
            );
        }
    }

    /// A same-crate pool and a raw connection onto `db`, with a live
    /// `order_totals` definition over `public.orders`. Returns its id too.
    async fn held_ready(db: &testkit::TestDatabase) -> (Pool, tokio_postgres::Client, i64) {
        let config = crate::config::Config::from_dsn(db.dsn().to_string()).expect("valid schema");
        let pool = Pool::new(&config).expect("build a same-crate pool");
        let raw = connect_raw(db).await;
        raw.batch_execute("create table public.orders (id bigint primary key, price numeric)")
            .await
            .expect("seed source table");
        let columns: HashMap<String, crate::defs::ast::ValueType> = ["id", "price"]
            .iter()
            .map(|name| (name.to_string(), crate::defs::ast::ValueType::Numeric))
            .collect();
        catalog::create_definition(
            &pool,
            "TRANSFORM order_totals FROM public.orders SELECT price AS total",
            &columns,
        )
        .await
        .expect("create definition");
        let id: i64 = raw
            .query_one(
                "select id from transform_definitions where target_table like '%.order_totals'",
                &[],
            )
            .await
            .expect("read the definition's id")
            .get(0);
        (pool, raw, id)
    }

    async fn poison_keys(raw: &tokio_postgres::Client, transform_id: i64, keys: &[&str]) {
        for key in keys {
            raw.execute(
                "insert into poison (transform_id, src_table, key, last_error) \
                 values ($1, 'public.orders', $2, 'test')",
                &[&transform_id, key],
            )
            .await
            .expect("insert poison marker");
        }
    }

    /// `row` parked for `key` as a page's folded change, with the
    /// relationship `to_col` values and the window `lsn` only the held row's
    /// projection fields read.
    fn parked_change(
        key: &str,
        row: &ParkedRow,
        to_col_values: &[(&str, &str)],
        at_lsn: u64,
    ) -> FoldedChange {
        FoldedChange {
            old_image: row.old_image.clone(),
            new_image: Some(format!(r#"{{"id": "{key}"}}"#)),
            src_changed: row.src_changed,
            origin_lsn: row.origin_lsn,
            lsn: lsn(at_lsn),
            hop_gen: row.hop_gen,
            group_key: row.group_key.clone(),
            to_col_values: to_col_values
                .iter()
                .map(|(c, v)| (c.to_string(), v.to_string()))
                .collect(),
            ..folded_key(key)
        }
    }

    async fn park_one(pool: &Pool, transform_id: i64, seg_seq: i64, change: FoldedChange) {
        let mut client = pool.get().await.expect("pool connection");
        let txn = client.transaction().await.expect("begin");
        park_batch_contribution(&txn, seg_seq, &[(transform_id, change)])
            .await
            .expect("park");
        txn.commit().await.expect("commit the park");
    }

    /// The key's held row, as [`HeldKey`], with its `join_values` (as
    /// `col=value`, sorted) and `lsn`, and how many rows the key has.
    async fn read_held(
        raw: &tokio_postgres::Client,
        key: &str,
    ) -> (HeldKey, Vec<String>, Option<PgLsn>, i64) {
        let count: i64 = raw
            .query_one("select count(*) from poison_held where key = $1", &[&key])
            .await
            .expect("count held rows")
            .get(0);
        let row = raw
            .query_one(
                "select src_table, seg_seq, old_image::text, origin_lsn, src_changed, hop_gen, \
                        group_key, \
                        array(select e.key || '=' || e.value \
                              from unnest(join_values) j, jsonb_each_text(j) e \
                              order by e.key || '=' || e.value collate \"C\"), \
                        lsn \
                 from poison_held where key = $1",
                &[&key],
            )
            .await
            .expect("read the held row");
        let group_key: Option<Vec<String>> = row.get(6);
        let held = HeldKey {
            src_table: row.get(0),
            seg_seq: row.get(1),
            prior_image: row.get(2),
            origin_lsn: row.get(3),
            src_changed: row.get(4),
            hop_gen: row.get(5),
            group_key: group_key.unwrap_or_default(),
        };
        (held, row.get(7), row.get(8), count)
    }

    /// #803: parks of one key, page parks and a build chunk's alike, merge
    /// into one `poison_held` row whose fields are [`HeldKey::merge`]'s,
    /// whatever order they commit in, plus the union of their join values
    /// and their greatest `lsn`; and the release stages the merged row's
    /// `Recompute` and deletes the row.
    #[tokio::test]
    async fn parks_of_one_key_merge_into_one_held_row_that_the_release_stages() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, raw, id) = held_ready(&db).await;

        let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut next = |bound: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % bound
        };
        let images = [None, Some(r#"{"v": "a"}"#), Some(r#"{"v": "b"}"#)];
        let groups = ["p", "q", "r"];
        let codes = ["x", "y", "z"];
        for case in 0..24 {
            let key = format!("{}", 100 + case);
            poison_keys(&raw, id, &[&key]).await;
            let mut model: Option<HeldKey> = None;
            let mut join_values: Vec<String> = Vec::new();
            let mut greatest_lsn: Option<PgLsn> = None;
            let parks = 1 + next(5);
            for _ in 0..parks {
                if next(6) == 0 {
                    let mut client = pool.get().await.expect("pool connection");
                    let txn = client.transaction().await.expect("begin");
                    evict_build_key(&txn, id, "public.orders", &key, "boom", None)
                        .await
                        .expect("build park");
                    txn.commit().await.expect("commit the build park");
                    // Its origin is the WAL position at the park, read back.
                    let origin: Option<PgLsn> = raw
                        .query_one("select origin_lsn from poison_held where key = $1", &[&key])
                        .await
                        .expect("read origin")
                        .get(0);
                    let build = HeldKey::parked(
                        "public.orders",
                        BUILD_PARK_SEG_SEQ,
                        None,
                        None,
                        None,
                        0,
                        None,
                    );
                    // The build park's own origin isn't known up front, so the
                    // model takes the row's: it is the earliest known one, or
                    // unknown, exactly when the merge says so.
                    model = Some(match model.take() {
                        None => HeldKey {
                            origin_lsn: origin,
                            ..build
                        },
                        Some(held) => {
                            let merged = held.clone().merge(build);
                            HeldKey {
                                origin_lsn: origin,
                                ..merged
                            }
                        }
                    });
                    continue;
                }
                let source = next(2) == 0;
                let row = ParkedRow {
                    seg_seq: 1 + next(10) as i64,
                    old_image: images[next(3) as usize].map(str::to_string),
                    origin_lsn: if next(4) == 0 {
                        None
                    } else {
                        lsn(1 + next(1_000))
                    },
                    src_changed: if source { at(1 + next(1_000)) } else { None },
                    hop_gen: if source { 0 } else { next(4) as i32 },
                    group_key: match next(3) {
                        0 => None,
                        _ => Some(
                            (0..next(3))
                                .map(|_| groups[next(3) as usize].to_string())
                                .collect(),
                        ),
                    },
                };
                let code = codes[next(3) as usize];
                let at_lsn = 1 + next(1_000);
                park_one(
                    &pool,
                    id,
                    row.seg_seq,
                    parked_change(&key, &row, &[("code", code)], at_lsn),
                )
                .await;
                // A key's held row keeps the first park's spelling and
                // seg_seq on a tie, as the park's `on conflict` does.
                let parked = HeldKey::parked(
                    "public.orders",
                    row.seg_seq,
                    row.old_image.clone(),
                    row.origin_lsn,
                    row.src_changed,
                    row.hop_gen,
                    row.group_key.clone(),
                );
                model = Some(match model.take() {
                    None => parked,
                    Some(held) => held.merge(parked),
                });
                join_values.push(format!("code={code}"));
                greatest_lsn = greatest_lsn.max(lsn(at_lsn));
            }
            join_values.sort();
            join_values.dedup();
            let model = model.expect("at least one park");

            let (held, held_join_values, held_lsn, count) = read_held(&raw, &key).await;
            assert_eq!(count, 1, "case {case}: one held row per key");
            assert_eq!(
                held, model,
                "case {case}: the SQL merge differs from HeldKey::merge"
            );
            assert_eq!(held_join_values, join_values, "case {case}: join values");
            assert_eq!(
                held_lsn, greatest_lsn,
                "case {case}: the greatest parked lsn"
            );

            let deleted = release_key(&pool, "order_totals", "public.orders", &key)
                .await
                .expect("release");
            assert_eq!(
                deleted, 1,
                "case {case}: the release deletes the one held row"
            );
            let staged: Vec<RecomputeFields> = raw
                .query(
                    "select src_table, key, hop_gen, group_key, src_changed, old_image::text, \
                            origin_lsn \
                     from (select * from seg_0 union all select * from seg_1 \
                           union all select * from seg_2 union all select * from seg_3) ring \
                     where key = $1 and op = 'recompute' and new_image is null",
                    &[&key],
                )
                .await
                .expect("read the staged recompute")
                .into_iter()
                .map(|row| {
                    (
                        row.get(0),
                        row.get(1),
                        row.get(2),
                        row.get(3),
                        row.get(4),
                        row.get(5),
                        row.get(6),
                    )
                })
                .collect();
            assert_eq!(
                staged,
                vec![recompute_fields(model.clone().recompute(&key))],
                "case {case}: the release stages the merged row's Recompute"
            );
            // #944: and one join value per `join_values` element beside it,
            // with the `Recompute`'s provenance.
            let carried: Vec<(String, i32, Option<SystemTime>, Option<PgLsn>)> = raw
                .query(
                    "select c || '=' || (new_image ->> c), hop_gen, src_changed, origin_lsn \
                     from (select * from seg_0 union all select * from seg_1 \
                           union all select * from seg_2 union all select * from seg_3) ring, \
                          jsonb_object_keys(new_image) c \
                     where key = $1 and op = 'recompute' and new_image is not null \
                     order by c || '=' || (new_image ->> c) collate \"C\"",
                    &[&key],
                )
                .await
                .expect("read the staged join values")
                .into_iter()
                .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)))
                .collect();
            let expected: Vec<_> = join_values
                .iter()
                .map(|value| (value.clone(), staged[0].2, staged[0].4, staged[0].6))
                .collect();
            assert_eq!(
                carried, expected,
                "case {case}: the release stages every join value"
            );
            assert_eq!(read_count(&raw, &key).await, 0, "case {case}: released");
        }
    }

    async fn read_count(raw: &tokio_postgres::Client, key: &str) -> i64 {
        raw.query_one("select count(*) from poison_held where key = $1", &[&key])
            .await
            .expect("count held rows")
            .get(0)
    }

    /// #803: a page parks its held rows in key order, whatever order its
    /// changes come in, so two pages parking overlapping keys queue on the
    /// first shared row rather than each holding a row the other waits on
    /// (ADR-0002 I5). A third session holds key `a`'s row: the page parking
    /// `[b, a]` waits on it without having touched `b`'s, and a second page
    /// parking `[a, b]` queues behind it, and both park once it lets go.
    #[tokio::test]
    async fn pages_parking_overlapping_keys_in_opposite_orders_lock_them_in_key_order() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, raw, id) = held_ready(&db).await;
        poison_keys(&raw, id, &["a", "b"]).await;
        let row = page(1, None, lsn(10));
        for key in ["a", "b"] {
            park_one(&pool, id, 1, parked_change(key, &row, &[], 10)).await;
        }

        let holder = connect_raw(&db).await;
        holder
            .batch_execute("begin; select 1 from poison_held where key = 'a' for update")
            .await
            .expect("hold a's row");

        let park = |order: [&'static str; 2], seg_seq: i64| {
            let pool = pool.clone();
            let row = page(seg_seq, None, lsn(seg_seq as u64 * 10));
            tokio::spawn(async move {
                let changes: Vec<(i64, FoldedChange)> = order
                    .iter()
                    .map(|key| (id, parked_change(key, &row, &[], seg_seq as u64 * 10)))
                    .collect();
                let mut client = pool.get().await.expect("pool connection");
                let txn = client.transaction().await.expect("begin");
                park_batch_contribution(&txn, seg_seq, &changes).await?;
                txn.commit().await?;
                Ok::<_, ApplyError>(())
            })
        };
        let waiting_on_a = |n: i64| {
            let sql = "select count(*) from pg_stat_activity \
                       where wait_event_type = 'Lock' and query like 'insert into poison_held%'";
            let raw = &raw;
            async move {
                for _ in 0..600 {
                    let waiting: i64 = raw.query_one(sql, &[]).await.expect("read waits").get(0);
                    if waiting >= n {
                        return;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                panic!("{n} parks never waited on a's row");
            }
        };

        let first = park(["b", "a"], 2);
        waiting_on_a(1).await;
        holder
            .batch_execute("savepoint s; select 1 from poison_held where key = 'b' for update nowait; release savepoint s")
            .await
            .expect("the page waiting on a's row must not hold b's: it parks a first");
        let second = park(["a", "b"], 3);
        waiting_on_a(2).await;
        holder
            .batch_execute("rollback")
            .await
            .expect("let go of a's row");

        first.await.expect("join").expect("the first page parks");
        second.await.expect("join").expect("the second page parks");
        for key in ["a", "b"] {
            let (held, _, held_lsn, count) = read_held(&raw, key).await;
            assert_eq!(count, 1);
            assert_eq!(held.seg_seq, 1, "the earliest segment's row");
            assert_eq!(held_lsn, lsn(30), "both pages merged into {key}'s row");
        }
    }

    /// #803: `converge`'s condition 4 holds back on a held key's earliest
    /// origin, which its one merged row keeps however its parks arrive, and
    /// on every token once a park's origin is unknown.
    #[tokio::test]
    async fn a_merged_held_row_holds_back_convergence_from_its_earliest_origin() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, raw, id) = held_ready(&db).await;
        poison_keys(&raw, id, &["1"]).await;
        let converged = |token: u64| {
            let raw = &raw;
            async move {
                super::super::converge::converged_through(raw, PgLsn::from(token))
                    .await
                    .expect("converged_through")
            }
        };
        assert!(converged(1_000).await, "nothing is held yet");

        park_one(
            &pool,
            id,
            2,
            parked_change("1", &page(2, None, lsn(500)), &[], 500),
        )
        .await;
        park_one(
            &pool,
            id,
            1,
            parked_change("1", &page(1, None, lsn(200)), &[], 200),
        )
        .await;
        park_one(
            &pool,
            id,
            3,
            parked_change("1", &page(3, None, lsn(800)), &[], 800),
        )
        .await;
        assert!(
            converged(199).await,
            "a token below the earliest origin isn't held back"
        );
        assert!(
            !converged(200).await,
            "the earliest origin holds back its own token"
        );
        assert!(!converged(1_000).await, "and every later one");

        park_one(
            &pool,
            id,
            4,
            parked_change("1", &page(4, None, None), &[], 900),
        )
        .await;
        assert!(
            !converged(1).await,
            "an unknown origin holds back every token"
        );
        assert_eq!(read_count(&raw, "1").await, 1);
    }

    #[test]
    fn only_a_cancelled_statement_counts_toward_the_retype_limit() {
        use tokio_postgres::error::SqlState;
        assert!(is_timeout_cancel(Some(&SqlState::QUERY_CANCELED)));
        for other in [
            SqlState::LOCK_NOT_AVAILABLE,
            SqlState::T_R_DEADLOCK_DETECTED,
            SqlState::T_R_SERIALIZATION_FAILURE,
            SqlState::ADMIN_SHUTDOWN,
        ] {
            assert!(!is_timeout_cancel(Some(&other)), "{other:?}");
        }
        assert!(!is_timeout_cancel(None), "a lost connection has no code");
    }

    /// A paused `order_totals` with a resume request (`timeout_cancels` 0),
    /// as `request_retype` leaves it, and the failure it records.
    async fn paused_with_request(raw: &tokio_postgres::Client) -> i64 {
        let id: i64 = raw
            .query_one(
                "update transform_definitions set status = 'paused' \
                 where target_table like '%.order_totals' returning id",
                &[],
            )
            .await
            .expect("pause")
            .get(0);
        raw.execute(
            "insert into resume_requests (transform_id) values ($1)",
            &[&id],
        )
        .await
        .expect("request");
        raw.execute(
            "insert into capture_failures (transform_id, source_table, columns, error, kind) \
             values ($1, 'public.orders', '{}', 'resuming: re-typing', 'capture')",
            &[&id],
        )
        .await
        .expect("failure");
        id
    }

    async fn failure_of(raw: &tokio_postgres::Client, id: i64) -> Option<String> {
        raw.query_opt(
            "select error from capture_failures where transform_id = $1",
            &[&id],
        )
        .await
        .expect("read failure")
        .map(|row| row.get(0))
    }

    async fn cancels_of(raw: &tokio_postgres::Client, id: i64) -> Option<i32> {
        raw.query_opt(
            "select timeout_cancels from resume_requests where transform_id = $1",
            &[&id],
        )
        .await
        .expect("read count")
        .map(|row| row.get(0))
    }

    fn retry<'a>(
        retrying: &'a (Vec<String>, String),
        labels: &'a [String],
        timeout_cancel: bool,
    ) -> TransientRetry<'a> {
        TransientRetry {
            target: "order_totals",
            labels,
            retyped: &[],
            retrying,
            error: "canceling statement due to statement timeout",
            timeout_cancel,
        }
    }

    /// #894 rules 3 and 5: each cancelled statement counts and rewrites the
    /// `resuming:` message with the attempt number; the third ends the
    /// request, naming the timeout and both remedies, the definition still
    /// paused.
    #[tokio::test]
    async fn the_limit_th_cancelled_retype_ends_the_request() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (_pool, mut raw, _src) = fuse_ready(&db).await;
        let id = paused_with_request(&raw).await;
        let retrying = (
            vec!["id".to_string()],
            "resuming: re-typing order_totals".to_string(),
        );
        let labels = vec!["public.order_totals.id".to_string()];
        for attempt in 1..RETYPE_TIMEOUT_CANCEL_LIMIT {
            record_transient_retry(
                &mut raw,
                id,
                "public.orders",
                &retry(&retrying, &labels, true),
            )
            .await
            .expect("record");
            assert_eq!(cancels_of(&raw, id).await, Some(attempt));
            let error = failure_of(&raw, id).await.expect("failure");
            assert!(
                error.starts_with("resuming: re-typing order_totals")
                    && error.contains(&format!(
                        "attempt {attempt} of {RETYPE_TIMEOUT_CANCEL_LIMIT}, last error: \
                         canceling statement due to statement timeout"
                    )),
                "{error}"
            );
        }
        record_transient_retry(
            &mut raw,
            id,
            "public.orders",
            &retry(&retrying, &labels, true),
        )
        .await
        .expect("record");
        assert_eq!(cancels_of(&raw, id).await, None, "the request ended");
        let error = failure_of(&raw, id).await.expect("failure");
        assert!(
            error.contains("public.order_totals.id")
                && error.contains("target table order_totals")
                && error.contains("statement_timeout")
                && error.contains("Raise statement_timeout for Trellis's role or database")
                && error.contains("restart Trellis")
                && error.contains("resume the definition again")
                && error.contains("DROP TRANSFORM order_totals and define it again")
                && !error.starts_with("resuming:"),
            "{error}"
        );
        assert_eq!(status_of(&raw).await, "paused");
    }

    /// #894 rule 2: a transient failure that isn't a cancelled statement is
    /// recorded with its error but never counted, however often it repeats.
    #[tokio::test]
    async fn a_transient_failure_that_is_not_a_cancel_is_recorded_but_never_counted() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (_pool, mut raw, _src) = fuse_ready(&db).await;
        let id = paused_with_request(&raw).await;
        let retrying = (vec![], "resuming: re-typing order_totals".to_string());
        let labels = vec!["public.order_totals.id".to_string()];
        for _ in 0..=RETYPE_TIMEOUT_CANCEL_LIMIT {
            let mut lock_timeout = retry(&retrying, &labels, false);
            lock_timeout.error = "canceling statement due to lock timeout";
            record_transient_retry(&mut raw, id, "public.orders", &lock_timeout)
                .await
                .expect("record");
        }
        assert_eq!(cancels_of(&raw, id).await, Some(0));
        let error = failure_of(&raw, id).await.expect("failure");
        assert!(
            error.starts_with("resuming:")
                && error.contains("last error: canceling statement due to lock timeout")
                && !error.contains(" of "),
            "{error}"
        );
    }

    /// #894 rule 5: a `RESUME` that completes between the re-type's failure
    /// and the record's write has deleted the request and the definition's
    /// `capture_failure`; the record must neither bring the failure back nor
    /// count against a request that is gone. (A drop in between cascades the
    /// request away, which reads the same.)
    #[tokio::test]
    async fn a_resume_completing_before_the_record_leaves_no_stale_failure() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (_pool, mut raw, _src) = fuse_ready(&db).await;
        let id = paused_with_request(&raw).await;
        // What `complete_resume` does.
        raw.batch_execute(
            "delete from resume_requests; delete from capture_failures; \
             update transform_definitions set status = 'waiting_to_backfill'",
        )
        .await
        .expect("complete the resume");
        let retrying = (vec![], "resuming: re-typing order_totals".to_string());
        let labels = vec!["public.order_totals.id".to_string()];
        // The call that would end the request, were there one.
        raw.execute(
            "insert into resume_requests (transform_id, timeout_cancels) values ($1, $2)",
            &[&id, &(RETYPE_TIMEOUT_CANCEL_LIMIT - 1)],
        )
        .await
        .expect("request");
        raw.execute("delete from resume_requests", &[])
            .await
            .expect("completed again");
        for timeout_cancel in [true, false] {
            record_transient_retry(
                &mut raw,
                id,
                "public.orders",
                &retry(&retrying, &labels, timeout_cancel),
            )
            .await
            .expect("record");
            assert_eq!(failure_of(&raw, id).await, None, "{timeout_cancel}");
            assert_eq!(cancels_of(&raw, id).await, None, "{timeout_cancel}");
        }
        assert_eq!(status_of(&raw).await, "waiting_to_backfill");
    }
}
