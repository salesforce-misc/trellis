//! The one seam every write to a Trellis-owned target table goes through
//! (issue #315).
//!
//! # Why a seam, not capture
//!
//! A target table is never captured by this instance's triggers (see
//! `defs::catalog::tables_to_capture`). A definition that reads another
//! definition's target (a chained hop) learns about that target's changes
//! only from the rows the writer stages here, inside the same transaction as
//! the write itself: image-less `Recompute` rows, or CDC-shaped rows for a
//! relationship endpoint (see the last section). That gives properties
//! capture would not:
//!
//! - **The key is correct by construction.** Each staged key is the target's
//!   own row identity (`ddl::pk_key_sql_expr`), produced by the code that
//!   wrote the row. Nothing reconstructs a NULL-safe group key from a
//!   captured image, which can't be done correctly for an aggregate target:
//!   it has no primary key to capture by.
//! - **Crash safety.** The staged rows commit or roll back with the write, so
//!   there is no window where a target changed but its downstream signal was
//!   lost, and no window where the signal exists without the change.
//!
//! It also closes the double-apply class #312 patched: a target change now
//! reaches the ring exactly once, not once from the apply and again from
//! capture.
//!
//! # Why it is structural
//!
//! Every target writer (`apply::apply_target`, `ledger::apply_ledger_target`,
//! the truncate clears (`apply::clear_target`), the Re-derive build's chunks
//! (`staging::build`), a rebuild's orphan delete,
//! `intake::resume_orphans`, and the direct aggregate build's delete of the
//! groups its rebuilt ledger lost, `defs::backfill`, #815) takes a
//! `&mut TargetMutations` and reports each physically-changed key into it. None of them returns the
//! changed keys to its caller, so a caller cannot forget to propagate them:
//! the only way a changed key leaves a writer is through [`TargetMutations::record`],
//! and the only way a [`TargetMutations`] leaves a transaction is through
//! [`TargetMutations::into_staged`] (or [`TargetMutations::flush`], which
//! calls it). A new writer has to take this parameter to compile against the
//! shared helpers, which is the point.
//!
//! The only target writes that bypass the seam are a definition's own
//! build's row writes (`defs::backfill::backfill_definition` and the chunk
//! queue), which run while that definition is not yet applying
//! (`TransformStatus::is_applying`). Nothing can read a target in that state:
//! `defs::catalog::create_definition_inner` refuses a definition whose source
//! is a target still being built (`CatalogError::TransformNotLive`),
//! `create_relationship` refuses such a target as an endpoint the same way
//! (#403), and a target whose build finishes with readers already attached (a
//! resumed upstream) parks a catch-up marker for itself so those readers
//! re-derive from its rebuilt state (and report `catching_up` until then):
//! every reader, including one reading it through a relationship, whose
//! settled projection that catch-up's discharge also refreshes (#507,
//! `intake::markers::park_target_catchup_if_read`). That catch-up re-reads
//! the target's current keys, so it can't tell a reader about a row the
//! rebuild deleted: such a delete goes through the seam (#815).
//! A chunk a worker held across a resume can't write once the rebuild has
//! made the target applying again: its claim fences every write it makes
//! (`defs::chunk_queue::ClaimFence`, #434).
//!
//! The seam only stages for readers that are already applying when the
//! writer checks, so a *new* reader parks a catch-up on its source target
//! once it starts applying, whichever way it was built (`defs::catalog::install_definition`,
//! `create_definition_inner`, `complete_direct_backfill`, or
//! `intake::markers`'s deferred-backfill flip). A write that raced the
//! reader's build reaches it through that catch-up: its fence is captured
//! after the reader is visibly applying, so it waits out every writer that
//! checked before then.
//!
//! A reader built by the Re-derive build parks no catch-up. Its build waits
//! on a fence instead, a transaction id taken after the build's start
//! commits, until every transaction with a lower id has ended
//! (`staging::build`'s "Seam-fed sources", #625 F6). That covers a writer
//! that checked before the start only if the writer had its id when it
//! checked, so the check takes the transaction's id first, in a statement
//! of its own, so that the check's snapshot is taken after it. A writer that
//! checks before its first write (the discharge's orphan sweep checks, then
//! reads, then deletes) would otherwise take its id after the fence, and
//! the build would read the target before the writer's write commits.
//!
//! # What gets staged
//!
//! One `StagedChange::Recompute` per changed key, for every target at least
//! one applying definition reads (a relationship-endpoint target the seam
//! feeds gets a CDC-shaped row instead; see the last section). The row is
//! image-less, so a downstream
//! aggregate re-derives the affected group from live state and a downstream
//! 1-1 re-reads the row: both idempotent, whatever batch the signal lands in.
//!
//! Each row also carries the key's **prior image**: the target row as it
//! stood before this transaction first touched it (`None` if the transaction
//! created it). Only relationship machinery reads it, until #624 drops it:
//! when an upstream write changes a row's join value, the live re-read only
//! names the new parent, and the prior image names the one the row left. A
//! downstream aggregate doesn't need it. When an upstream write moves a row
//! from group A to group B, the key's ledger entry still names A (#623 D5).
//!
//! Writers capture the prior image with the same statement that already
//! row-locks the key before writing it (a `SELECT ... FOR UPDATE` pre-lock,
//! extended to return each row's image), or, for a delete with no pre-lock (a
//! truncate clear, a rebuild's orphan delete), with the delete's own
//! `RETURNING`.
//! Postgres 17's `RETURNING` cannot name the pre-update row, so the pre-lock
//! is the only place an update's old image can come from.
//!
//! [`TargetMutations::image_sql`] only returns an image expression for a
//! target that has a reader, so a terminal target (the common case) pays
//! nothing for the capture.
//!
//! # The write token
//!
//! [`TargetMutations::into_staged`] also reads the transaction's **write
//! token**, `pg_current_wal_insert_lsn()`, into [`Propagation::write_token`]
//! (issue #401, step 1 of #375's direction 1). It orders one key's writes in
//! the same WAL space as a capture trigger's `lsn`, which is what lets the seam
//! stand in as a CDC-shaped feed for a target (steps 2 and 3, #402/#403): an
//! endpoint target's CDC-shaped seam rows carry it as their `lsn` (see the last
//! section).
//!
//! Where it is read is the whole point. Two writers of one key serialize on
//! that key's row lock: the second acquires it only after the first commits,
//! and then writes WAL of its own. A token read after every lock and write
//! the transaction takes is therefore strictly above the previous holder's
//! commit LSN, so one key's tokens increase in commit order. `into_staged`
//! runs after every writer in the transaction has finished (it is the only
//! way the accumulator leaves the transaction), so reading it there is after
//! the last lock by construction. A token read *before* a lock is not
//! ordered against the previous holder's commit at all.
//!
//! The token is **pre-commit**: it is below the writer's own commit LSN, so it
//! is not a commit `end_lsn`. A consumer may rely on "token above a position
//! read by another transaction means the writer committed after that read", and
//! on per-key order, but never on "a token at or below X means the writer had
//! committed by X": the gap to the commit can be long (a rebuild's orphan
//! delete, `intake::resume_orphans`, commits only when its whole discharge
//! does). It is never an `origin_lsn` either: a seam row's origin stays the
//! conservative "unknown" `await_converged` already gates on (see
//! `staging::converge::converged_through`).
//! It is read only when this transaction stages something (some key of a
//! target an applying definition reads, or that the seam feeds as an
//! endpoint), so a terminal target pays no extra round trip.
//!
//! # Spilling a large write (issue #924)
//!
//! The accumulator keeps its touched set in memory, which is what a drain
//! page needs: a page's keys are capped (ADR-0002 I8), and it stages them
//! through [`TargetMutations::into_staged`] into one `Vec`. A rebuild's
//! orphan sweep (`intake::resume_orphans`) and a direct aggregate build's
//! delete of the groups its rebuilt ledger lost (`defs::backfill`, #926) are
//! not capped: either can delete tens of millions of rows in one
//! transaction, and holding each key with its
//! prior image until the flush would hold them all in memory, and re-reading
//! an endpoint target's new images would bind them all in one statement,
//! past PostgreSQL's 1 GB message limit.
//!
//! So a writer that changes keys in batches calls
//! [`TargetMutations::spill_over`] between them. Once the in-memory set
//! holds a batch of keys, it moves them into a temporary table
//! (`pg_temp.trellis_target_mutations`, one row per `(target, key)`) with an
//! upsert that keeps [`TargetMutations::record`]'s merge rules across
//! batches: the row already there keeps its prior image, even a missing one
//! (the key was created here), and takes the highest `hop_gen`, the earliest
//! `src_changed`, and the earliest `origin_lsn`, unknown if either is. A
//! target nothing reads is dropped at the spill, not held. A transaction
//! that never reaches a batch never creates the table.
//!
//! [`TargetMutations::flush`] then spills what is left, checks the hop bound
//! over every key, reads the write token once (spilling takes no target lock,
//! so it is still after the last one), and stages each target's keys a batch
//! at a time, in key order, appending each batch's rows before it reads the
//! next. Each batch is staged as `into_staged` stages its keys, new-image
//! re-read included, so a spilled flush appends the same rows an in-memory
//! one would. Only `flush` stages a spilled accumulator:
//! [`TargetMutations::into_staged`] stages only the keys held in memory, so
//! it refuses one (`ApplyError::SpilledMutationsNotFlushed`) rather than drop
//! the keys in the table.
//!
//! # Standing in for a relationship endpoint's CDC (issues #402, #403)
//!
//! A relationship's settled parent projection, its reverse deltas and a
//! from-side's `group_key` (issues #129-#136) are driven by image-bearing,
//! LSN-ordered changes. A plain source endpoint gets those from CDC. A
//! target that is a relationship endpoint gets them from the seam alone
//! (#375's direction 1): endpoint targets are uncaptured like every other
//! target. For such a target ([`TargetInfo::endpoint_feed`], any relationship
//! naming it on either side, whether or not an applying definition reads through
//! it yet), each changed key is staged as a [`StagedChange::Cdc`] row rather
//! than a `Recompute`:
//!
//! - **`old_image`** is the key's prior image (above), and **`new_image`**
//!   is the row as this transaction left it, re-read by key in
//!   [`TargetMutations::into_staged`] after every write. A re-read, rather
//!   than each writer's own `RETURNING`, so that no writer can hand in a
//!   wrong or missing new image: every recorded key is either still
//!   row-locked by this transaction or gone. The op follows from which
//!   images exist; a key created and deleted in the same transaction
//!   changed nothing any consumer saw and stages nothing.
//! - **`lsn`** is the write token, so the fold's first/last image rules
//!   order one key's seam rows by the order their writers committed.
//! - **`group_key`** is the union of the target's outbound relationships'
//!   `from_col` values across both images, the same rule a capture trigger
//!   applies to a changed row (`capture::sql`).
//! - **`origin_lsn`** stays `None`, as for a `Recompute` (see "The write
//!   token").
//!
//! Every consumer of the target then sees the same rows it would from CDC,
//! a direct transform reader included: it applies a delta from the images
//! instead of re-reading. Only endpoint targets get these rows; every other
//! target keeps the image-less `Recompute`. An aggregate target qualifies
//! too: its NULL-able grouping key is re-read NULL-safely.
//!
//! **One feed per target.** A seam row and a CDC row for the same write
//! would be two deltas, applied twice when they land in different batches,
//! which is why the seam only took this over once endpoint targets were no
//! longer captured (#403). Targets are not trigger-captured (#807).
//!
//! **A target becoming an endpoint.** [`TargetInfo`] is resolved once per
//! transaction, so a writer that resolved it before a `create_relationship`
//! naming the target committed stages its `Recompute` (or nothing, with no
//! reader), not a CDC-shaped row. That write is invisible to the
//! relationship's own projection seed as well, so its projection misses
//! it. The miss is bounded: no definition can read through a relationship
//! before the relationship commits, and the first one to do so re-seeds the
//! projection's missing rows, and every column it reads, from the live table
//! (`catalog::ensure_relationship_projection_in_txn`). What that catch-up
//! can't undo (it only adds rows) is a projection row for a to-side row the
//! writer deleted or re-keyed. That is the drift a plain source to-side's
//! projection already accumulates before its capture is installed.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::SystemTime;

use tokio_postgres::Transaction;
use tokio_postgres::types::{PgLsn, ToSql};

use super::append::{self, CdcOp, StagedChange};
use super::apply::{
    ApplyError, MAX_HOP_GEN, bounds_keyset_by_array, earliest_src_changed, live_row_columns,
    pk_keyset_col, row_as_text_jsonb_sql,
};
use super::fold::earliest_origin;
use crate::defs::catalog;
use crate::defs::ddl::{self, PrimaryKeyColumn};
use crate::pool::{quote_ident, quote_literal};

/// One changed key's accumulated state — see [`TargetMutations::record`].
#[derive(Debug, Clone)]
struct KeyMutation {
    prior_image: Option<String>,
    hop_gen: i32,
    src_changed: Option<SystemTime>,
    /// The earliest source commit behind the key's writes, `None` if any is
    /// unknown (issue #469): see [`StagedChange::Recompute::origin_lsn`].
    origin_lsn: Option<PgLsn>,
}

/// What [`TargetMutations`] knows about one target, resolved once per
/// accumulator (so once per Phase 3 transaction).
#[derive(Debug, Clone)]
struct TargetInfo {
    /// Whether anything consumes this target's changes: an applying definition
    /// reads it as its source, or the seam feeds it as a relationship
    /// endpoint (`endpoint_feed`).
    has_readers: bool,
    /// The target's live column list, only resolved when `has_readers`.
    image_columns: Vec<String>,
    /// `Some` when the seam is this target's change feed as a relationship
    /// endpoint, so its rows are staged CDC-shaped (see the module doc).
    endpoint_feed: Option<EndpointFeed>,
}

/// What staging CDC-shaped rows for an endpoint target needs beyond its
/// images.
#[derive(Debug, Clone)]
struct EndpointFeed {
    /// The target's row identity, in declared order: what every writer's
    /// recorded key encodes (`ddl::pk_key_sql_expr`), used to re-read each
    /// key's new image.
    key_columns: Vec<PrimaryKeyColumn>,
    /// The `from_col` of every relationship whose from-side is this target,
    /// sorted and deduplicated; empty when it is only ever a to-side.
    group_key_columns: Vec<String>,
}

/// Resolves the seam's [`EndpointFeed`] for `target`, or `None` when it is no
/// relationship's endpoint.
///
/// An aggregate target qualifies like any other: its row identity is its
/// `UNIQUE NULLS NOT DISTINCT` grouping columns, which [`read_new_images`]
/// matches NULL-safely.
async fn resolve_endpoint_feed(
    txn: &Transaction<'_>,
    target: &str,
) -> Result<Option<EndpointFeed>, ApplyError> {
    if !catalog::is_relationship_endpoint(txn, target).await? {
        return Ok(None);
    }
    let key_columns = ddl::identity_key_columns(txn, target).await?;
    if key_columns.is_empty() {
        // Unreachable for a table Trellis created: a 1-1 target has a
        // primary key and an aggregate target its grouping-column
        // constraint. With no identity there is nothing to re-read a key by,
        // so the target falls back to image-less recomputes.
        return Ok(None);
    }
    let mut group_key_columns = match target.split_once('.') {
        Some((schema, table)) => catalog::relationships_from_table_in(txn, schema, table)
            .await?
            .into_iter()
            .map(|r| r.def.from_col)
            .collect(),
        None => Vec::new(),
    };
    group_key_columns.sort();
    group_key_columns.dedup();
    Ok(Some(EndpointFeed {
        key_columns,
        group_key_columns,
    }))
}

/// Every key one transaction changed in every target table it wrote, keyed
/// by the target's qualified identity (`transform_definitions.target_table`).
/// See the module doc comment.
#[derive(Debug, Default)]
#[must_use = "a TargetMutations must be staged (into_staged/flush) in the transaction that wrote it"]
pub struct TargetMutations {
    targets: HashMap<String, TargetInfo>,
    touched: BTreeMap<String, BTreeMap<String, KeyMutation>>,
    /// `Some(batch)` once [`Self::spill_over`] has moved the touched set
    /// into [`SPILL_TABLE`]: the most keys one of the spill's statements
    /// binds. See the module doc's "Spilling a large write".
    spill_batch: Option<usize>,
    /// The targets with a key in [`SPILL_TABLE`], so the table exists iff
    /// this is non-empty.
    spilled: BTreeSet<String>,
    stats: SeamStats,
    /// Test-only: `Some(has_readers)` treats every target as read (or
    /// unread) without asking the catalog, for unit tests that drive a
    /// writer against a bare database with no Trellis schema in it.
    #[cfg(test)]
    assume_readers: Option<bool>,
}

/// How much of a transaction's changed keys one [`TargetMutations`] held,
/// and bound, at once (issue #924).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SeamStats {
    /// The most changed keys held in memory at once, as
    /// [`TargetMutations::spill_over`] and the flush found them.
    pub most_held: usize,
    /// The most keys one of the seam's own statements bound: a spill's
    /// upsert or a re-read of new images.
    pub largest_statement: usize,
    /// Whether the touched set was spilled to [`SPILL_TABLE`].
    pub spilled: bool,
}

impl SeamStats {
    fn bound(&mut self, keys: usize) {
        self.largest_statement = self.largest_statement.max(keys);
    }
}

/// The temporary table a spilled [`TargetMutations`] keeps its touched set
/// in, one row per `(target, key)`. See the module doc's "Spilling a large
/// write".
const SPILL_TABLE: &str = "pg_temp.trellis_target_mutations";

/// [`TargetMutations::into_staged`]'s output: the downstream rows to append,
/// every target whose propagation would exceed [`MAX_HOP_GEN`] with the worst
/// hop generation seen, and the transaction's write token.
pub(crate) struct Propagation {
    pub changes: Vec<StagedChange>,
    pub hop_bound_tables: Vec<String>,
    pub worst_hop_gen: i32,
    /// `pg_current_wal_insert_lsn()`, read after every row lock and write
    /// this transaction took (see the module doc's "The write token"), or
    /// `None` when the transaction stages nothing. One token covers every
    /// key the transaction changed.
    pub write_token: Option<PgLsn>,
}

impl TargetMutations {
    pub fn new() -> Self {
        Self::default()
    }

    /// A [`TargetMutations`] that never consults the catalog and treats every
    /// target as unread — see `assume_readers`.
    #[cfg(test)]
    pub(crate) fn assuming_unread() -> Self {
        Self {
            assume_readers: Some(false),
            ..Self::default()
        }
    }

    /// A [`TargetMutations`] that never consults the catalog and treats every
    /// target as read (with no image columns) — see `assume_readers`.
    #[cfg(test)]
    pub(crate) fn assuming_read() -> Self {
        Self {
            assume_readers: Some(true),
            ..Self::default()
        }
    }

    async fn info(
        &mut self,
        txn: &Transaction<'_>,
        target: &str,
    ) -> Result<&TargetInfo, ApplyError> {
        #[cfg(test)]
        if let Some(has_readers) = self.assume_readers {
            self.targets
                .entry(target.to_string())
                .or_insert(TargetInfo {
                    has_readers,
                    image_columns: Vec::new(),
                    endpoint_feed: None,
                });
        }
        if !self.targets.contains_key(target) {
            // The transaction's id before the first check, in a statement
            // of its own (#625 F6): see the module doc on a reader's
            // Re-derive build. A no-op once it has one.
            if self.targets.is_empty() {
                txn.execute("select pg_current_xact_id()", &[]).await?;
            }
            // Mirrors `catalog::dependents_of`'s own status filter: a reader
            // apply doesn't maintain yet is excluded from applies anyway, and
            // catches up from its own backfill or catch-up marker.
            let direct_readers: bool = txn
                .query_one(
                    &format!(
                        "select exists (select 1 from transform_definitions \
                         where source_table = $1 and {})",
                        crate::defs::model::APPLYING_SQL
                    ),
                    &[&target],
                )
                .await?
                .get(0);
            // Any relationship, not only one a live definition reads: a
            // to-one relationship's settled parent projection is maintained
            // from the moment the relationship exists, so a reader that goes
            // live later finds it current.
            let endpoint_feed = resolve_endpoint_feed(txn, target).await?;
            let has_readers = direct_readers || endpoint_feed.is_some();
            let image_columns = if has_readers {
                live_row_columns(txn, target).await?
            } else {
                Vec::new()
            };
            self.targets.insert(
                target.to_string(),
                TargetInfo {
                    has_readers,
                    image_columns,
                    endpoint_feed,
                },
            );
        }
        Ok(&self.targets[target])
    }

    /// The SQL expression (over `alias`) that renders one of `target`'s rows
    /// as the prior-image text a writer should capture, or `None` when no
    /// applying definition reads `target` and so nothing would ever consume it.
    /// Built with `apply::row_as_text_jsonb_sql`, the same rendering every
    /// other image in the ring uses (issue #248).
    pub async fn image_sql(
        &mut self,
        txn: &Transaction<'_>,
        target: &str,
        alias: &str,
    ) -> Result<Option<String>, ApplyError> {
        let info = self.info(txn, target).await?;
        Ok(info
            .has_readers
            .then(|| row_as_text_jsonb_sql(alias, &info.image_columns)))
    }

    /// The columns [`Self::image_sql`] renders for `target`, or `None` when
    /// nothing reads it. For a writer that builds each prior image column by
    /// column rather than from one row alias (#623 D3's ledger group upsert,
    /// `super::ledger`).
    pub(crate) async fn image_columns(
        &mut self,
        txn: &Transaction<'_>,
        target: &str,
    ) -> Result<Option<Vec<String>>, ApplyError> {
        let info = self.info(txn, target).await?;
        Ok(info.has_readers.then(|| info.image_columns.clone()))
    }

    /// Records that this transaction physically changed (wrote or deleted)
    /// `key` in `target`. `prior_image` is the row before the write (see the
    /// module doc comment); pass `None` for a key the write created, or when
    /// [`Self::image_sql`] returned `None`.
    ///
    /// A key touched more than once in one transaction keeps its *first*
    /// prior image (the state every downstream consumer last saw), the
    /// highest `hop_gen`, the earliest `src_changed`, and the earliest
    /// `origin_lsn` (unknown if any is).
    pub fn record(
        &mut self,
        target: &str,
        key: String,
        prior_image: Option<String>,
        hop_gen: i32,
        src_changed: Option<SystemTime>,
        origin_lsn: Option<PgLsn>,
    ) {
        self.touched
            .entry(target.to_string())
            .or_default()
            .entry(key)
            .and_modify(|m| {
                m.hop_gen = m.hop_gen.max(hop_gen);
                m.src_changed = earliest_src_changed(m.src_changed, src_changed);
                m.origin_lsn = earliest_origin(m.origin_lsn, origin_lsn);
            })
            .or_insert(KeyMutation {
                prior_image,
                hop_gen,
                src_changed,
                origin_lsn,
            });
    }

    /// Turns every recorded key of a target with an applying reader into its
    /// downstream `Recompute` row at `hop_gen + 1`, or, for a target the seam
    /// feeds as a relationship endpoint, its CDC-shaped row (re-reading each
    /// key's new image; see the module doc). A key whose next hop
    /// would pass [`MAX_HOP_GEN`] is not staged; its target is reported in
    /// [`Propagation::hop_bound_tables`] instead, for the caller to fail the
    /// transaction with [`ApplyError::HopBoundExceeded`] (alongside any other
    /// hop-bounded rows it stages).
    ///
    /// Also reads [`Propagation::write_token`]. The caller must not take any
    /// further row lock on a target after this (see the module doc's "The
    /// write token"). Every caller calls it after its last target write: the
    /// drain's Phase 3 and the Re-derive build's chunks just before commit,
    /// and
    /// `intake::resume_orphans` at the start of a discharge that then writes
    /// the ring and catalog (no target) before it commits.
    ///
    /// An accumulator [`Self::spill_over`] has spilled is refused with
    /// [`ApplyError::SpilledMutationsNotFlushed`]: the keys in its spill
    /// table aren't held here, and only [`Self::flush`] stages them.
    pub(crate) async fn into_staged(
        mut self,
        txn: &Transaction<'_>,
    ) -> Result<Propagation, ApplyError> {
        self.stage_held(txn).await
    }

    /// [`Self::into_staged`]'s work, leaving the accumulator's stats behind.
    async fn stage_held(&mut self, txn: &Transaction<'_>) -> Result<Propagation, ApplyError> {
        // Only the batched writers spill, and they stage through `flush`: a
        // drain page's touched set is capped (ADR-0002 I8), and it never
        // calls `spill_over`. The keys already in the spill table never
        // reach `touched`, so staging only what is held would drop them.
        if self.spill_batch.is_some() {
            return Err(ApplyError::SpilledMutationsNotFlushed);
        }
        let mut propagation = Propagation {
            changes: Vec::new(),
            hop_bound_tables: Vec::new(),
            worst_hop_gen: 0,
            write_token: None,
        };
        let touched = std::mem::take(&mut self.touched);
        for (target, keys) in touched {
            let info = self.info(txn, &target).await?;
            if !info.has_readers {
                continue;
            }
            let info = info.clone();
            // Every writer in this transaction has already taken its row
            // locks and written, so this is after the last of them. Read
            // once, before the first staged row, so a row built here can
            // carry it.
            let token = match propagation.write_token {
                Some(token) => token,
                None => {
                    let token = read_write_token(txn).await?;
                    propagation.write_token = Some(token);
                    token
                }
            };
            stage_target(
                txn,
                &target,
                &info,
                keys,
                token,
                &mut propagation,
                &mut self.stats,
            )
            .await?;
        }
        Ok(propagation)
    }

    /// [`Self::into_staged`] plus the append, for a writer outside the drain
    /// path (which folds its own hop-bound check into a wider one).
    pub async fn flush(self, txn: &Transaction<'_>) -> Result<(), ApplyError> {
        self.flush_counted(txn).await.map(|_| ())
    }

    /// [`Self::flush`], returning how much the accumulator held and bound
    /// at once. A spilled accumulator stages from [`SPILL_TABLE`] a batch
    /// at a time instead (see the module doc's "Spilling a large write").
    pub(crate) async fn flush_counted(
        mut self,
        txn: &Transaction<'_>,
    ) -> Result<SeamStats, ApplyError> {
        if let Some(batch) = self.spill_batch {
            self.spill(txn, batch).await?;
            self.flush_spilled(txn, batch).await?;
            return Ok(self.stats);
        }
        self.stats.most_held = self.stats.most_held.max(self.held());
        let mut propagation = self.stage_held(txn).await?;
        if !propagation.hop_bound_tables.is_empty() {
            propagation.hop_bound_tables.sort();
            propagation.hop_bound_tables.dedup();
            return Err(ApplyError::HopBoundExceeded {
                hop_gen: propagation.worst_hop_gen,
                tables: propagation.hop_bound_tables,
            });
        }
        append::append(txn, &propagation.changes).await?;
        Ok(self.stats)
    }

    /// How many keys the in-memory touched set holds.
    fn held(&self) -> usize {
        self.touched.values().map(BTreeMap::len).sum()
    }

    /// Moves the touched set into [`SPILL_TABLE`] once it holds `batch` keys
    /// or more, so a writer that changes an unbounded number of keys in one
    /// transaction holds at most about two batches of them in memory, and
    /// binds at most `batch` per statement, however many it changes (issue
    /// #924). The key's merge rules carry over to the table (see
    /// [`Self::record`]), and [`Self::flush`] stages from it.
    ///
    /// A writer calls it between its own batches, with no statement of its
    /// own in flight. It takes no target lock, so the write token
    /// [`Self::flush`] reads is still after the transaction's last one.
    /// Under `batch` keys it does nothing, so a transaction that never
    /// reaches a batch never creates the table. The drain doesn't call it:
    /// a page's keys are capped (ADR-0002 I8), and it stages through
    /// [`Self::into_staged`], which refuses a spilled accumulator.
    pub(crate) async fn spill_over(
        &mut self,
        txn: &Transaction<'_>,
        batch: usize,
    ) -> Result<(), ApplyError> {
        let batch = batch.max(1);
        if self.held() < batch {
            return Ok(());
        }
        self.spill(txn, batch).await
    }

    /// Moves every key the touched set holds into [`SPILL_TABLE`], at most
    /// `batch` per statement, and marks the accumulator spilled. A target
    /// nothing reads is dropped instead: nothing would stage it.
    async fn spill(&mut self, txn: &Transaction<'_>, batch: usize) -> Result<(), ApplyError> {
        self.spill_batch = Some(batch);
        self.stats.spilled = true;
        self.stats.most_held = self.stats.most_held.max(self.held());
        let touched = std::mem::take(&mut self.touched);
        for (target, keys) in touched {
            if !self.info(txn, &target).await?.has_readers || keys.is_empty() {
                continue;
            }
            if self.spilled.is_empty() {
                txn.batch_execute(&format!(
                    "create temporary table {SPILL_TABLE} (\
                         target text not null, \
                         key text collate \"C\" not null, \
                         prior_image text, \
                         hop_gen integer not null, \
                         src_changed timestamptz, \
                         origin_lsn pg_lsn, \
                         primary key (target, key)) \
                     on commit drop"
                ))
                .await?;
            }
            self.spilled.insert(target.clone());
            let keys: Vec<(String, KeyMutation)> = keys.into_iter().collect();
            for chunk in keys.chunks(batch) {
                self.stats.bound(chunk.len());
                upsert_spilled(txn, &target, chunk).await?;
            }
        }
        Ok(())
    }

    /// [`Self::flush`] for a spilled accumulator, once [`Self::spill`] has
    /// moved the rest of its keys into [`SPILL_TABLE`]: the hop bound over
    /// every key, the write token, and then each target's keys a batch at a
    /// time, in key order, staged and appended as [`Self::into_staged`]
    /// stages them.
    async fn flush_spilled(
        &mut self,
        txn: &Transaction<'_>,
        batch: usize,
    ) -> Result<(), ApplyError> {
        if self.spilled.is_empty() {
            return Ok(());
        }
        let bounded = txn
            .query(
                &format!(
                    "select target, max(hop_gen) from {SPILL_TABLE} \
                     where hop_gen >= $1 group by target order by target"
                ),
                &[&MAX_HOP_GEN],
            )
            .await?;
        if !bounded.is_empty() {
            let worst: i32 = bounded
                .iter()
                .map(|row| row.get::<_, i32>(1))
                .max()
                .unwrap_or(0);
            return Err(ApplyError::HopBoundExceeded {
                hop_gen: worst + 1,
                tables: bounded.into_iter().map(|row| row.get(0)).collect(),
            });
        }
        // As in `into_staged`: every write is done, and spilling took no
        // target lock.
        let token = read_write_token(txn).await?;
        let read = txn
            .prepare(&format!(
                "select key, prior_image, hop_gen, src_changed, origin_lsn \
                 from {SPILL_TABLE} where target = $1 order by key"
            ))
            .await?;
        let fetch = i32::try_from(batch).unwrap_or(i32::MAX);
        for target in std::mem::take(&mut self.spilled) {
            let info = self.info(txn, &target).await?.clone();
            let portal = txn.bind(&read, &[&target]).await?;
            loop {
                let rows = txn.query_portal(&portal, fetch).await?;
                if rows.is_empty() {
                    break;
                }
                let keys: BTreeMap<String, KeyMutation> = rows
                    .iter()
                    .map(|row| {
                        (
                            row.get(0),
                            KeyMutation {
                                prior_image: row.get(1),
                                hop_gen: row.get(2),
                                src_changed: row.get(3),
                                origin_lsn: row.get(4),
                            },
                        )
                    })
                    .collect();
                let mut propagation = Propagation {
                    changes: Vec::new(),
                    hop_bound_tables: Vec::new(),
                    worst_hop_gen: 0,
                    write_token: Some(token),
                };
                stage_target(
                    txn,
                    &target,
                    &info,
                    keys,
                    token,
                    &mut propagation,
                    &mut self.stats,
                )
                .await?;
                append::append(txn, &propagation.changes).await?;
            }
        }
        txn.batch_execute(&format!("drop table {SPILL_TABLE}"))
            .await?;
        Ok(())
    }
}

/// Adds `keys` of `target` to [`SPILL_TABLE`], merging each with the row a
/// previous spill left for it by [`TargetMutations::record`]'s rules: the
/// first prior image wins (the row already there, even with no image), the
/// highest `hop_gen`, the earliest `src_changed`, and the earliest
/// `origin_lsn`, unknown if either is.
async fn upsert_spilled(
    txn: &Transaction<'_>,
    target: &str,
    keys: &[(String, KeyMutation)],
) -> Result<(), ApplyError> {
    let names: Vec<&str> = keys.iter().map(|(key, _)| key.as_str()).collect();
    let priors: Vec<Option<&str>> = keys.iter().map(|(_, m)| m.prior_image.as_deref()).collect();
    let hops: Vec<i32> = keys.iter().map(|(_, m)| m.hop_gen).collect();
    let changed: Vec<Option<SystemTime>> = keys.iter().map(|(_, m)| m.src_changed).collect();
    let origins: Vec<Option<PgLsn>> = keys.iter().map(|(_, m)| m.origin_lsn).collect();
    txn.execute(
        &format!(
            "insert into {SPILL_TABLE} as m \
                 (target, key, prior_image, hop_gen, src_changed, origin_lsn) \
             select $1, k.key, k.prior, k.hop, k.changed, k.origin \
             from unnest($2::text[], $3::text[], $4::integer[], $5::timestamptz[], \
                         $6::pg_lsn[]) as k(key, prior, hop, changed, origin) \
             on conflict (target, key) do update set \
                 hop_gen = greatest(m.hop_gen, excluded.hop_gen), \
                 src_changed = least(m.src_changed, excluded.src_changed), \
                 origin_lsn = case when m.origin_lsn is null \
                                     or excluded.origin_lsn is null then null \
                                   else least(m.origin_lsn, excluded.origin_lsn) end"
        ),
        &[&target, &names, &priors, &hops, &changed, &origins],
    )
    .await?;
    Ok(())
}

/// Stages one target's `keys` into `propagation`, as
/// [`TargetMutations::into_staged`] describes: a `Recompute` per key, or for
/// an endpoint target a CDC-shaped row from the key's prior image and its
/// re-read new image, carrying `token`. A key past [`MAX_HOP_GEN`] is
/// reported instead.
async fn stage_target(
    txn: &Transaction<'_>,
    target: &str,
    info: &TargetInfo,
    keys: BTreeMap<String, KeyMutation>,
    token: PgLsn,
    propagation: &mut Propagation,
    stats: &mut SeamStats,
) -> Result<(), ApplyError> {
    let mut new_images = match &info.endpoint_feed {
        Some(feed) => {
            stats.bound(keys.len());
            read_new_images(txn, target, &info.image_columns, feed, &keys).await?
        }
        None => HashMap::new(),
    };
    for (key, m) in keys {
        let next_hop = m.hop_gen + 1;
        if next_hop > MAX_HOP_GEN {
            propagation.hop_bound_tables.push(target.to_string());
            propagation.worst_hop_gen = propagation.worst_hop_gen.max(next_hop);
            continue;
        }
        let Some(new) = new_images.remove(&key) else {
            propagation.changes.push(StagedChange::Recompute {
                src_table: target.to_string(),
                key,
                hop_gen: next_hop,
                group_key: None,
                src_changed: m.src_changed,
                prior_image: m.prior_image,
                origin_lsn: m.origin_lsn,
            });
            continue;
        };
        let op = match (&m.prior_image, &new.image) {
            (None, None) => continue,
            (None, Some(_)) => CdcOp::Insert,
            (Some(_), Some(_)) => CdcOp::Update,
            (Some(_), None) => CdcOp::Delete,
        };
        propagation.changes.push(StagedChange::Cdc {
            src_table: target.to_string(),
            key,
            op,
            lsn: Some(token),
            old_image: m.prior_image,
            new_image: new.image,
            origin_lsn: m.origin_lsn,
            src_changed: m.src_changed,
            hop_gen: next_hop,
            group_key: new.group_key,
        });
    }
    Ok(())
}

/// One key's state as [`read_new_images`] found it after the transaction's
/// last write.
struct NewImage {
    /// The row as this transaction left it, `None` if it deleted the row.
    image: Option<String>,
    /// The union of `EndpointFeed::group_key_columns`' values across the
    /// key's prior and new images, `None` when there are none.
    group_key: Option<Vec<String>>,
}

/// Re-reads every key in `keys` from `target` by its row identity, in one
/// statement, after every write this transaction made, and computes each
/// key's `group_key` from its prior image and the re-read row (the same text
/// both images render, `<col>::text`). A plain read that takes no row lock,
/// so it leaves the write token's "no lock after the token" rule intact.
///
/// Every key comes back: an absent row is a deleted one (this transaction
/// holds the deleted row's lock, so nothing else can have re-created it).
///
/// An aggregate target's key can carry a NULL grouping component (issue
/// #110's encoding, decoded by `ddl::split_pk_key`). See
/// [`new_images_query`] for how those keys are matched without giving up
/// the index. Presence is read off `t.ctid`, since a key column can itself be
/// NULL on a row that exists.
async fn read_new_images(
    txn: &Transaction<'_>,
    target: &str,
    image_columns: &[String],
    feed: &EndpointFeed,
    keys: &BTreeMap<String, KeyMutation>,
) -> Result<HashMap<String, NewImage>, ApplyError> {
    if keys.is_empty() {
        return Ok(HashMap::new());
    }
    let query = new_images_query(target, image_columns, feed, keys)?;
    let rows = super::ledger::query_by_entry_key(txn, &query.sql, &query.params()).await?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let group_key: Option<Vec<String>> = row.get(2);
            (
                row.get(0),
                NewImage {
                    image: row.get(1),
                    group_key: group_key.filter(|values| !values.is_empty()),
                },
            )
        })
        .collect())
}

/// [`read_new_images`]' statement and the arrays it binds.
struct NewImagesQuery<'a> {
    sql: String,
    arms: Vec<NullPatternArm<'a>>,
}

/// The keys of one [`read_new_images`] call whose identity is `NULL` in
/// exactly the same columns, bound as one `union all` arm.
#[derive(Default)]
struct NullPatternArm<'a> {
    keys: Vec<&'a str>,
    priors: Vec<Option<&'a str>>,
    /// One array per identity column that is not `NULL` in this pattern, in
    /// column order. A `NULL` column binds nothing: the arm matches it with
    /// `is null`.
    parts: Vec<Vec<String>>,
}

impl NewImagesQuery<'_> {
    /// The bind parameters, in the order `sql` numbers them.
    fn params(&self) -> Vec<&(dyn ToSql + Sync)> {
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
        for arm in &self.arms {
            params.push(&arm.keys);
            params.push(&arm.priors);
            for part in &arm.parts {
                params.push(part);
            }
        }
        params
    }
}

/// Builds [`read_new_images`]' statement: one `union all` arm per pattern of
/// `NULL` identity columns present among `keys` (issue #433), usually just
/// one. Within an arm a `NULL` column matches with `t.<col> is null` and
/// every other column with `=`, so each arm probes the target's index.
///
/// Matching a whole batch with `is not distinct from` on any column one of
/// its keys binds a NULL for (the pre-#433 shape) is correct but not
/// indexable: on a 500k-group aggregate target, a 500-key batch with one
/// NULL group took 8.5s against 0.4ms without it. This is the same split
/// `intake::resume_orphans` makes per NULL pattern.
///
/// A single-column identity's non-`NULL` arm also restricts the column to
/// its array (`t.<col> = any(<array>)`), which the match already implies,
/// so the target's side of whatever join the planner picks is bounded by
/// the batch (#790). A condition in a left join's `on` that names only the
/// target filters the target's scan. Without it, a target analyzed while
/// small and grown since was read in full: at 1M rows, a 5,000-key batch
/// hashed a sequential scan of the target, 102 ms against 31 ms through the
/// index. [`read_new_images`] also runs it under `ENTRY_PLAN_SETTINGS` (no
/// sequential scan): PostgreSQL 16, unlike 17, still scanned a 400k-row
/// target and filtered it by the bound.
///
/// A composite identity is never restricted this way: see
/// [`bounds_keyset_by_array`] for why one `= any` per column is worse
/// than the scan it avoids.
fn new_images_query<'a>(
    target: &str,
    image_columns: &[String],
    feed: &EndpointFeed,
    keys: &'a BTreeMap<String, KeyMutation>,
) -> Result<NewImagesQuery<'a>, ApplyError> {
    let pk = &feed.key_columns;
    // `pattern[i]`: identity column `i` is NULL.
    let mut arms: BTreeMap<Vec<bool>, NullPatternArm<'a>> = BTreeMap::new();
    for (key, m) in keys {
        let decoded = ddl::split_pk_key(pk, target, key)?;
        let pattern: Vec<bool> = decoded.iter().map(Option::is_none).collect();
        let arm = arms
            .entry(pattern)
            .or_insert_with_key(|pattern| NullPatternArm {
                parts: vec![Vec::new(); pattern.iter().filter(|null| !**null).count()],
                ..NullPatternArm::default()
            });
        arm.keys.push(key);
        arm.priors.push(m.prior_image.as_deref());
        for (column, part) in arm.parts.iter_mut().zip(decoded.into_iter().flatten()) {
            column.push(part.into_owned());
        }
    }

    let image = row_as_text_jsonb_sql("t", image_columns);
    let group_key_sql = if feed.group_key_columns.is_empty() {
        "null::text[]".to_string()
    } else {
        let values: Vec<String> = feed
            .group_key_columns
            .iter()
            .flat_map(|col| {
                [
                    format!("k.prior::jsonb ->> {}", quote_literal(col)),
                    format!("t.{}::text", quote_ident(col)),
                ]
            })
            .collect();
        format!(
            "array(select distinct v from unnest(array[{}]::text[]) as g(v) \
             where v is not null order by v)",
            values.join(", ")
        )
    };
    let target_ident = ddl::qualified_target_table_ident(target);
    let mut next_param = 1;
    let mut param = || {
        let placeholder = format!("${next_param}");
        next_param += 1;
        placeholder
    };
    let selects: Vec<String> = arms
        .keys()
        .map(|pattern| {
            let mut arrays = vec![
                format!("{}::text[]", param()),
                format!("{}::text[]", param()),
            ];
            let mut aliases = vec!["key".to_string(), "prior".to_string()];
            let mut matched = Vec::with_capacity(pk.len());
            let mut bounds = Vec::new();
            for (i, (column, &null)) in pk.iter().zip(pattern).enumerate() {
                let col = quote_ident(&column.name);
                if null {
                    matched.push(format!("t.{col} is null"));
                } else {
                    let array = format!("{}::text[]::{}[]", param(), column.data_type);
                    if bounds_keyset_by_array(pk) {
                        bounds.push(format!("t.{col} = any({array})"));
                    }
                    arrays.push(array);
                    aliases.push(pk_keyset_col(i));
                    matched.push(format!("t.{col} = k.{}", pk_keyset_col(i)));
                }
            }
            matched.extend(bounds);
            format!(
                "select k.key, \
                        case when t.ctid is null then null \
                             else ({image})::text end, \
                        {group_key_sql} \
                 from unnest({}) as k({}) \
                 left join {target_ident} t on {}",
                arrays.join(", "),
                aliases.join(", "),
                matched.join(" and "),
            )
        })
        .collect();
    Ok(NewImagesQuery {
        sql: selects.join(" union all "),
        arms: arms.into_values().collect(),
    })
}

/// The current WAL insert position, as this transaction's write token — see
/// the module doc's "The write token".
async fn read_write_token(txn: &Transaction<'_>) -> Result<PgLsn, ApplyError> {
    Ok(txn
        .query_one("select pg_current_wal_insert_lsn()", &[])
        .await?
        .get(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_touched_twice_keeps_its_first_prior_image() {
        let mut m = TargetMutations::new();
        m.record(
            "public.t",
            "1".into(),
            Some("{\"g\":\"a\"}".into()),
            0,
            None,
            None,
        );
        m.record(
            "public.t",
            "1".into(),
            Some("{\"g\":\"b\"}".into()),
            3,
            None,
            None,
        );
        m.record("public.t", "2".into(), None, 1, None, None);
        assert_eq!(m.touched["public.t"].len(), 2);
        let key = &m.touched["public.t"]["1"];
        assert_eq!(key.prior_image.as_deref(), Some("{\"g\":\"a\"}"));
        assert_eq!(key.hop_gen, 3);
    }

    #[test]
    fn a_key_created_in_the_transaction_keeps_no_prior_image() {
        let mut m = TargetMutations::new();
        m.record("public.t", "1".into(), None, 0, None, None);
        m.record(
            "public.t",
            "1".into(),
            Some("{\"g\":\"a\"}".into()),
            0,
            None,
            None,
        );
        assert_eq!(m.touched["public.t"]["1"].prior_image, None);
    }

    async fn connect(dsn: &str) -> tokio_postgres::Client {
        let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }

    async fn wal_insert_lsn(client: &impl tokio_postgres::GenericClient) -> PgLsn {
        client
            .query_one("select pg_current_wal_insert_lsn()", &[])
            .await
            .expect("read the WAL insert position")
            .get(0)
    }

    /// Caution 1 of #375's direction 1: two writers of one key serialize on
    /// its row lock, so a token read after the lock orders the second writer
    /// above the first one's commit. Writer B is shaped like every real seam
    /// writer (`image_sql`, then the locking write, then `record`, then
    /// `into_staged`) and blocks on writer A's lock mid-write.
    ///
    /// - A's own token is below A's commit (caution 2: pre-commit).
    /// - A position B reads before its lock (where `image_sql` runs) is below
    ///   A's commit too, which is why the token must not be read there.
    /// - B's token is at or above a position read after A's commit returned,
    ///   so above A's commit record.
    #[tokio::test]
    async fn a_second_writer_of_a_key_takes_its_token_after_the_first_writers_commit() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut a = connect(db.dsn()).await;
        let mut b = connect(db.dsn()).await;
        a.batch_execute("create table t (id int primary key, v int); insert into t values (1, 0)")
            .await
            .expect("create and seed t");

        let txn_a = a.transaction().await.expect("begin A");
        let mut m_a = TargetMutations::assuming_read();
        txn_a
            .execute("update t set v = 1 where id = 1", &[])
            .await
            .expect("A's write");
        m_a.record("public.t", "1".into(), None, 0, None, None);
        let token_a = m_a
            .into_staged(&txn_a)
            .await
            .expect("stage A")
            .write_token
            .expect("A staged a row, so it read a token");

        let txn_b = b.transaction().await.expect("begin B");
        let mut m_b = TargetMutations::assuming_read();
        m_b.image_sql(&txn_b, "public.t", "t")
            .await
            .expect("B resolves its image expression before locking");
        let before_b_locks = wal_insert_lsn(&txn_b).await;
        let (after_a_commits, written_b) = {
            let write_b = txn_b.execute("update t set v = 2 where id = 1", &[]);
            tokio::pin!(write_b);
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(100), &mut write_b)
                    .await
                    .is_err(),
                "B's write waits on A's row lock"
            );
            txn_a.commit().await.expect("commit A");
            let after_a_commits = wal_insert_lsn(&a).await;
            (after_a_commits, write_b.await.expect("B's write"))
        };
        assert_eq!(written_b, 1);
        m_b.record("public.t", "1".into(), None, 0, None, None);
        let token_b = m_b
            .into_staged(&txn_b)
            .await
            .expect("stage B")
            .write_token
            .expect("B staged a row, so it read a token");
        txn_b.commit().await.expect("commit B");

        assert!(
            token_a < after_a_commits,
            "A's token ({token_a}) is pre-commit, below A's commit (<= {after_a_commits})"
        );
        assert!(
            before_b_locks < after_a_commits,
            "a position B read before its lock ({before_b_locks}) is not ordered after A's \
             commit (<= {after_a_commits})"
        );
        assert!(
            token_b >= after_a_commits,
            "B's token ({token_b}) is at or above a position read after A committed \
             ({after_a_commits})"
        );
    }

    /// The token is read at staging time, after every write the transaction
    /// made, not when the first key was recorded.
    #[tokio::test]
    async fn the_token_follows_the_transactions_last_write() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut client = connect(db.dsn()).await;
        client
            .batch_execute("create table t (id int primary key, v int)")
            .await
            .expect("create t");

        let txn = client.transaction().await.expect("begin");
        let mut m = TargetMutations::assuming_read();
        txn.execute("insert into t values (1, 0)", &[])
            .await
            .expect("first write");
        m.record("public.t", "1".into(), None, 0, None, None);
        let after_first_write = wal_insert_lsn(&txn).await;
        txn.execute(
            "insert into t select g, 0 from generate_series(2, 100) g",
            &[],
        )
        .await
        .expect("last write");
        for id in 2..=100 {
            m.record("public.t", id.to_string(), None, 0, None, None);
        }
        let after_last_write = wal_insert_lsn(&txn).await;
        let propagation = m.into_staged(&txn).await.expect("stage");
        let token = propagation.write_token.expect("rows staged, so a token");

        assert_eq!(propagation.changes.len(), 100);
        assert!(
            after_first_write < after_last_write,
            "the last write wrote WAL"
        );
        assert!(
            token >= after_last_write,
            "the token ({token}) is at or after the last write ({after_last_write})"
        );
    }

    /// One [`SPILL_TABLE`] row: key, prior image, `hop_gen`, `src_changed`,
    /// `origin_lsn`.
    type SpilledRow = (
        String,
        Option<String>,
        i32,
        Option<SystemTime>,
        Option<PgLsn>,
    );

    /// Issue #924: a key spilled by one batch and touched again by a later
    /// one merges in the spill table by [`TargetMutations::record`]'s rules:
    /// the first prior image (even a missing one, for a key created here),
    /// the highest `hop_gen`, the earliest `src_changed`, and the earliest
    /// `origin_lsn`, unknown if either side is. A target nothing reads is
    /// dropped at the spill, not held. Under a batch of keys, nothing spills.
    #[tokio::test]
    async fn a_key_spilled_by_two_batches_merges_like_one_record() {
        use std::time::Duration;
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut client = connect(db.dsn()).await;
        let txn = client.transaction().await.expect("begin");
        let at = |secs| SystemTime::UNIX_EPOCH + Duration::from_secs(secs);
        let lsn = |n: u64| Some(PgLsn::from(n));

        let mut m = TargetMutations::assuming_read();
        m.record(
            "public.t",
            "a".into(),
            Some("first".into()),
            1,
            Some(at(20)),
            lsn(200),
        );
        m.spill_over(&txn, 3).await.expect("under a batch");
        assert!(!m.stats.spilled, "one key is under a batch of 3");
        m.record("public.t", "b".into(), None, 2, None, lsn(50));
        m.record(
            "public.t",
            "c".into(),
            Some("c0".into()),
            0,
            Some(at(5)),
            None,
        );
        m.spill_over(&txn, 3).await.expect("spill the first batch");
        assert!(m.stats.spilled, "three keys are a batch");
        assert_eq!(m.held(), 0, "a spill holds nothing in memory");

        // The second batch touches every key again.
        m.record(
            "public.t",
            "a".into(),
            Some("second".into()),
            4,
            Some(at(10)),
            None,
        );
        m.record(
            "public.t",
            "b".into(),
            Some("b1".into()),
            1,
            Some(at(7)),
            lsn(40),
        );
        m.record(
            "public.t",
            "c".into(),
            Some("c1".into()),
            3,
            Some(at(1)),
            lsn(1),
        );
        m.record("public.t", "d".into(), Some("d0".into()), 0, None, lsn(9));
        m.spill_over(&txn, 3).await.expect("spill the second batch");
        assert_eq!(m.stats.most_held, 4);
        assert_eq!(
            m.stats.largest_statement, 3,
            "each upsert binds at most a batch"
        );

        let rows: Vec<SpilledRow> = txn
            .query(
                &format!(
                    "select key, prior_image, hop_gen, src_changed, origin_lsn \
                     from {SPILL_TABLE} where target = 'public.t' order by key"
                ),
                &[],
            )
            .await
            .expect("read the spill")
            .into_iter()
            .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4)))
            .collect();
        assert_eq!(
            rows,
            vec![
                ("a".into(), Some("first".into()), 4, Some(at(10)), None),
                ("b".into(), None, 2, Some(at(7)), lsn(40)),
                ("c".into(), Some("c0".into()), 3, Some(at(1)), None),
                ("d".into(), Some("d0".into()), 0, None, lsn(9)),
            ],
            "first image, max hop_gen, earliest src_changed, earliest origin or unknown"
        );

        let mut unread = TargetMutations::assuming_unread();
        unread.record("public.u", "1".into(), None, 0, None, None);
        unread
            .spill_over(&txn, 1)
            .await
            .expect("spill an unread target");
        assert_eq!(unread.held(), 0, "an unread target's keys are dropped");
        assert!(unread.spilled.is_empty(), "and never reach the table");
    }

    /// Issue #924: a spilled flush refuses a key past [`MAX_HOP_GEN`] with
    /// the same error an in-memory flush gives: the worst next hop, and
    /// each bounded target once, in name order. The bounded keys are spread
    /// over two spills and the flush's own, and a target under the bound
    /// sits between them.
    #[tokio::test]
    async fn a_spilled_flush_bounds_hops_like_an_in_memory_one() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut client = connect(db.dsn()).await;
        let batches: [&[(&str, &str, i32)]; 3] = [
            &[("public.z", "1", MAX_HOP_GEN), ("public.m", "1", 0)],
            &[("public.a", "1", MAX_HOP_GEN - 1), ("public.m", "2", 1)],
            &[("public.a", "1", MAX_HOP_GEN + 2), ("public.z", "2", 0)],
        ];

        let mut errors = Vec::new();
        for spill in [None, Some(2)] {
            let txn = client.transaction().await.expect("begin");
            let mut m = TargetMutations::assuming_read();
            for batch in batches {
                for &(target, key, hop_gen) in batch {
                    m.record(target, key.into(), None, hop_gen, None, None);
                }
                if let Some(batch) = spill {
                    m.spill_over(&txn, batch).await.expect("spill");
                }
            }
            assert_eq!(m.stats.spilled, spill.is_some());
            match m.flush_counted(&txn).await {
                Err(ApplyError::HopBoundExceeded { hop_gen, tables }) => {
                    errors.push((hop_gen, tables));
                }
                other => panic!("expected the hop bound, got {other:?}"),
            }
            txn.rollback().await.expect("rollback");
        }
        assert_eq!(
            errors[0],
            (
                MAX_HOP_GEN + 3,
                vec!["public.a".to_string(), "public.z".to_string()]
            ),
            "the in-memory flush"
        );
        assert_eq!(errors[1], errors[0], "the spilled flush matches it");
    }

    /// A transaction that stages nothing pays no round trip for a token.
    #[tokio::test]
    async fn a_transaction_that_stages_nothing_reads_no_token() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut client = connect(db.dsn()).await;

        let txn = client.transaction().await.expect("begin");
        let mut unread = TargetMutations::assuming_unread();
        unread.record("public.t", "1".into(), None, 0, None, None);
        let propagation = unread.into_staged(&txn).await.expect("stage unread");
        assert!(propagation.changes.is_empty());
        assert_eq!(
            propagation.write_token, None,
            "a terminal target reads no token"
        );

        let untouched = TargetMutations::assuming_read();
        let propagation = untouched.into_staged(&txn).await.expect("stage untouched");
        assert_eq!(propagation.write_token, None, "no key changed, no token");
    }

    /// Issue #926: `into_staged` refuses an accumulator that has spilled,
    /// rather than stage the keys it holds and drop the ones in the spill
    /// table, whether or not the spill kept any (an unread target's keys
    /// are dropped at the spill).
    #[tokio::test]
    async fn into_staged_refuses_a_spilled_accumulator() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut client = connect(db.dsn()).await;
        let txn = client.transaction().await.expect("begin");
        for mut m in [
            TargetMutations::assuming_read(),
            TargetMutations::assuming_unread(),
        ] {
            m.record("public.t", "1".into(), None, 0, None, None);
            m.record("public.t", "2".into(), None, 0, None, None);
            m.spill_over(&txn, 2).await.expect("spill a batch");
            assert!(m.stats.spilled);
            m.record("public.t", "3".into(), None, 0, None, None);
            match m.into_staged(&txn).await {
                Err(ApplyError::SpilledMutationsNotFlushed) => {}
                Err(other) => panic!("expected the spilled refusal, got {other:?}"),
                Ok(propagation) => panic!(
                    "a spilled accumulator staged {} of its keys in memory",
                    propagation.changes.len()
                ),
            }
        }
    }

    /// `read_new_images` over a composite, mixed-type row identity (a text
    /// part carrying the composite separator, a `date` part) and a
    /// mixed-case join-key column: an updated key comes back with its new
    /// image, a deleted one with none, and each `group_key` is the union of
    /// the prior and new images' join-key values.
    #[tokio::test]
    async fn read_new_images_matches_composite_mixed_type_keys() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut client = connect(db.dsn()).await;
        client
            .batch_execute(
                "set datestyle to 'ISO, YMD'; \
                 create table t (a text, b date, \"Grp\" int, primary key (a, b)); \
                 insert into t values ('x' || chr(31) || 'y', '2026-01-02', 1), \
                                      ('z', '2026-03-04', 2)",
            )
            .await
            .expect("create t");
        let txn = client.transaction().await.expect("begin");
        let key_columns = ddl::identity_key_columns(&txn, "public.t")
            .await
            .expect("pk");
        let columns = live_row_columns(&txn, "public.t").await.expect("columns");
        let key_sql = ddl::pk_key_sql_expr(&key_columns, Some("t"));
        let image = row_as_text_jsonb_sql("t", &columns);
        let before: Vec<(String, String)> = txn
            .query(
                &format!("select {key_sql}, ({image})::text from t order by \"Grp\" for update"),
                &[],
            )
            .await
            .expect("pre-lock")
            .into_iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect();
        txn.batch_execute(
            "update t set \"Grp\" = 5 where a = 'x' || chr(31) || 'y'; \
             delete from t where a = 'z'",
        )
        .await
        .expect("write");
        let mut keys = BTreeMap::new();
        for (key, prior) in &before {
            keys.insert(
                key.clone(),
                KeyMutation {
                    prior_image: Some(prior.clone()),
                    hop_gen: 0,
                    src_changed: None,
                    origin_lsn: None,
                },
            );
        }
        let feed = EndpointFeed {
            key_columns,
            group_key_columns: vec!["Grp".to_string()],
        };
        let mut got = read_new_images(&txn, "public.t", &columns, &feed, &keys)
            .await
            .expect("re-read");
        let updated = got
            .remove(&before[0].0)
            .expect("the updated key comes back");
        assert_eq!(
            updated.image.as_deref(),
            Some(r#"{"a": "x\u001fy", "b": "2026-01-02", "Grp": "5"}"#)
        );
        assert_eq!(
            updated.group_key,
            Some(vec!["1".to_string(), "5".to_string()])
        );
        let deleted = got
            .remove(&before[1].0)
            .expect("the deleted key comes back");
        assert_eq!(deleted.image, None);
        assert_eq!(deleted.group_key, Some(vec!["2".to_string()]));
        assert!(got.is_empty());
    }

    /// `read_new_images` over an aggregate target's row identity: `UNIQUE
    /// NULLS NOT DISTINCT` grouping columns whose keys carry NULL components
    /// (issue #110's encoding). A plain `=` match would never find a NULL
    /// group's row, and a presence test on a key column would read the row
    /// as deleted; either way an existing NULL group would stage as a delete.
    #[tokio::test]
    async fn read_new_images_matches_null_grouping_components() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut client = connect(db.dsn()).await;
        client
            .batch_execute(
                "create table t (g int, h text, total int, unique nulls not distinct (g, h)); \
                 insert into t values (null, 'a', 1), (1, null, 2), (null, null, 3), (2, 'b', 4)",
            )
            .await
            .expect("create t");
        let txn = client.transaction().await.expect("begin");
        let key_columns = ddl::identity_key_columns(&txn, "public.t")
            .await
            .expect("identity");
        assert!(
            key_columns.iter().all(|c| c.nullable),
            "an aggregate-style identity"
        );
        let columns = live_row_columns(&txn, "public.t").await.expect("columns");
        let key_sql = ddl::pk_key_sql_expr(&key_columns, Some("t"));
        let image = row_as_text_jsonb_sql("t", &columns);
        let before: Vec<(String, String)> = txn
            .query(
                &format!("select {key_sql}, ({image})::text from t order by total for update"),
                &[],
            )
            .await
            .expect("pre-lock")
            .into_iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect();
        txn.batch_execute(
            "update t set total = 10 where g is null and h = 'a'; \
             delete from t where g = 1 and h is null; \
             update t set total = 30 where g is null and h is null; \
             update t set total = 40 where g = 2",
        )
        .await
        .expect("write");
        let keys: BTreeMap<String, KeyMutation> = before
            .iter()
            .map(|(key, prior)| {
                (
                    key.clone(),
                    KeyMutation {
                        prior_image: Some(prior.clone()),
                        hop_gen: 0,
                        src_changed: None,
                        origin_lsn: None,
                    },
                )
            })
            .collect();
        let feed = EndpointFeed {
            key_columns,
            group_key_columns: Vec::new(),
        };
        let got = read_new_images(&txn, "public.t", &columns, &feed, &keys)
            .await
            .expect("re-read");
        let images: Vec<Option<&str>> = before
            .iter()
            .map(|(key, _)| got[key].image.as_deref())
            .collect();
        assert_eq!(
            images,
            vec![
                Some(r#"{"g": null, "h": "a", "total": "10"}"#),
                None,
                Some(r#"{"g": null, "h": null, "total": "30"}"#),
                Some(r#"{"g": "2", "h": "b", "total": "40"}"#),
            ],
        );
    }

    /// Issue #433: a batch of keys that includes NULL grouping components
    /// still re-reads through the target's `UNIQUE NULLS NOT DISTINCT`
    /// index, at a single-column and a composite identity. Before the fix,
    /// one NULL key switched the whole batch's match on that column to `is
    /// not distinct from`, which can't use the index, so the plan was a
    /// nested loop over a sequential scan of the target.
    #[tokio::test]
    async fn read_new_images_probes_the_index_when_a_batch_binds_a_null() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut client = connect(db.dsn()).await;
        client
            .batch_execute(
                "create table single (g int, total int, unique nulls not distinct (g)); \
                 insert into single select g, g from generate_series(1, 200000) g; \
                 insert into single values (null, 0); \
                 analyze single; \
                 create table composite (g int, h text, total int, \
                                         unique nulls not distinct (g, h)); \
                 insert into composite select g, 'k' || g, g from generate_series(1, 200000) g; \
                 insert into composite values (null, 'k1', 0), (5, null, 0); \
                 analyze composite;",
            )
            .await
            .expect("seed large aggregate-style targets");
        let txn = client.transaction().await.expect("begin");
        // Each table's 500-key batch: its NULL-bearing keys plus plain keys
        // spread across the table.
        let cases = [
            ("public.single", "t.g is null or t.g % 397 = 0"),
            (
                "public.composite",
                "t.g is null or t.h is null or t.g % 397 = 0",
            ),
        ];
        for (table, batch) in cases {
            let key_columns = ddl::identity_key_columns(&txn, table)
                .await
                .expect("identity");
            let columns = live_row_columns(&txn, table).await.expect("columns");
            let key_sql = ddl::pk_key_sql_expr(&key_columns, Some("t"));
            let keys: BTreeMap<String, KeyMutation> = txn
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
                .map(|row| {
                    (
                        row.get(0),
                        KeyMutation {
                            prior_image: None,
                            hop_gen: 0,
                            src_changed: None,
                            origin_lsn: None,
                        },
                    )
                })
                .collect();
            let feed = EndpointFeed {
                key_columns,
                group_key_columns: Vec::new(),
            };
            let query = new_images_query(table, &columns, &feed, &keys).expect("query");
            assert_eq!(keys.len(), 500);
            assert_eq!(
                query.arms.len(),
                if table == "public.single" { 2 } else { 3 },
                "{table}: one arm per NULL pattern in the batch"
            );
            let plan: String = txn
                .query(&format!("explain {}", query.sql), &query.params())
                .await
                .expect("explain")
                .into_iter()
                .map(|row| row.get::<_, String>(0))
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                plan.contains("Index") && !plan.contains("Seq Scan"),
                "{table}: every arm should probe the identity index, got:\n{plan}"
            );
        }

        // The pre-#433 shape over the same single-column batch can only plan
        // a sequential scan, so the difference above is real.
        let groups: Vec<Option<String>> = std::iter::once(None)
            .chain((1..500).map(|i| Some((i * 397).to_string())))
            .collect();
        let plan: String = txn
            .query(
                "explain select k.c0, t.total from unnest($1::text[]::int[]) as k(c0) \
                 left join single t on t.g is not distinct from k.c0",
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
            "the pre-#433 shape should not be able to probe the index, got:\n{plan}"
        );
    }

    /// Issue #790: `read_new_images` reads only its batch's rows of a
    /// single-column key's target whose statistics lag its size (analyzed at
    /// 100 rows, then grown to 400k with autovacuum off). Left to the join
    /// alone, the planner hashed a 5,000-key batch against a sequential scan
    /// of the target; the arm's `= any` restriction caps the target's side
    /// at the batch. The statement is explained the way [`read_new_images`]
    /// runs it, under `ENTRY_PLAN_SETTINGS`: with the bound alone,
    /// PostgreSQL 16 still scanned the target and filtered it (CI).
    ///
    /// A composite identity is left unrestricted, and with fresh statistics
    /// it must not be matched by comparing every row with every key: here an
    /// aggregate's nullable four-column identity at 1M rows, with
    /// `NULL`-bearing keys (so one arm per pattern). One `= any` per column
    /// made the planner expect a row from the target and loop over every key
    /// for each bounded row, 12.5M comparisons for 5,000 keys (316 ms against
    /// 16 ms).
    ///
    /// Either way every key must come back with its row.
    #[tokio::test]
    async fn read_new_images_reads_only_the_batch_while_target_statistics_lag() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut client = connect(db.dsn()).await;
        client
            .batch_execute(
                "create table single (id int primary key, total int) \
                     with (autovacuum_enabled = false); \
                 create table composite (g int, h text, i int, j text, total int, \
                                         unique nulls not distinct (g, h, i, j)) \
                     with (autovacuum_enabled = false); \
                 insert into single select i, i from generate_series(1, 100) i; \
                 analyze single; \
                 insert into single select i, i from generate_series(101, 400000) i; \
                 insert into composite select i, 'k' || i, i, 'j' || i, i \
                     from generate_series(1, 1000000) i; \
                 insert into composite values (null, 'k1', 1, 'j1', 0), (5, null, 5, 'j5', 0); \
                 analyze composite; \
                 create table pair (g int, h text, total int, primary key (g, h)) \
                     with (autovacuum_enabled = false); \
                 insert into pair select i, 'k' || i, i from generate_series(1, 100) i; \
                 analyze pair; \
                 insert into pair select i, 'k' || i, i from generate_series(101, 1000000) i;",
            )
            .await
            .expect("seed the targets");
        let txn = client.transaction().await.expect("begin");
        let cases = [
            ("public.single", "t.total % 79 = 0", 1, true),
            (
                "public.composite",
                "t.g is null or t.h is null or t.total % 79 = 0",
                3,
                false,
            ),
            ("public.pair", "t.total % 79 = 0", 1, false),
        ];
        for (table, batch, patterns, bounded) in cases {
            let key_columns = ddl::identity_key_columns(&txn, table)
                .await
                .expect("identity");
            let columns = live_row_columns(&txn, table).await.expect("columns");
            let key_sql = ddl::pk_key_sql_expr(&key_columns, Some("t"));
            let keys: BTreeMap<String, KeyMutation> = txn
                .query(
                    &format!(
                        "select {key_sql} from {table} t where {batch} \
                         order by t.total limit 5000"
                    ),
                    &[],
                )
                .await
                .expect("keys")
                .into_iter()
                .map(|row| {
                    (
                        row.get(0),
                        KeyMutation {
                            prior_image: None,
                            hop_gen: 0,
                            src_changed: None,
                            origin_lsn: None,
                        },
                    )
                })
                .collect();
            assert_eq!(keys.len(), 5000);
            let feed = EndpointFeed {
                key_columns,
                group_key_columns: Vec::new(),
            };
            let query = new_images_query(table, &columns, &feed, &keys).expect("query");
            assert_eq!(
                query.arms.len(),
                patterns,
                "{table}: one arm per NULL pattern"
            );
            assert_eq!(
                query.sql.contains("= any("),
                bounded,
                "{table}: only a single-column key is bounded:\n{}",
                query.sql
            );
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
            let target = &table["public.".len()..];
            let scans: Vec<&str> = plan
                .lines()
                .filter(|line| line.contains(&format!(" on {target} ")))
                .collect();
            assert!(
                !scans.is_empty(),
                "{table}: no scan of the target in:\n{plan}"
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
                    "{table}: the target must be read through the batch's keys, got:\n{plan}"
                );
            }
            let filtered: u64 = plan
                .lines()
                .filter_map(|line| line.split("Rows Removed by Join Filter: ").nth(1))
                .map(|n| n.trim().parse::<u64>().expect("a row count"))
                .sum();
            assert!(
                filtered < keys.len() as u64,
                "{table}: the target must be matched to the keys without comparing \
                 every row with every key, got:\n{plan}"
            );
            // The estimates lag with the statistics, so the scans' actual
            // rows say what was read (#791).
            let read = testkit::plan::rows_read(&plan, target);
            assert!(
                read <= 2 * keys.len() as u64,
                "{table}: the target must be read for the batch's keys alone, \
                 {read} rows read, got:\n{plan}"
            );
            let before = seq_scans_in_txn(&txn, table).await;
            let got = read_new_images(&txn, table, &columns, &feed, &keys)
                .await
                .expect("re-read");
            assert_eq!(
                seq_scans_in_txn(&txn, table).await,
                before,
                "{table}: read_new_images must not scan the target, as its plan above doesn't"
            );
            let missing: Vec<&String> = keys
                .keys()
                .filter(|key| got.get(*key).is_none_or(|new| new.image.is_none()))
                .collect();
            assert!(
                missing.is_empty(),
                "{table}: every key finds its own row, missing {missing:?}"
            );
        }
    }

    /// How many sequential scans of `table` this transaction has started so
    /// far (`pg_stat_xact_user_tables` counts the open transaction's own).
    /// The plan test explains its statement through `query_by_entry_key`;
    /// this checks that [`read_new_images`] runs it that way too, which
    /// PostgreSQL 16 would otherwise plan as a scan of a stale target (#790).
    async fn seq_scans_in_txn(txn: &Transaction<'_>, table: &str) -> i64 {
        txn.query_one(
            "select seq_scan from pg_stat_xact_user_tables where relid = $1::text::regclass",
            &[&table],
        )
        .await
        .expect("the transaction's scan count")
        .get(0)
    }
}
