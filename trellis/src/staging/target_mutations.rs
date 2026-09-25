//! The one seam every write to a Trellis-owned target table goes through
//! (issue #315).
//!
//! # Why a seam, not a publication
//!
//! A target table is never a member of this instance's CDC publication (see
//! `defs::catalog::publication_tables`). A definition that reads another
//! definition's target (a chained hop) learns about that target's changes
//! only from the rows the writer stages here, inside the same transaction as
//! the write itself: image-less `Recompute` rows, or CDC-shaped rows for a
//! relationship endpoint (see the last section). That gives three properties
//! CDC could not:
//!
//! - **The key is correct by construction.** Each staged key is the target's
//!   own row identity (`ddl::pk_key_sql_expr`, or the aggregate
//!   `derive_group_key` text that matches it), produced by the code that
//!   wrote the row. Nothing reconstructs a NULL-safe group key from raw
//!   `pgoutput` bytes, which `intake::extract_key` cannot do correctly for an
//!   aggregate target.
//! - **No replica-identity cost.** A target needs no `REPLICA IDENTITY FULL`,
//!   so its updates add no old-row images to WAL and no pressure on the slot.
//! - **Crash safety.** The staged rows commit or roll back with the write, so
//!   there is no window where a target changed but its downstream signal was
//!   lost, and no window where the signal exists without the change.
//!
//! It also closes the double-apply class #312 patched: a target change now
//! reaches the ring exactly once, not once from the apply and again from CDC.
//!
//! # Why it is structural
//!
//! Every target writer (`apply::apply_target`, `apply_aggregate::apply_aggregate_target`,
//! the truncate clears (`apply::clear_target`), `quarantine::recompute_column`,
//! `defs::backfill::backfill_altered_columns`, and a rebuild's orphan delete,
//! `intake::resume_orphans`) takes a `&mut TargetMutations`
//! and reports each physically-changed key into it. None of them returns the
//! changed keys to its caller, so a caller cannot forget to propagate them:
//! the only way a changed key leaves a writer is through [`TargetMutations::record`],
//! and the only way a [`TargetMutations`] leaves a transaction is through
//! [`TargetMutations::into_staged`] (or [`TargetMutations::flush`], which
//! calls it). A new writer has to take this parameter to compile against the
//! shared helpers, which is the point.
//!
//! The only target writes that bypass the seam are a definition's own
//! initial build (`defs::backfill::backfill_definition` and the chunk queue),
//! which run while that definition is not yet applying
//! (`TransformStatus::is_applying`). Nothing can read a target in that state:
//! `defs::catalog::create_definition_inner` refuses a definition whose source
//! is a target still being built (`CatalogError::TransformNotLive`),
//! `create_relationship` refuses such a target as an endpoint the same way
//! (#403), and a target whose build finishes with readers already attached (a
//! resumed upstream) parks a catch-up marker for itself so those readers
//! re-derive from its rebuilt state (and report `catching_up` until then).
//! A chunk a worker held across a resume can't write once the rebuild has
//! made the target applying again: its claim fences every write it makes
//! (`defs::chunk_queue::ClaimFence`, #434).
//!
//! The seam only stages for readers that are already applying when the
//! writer checks, so a *new* reader parks a catch-up on its source target
//! once it starts applying, whichever way it was built (`defs::catalog::install_definition`,
//! `create_definition_inner`, `complete_direct_backfill`, or
//! `intake::publication`'s deferred-backfill flip). A write that raced the
//! reader's build reaches it through that catch-up: its fence is captured
//! after the reader is visibly applying, so it waits out every writer that
//! checked before then.
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
//! created it). A downstream aggregate grouped by a non-key column of this
//! target needs it. When an upstream write moves a row from group A to group
//! B, the live re-read only names B, so without the prior image group A would
//! keep the row's stale contribution. With it, the aggregate re-derives both
//! A and B (see `apply_aggregate::accumulate_changes`). A deleted key's prior
//! image is how the aggregate finds the group it left.
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
//! the same WAL space as CDC's commit LSNs, which is what lets the seam stand
//! in as a CDC-shaped feed for a target (steps 2 and 3, #402/#403): an
//! endpoint target's CDC-shaped seam rows carry it as their `lsn` (see the
//! last section).
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
//! read by another transaction means the writer committed after that read",
//! and on per-key order, but never on "a token at or below X means the writer
//! had committed by X": the gap to the commit can be long (a rebuild's orphan
//! delete, `intake::resume_orphans`, waits on intake before committing). It
//! is never an `origin_lsn` either: a seam row's
//! origin stays the conservative "unknown" `await_converged` already gates on
//! (see `staging::converge::converged_through`).
//!
//! It is read only when this transaction stages something (some key of a
//! target an applying definition reads, or that the seam feeds as an
//! endpoint), so a terminal target pays no extra round trip.
//!
//! # Standing in for a relationship endpoint's CDC (issues #402, #403)
//!
//! A relationship's settled parent projection, its reverse deltas and a
//! from-side's `group_key` (issues #129-#136) are driven by image-bearing,
//! LSN-ordered changes. A plain source endpoint gets those from CDC. A
//! target that is a relationship endpoint gets them from the seam alone
//! (#375's direction 1): endpoint targets are unpublished like every other
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
//! - **`lsn`** is the write token, so the fold's first/last image rules and
//!   #321's `min_image_lsn` order one key's seam rows by the order their
//!   writers committed.
//! - **`group_key`** is the union of the target's outbound relationships'
//!   `from_col` values across both images, the same rule intake applies to
//!   a decoded change (`intake::touched_group_key`).
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
//! which is why the seam only took this over once endpoint targets left the
//! publication (#403). The one place both can still exist is the upgrade
//! that unpublished them: an endpoint target's CDC already in the slot
//! before intake's `ALTER PUBLICATION ... DROP TABLE` still arrives. A write
//! staged by the previous binary reached the ring as a `Recompute` plus that
//! CDC, which #321's recompute horizon absorbs for an aggregate reader
//! (`aggregate_recompute_race_windows.rs`'s chained W1 pins it). A write this
//! binary made before the drop committed would reach it as two deltas;
//! pre-release, that window is accepted rather than migrated.
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
//! projection already accumulates before any consumer publishes the table,
//! and narrower than the published-endpoint design it replaces, where every
//! write between the relationship's commit and intake's `ADD TABLE` was
//! missed the same way.

use std::collections::{BTreeMap, HashMap};
use std::time::SystemTime;

use tokio_postgres::Transaction;
use tokio_postgres::types::{PgLsn, ToSql};

use super::append::{self, CdcOp, StagedChange};
use super::apply::{
    ApplyError, MAX_HOP_GEN, earliest_src_changed, live_row_columns, pk_keyset_col,
    row_as_text_jsonb_sql,
};
use super::fold::earliest_origin;
use crate::defs::catalog;
use crate::defs::ddl::{self, PrimaryKeyColumn};
use crate::defs::model::TransformStatus;
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
    // Quoted: `identity_key_columns` resolves its argument with
    // `to_regclass`, which would case-fold a bare mixed-case name.
    let key_columns =
        ddl::identity_key_columns(txn, &ddl::qualified_target_table_ident(target)).await?;
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
    /// Test-only: `Some(has_readers)` treats every target as read (or
    /// unread) without asking the catalog, for unit tests that drive a
    /// writer against a bare database with no Trellis schema in it.
    #[cfg(test)]
    assume_readers: Option<bool>,
}

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
            // Mirrors `catalog::dependents_of`'s own status filter: a reader
            // apply doesn't maintain yet is excluded from applies anyway, and
            // catches up from its own backfill or catch-up marker.
            let direct_readers: bool = txn
                .query_one(
                    "select exists (select 1 from transform_definitions \
                     where source_table = $1 and status = any($2))",
                    &[&target, &TransformStatus::applying()],
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

    /// Every key recorded for `target`, in key order — for tests.
    #[cfg(test)]
    pub(crate) fn recorded_keys(&self, target: &str) -> Vec<String> {
        self.touched
            .get(target)
            .map(|keys| keys.keys().cloned().collect())
            .unwrap_or_default()
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
    /// drain's Phase 3, `quarantine::recompute_column` and
    /// `defs::backfill::backfill_altered_columns` just before commit, and
    /// `intake::resume_orphans` at the start of a discharge that then writes
    /// the ring and catalog (no target) and may wait on intake before it
    /// commits.
    pub(crate) async fn into_staged(
        mut self,
        txn: &Transaction<'_>,
    ) -> Result<Propagation, ApplyError> {
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
            let endpoint_feed = info.endpoint_feed.clone();
            let image_columns = info.image_columns.clone();
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
            let mut new_images = match &endpoint_feed {
                Some(feed) => read_new_images(txn, &target, &image_columns, feed, &keys).await?,
                None => HashMap::new(),
            };
            for (key, m) in keys {
                let next_hop = m.hop_gen + 1;
                if next_hop > MAX_HOP_GEN {
                    propagation.hop_bound_tables.push(target.clone());
                    propagation.worst_hop_gen = propagation.worst_hop_gen.max(next_hop);
                    continue;
                }
                let Some(new) = new_images.remove(&key) else {
                    propagation.changes.push(StagedChange::Recompute {
                        src_table: target.clone(),
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
                    src_table: target.clone(),
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
        }
        Ok(propagation)
    }

    /// [`Self::into_staged`] plus the append, for a writer outside the drain
    /// path (which folds its own hop-bound check into a wider one).
    pub async fn flush(self, txn: &Transaction<'_>) -> Result<(), ApplyError> {
        let mut propagation = self.into_staged(txn).await?;
        if !propagation.hop_bound_tables.is_empty() {
            propagation.hop_bound_tables.sort();
            propagation.hop_bound_tables.dedup();
            return Err(ApplyError::HopBoundExceeded {
                hop_gen: propagation.worst_hop_gen,
                tables: propagation.hop_bound_tables,
            });
        }
        append::append(txn, &propagation.changes).await?;
        Ok(())
    }
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
    let rows = txn.query(&query.sql, &query.params()).await?;
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
            for (i, (column, &null)) in pk.iter().zip(pattern).enumerate() {
                let col = quote_ident(&column.name);
                if null {
                    matched.push(format!("t.{col} is null"));
                } else {
                    arrays.push(format!("{}::text[]::{}[]", param(), column.data_type));
                    aliases.push(pk_keyset_col(i));
                    matched.push(format!("t.{col} = k.{}", pk_keyset_col(i)));
                }
            }
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
/// the module doc's "The write token". The same device #321's recompute
/// horizon uses.
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
}
