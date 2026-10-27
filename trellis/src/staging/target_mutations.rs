//! The one seam every write to a Trellis-owned target table goes through.
//!
//! # Why a seam, not capture
//!
//! A target table is never captured by this instance's triggers (see
//! `defs::catalog::tables_to_capture`). A definition that reads another
//! definition's target (a chained hop) learns about that target's changes
//! only from the rows the writer stages here, inside the same transaction as
//! the write itself: image-less `Recompute` rows, or CDC-shaped rows for a
//! target whose readers need exact images (see "The image feed"). That gives
//! properties
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
//! It also closes the double-apply class: a target change
//! reaches the ring exactly once, not once from the apply and again from
//! capture.
//!
//! # Why it is structural
//!
//! Every target writer (`apply::apply_target`, `group_basis::apply_group_target`,
//! the truncate clears (`apply::clear_target`), the Re-derive build's chunks
//! (`staging::build`), a rebuild's sweep) takes a
//! `&mut TargetMutations` and reports each physically-changed key into it. None of them returns the
//! changed keys to its caller, so a caller cannot forget to propagate them:
//! the only way a changed key leaves a writer is through [`TargetMutations::record`],
//! and the only way a [`TargetMutations`] leaves a transaction is through
//! [`TargetMutations::into_staged`] (or [`TargetMutations::flush`], which
//! calls it). A new writer has to take this parameter to compile against the
//! shared helpers, which is the point.
//!
//! A group-basis build that starts from hidden state a `RESUME` emptied
//! (`staging::group_basis`, `staging::join`) writes through the seam too, but
//! the prior images it and the pages meanwhile send can match no row the
//! readers hold, so when it finishes with readers already attached it
//! rebuilds them (`intake::catch_up::park_target_catchup_if_read`). They read
//! `backfilling` until then, so `live` holds along a chain. A chunk a worker
//! held across a resume can't write once the rebuild has made the target
//! applying again: its claim fences every write it makes
//! (`defs::chunk_queue::ClaimFence`).
//!
//! The seam only stages for readers that are already applying when the
//! writer checks, so a *new* reader built over a target waits on a fence
//! until the writers that checked before it applied have ended.
//!
//! A reader built by the Re-derive build waits
//! on a fence, a transaction id taken after the build's start
//! commits, until every transaction with a lower id has ended
//! (`staging::build`'s "Seam-fed sources"). That covers a writer
//! that checked before the start only if the writer had its id when it
//! checked, so the check takes the transaction's id first, in a statement
//! of its own, so that the check's snapshot is taken after it. A writer that
//! checks before its first write would otherwise take its id after the
//! fence, and the build would read the target before the writer's write
//! commits.
//!
//! # The image feed
//!
//! Some readers of a target need exact, ordered images of its changes, as
//! capture gives a source:
//!
//! - an applying aggregate on the group basis (`super::group_basis`), which
//!   applies `f(new) - f(old)` per change: an image-less `Recompute` names a
//!   row's new group but not the one it left (ADR-0002, "Multi-hop");
//! - an applying `JOIN` transform with the target on either side.
//!
//! A `JOIN`
//! side consumes the images as it consumes captured ones (`super::join`).
//!
//! A target with any of them ([`TargetInfo::image_feed`]) stages each
//! changed key as a [`StagedChange::Cdc`] row rather than a `Recompute`:
//!
//! - **`old_image`** is the key's prior image (above), and **`new_image`**
//!   is the row as this transaction left it, re-read by key in
//!   [`TargetMutations::into_staged`] after every write. A re-read, rather
//!   than each writer's own `RETURNING`, so that no writer can hand in a
//!   wrong or missing new image: every recorded key is either still
//!   row-locked by this transaction or gone. The op follows from which
//!   images exist; a key created and deleted in the same transaction
//!   changed nothing any consumer saw and stages nothing. An aggregate
//!   group that has no visible row is absent from both, so a reader's sums
//!   telescope across its writes.
//! - **`lsn`** is the write token, so one key's seam rows order by the order
//!   their writers committed, and its images chain: each row's old image is
//!   the previous row's new image.
//! - **`row_txid`** is the writer's transaction, as for any ring row, which
//!   orders the change against a group-basis reader's bases.
//! - **`origin_lsn`** stays `None`, as for a `Recompute` (see "The write
//!   token").
//!
//! Every target without such a reader keeps the image-less `Recompute`, and
//! a terminal target stages nothing. An aggregate target qualifies like any
//! other: its NULL-able grouping key is re-read NULL-safely.
//!
//! **One feed per target.** Every reader of an image-fed target consumes the
//! same rows: a 1-1 or ledger reader applies a delta from the images instead
//! of re-reading. A seam row and a CDC row for the same write would be two
//! deltas, applied twice when they land in different batches. Targets are
//! not trigger-captured.
//!
//! **A target gaining a reader that needs images.** [`TargetInfo`] is
//! resolved once per transaction, so a writer that resolved it before the
//! reader existed stages its `Recompute` (or nothing, with no reader), not a
//! CDC-shaped row.
//!
//! A group-basis or `JOIN` reader is built by the Re-derive build, whose plan
//! waits on its fence (`staging::build`'s "Seam-fed sources") until every
//! such writer has ended, so it reads their writes. The group basis' build
//! runs after that fence, in the plan job.

use std::collections::{BTreeMap, HashMap};
use std::time::SystemTime;

use tokio_postgres::Transaction;
use tokio_postgres::types::{PgLsn, ToSql};

use super::append::{self, CdcOp, StagedChange};
use super::apply::{
    ApplyError, MAX_HOP_GEN, bounds_keyset_by_array, earliest_src_changed, live_row_columns,
    pk_keyset_col, row_as_text_jsonb_sql,
};
use super::fold::earliest_origin;
use crate::defs::ddl::{self, PrimaryKeyColumn};
use crate::pool::quote_ident;

/// One changed key's accumulated state — see [`TargetMutations::record`].
#[derive(Debug, Clone)]
struct KeyMutation {
    prior_image: Option<String>,
    hop_gen: i32,
    src_changed: Option<SystemTime>,
    /// The earliest source commit behind the key's writes, `None` if any is
    /// unknown: see [`StagedChange::Recompute::origin_lsn`].
    origin_lsn: Option<PgLsn>,
}

/// What [`TargetMutations`] knows about one target, resolved once per
/// accumulator (so once per Phase 3 transaction).
#[derive(Debug, Clone)]
struct TargetInfo {
    /// Whether anything consumes this target's changes: an applying definition
    /// reads it (as its source or a `JOIN`'s joined table).
    has_readers: bool,
    /// The target's live column list, only resolved when `has_readers`.
    image_columns: Vec<String>,
    /// `Some` when the target's feed carries exact images, so its rows are
    /// staged CDC-shaped (see the module doc's "The image feed").
    image_feed: Option<ImageFeed>,
}

/// What staging CDC-shaped rows for a target needs beyond its images.
#[derive(Debug, Clone)]
struct ImageFeed {
    /// The target's row identity, in declared order: what every writer's
    /// recorded key encodes (`ddl::pk_key_sql_expr`), used to re-read each
    /// key's new image.
    key_columns: Vec<PrimaryKeyColumn>,
}

/// Whether an applying definition that needs exact images reads `$1` (see
/// the module doc's "The image feed"): a `JOIN` transform with the table on
/// either side, or an aggregate on the group basis, which is the one kind of
/// definition with a hidden table named by its id in the catalog's schema
/// (`super::group_basis::hidden_table_name`).
fn image_readers_sql() -> String {
    format!(
        "select exists (select 1 from transform_definitions d \
             where (d.source_table = $1 or d.join_table = $1) and {} \
               and (d.join_table is not null or exists ( \
                   select 1 from pg_catalog.pg_class c \
                   where c.relname = {} \
                     and c.relnamespace = (select relnamespace from pg_catalog.pg_class \
                                           where oid = 'transform_definitions'::regclass))))",
        crate::defs::model::applying_sql("d"),
        super::group_basis::hidden_table_name_sql("d.id"),
    )
}

/// Resolves the seam's [`ImageFeed`] for `target`, or `None` when its feed
/// stays image-less: no applying definition that needs images reads it
/// (`image_readers`).
///
/// An aggregate target qualifies like any other: its row identity is its
/// `UNIQUE NULLS NOT DISTINCT` grouping columns, which [`read_new_images`]
/// matches NULL-safely.
async fn resolve_image_feed(
    txn: &Transaction<'_>,
    target: &str,
    image_readers: bool,
) -> Result<Option<ImageFeed>, ApplyError> {
    if !image_readers {
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
    Ok(Some(ImageFeed { key_columns }))
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
                    image_feed: None,
                });
        }
        if !self.targets.contains_key(target) {
            // The transaction's id before the first check, in a statement
            // of its own: see the module doc on a reader's
            // Re-derive build. A no-op once it has one.
            if self.targets.is_empty() {
                txn.execute("select pg_current_xact_id()", &[]).await?;
            }
            // Mirrors `catalog::dependents_of`'s own status filter: a reader
            // apply doesn't maintain yet is excluded from applies anyway, and
            // its build reads the target after this commit.
            let readers = txn
                .query_one(
                    &format!(
                        "select exists (select 1 from transform_definitions \
                         where (source_table = $1 or join_table = $1) and {}), ({})",
                        crate::defs::model::APPLYING_SQL,
                        image_readers_sql(),
                    ),
                    &[&target],
                )
                .await?;
            let (direct_readers, image_readers): (bool, bool) = (readers.get(0), readers.get(1));
            let image_feed = resolve_image_feed(txn, target, image_readers).await?;
            let has_readers = direct_readers || image_feed.is_some();
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
                    image_feed,
                },
            );
        }
        Ok(&self.targets[target])
    }

    /// The SQL expression (over `alias`) that renders one of `target`'s rows
    /// as the prior-image text a writer should capture, or `None` when no
    /// applying definition reads `target` and so nothing would ever consume it.
    /// Built with `apply::row_as_text_jsonb_sql`, the same rendering every
    /// other image in the ring uses.
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
    /// column rather than from one row alias (`super::group_basis`'s group
    /// writes).
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
    /// downstream `Recompute` row at `hop_gen + 1`, or, for a target on the
    /// image feed, its CDC-shaped row (re-reading each
    /// key's new image; see the module doc). A key whose next hop
    /// would pass [`MAX_HOP_GEN`] is not staged; its target is reported in
    /// [`Propagation::hop_bound_tables`] instead, for the caller to fail the
    /// transaction with [`ApplyError::HopBoundExceeded`] (alongside any other
    /// hop-bounded rows it stages).
    ///
    /// Also reads [`Propagation::write_token`]. The caller must not take any
    /// further row lock on a target after this (see the module doc's "The
    /// write token"). Every caller calls it after its last target write: the
    /// drain's Phase 3 and the Re-derive build's chunks just before commit.
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
            stage_target(txn, &target, &info, keys, token, &mut propagation).await?;
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

/// Stages one target's `keys` into `propagation`, as
/// [`TargetMutations::into_staged`] describes: a `Recompute` per key, or for
/// a target on the image feed a CDC-shaped row from the key's prior image and its
/// re-read new image, carrying `token`. A key past [`MAX_HOP_GEN`] is
/// reported instead.
async fn stage_target(
    txn: &Transaction<'_>,
    target: &str,
    info: &TargetInfo,
    keys: BTreeMap<String, KeyMutation>,
    token: PgLsn,
    propagation: &mut Propagation,
) -> Result<(), ApplyError> {
    let mut new_images = match &info.image_feed {
        Some(feed) => read_new_images(txn, target, &info.image_columns, feed, &keys).await?,
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
        });
    }
    Ok(())
}

/// One key's state as [`read_new_images`] found it after the transaction's
/// last write.
struct NewImage {
    /// The row as this transaction left it, `None` if it deleted the row.
    image: Option<String>,
}

/// Re-reads every key in `keys` from `target` by its row identity, in one
/// statement, after every write this transaction made. A plain read that takes no row lock,
/// so it leaves the write token's "no lock after the token" rule intact.
///
/// Every key comes back: an absent row is a deleted one (this transaction
/// holds the deleted row's lock, so nothing else can have re-created it).
///
/// An aggregate target's key can carry a NULL grouping component (the
/// encoding `ddl::split_pk_key` decodes). See
/// [`new_images_query`] for how those keys are matched without giving up
/// the index. Presence is read off `t.ctid`, since a key column can itself be
/// NULL on a row that exists.
async fn read_new_images(
    txn: &Transaction<'_>,
    target: &str,
    image_columns: &[String],
    feed: &ImageFeed,
    keys: &BTreeMap<String, KeyMutation>,
) -> Result<HashMap<String, NewImage>, ApplyError> {
    if keys.is_empty() {
        return Ok(HashMap::new());
    }
    let query = new_images_query(target, image_columns, feed, keys)?;
    let rows = super::ledger::query_by_entry_key(txn, &query.sql, &query.params()).await?;
    Ok(rows
        .into_iter()
        .map(|row| (row.get(0), NewImage { image: row.get(1) }))
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
            for part in &arm.parts {
                params.push(part);
            }
        }
        params
    }
}

/// Builds [`read_new_images`]' statement: one `union all` arm per pattern of
/// `NULL` identity columns present among `keys`, usually just
/// one. Within an arm a `NULL` column matches with `t.<col> is null` and
/// every other column with `=`, so each arm probes the target's index.
///
/// Matching a whole batch with `is not distinct from` on any column one of
/// its keys binds a NULL for is correct but not
/// indexable: on a 500k-group aggregate target, a 500-key batch with one
/// NULL group took 8.5s against 0.4ms without it.
///
/// A single-column identity's non-`NULL` arm also restricts the column to
/// its array (`t.<col> = any(<array>)`), which the match already implies,
/// so the target's side of whatever join the planner picks is bounded by
/// the batch. A condition in a left join's `on` that names only the
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
    feed: &ImageFeed,
    keys: &'a BTreeMap<String, KeyMutation>,
) -> Result<NewImagesQuery<'a>, ApplyError> {
    let pk = &feed.key_columns;
    // `pattern[i]`: identity column `i` is NULL.
    let mut arms: BTreeMap<Vec<bool>, NullPatternArm<'a>> = BTreeMap::new();
    for key in keys.keys() {
        let decoded = ddl::split_pk_key(pk, target, key)?;
        let pattern: Vec<bool> = decoded.iter().map(Option::is_none).collect();
        let arm = arms
            .entry(pattern)
            .or_insert_with_key(|pattern| NullPatternArm {
                parts: vec![Vec::new(); pattern.iter().filter(|null| !**null).count()],
                ..NullPatternArm::default()
            });
        arm.keys.push(key);
        for (column, part) in arm.parts.iter_mut().zip(decoded.into_iter().flatten()) {
            column.push(part.into_owned());
        }
    }

    let image = row_as_text_jsonb_sql("t", image_columns);
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
            let mut arrays = vec![format!("{}::text[]", param())];
            let mut aliases = vec!["key".to_string()];
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
                             else ({image})::text end \
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

    /// Two writers of one key serialize on
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

    /// `read_new_images` over a composite, mixed-type row identity (a text
    /// part carrying the composite separator, a `date` part) and a
    /// mixed-case column: an updated key comes back with its new image, and
    /// a deleted one with none.
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
        let feed = ImageFeed { key_columns };
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
        let deleted = got
            .remove(&before[1].0)
            .expect("the deleted key comes back");
        assert_eq!(deleted.image, None);
        assert!(got.is_empty());
    }

    /// `read_new_images` over an aggregate target's row identity: `UNIQUE
    /// NULLS NOT DISTINCT` grouping columns whose keys carry NULL components
    /// (the NULL-component key encoding). A plain `=` match would never find a NULL
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
        let feed = ImageFeed { key_columns };
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

    /// A batch of keys that includes NULL grouping components
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
            let feed = ImageFeed { key_columns };
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

        // An `is not distinct from` match over the same single-column batch
        // can only plan a sequential scan, so the difference above is real.
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
            "an `is not distinct from` match should not be able to probe the index, got:\n{plan}"
        );
    }

    /// `read_new_images` reads only its batch's rows of a
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
            let feed = ImageFeed { key_columns };
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
            // What the scans actually read, not what the planner expected
            // them to.
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
    /// PostgreSQL 16 would otherwise plan as a scan of a stale target.
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
