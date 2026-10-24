//! Issues #330, #485 and #436: the target rows a definition must drop because
//! its source no longer has them.
//!
//! A backfill marker's discharge ([`super::markers::run_pending_backfills`])
//! re-reads a table by enumerating its *current* keys as image-less
//! `Recompute`s, and a chunked or direct build copies the rows it reads.
//! Neither can reach a target row the source stopped backing while the
//! definition wasn't applying:
//!
//! - **A resumed definition (ADR-0014, #330)**: rows whose source rows went
//!   away while it was frozen.
//! - **A build's go-live catch-up (#485)**: a definition that is
//!   `backfilling` skips the CDC for its source, so a delete that drains
//!   meanwhile never reaches its target, and the build may have read the row
//!   before it was deleted.
//!
//! Either way it is a 1-1 row whose source row was deleted. Nothing
//! enumerates a key that isn't there. (An aggregate or a `JOIN` is rebuilt by
//! one absolute write that replaces every group, so it is never swept.)
//!
//! So the discharge also deletes those rows directly: every target row with
//! no source row that maps to its key, found by one anti-join per swept
//! target ([`Sweep`]). It is a target write like any other, so it goes
//! through the target-mutation seam (`staging::target_mutations`, issue
//! #315): each deleted key is recorded with its prior image and flushed as a
//! downstream `Recompute` in the discharge's own transaction.
//!
//! # One snapshot for the anti-join and the read (issue #436)
//!
//! The anti-joins are branches of the discharge's read: the one cursor that
//! also enumerates the marker's table
//! (`markers::declare_read`). One statement reads one snapshot, so a
//! row is judged unbacked on exactly the snapshot, call it *S*, whose keys
//! the enumeration stages. The cursor returns each unbacked row's key, and
//! [`Sweep::finish`] deletes by key after the watermark wait, once the read is
//! fetched.
//!
//! Every commit visible to *S* is in both halves of the read. Every commit
//! after *S* reaches the ring's active segment, and the maintenance loop
//! running this pass is the only thing that seals one, so its CDC drains
//! only after this discharge commits, when a swept definition is `live` or
//! `catching_up` and applies it. For a key of a swept target:
//!
//! - **Unbacked at *S***: a 1-1 row is deleted only if the source still has no
//!   row of its key when the delete runs (see "A 1-1 row backed again").
//! - **Backed at *S***: kept, and the source row backing it at *S* is
//!   enumerated, so its `Recompute` re-derives the 1-1 row from live state.
//!
//! Neither case depends on when, between *S* and the drain, the source
//! changed, which is what closes the race #436 was filed for. Before it, the
//! anti-join was a `DELETE` of its own, one per target, before the cursor's
//! `DECLARE`. Its snapshot preceded *S*, by as much as every later target's
//! anti-join (#485), so a row still backed when its anti-join ran, deleted
//! before *S* and re-inserted after it, was neither deleted nor enumerated.
//! Moving that `DELETE` after the watermark wait instead (as #330's spike
//! did) opens the mirror race. Judging on *S* and deleting later has neither.
//!
//! One kind of swept definition is outside that argument: one this discharge
//! dispatches to a chunked or direct build is `backfilling` once it commits,
//! so it skips the CDC after *S*. Its sweep here only removes early what its
//! target held from before the pause, so a reader doesn't see those rows for
//! the length of the build. The build's go-live catch-up sweeps again, and
//! that sweep is exact by the argument above.
//!
//! # Why the delete comes last (issues #503 and #716)
//!
//! The delete runs once the read is fetched, after the watermark wait. A
//! target row the sweep deletes stays locked only from there to the
//! discharge's commit, not across the watermark wait, where a drain of an
//! already-sealed segment that touched it used to wait for the discharge.
//!
//! Such a drain can still touch the rows the sweep deletes (a bulk delete's
//! CDC for a `catching_up` definition, say), so the sweep takes its locks in
//! a drain page's order (ADR-0002 I5): the targets by name, and each class of
//! row in key order.
//! The cursor returns rows in no useful order, and a page of it holds some
//! of every target's, so the read only stages the keys ([`Sweep::push`],
//! [`Sweep::stage`]) and [`Sweep::finish`] deletes them all at the end.
//! Deleting each page of the
//! read as it comes would take a later page's low keys after an earlier
//! page's high ones, and one target's locks after another's, so a drain
//! holding the low key, or the other target, would wait on the discharge
//! while the discharge waits on it.
//!
//! Each delete also re-checks the definition's status (and a 1-1 delete the
//! source, below), which by then is after the watermark wait: a pause that
//! landed since the discharge read it (#331) leaves the target as the pause
//! found it, and its own resume comes back here. The dispatch and the flips
//! that move a swept definition out of the status it was read in come after
//! the fetch.
//!
//! # Batches in key order (issue #884)
//!
//! A resume can leave tens of millions of rows unbacked, more keys than one
//! statement can bind: PostgreSQL caps a message at 1 GB, and a marker
//! whose sweep hit it would fail on every retry. So no statement of
//! [`Sweep::finish`] binds more than [`SWEEP_BATCH`] keys, and it takes
//! each class of row in batches that follow one another in key order. The
//! read stages each page's keys in a temporary table per target as the page
//! arrives (issue #904), so the sweep holds at most a page of them in
//! memory, and [`Sweep::finish`] reads them back through a cursor sorted by
//! the rows' own key columns ([`StagedKeys`]). Each batch's lock is sorted,
//! so the batches together lock in key order, as one statement over every
//! key would. Rust can't sort the keys itself: a column sorts by its type
//! and its collation, and it never holds them all at once.
//!
//! The rows the sweep changes reach the target-mutation seam the same way:
//! after each batch, the seam moves the keys it holds into a temporary table
//! of its own once they reach a batch (issue #924,
//! `staging::target_mutations`, "Spilling a large write"), and the flush at
//! the end of [`Sweep::finish`] stages them from it a batch at a time. So
//! neither the deleted keys nor their prior images pile up in memory, and no
//! re-read of an endpoint target's new images binds more than a batch.
//!
//! # A 1-1 row backed again (issue #883)
//!
//! A change after *S* drains after the commit, but a page can still
//! re-derive a 1-1 row from it before then: a `Recompute` of the key staged
//! before *S* (an earlier discharge's enumeration, say) drains from a sealed
//! segment while the discharge runs, and its read sees a source row inserted
//! after *S*. The page writes the row and stamps its ledger entry with a
//! basis that holds the insert, so the insert's own Apply, drained after the
//! commit, is refused (ADR-0002 I2). Deleting that row by key would lose it
//! for good.
//!
//! So a 1-1 delete re-checks the source, and keeps a row the source backs
//! again. The sweep doesn't hold the row's ledger entry, which every other
//! writer of a 1-1 row holds (`staging::one_to_one_ledger`); the row lock
//! stands in for it. A page that re-derives a 1-1 row writes it under the
//! row's lock, from a read of the source it made before the write (an Apply
//! that drains before the commit is of a change committed before *S*, which
//! the read already saw). The sweep locks its rows before it deletes them
//! (see "Why the delete comes last"), so by the time the delete runs every
//! page that wrote one of them has committed, and the delete, a statement of
//! its own in a `READ COMMITTED` transaction (the discharge sets that level,
//! whatever the server's default), reads a snapshot that holds every source
//! row such a page read. A row written since the lock, which the lock didn't
//! find, the delete sees only if its page has committed, and then it sees
//! that page's source row too. A delete that took the locks itself would
//! read the source on a snapshot from before it queued on them, and could
//! delete a row a page wrote while it waited.
//!
//! Keeping a 1-1 row the source backs again is safe because whatever backed
//! it writes the whole row: the page that re-derived it, or the change that
//! re-inserted it, whose Apply drains after the commit and is newer than the
//! entry's basis. A row the source still doesn't back is deleted, and its
//! entry stays as it was: live, if the change that emptied the key was
//! skipped while the definition was paused or building, or is still pending.
//! Either way the entry's basis predates that change, so the next change to
//! the key applies (I2), as it would to a tombstone.
//!
//! # Which definitions are swept
//!
//! The discharge sweeps two sets, each scoped to the status the discharge
//! read it in (a pause since then leaves the target as the pause found it):
//!
//! - **The `waiting_to_backfill` definitions it dispatches** (a resumed
//!   definition, or a new one whose target is still empty). A `live` sibling
//!   on the same source is not rebuilt, and its target is none of this
//!   pass's business.
//! - **Every `catching_up` definition that reads the marker's table**
//!   (#485): a superset of the ones this discharge flips `live`
//!   (`go_live_caught_up`). A definition that reads several tables is swept
//!   at each of its catch-ups; only the last one flips it.
//!
//! A discharge with nothing to enumerate (only background builds read the
//! table) still declares the read over its sweep's branches alone, and
//! fetches it at once: there is no watermark wait to hold it.
//!
//! ## The go-live sweep (issue #485)
//!
//! A ring rebuild goes `live` in the discharge's own transaction. A chunked
//! or direct build runs on drain threads after the dispatching pass
//! commits, and its definition stays `backfilling`, so its CDC is skipped,
//! until the build finishes. It then moves to `catching_up` (it applies from
//! then on) and parks its go-live catch-ups (#476). A source row deleted
//! while it was `backfilling` is removed by nothing else: its chunk either
//! copied it before the delete or never saw it, its CDC delete was skipped,
//! and the catch-up's re-read only enumerates keys that still exist. So the
//! sweep runs again in the discharge of each go-live catch-up, in its read.
//!
//! # Keeping the anti-join hashable
//!
//! A key column compared with `IS NOT DISTINCT FROM` forces a nested-loop
//! anti-join (each target row rescans the source), which took ~20 s for a
//! 50k-row 1-1 target in #330's spike. `=` lets Postgres hash it. A 1-1
//! target's key is its source's: a `PRIMARY KEY`, all `NOT NULL`, gets `=`.
//! Over a source keyed by a nullable unique index (issue #1141) the key's
//! columns can be `NULL`, so for those keys the anti-join is split by which
//! key columns are `NULL` in the target row: within one such pattern a `NULL` column matches
//! a source row whose key is `IS NULL`, and every other column matches with
//! `=`. That's the same rule [`keyset_match_cols`] applies per
//! batch. There is one branch per pattern present in the target when the
//! sweep is planned, usually one. The patterns are read before *S*, so one
//! more branch catches a row with any other pattern, with `IS NOT DISTINCT
//! FROM`: it scans only the target rows no other branch covers, normally
//! none. The cursor is planned for all its rows (`cursor_tuple_fraction`),
//! not the first 10% a cursor is planned for by default, which could favor
//! a nested loop.

use std::collections::BTreeSet;

use tokio_postgres::Transaction;
use tokio_postgres::types::ToSql;

use crate::defs::ast::KeySpace;
use crate::defs::catalog::CatalogError;
use crate::defs::ddl::{self, PrimaryKeyColumn};
use crate::defs::{TransformStatus, grammar};
use crate::pool::quote_ident;
use crate::staging::apply::bounds_keyset_by_array;
use crate::staging::target_mutations::{SeamStats, TargetMutations};

use super::IntakeError;

/// One target key column and the source-side SQL whose value must equal it
/// for a source row to back the target row.
struct KeyPart {
    /// The target column, quoted.
    target_col: String,
    /// The matching value over the source (aliased `s`), already cast to
    /// the target column's type where the two can differ.
    source_sql: String,
    /// Whether the target column can hold `NULL`.
    nullable: bool,
}

/// How one rebuilt target's rows are matched against its source.
struct Match {
    parts: Vec<KeyPart>,
}

/// What a discharge's sweep deleted ([`Sweep::finish`]).
#[derive(Debug, Default)]
pub(super) struct Swept {
    /// Target rows deleted, across every swept target.
    pub(super) deleted: usize,
    /// The most keys one of the sweep's statements bound (issue #884).
    pub(super) largest_batch: usize,
    /// The most unbacked keys the sweep held in memory at once: at most one
    /// page of the read (issue #904).
    pub(super) most_buffered: usize,
    /// What the target-mutation seam held and bound for the rows the sweep
    /// changed (issue #924): it spills to a temporary table once it holds a
    /// batch of them ([`TargetMutations::spill_over`]).
    pub(super) seam: SeamStats,
}

impl Swept {
    /// Notes a statement that binds `keys` keys.
    fn bound(&mut self, keys: usize) {
        self.largest_batch = self.largest_batch.max(keys);
    }
}

/// The tag the discharge read's enumeration branch selects. Each swept
/// target's branches select its index in [`Sweep`] instead.
pub(super) const ENUMERATION_TAG: i32 = -1;

/// One target a [`Sweep`] covers.
struct SweptTarget {
    /// The definition, and the status the discharge read it in.
    id: i64,
    status: TransformStatus,
    /// Where [`Sweep::finish`] takes this target's locks: a drain page's
    /// target order (ADR-0002 I5), by the definition's bare target name, the
    /// key `staging::apply` orders a page's targets by.
    lock_order: String,
    target: String,
    target_ident: String,
    key_cols: Vec<PrimaryKeyColumn>,
    /// The read's `UNION ALL` branches that find this target's unbacked
    /// rows ([`orphan_branch_sql`]).
    branches: Vec<String>,
    /// The deleted row's encoded key, then its prior image if the seam wants
    /// one.
    returning: String,
    has_image: bool,
    /// Issue #883: for a 1-1 target, its source (quoted): a row is deleted
    /// only if the source still has no row of its key when the delete runs,
    /// since a page may have re-derived it from a row inserted since the
    /// read ([`delete_statement`]).
    source_guard: Option<String>,
    /// The unbacked keys the read has returned for this target in its
    /// current page, by column: `page[j]` holds each key's `j`th column, as
    /// text. [`Sweep::stage`] moves them into [`SweptTarget::staged`] once
    /// the page is fetched (issue #904).
    page: Vec<Vec<Option<String>>>,
    /// Every unbacked key staged so far, in a temporary table of its own.
    staged: StagedKeys,
}

/// The targets one discharge sweeps, and what it has deleted from them so
/// far. See the module doc for the protocol: [`Sweep::add`] each set of
/// definitions, declare the read over [`Sweep::branches`] (with the
/// enumeration, if any, in the same statement), then pass each unbacked key
/// it fetches to [`Sweep::push`], [`Sweep::stage`] each page once it is
/// fetched, and end with [`Sweep::finish`], which deletes them. All of it
/// runs in one transaction, which the staged keys' tables live for.
pub(super) struct Sweep {
    targets: Vec<SweptTarget>,
    mutations: TargetMutations,
    swept: Swept,
    /// The most keys one of [`Sweep::finish`]'s statements binds:
    /// [`SWEEP_BATCH`], or a test's smaller cap.
    batch: usize,
    /// The keys [`Sweep::push`] has buffered since the last
    /// [`Sweep::stage`], across every target.
    buffered: usize,
}

impl Default for Sweep {
    fn default() -> Self {
        Self {
            targets: Vec::new(),
            mutations: TargetMutations::default(),
            swept: Swept::default(),
            batch: SWEEP_BATCH,
            buffered: 0,
        }
    }
}

/// The most keys one statement of [`Sweep::finish`] binds (issue #884), and
/// the most rows one fetch of a [`StagedKeys`] cursor returns.
///
/// A resume can leave tens of millions of rows unbacked, and their keys in
/// one bound array would reach PostgreSQL's 1 GB message limit, so the
/// marker would fail on every retry. 10,000 is the discharge read's own
/// page (`markers::BACKFILL_PAGE_ROWS`), the batch each sweep statement
/// bound before #716 gathered the read's pages into one. The keyed
/// statements' plans are measured at that size: 5,000 to 10,000 keys through
/// the target's index in tens of milliseconds (#790, #819). It is a tenth
/// of a drain page's cap (`staging::apply::DEFAULT_DRAIN_BATCH_CAP`, ADR-0002
/// I8), which a sweep doesn't need: its batches all run in one transaction,
/// so a bigger one saves round trips and nothing else.
pub(super) const SWEEP_BATCH: usize = 10_000;

impl Sweep {
    /// A sweep whose statements bind at most `batch` keys, for a test that
    /// spans several batches without [`SWEEP_BATCH`]'s keys.
    #[cfg(test)]
    fn with_batch(batch: usize) -> Self {
        Self {
            batch,
            ..Self::default()
        }
    }

    /// Adds the target of each of `ids` that is still in `status`.
    ///
    /// `status` is the status the caller read `ids` in: `waiting_to_backfill`
    /// for the definitions a discharge dispatches, `catching_up` for the ones
    /// its catch-up may flip `live`. [`Sweep::finish`] checks it again, so a
    /// pause that landed since (#331) leaves the target as the pause found
    /// it, and its own resume comes back here.
    pub(super) async fn add(
        &mut self,
        txn: &Transaction<'_>,
        ids: &[i64],
        status: TransformStatus,
    ) -> Result<(), IntakeError> {
        if ids.is_empty() {
            return Ok(());
        }
        let defs = txn
            .query(
                "select id, definition_text, target_table, source_table, grammar_version \
                 from transform_definitions where id = any($1) and status = $2 order by id",
                &[&ids, &status.as_str()],
            )
            .await?;
        for row in defs {
            let id: i64 = row.get(0);
            let text: String = row.get(1);
            let target: String = row.get(2);
            let source_table: String = row.get(3);
            // Issue #518: an error, like every other discharge-time reader
            // of the catalog, so the marker fails and backs off (#407).
            let def = grammar::load(&text, row.get(4)).map_err(CatalogError::from)?;
            let target_ident = ddl::qualified_target_table_ident(&target);
            let key_cols = ddl::identity_key_columns(txn, &target).await?;
            if key_cols.is_empty() {
                // Only a target dropped since the catalog read above: every
                // target Trellis creates has a key.
                crate::instance_log::debug!(target = %target, "swept target is gone; nothing to sweep");
                continue;
            }
            let lock_order = def.target.clone();
            let matching = match &def.key_space {
                // A JOIN's rows come from its own build (#1109), not an orphan
                // sweep, and a request or a reinstalled capture on either of
                // its tables rebuilds it (`staging::build::rebuild_candidates`),
                // so no marker reaches here for a live one. A re-read that
                // does (a JOIN not applying) fails the marker so it backs off
                // (#407) instead of taking down the worker.
                KeySpace::Join(_) => {
                    return Err(IntakeError::UnsweepableTarget {
                        target,
                        reason:
                            "a JOIN target is rebuilt, not swept; resume the JOIN transform, or \
                        drop it, to clear this"
                                .to_string(),
                    });
                }
                KeySpace::OneToOne => one_to_one_match(&key_cols),
                // An aggregate is rebuilt by its plan job (`staging::build`),
                // which replaces every group, so there is nothing to sweep.
                KeySpace::Aggregate { .. } => {
                    return Err(IntakeError::UnsweepableTarget {
                        target,
                        reason: "an aggregate target is rebuilt, not swept; resume the \
                                 transform, or drop it, to clear this"
                            .to_string(),
                    });
                }
            };
            let tag = i32::try_from(self.targets.len()).expect("fewer than 2^31 swept targets");
            let branches = orphan_branches(
                txn,
                tag,
                &target_ident,
                &ddl::qualified_source_table(&source_table),
                &matching,
            )
            .await?;
            let image_expr = self.mutations.image_sql(txn, &target, "t").await?;
            let source_guard = matches!(def.key_space, KeySpace::OneToOne)
                .then(|| ddl::qualified_source_table(&source_table));
            let mut returning = ddl::pk_key_sql_expr(&key_cols, Some("t"));
            if let Some(expr) = &image_expr {
                returning.push_str(&format!(", ({expr})::text"));
            }
            let staged = StagedKeys::new(
                &tag.to_string(),
                key_relation(&target_ident, &key_cols),
                key_types(&key_cols),
            );
            self.targets.push(SweptTarget {
                id,
                status,
                lock_order,
                target,
                target_ident,
                key_cols,
                branches,
                returning,
                has_image: image_expr.is_some(),
                source_guard,
                page: Vec::new(),
                staged,
            });
        }
        Ok(())
    }

    /// Whether there is no target to sweep.
    pub(super) fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }

    /// Every target's `UNION ALL` branches for the discharge's read. Each
    /// selects `(tag, NULL, key columns as text)`: the tag names the target
    /// (for [`Sweep::push`]), and the text array holds the unbacked row's
    /// key columns in key order.
    pub(super) fn branches(&self) -> impl Iterator<Item = &str> {
        self.targets
            .iter()
            .flat_map(|t| t.branches.iter().map(String::as_str))
    }

    /// Records an unbacked key the read returned for the target `tag` names
    /// (its key columns as text, in key order), for [`Sweep::stage`] to
    /// stage once its page is fetched and [`Sweep::finish`] to delete.
    pub(super) fn push(&mut self, tag: i32, key: Vec<Option<String>>) {
        let target = usize::try_from(tag)
            .ok()
            .and_then(|i| self.targets.get_mut(i))
            .unwrap_or_else(|| panic!("the discharge read returned an unknown sweep tag {tag}"));
        if target.page.is_empty() {
            target.page = vec![Vec::new(); key.len()];
        }
        for (column, part) in target.page.iter_mut().zip(key) {
            column.push(part);
        }
        self.buffered += 1;
        self.swept.most_buffered = self.swept.most_buffered.max(self.buffered);
    }

    /// Moves every key [`Sweep::push`] has buffered into its target's
    /// temporary table ([`StagedKeys`]), at most a batch per statement, so
    /// the sweep holds no more than one page of the read in memory however
    /// many keys the read finds (issue #904). The discharge calls it once
    /// per page it fetches, in the transaction [`Sweep::finish`] runs in.
    pub(super) async fn stage(&mut self, txn: &Transaction<'_>) -> Result<(), IntakeError> {
        if self.buffered == 0 {
            return Ok(());
        }
        for target in &mut self.targets {
            let page = std::mem::take(&mut target.page);
            target
                .staged
                .add(txn, &page, self.batch, &mut self.swept)
                .await?;
        }
        self.buffered = 0;
        Ok(())
    }

    /// Deletes every key [`Sweep::push`] recorded from its target, if the
    /// target's definition is still in the status [`Sweep::add`] read it
    /// in, flushes each deleted row through the target-mutation seam, and
    /// returns what the sweep deleted.
    ///
    /// The read judged these rows unbacked on its snapshot; a row is deleted
    /// only if the source still doesn't back it (issue #883). See the module
    /// doc.
    ///
    /// It runs once the whole read is fetched, and takes its locks in a
    /// drain page's order (ADR-0002 I5, issues #716 and #884): the targets
    /// by target name, and each target's rows in key order, in batches of at most [`SWEEP_BATCH`] keys that Postgres sorts
    /// ([`StagedKeys`]). See the module doc's "Why the delete comes last".
    pub(super) async fn finish(mut self, txn: &Transaction<'_>) -> Result<Swept, IntakeError> {
        self.stage(txn).await?;
        let Sweep {
            mut targets,
            mut mutations,
            mut swept,
            batch,
            buffered: _,
        } = self;
        targets.sort_by(|a, b| a.lock_order.cmp(&b.lock_order));
        for target in targets {
            if target.staged.is_empty() {
                continue;
            }
            swept.deleted += sweep_rows(txn, target, &mut mutations, batch, &mut swept).await?;
        }
        swept.seam = mutations.flush_counted(txn).await?;
        Ok(swept)
    }
}

/// [`Sweep::finish`]'s work on a 1-1 target: deletes the keys `target`
/// staged if its definition is still in the status the sweep read it in,
/// records each deleted row in `mutations`, and returns how many rows it
/// deleted.
///
/// The keys come back through [`StagedKeys`], so each batch of at most
/// `batch` is locked and then deleted ([`delete_keys`]) in key order, and
/// the batches come in key order too: every lock is taken in the order a
/// drain page's pre-lock takes the same rows.
async fn sweep_rows(
    txn: &Transaction<'_>,
    target: SweptTarget,
    mutations: &mut TargetMutations,
    batch: usize,
    swept: &mut Swept,
) -> Result<usize, IntakeError> {
    let keys = &target.staged;
    keys.open(txn).await?;
    let mut deleted = 0;
    while let Some(arrays) = keys.next(txn, batch).await? {
        swept.bound(arrays[0].len());
        let rows = delete_keys(txn, &target, &arrays).await?;
        for row in &rows {
            let prior = target.has_image.then(|| row.get::<_, String>(1));
            mutations.record(&target.target, row.get(0), prior, 0, None, None);
        }
        mutations.spill_over(txn, batch).await?;
        deleted += rows.len();
    }
    keys.close(txn).await?;
    target.staged.drop_table(txn).await?;
    if deleted > 0 {
        crate::instance_log::info!(
            target = %target.target,
            deleted,
            "dropped target rows the source no longer backs"
        );
    }
    Ok(deleted)
}

/// The cursor [`StagedKeys::open`] declares. Only one staged table is read
/// at a time.
const STAGED_CURSOR: &str = "trellis_sweep_keys";

/// A swept 1-1 target's unbacked keys, staged in a temporary table whose
/// columns (`c0`, `c1`, …, as [`keyset_col`] names them) have the type and
/// collation of the rows' own key columns, so Postgres hands them back
/// distinct and in its own key order, at most a batch at a time
/// (issue #884).
///
/// That order is what lets [`Sweep::finish`] lock a class of rows in
/// batches: each batch's lock is sorted, and each batch's keys all sort
/// after the last one's, so the batches together take the locks in key
/// order, as a drain page takes them (ADR-0002 I5). Rust can't sort them
/// so: a key column's order is its collation's, and a type's own.
///
/// The keys reach the table a page of the read at a time
/// ([`Sweep::stage`], issue #904), at most a batch per statement, and leave
/// it through a cursor, so no statement binds or returns more than a batch
/// and the sweep never holds more than a page of keys in memory. The table
/// is created by the first page that has keys for it (a sweep that finds
/// none creates nothing) and dropped by [`StagedKeys::drop_table`] once its
/// target is swept, or at the transaction's end, whichever comes first.
/// The read, the staging and [`Sweep::finish`] all run in the discharge's
/// one transaction, so `on commit drop` outlives every use of it.
struct StagedKeys {
    /// The table, `pg_temp.trellis_sweep_<name>`.
    table: String,
    /// The `select` (with no `where`) of `c0`, `c1`, … over the rows' own
    /// key columns, which gives each column their type and collation.
    relation: String,
    /// The key columns' types, in key order.
    types: Vec<String>,
    /// Whether the table exists yet.
    created: bool,
}

impl StagedKeys {
    /// Keys staged as `pg_temp.trellis_sweep_<name>`, with the columns
    /// `relation` selects, cast from text as `types`. Nothing is created
    /// until the first key is added. A swept target's table is named by its
    /// tag.
    fn new(name: &str, relation: String, types: Vec<String>) -> Self {
        Self {
            table: format!("pg_temp.trellis_sweep_{name}"),
            relation,
            types,
            created: false,
        }
    }

    /// Whether no key has been staged.
    fn is_empty(&self) -> bool {
        !self.created
    }

    /// Creates the table, empty, if it doesn't exist yet.
    async fn create(&mut self, txn: &Transaction<'_>) -> Result<(), tokio_postgres::Error> {
        if !self.created {
            txn.batch_execute(&format!(
                "create temporary table {} on commit drop as {} limit 0",
                self.table, self.relation
            ))
            .await?;
            self.created = true;
        }
        Ok(())
    }

    /// Adds `arrays`' keys (column `j`'s values in `arrays[j]`, as text),
    /// at most `batch` per statement.
    async fn add(
        &mut self,
        txn: &Transaction<'_>,
        arrays: &[Vec<Option<String>>],
        batch: usize,
        swept: &mut Swept,
    ) -> Result<(), tokio_postgres::Error> {
        let batch = batch.max(1);
        let count = arrays.first().map_or(0, Vec::len);
        if count == 0 {
            return Ok(());
        }
        self.create(txn).await?;
        let sql = format!(
            "insert into {} select * from {} as k",
            self.table,
            typed_keys(&self.types)
        );
        for start in (0..count).step_by(batch) {
            let end = count.min(start + batch);
            let params: Vec<&[Option<String>]> =
                arrays.iter().map(|column| &column[start..end]).collect();
            let params: Vec<&(dyn ToSql + Sync)> =
                params.iter().map(|a| a as &(dyn ToSql + Sync)).collect();
            swept.bound(end - start);
            txn.execute(&sql, &params).await?;
        }
        Ok(())
    }

    /// Opens the cursor [`StagedKeys::next`] reads: the distinct keys, in
    /// key order (ascending, `NULL`s last, as a page's locks order them).
    async fn open(&self, txn: &Transaction<'_>) -> Result<(), tokio_postgres::Error> {
        let cols: Vec<String> = (0..self.types.len()).map(keyset_col).collect();
        let text: Vec<String> = cols.iter().map(|c| format!("k.{c}::text")).collect();
        let order: Vec<String> = cols.iter().map(|c| format!("k.{c}")).collect();
        txn.batch_execute(&format!(
            "declare {STAGED_CURSOR} no scroll cursor for \
             select {} from (select distinct {} from {}) k order by {}",
            text.join(", "),
            cols.join(", "),
            self.table,
            order.join(", "),
        ))
        .await
    }

    /// The next at most `batch` keys, by column as [`StagedKeys::add`]
    /// takes them, or `None` once there are none left.
    async fn next(
        &self,
        txn: &Transaction<'_>,
        batch: usize,
    ) -> Result<Option<Vec<Vec<Option<String>>>>, tokio_postgres::Error> {
        let rows = txn
            .query(
                &format!("fetch forward {} from {STAGED_CURSOR}", batch.max(1)),
                &[],
            )
            .await?;
        if rows.is_empty() {
            return Ok(None);
        }
        let mut arrays = vec![Vec::with_capacity(rows.len()); self.types.len()];
        for row in &rows {
            for (j, column) in arrays.iter_mut().enumerate() {
                column.push(row.get::<_, Option<String>>(j));
            }
        }
        Ok(Some(arrays))
    }

    /// Closes the cursor.
    async fn close(&self, txn: &Transaction<'_>) -> Result<(), tokio_postgres::Error> {
        txn.batch_execute(&format!("close {STAGED_CURSOR}")).await
    }

    /// Drops the table, if it was created.
    async fn drop_table(&self, txn: &Transaction<'_>) -> Result<(), tokio_postgres::Error> {
        if self.created {
            txn.batch_execute(&format!("drop table {}", self.table))
                .await?;
        }
        Ok(())
    }
}

/// A target's key columns as `c0`, `c1`, … (see [`keyset_col`]), over the
/// target itself (`target_ident`): the relation [`StagedKeys::create`]
/// copies their types and collations from.
fn key_relation(target_ident: &str, key_cols: &[PrimaryKeyColumn]) -> String {
    let cols: Vec<String> = key_cols
        .iter()
        .enumerate()
        .map(|(i, col)| format!("t.{} as {}", quote_ident(&col.name), keyset_col(i)))
        .collect();
    format!("select {} from {target_ident} as t", cols.join(", "))
}

/// A target's key columns' types, in key order.
fn key_types(key_cols: &[PrimaryKeyColumn]) -> Vec<String> {
    key_cols.iter().map(|c| c.data_type.clone()).collect()
}

/// A 1-1 target's key is the source's own key, column for column, by name
/// (`ddl::create_target_table` mirrors the source's primary key onto it).
fn one_to_one_match(key_cols: &[PrimaryKeyColumn]) -> Match {
    Match {
        parts: key_cols
            .iter()
            .map(|c| {
                let col = quote_ident(&c.name);
                KeyPart {
                    source_sql: format!("s.{col}"),
                    target_col: col,
                    nullable: c.nullable,
                }
            })
            .collect(),
    }
}

/// The read's `UNION ALL` branches that select `target_ident`'s unbacked
/// rows under `tag`: one per pattern of `NULL` key columns present in
/// the target now (see the module doc), and, for a target with a nullable
/// key column, one more for any other pattern.
///
/// The patterns are read here, before the read's snapshot, so a row with
/// a new pattern can reach the target in between. The last branch catches
/// it with `IS NOT DISTINCT FROM`, which can't hash, but it only scans the
/// target rows no earlier branch covers: normally none.
async fn orphan_branches(
    txn: &Transaction<'_>,
    tag: i32,
    target_ident: &str,
    source_ident: &str,
    matching: &Match,
) -> Result<Vec<String>, IntakeError> {
    let nullable: Vec<usize> = (0..matching.parts.len())
        .filter(|&i| matching.parts[i].nullable)
        .collect();
    if nullable.is_empty() {
        return Ok(vec![orphan_branch_sql(
            tag,
            target_ident,
            source_ident,
            matching,
            &nullable,
            OrphanPattern::Exactly(0),
        )]);
    }
    // Bit `j` set: the `j`th nullable column is `NULL`.
    let mask = null_mask_sql(matching, &nullable);
    let patterns: Vec<i64> = txn
        .query(
            &format!("select distinct ({mask})::bigint from {target_ident} as t"),
            &[],
        )
        .await?
        .into_iter()
        .map(|row| row.get::<_, i64>(0))
        .collect();
    let mut branches: Vec<String> = patterns
        .iter()
        .map(|&pattern| {
            orphan_branch_sql(
                tag,
                target_ident,
                source_ident,
                matching,
                &nullable,
                OrphanPattern::Exactly(pattern as u64),
            )
        })
        .collect();
    branches.push(orphan_branch_sql(
        tag,
        target_ident,
        source_ident,
        matching,
        &nullable,
        OrphanPattern::NoneOf(&patterns),
    ));
    Ok(branches)
}

/// The bitmask of which of the `nullable` key columns are `NULL` in target
/// row `t`.
fn null_mask_sql(matching: &Match, nullable: &[usize]) -> String {
    nullable
        .iter()
        .enumerate()
        .map(|(j, &i)| {
            format!(
                "(case when t.{} is null then {} else 0 end)",
                matching.parts[i].target_col,
                1u64 << j
            )
        })
        .collect::<Vec<_>>()
        .join(" + ")
}

/// Which target rows one [`orphan_branch_sql`] branch covers.
enum OrphanPattern<'a> {
    /// Rows whose nullable key columns are `NULL` exactly where the bits say
    /// (bit `j` for `nullable[j]`): matched with `=` and `IS NULL`, so the
    /// anti-join hashes.
    Exactly(u64),
    /// Rows with any other pattern: matched with `IS NOT DISTINCT FROM`.
    NoneOf(&'a [i64]),
}

/// One branch of the read: `(tag, NULL, key columns as text)` for every
/// row of the target that `pattern` covers and no source row backs.
fn orphan_branch_sql(
    tag: i32,
    target_ident: &str,
    source_ident: &str,
    matching: &Match,
    nullable: &[usize],
    pattern: OrphanPattern<'_>,
) -> String {
    let mut filter = Vec::new();
    let mut matches = Vec::new();
    match pattern {
        OrphanPattern::Exactly(pattern) => {
            let is_null = |i: usize| {
                nullable
                    .iter()
                    .position(|&n| n == i)
                    .is_some_and(|j| pattern & (1 << j) != 0)
            };
            for (i, part) in matching.parts.iter().enumerate() {
                let col = &part.target_col;
                if is_null(i) {
                    filter.push(format!("t.{col} is null and "));
                    matches.push(format!("{} is null", part.source_sql));
                } else {
                    if part.nullable {
                        filter.push(format!("t.{col} is not null and "));
                    }
                    matches.push(format!("{} = t.{col}", part.source_sql));
                }
            }
        }
        OrphanPattern::NoneOf(patterns) => {
            let listed: Vec<String> = patterns.iter().map(i64::to_string).collect();
            filter.push(format!(
                "({})::bigint <> all('{{{}}}'::bigint[]) and ",
                null_mask_sql(matching, nullable),
                listed.join(",")
            ));
            for part in &matching.parts {
                matches.push(format!(
                    "{} is not distinct from t.{}",
                    part.source_sql, part.target_col
                ));
            }
        }
    }
    let key: Vec<String> = matching
        .parts
        .iter()
        .map(|p| format!("t.{}::text", p.target_col))
        .collect();
    let unbacked = format!(
        "not exists (select 1 from {source_ident} as s where {})",
        matches.join(" and "),
    );
    format!(
        "select {tag}::int4, null::text, array[{}]::text[] from {target_ident} as t \
         where {}{unbacked}",
        key.join(", "),
        filter.concat(),
    )
}

/// Runs [`lock_statement`] and then [`delete_statement`] for `arrays`, each
/// under `ENTRY_PLAN_SETTINGS` (through `ledger::query_by_entry_key`), and
/// returns the deleted rows: [`Sweep::finish`]'s statements for one batch
/// of a 1-1 target's keys, as they run, which their plan tests run too.
///
/// The delete alone would lock the rows in its plan's order (the target's
/// heap order, under a hash join), so the lock takes them first, in key
/// order, as a drain page takes the same rows (ADR-0002 I5, issue #716).
///
/// They stay two statements in a `read committed` transaction: a 1-1
/// delete re-checks the source on its own snapshot, which must be taken
/// after the lock has waited out every page writing those rows (issue #883,
/// `a_sweep_rechecks_the_source_on_a_snapshot_taken_after_its_locks`).
async fn delete_keys(
    txn: &Transaction<'_>,
    target: &SweptTarget,
    arrays: &[Vec<Option<String>>],
) -> Result<Vec<tokio_postgres::Row>, tokio_postgres::Error> {
    lock_keys(txn, target, arrays).await?;
    delete_locked(txn, target, arrays).await
}

/// [`delete_keys`]'s lock alone ([`lock_statement`]).
async fn lock_keys(
    txn: &Transaction<'_>,
    target: &SweptTarget,
    arrays: &[Vec<Option<String>>],
) -> Result<(), tokio_postgres::Error> {
    let status = target.status.as_str();
    let mut params: Vec<&(dyn ToSql + Sync)> =
        arrays.iter().map(|a| a as &(dyn ToSql + Sync)).collect();
    params.push(&target.id);
    params.push(&status);
    crate::staging::ledger::query_by_entry_key(txn, &lock_statement(target, arrays), &params)
        .await?;
    Ok(())
}

/// [`delete_keys`]'s delete alone ([`delete_statement`]), of rows
/// [`lock_keys`] has locked.
async fn delete_locked(
    txn: &Transaction<'_>,
    target: &SweptTarget,
    arrays: &[Vec<Option<String>>],
) -> Result<Vec<tokio_postgres::Row>, tokio_postgres::Error> {
    let status = target.status.as_str();
    let mut params: Vec<&(dyn ToSql + Sync)> =
        arrays.iter().map(|a| a as &(dyn ToSql + Sync)).collect();
    params.push(&target.id);
    params.push(&status);
    crate::staging::ledger::query_by_entry_key(txn, &delete_statement(target, arrays), &params)
        .await
}

/// How [`lock_statement`] and [`delete_statement`] find `target`'s rows of
/// the keys in `arrays` (column `j`'s values in `arrays[j]`, bound as
/// `$j+1`), if the definition is still in the status the sweep read it in
/// (`$arity+1`, `$arity+2`): the keyset relation, to alias `k`, and the
/// condition on the target (aliased `t`) and `k`.
struct Keyset {
    relation: String,
    condition: String,
}

/// The [`Keyset`] of `arrays` over `target`.
fn keyset(target: &SweptTarget, arrays: &[Vec<Option<String>>]) -> Keyset {
    let arity = target.key_cols.len();
    let target_cols = target_key_cols(target);
    let patterns = null_patterns(arrays);
    let bound = keyset_bound(&target_cols, &target.key_cols, &patterns);
    Keyset {
        relation: typed_keys(&key_types(&target.key_cols)),
        condition: format!(
            "{}{bound} and exists ( \
                 select 1 from transform_definitions where id = ${} and status = ${} \
             )",
            keyset_match_cols(&target_cols, &patterns),
            arity + 1,
            arity + 2,
        ),
    }
}

/// The keys bound as `$1` … `$n` (`text[]`, one per key column, `n` being
/// `types.len()`) as a relation of `c0`, `c1`, … ([`keyset_col`]), each
/// cast from its text to its column's type in `types`.
fn typed_keys(types: &[String]) -> String {
    let cols: Vec<String> = (0..types.len()).map(keyset_col).collect();
    // Each value is cast back from its text on its own (rather than the
    // array as a whole), so any key type with a text input works, an
    // array-typed one included.
    let typed: Vec<String> = types
        .iter()
        .zip(&cols)
        .map(|(t, k)| format!("{k}::{t} as {k}"))
        .collect();
    let arrays_sql: Vec<String> = (1..=types.len()).map(|i| format!("${i}::text[]")).collect();
    format!(
        "(select {} from unnest({}) as u({}))",
        typed.join(", "),
        arrays_sql.join(", "),
        cols.join(", "),
    )
}

/// `target`'s key columns, qualified by its alias `t`, in key order.
fn target_key_cols(target: &SweptTarget) -> Vec<String> {
    target
        .key_cols
        .iter()
        .map(|c| format!("t.{}", quote_ident(&c.name)))
        .collect()
}

/// [`delete_keys`]'s lock: `target`'s rows of the keys in `arrays` (see
/// [`keyset`]), `for update` in the order a drain page takes the same rows
/// (ADR-0002 I5). A page locks a 1-1 target's rows by `NULL` pattern (`staging::apply`'s pre-lock, issue
/// #1141): the keys with no `NULL` part first, then each pattern's keys, the
/// patterns ordered by which columns are `NULL` (a key whose first nullable
/// column is `NULL` after one whose first nullable column isn't), each
/// ascending by key. So a 1-1 target's nullable key columns lead its order
/// with their `is null` flags. A key with no nullable column has none, and
/// for a single nullable column the flag orders as `NULL`s last does.
fn lock_statement(target: &SweptTarget, arrays: &[Vec<Option<String>>]) -> String {
    let Keyset {
        relation,
        condition,
    } = keyset(target, arrays);
    let mut order: Vec<String> = Vec::new();
    if target.source_guard.is_some() {
        order.extend(
            target
                .key_cols
                .iter()
                .filter(|c| c.nullable)
                .map(|c| format!("t.{} is null", quote_ident(&c.name))),
        );
    }
    order.extend(target_key_cols(target));
    format!(
        "select 1 from {} as t, {relation} as k where {condition} \
         order by {} for update of t",
        target.target_ident,
        order.join(", "),
    )
}

/// [`delete_keys`]'s delete: `target`'s rows of the keys in `arrays` (see
/// [`keyset`]) that are still unbacked when it runs, returning each deleted
/// row's key (and prior image): a row whose source has no row of its key
/// (issue #883). The 1-1 re-check relies on
/// [`lock_statement`] having run first, in its own statement: see the module
/// doc's "A 1-1 row backed again".
fn delete_statement(target: &SweptTarget, arrays: &[Vec<Option<String>>]) -> String {
    let Keyset {
        relation,
        condition,
    } = keyset(target, arrays);
    let mut guard = String::new();
    if let Some(source) = &target.source_guard {
        // A 1-1 target's key is the source's own key, column for column
        // (`one_to_one_match`). The source's side is bounded by the keys as
        // the target's is ([`keyset_bound`]), so a source whose statistics
        // lag isn't scanned whole.
        let source_cols: Vec<String> = target
            .key_cols
            .iter()
            .map(|c| format!("s.{}", quote_ident(&c.name)))
            .collect();
        let patterns = null_patterns(arrays);
        let bound = keyset_bound(&source_cols, &target.key_cols, &patterns);
        if target.key_cols.iter().any(|c| c.nullable) {
            // A source keyed by a nullable unique index (issue #1141): a
            // clause per `NULL` pattern, as [`source_guard`]'s.
            guard.push_str(&source_guard(source, &target.key_cols, &patterns, &bound));
        } else {
            let matches: Vec<String> = source_cols
                .iter()
                .zip(target_key_cols(target))
                .map(|(s, t)| format!("{s} = {t}"))
                .collect();
            guard.push_str(&format!(
                " and not exists (select 1 from {source} as s where {}{bound})",
                matches.join(" and "),
            ));
        }
    }
    format!(
        "delete from {} as t using {relation} as k where {condition}{guard} returning {}",
        target.target_ident, target.returning,
    )
}

/// [`delete_statement`]'s guard for a 1-1 target whose key has a nullable
/// column (issue #1141): it keeps a row (aliased `t`) that `source` (quoted)
/// still has a row of the key of. One clause per `NULL` pattern of the
/// batch's keys, matching the source with `=` on the pattern's non-`NULL`
/// columns and `is null` on its `NULL` ones, so each comparison is an `=`
/// that hashes (`IS NOT DISTINCT FROM` would force a nested loop). `bound` is [`keyset_bound`]'s over the source's
/// columns.
fn source_guard(
    source: &str,
    key_cols: &[PrimaryKeyColumn],
    patterns: &[Vec<bool>],
    bound: &str,
) -> String {
    patterns
        .iter()
        .map(|pattern| {
            let mut other = Vec::with_capacity(key_cols.len());
            let mut matches = Vec::with_capacity(key_cols.len());
            for (c, &null) in key_cols.iter().zip(pattern) {
                let col = quote_ident(&c.name);
                if null {
                    other.push(format!("t.{col} is not null"));
                    matches.push(format!("s.{col} is null"));
                } else {
                    other.push(format!("t.{col} is null"));
                    matches.push(format!("s.{col} = t.{col}"));
                }
            }
            format!(
                " and ({} or not exists (select 1 from {source} as s where {}{bound}))",
                other.join(" or "),
                matches.join(" and "),
            )
        })
        .collect()
}

/// The `k`-alias column name for the `i`th key column in a keyset
/// `unnest(...)`. Named `c0`, `c1`, … so they never
/// collide with the source/target's own (arbitrarily-named) key columns
/// when both appear in one query's join condition.
fn keyset_col(i: usize) -> String {
    format!("c{i}")
}

/// The distinct patterns of `NULL` key columns among a keyset's keys
/// (`arrays[j]` holding column `j`'s value for each key): `pattern[i]` is whether
/// column `i` is `NULL`. Sorted, so the SQL [`keyset_match_cols`] renders
/// from them is deterministic. A batch with no `NULL` key has exactly one pattern,
/// all `false`.
fn null_patterns(arrays: &[Vec<Option<String>>]) -> Vec<Vec<bool>> {
    let group_count = arrays.first().map_or(0, Vec::len);
    (0..group_count)
        .map(|g| arrays.iter().map(|a| a[g].is_none()).collect())
        .collect::<BTreeSet<Vec<bool>>>()
        .into_iter()
        .collect()
}

/// Matches `cols` against the keyset relation `k`: one arm per
/// [`null_patterns`] entry, `or`ed together (and parenthesized) when there
/// is more than one. Within an arm, column `i` is `<col> = k.c<i>`, or
/// `<col> is null and k.c<i> is null` where the pattern has it `NULL`. A
/// key matches only its own pattern's arm, so this is exactly `is not
/// distinct from` on every column, but indexable (issue #445).
fn keyset_match_cols(cols: &[String], patterns: &[Vec<bool>]) -> String {
    let arms: Vec<String> = patterns
        .iter()
        .map(|pattern| {
            cols.iter()
                .zip(pattern)
                .enumerate()
                .map(|(i, (col, &null))| {
                    let k = keyset_col(i);
                    if null {
                        format!("{col} is null and k.{k} is null")
                    } else {
                        format!("{col} = k.{k}")
                    }
                })
                .collect::<Vec<_>>()
                .join(" and ")
        })
        .collect();
    debug_assert!(!arms.is_empty(), "a keyset match needs at least one group");
    match arms.as_slice() {
        [arm] => arm.clone(),
        _ => format!(
            "({})",
            arms.iter()
                .map(|arm| format!("({arm})"))
                .collect::<Vec<_>>()
                .join(" or ")
        ),
    }
}

/// Restricts a single-column key to the keyset's own array (` and <col> =
/// any($1::text[]::<type>[])`), which [`keyset_match_cols`] already implies,
/// so the target's side of whatever join the planner picks is bounded by the
/// keys (#790). At 1M rows analyzed at 100, a 5,000-key delete hashed a
/// sequential scan of the target, 128 ms against 32 ms through its index.
/// The delete also runs under `ENTRY_PLAN_SETTINGS` (no sequential scan):
/// PostgreSQL 16, unlike 17, still scanned a 400k-row target and filtered it
/// by the bound.
///
/// Empty otherwise:
/// - **A composite key** ([`bounds_keyset_by_array`]). One bound per column
///   makes the planner expect a row or two from the target and compare every
///   bounded row with every key, quadratic in the batch even with fresh
///   statistics: 2.6 s against 33 ms for 5,000 keys of a four-column key at
///   3M rows.
/// - **A keyset with a `NULL` key**, which has two [`null_patterns`]. The
///   match is then an `or` of the patterns, which no hash or merge join can
///   take, so the planner already probes the index once per key.
/// - **An array-typed column.** Its keyset values are cast one by one (see
///   [`delete_statement`]), and a bound casts the array whole.
fn keyset_bound(cols: &[String], key_cols: &[PrimaryKeyColumn], patterns: &[Vec<bool>]) -> String {
    let [pattern] = patterns else {
        return String::new();
    };
    if !bounds_keyset_by_array(key_cols) {
        return String::new();
    }
    cols.iter()
        .zip(key_cols)
        .zip(pattern)
        .enumerate()
        .filter(|(_, ((_, c), null))| !**null && !c.data_type.ends_with(']'))
        .map(|(i, ((col, c), _))| {
            format!(" and {col} = any(${}::text[]::{}[])", i + 1, c.data_type)
        })
        .collect()
}

#[cfg(test)]
mod db_tests {
    //! The sweep against a real catalog and real targets: issue #485 runs it
    //! at every build's go-live, so a target with nothing to delete must
    //! cost one hashed anti-join per target, not a probe of the source per
    //! target row.

    use super::*;
    use crate::defs::ValueType;
    use crate::pool::Pool;
    use std::collections::HashMap;
    use tokio_postgres::NoTls;

    /// A same-crate pool plus a raw connection onto `db`.
    async fn connect(db: &testkit::TestDatabase) -> (Pool, tokio_postgres::Client) {
        let config = crate::config::Config::from_dsn(db.dsn().to_string()).expect("valid schema");
        let pool = Pool::new(&config).expect("build a same-crate pool");
        let (raw, connection) = tokio_postgres::connect(db.dsn(), NoTls)
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
        (pool, raw)
    }

    /// The `catching_up` definitions, by id.
    async fn catching_up(raw: &tokio_postgres::Client) -> Vec<i64> {
        raw.query(
            "select id from transform_definitions where status = 'catching_up' order by id",
            &[],
        )
        .await
        .expect("read the built definitions")
        .into_iter()
        .map(|row| row.get(0))
        .collect()
    }

    /// [`sweep`], committed, on a connection of its own and inside `scope`,
    /// so a test can freeze it at a pause point and drive another
    /// transaction against the locks it holds.
    fn spawn_sweep(
        client: tokio_postgres::Client,
        ids: Vec<i64>,
        scope: std::sync::Arc<crate::staging::interleave::PauseScope>,
    ) -> tokio::task::JoinHandle<Result<Swept, IntakeError>> {
        spawn_batched_sweep(client, ids, scope, SWEEP_BATCH)
    }

    /// [`spawn_sweep`] with statements of at most `batch` keys.
    fn spawn_batched_sweep(
        mut client: tokio_postgres::Client,
        ids: Vec<i64>,
        scope: std::sync::Arc<crate::staging::interleave::PauseScope>,
        batch: usize,
    ) -> tokio::task::JoinHandle<Result<Swept, IntakeError>> {
        tokio::spawn(crate::staging::interleave::with_scope(scope, async move {
            let txn = client.transaction().await?;
            let mut sweep = Sweep::with_batch(batch);
            sweep.add(&txn, &ids, TransformStatus::CatchingUp).await?;
            if super::super::markers::declare_read(&txn, None, &sweep).await? {
                super::super::markers::fetch_read(&txn, "", &mut sweep).await?;
            }
            let swept = sweep.finish(&txn).await?;
            txn.commit().await?;
            Ok(swept)
        }))
    }

    /// The first unbacked key the sweep of `ids` reads, declared as
    /// `markers::declare_read` declares it.
    async fn first_unbacked(db: &testkit::TestDatabase, ids: &[i64]) -> Vec<Option<String>> {
        let (_, mut probe) = connect(db).await;
        let txn = probe.transaction().await.expect("begin");
        let mut planned = Sweep::default();
        planned
            .add(&txn, ids, TransformStatus::CatchingUp)
            .await
            .expect("plan the sweep");
        let branches: Vec<&str> = planned.branches().collect();
        txn.batch_execute(&format!(
            "set local cursor_tuple_fraction = 1; \
             declare probe cursor for {}",
            branches.join(" union all ")
        ))
        .await
        .expect("declare the read");
        txn.query_one("fetch forward 1 from probe", &[])
            .await
            .expect("the read's first row")
            .get(2)
    }

    /// Waits until a backend queues on a lock `pid` holds: a forced state,
    /// reached once the other side's statement runs, not a convergence.
    async fn wait_blocked_behind(ctl: &tokio_postgres::Client, pid: i32) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        loop {
            let blocked: bool = ctl
                .query_one(
                    "select exists (select 1 from pg_stat_activity \
                     where $1 = any(pg_blocking_pids(pid)))",
                    &[&pid],
                )
                .await
                .expect("read pg_stat_activity")
                .get(0);
            if blocked {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "nothing queued behind the frozen sweep (pid {pid}) within 60 s"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    /// Issue #716: the sweep locks a target's rows in key order before it
    /// deletes them, over every key its read found, as a drain page's
    /// pre-lock takes them (ADR-0002 I5). With one row more than a batch
    /// (issue #884), it does so in two batches. Every row of a 1-1
    /// copy is unbacked and laid out in descending key order, so the read's
    /// first page holds the highest keys and its second the lowest. A page
    /// locks the lowest, the sweep runs into it, and the page then locks the
    /// highest.
    ///
    /// Deleting per read page, the sweep had deleted the highest row by
    /// then: a deadlock (40P01). Locking every key in order, the sweep
    /// queues on the lowest holding none of the rest, and both commit.
    #[tokio::test]
    async fn a_sweep_locks_rows_in_key_order_before_it_deletes_them() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, raw) = connect(&db).await;
        // One more row than a page of the read (`markers::BACKFILL_PAGE_ROWS`).
        raw.batch_execute(
            "create table public.orders (id bigint primary key, g text, a numeric); \
             insert into public.orders select n, 'a', n from generate_series(1, 10001) n",
        )
        .await
        .expect("seed orders");
        let columns = HashMap::from([
            ("id".to_string(), ValueType::Numeric),
            ("g".to_string(), ValueType::Text),
            ("a".to_string(), ValueType::Numeric),
        ]);
        crate::intake::markers::feed_from_a_test_definition(&raw, "public.orders")
            .await
            .expect("make orders read as another definition's target");
        crate::defs::catalog::install_definition(
            &pool,
            "TRANSFORM orders_copy FROM orders SELECT a AS a",
            &columns,
            "public",
        )
        .await
        .expect("register");
        crate::intake::markers::settle_builds(&pool).await;
        let ids = catching_up(&raw).await;
        assert_eq!(ids.len(), 1, "the build finished");
        raw.batch_execute(
            "delete from public.orders; \
             create temp table copied as select * from public.orders_copy; \
             delete from public.orders_copy; \
             insert into public.orders_copy select * from copied order by id desc; \
             analyze public.orders, public.orders_copy",
        )
        .await
        .expect("unback every row, laid out in descending key order");
        assert_eq!(
            first_unbacked(&db, &ids).await,
            vec![Some("10001".to_string())],
            "the read must return the copy in its heap order"
        );

        let (_, mut pager) = connect(&db).await;
        let page = pager.transaction().await.expect("begin the page");
        page.execute(
            "select 1 from public.orders_copy where id = 1 for update",
            &[],
        )
        .await
        .expect("the page locks the lowest row");
        let page_pid: i32 = page
            .query_one("select pg_backend_pid()", &[])
            .await
            .expect("the page's backend")
            .get(0);
        let (_, sweeper) = connect(&db).await;
        let sweep = spawn_sweep(sweeper, ids, crate::staging::interleave::PauseScope::new());
        let (_, ctl) = connect(&db).await;
        wait_blocked_behind(&ctl, page_pid).await;
        let highest = page
            .execute(
                "select 1 from public.orders_copy where id = 10001 for update",
                &[],
            )
            .await;
        let paged = match highest {
            Ok(_) => page.commit().await,
            Err(e) => Err(e),
        };

        let swept = sweep.await.expect("the sweep task");
        assert!(
            swept.is_ok() && paged.is_ok(),
            "the sweep and the page must not deadlock: sweep {:?}, page {:?}",
            swept.as_ref().err(),
            paged.as_ref().err()
        );
        assert_eq!(swept.expect("swept").deleted, 10001, "every row");
    }

    /// Issue #884: the sweep's batches follow the key column's collation,
    /// not the database's. `items` is keyed by `text collate "C"`, which its
    /// 1-1 copy's key keeps (#769), and the database sorts `en-US`. The copy
    /// loses all 4 rows, in batches of 2: `A`, `B`, then `a`, `b`. A page
    /// locks row `B`, the sweep's first batch queues on it, and the page then
    /// locks row `a`.
    ///
    /// Staging the keys in the database's collation, the sweep's first batch
    /// was `a` and `A`, so it held `a` by the time its second batch (`b`,
    /// `B`) queued on `B`, and the page then queued on `a`: a deadlock
    /// (40P01). In the key's own collation, the sweep holds only `A` while it
    /// waits, and both commit.
    #[tokio::test]
    async fn a_sweep_batches_keys_in_the_key_columns_collation() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, raw) = connect(&db).await;
        raw.batch_execute(
            "create table public.items (k text collate \"C\" primary key, a numeric); \
             insert into public.items values ('A', 1), ('B', 2), ('a', 3), ('b', 4)",
        )
        .await
        .expect("seed items");
        let columns = HashMap::from([
            ("k".to_string(), ValueType::Text),
            ("a".to_string(), ValueType::Numeric),
        ]);
        crate::intake::markers::feed_from_a_test_definition(&raw, "public.items")
            .await
            .expect("make items read as another definition's target");
        crate::defs::catalog::install_definition(
            &pool,
            "TRANSFORM items_copy FROM items SELECT a AS a",
            &columns,
            "public",
        )
        .await
        .expect("register");
        crate::intake::markers::settle_builds(&pool).await;
        let ids = catching_up(&raw).await;
        assert_eq!(ids.len(), 1, "the build finished");
        let row = raw
            .query_one(
                "select (select array_agg(k order by k) from public.items_copy) \
                        = array['A', 'B', 'a', 'b'], \
                        'a' < 'B'",
                &[],
            )
            .await
            .expect("read the collations");
        assert_eq!(
            (row.get::<_, bool>(0), row.get::<_, bool>(1)),
            (true, true),
            "the copy's key sorts in \"C\" and the database's text in en-US"
        );
        raw.batch_execute("delete from public.items")
            .await
            .expect("unback every item");

        let (_, mut pager) = connect(&db).await;
        let page = pager.transaction().await.expect("begin the page");
        page.execute(
            "select 1 from public.items_copy where k = 'B' for update",
            &[],
        )
        .await
        .expect("the page locks row B");
        let page_pid: i32 = page
            .query_one("select pg_backend_pid()", &[])
            .await
            .expect("the page's backend")
            .get(0);
        let (_, sweeper) = connect(&db).await;
        let sweep = spawn_batched_sweep(
            sweeper,
            ids,
            crate::staging::interleave::PauseScope::new(),
            2,
        );
        let (_, ctl) = connect(&db).await;
        wait_blocked_behind(&ctl, page_pid).await;
        let low = page
            .execute(
                "select 1 from public.items_copy where k = 'a' for update",
                &[],
            )
            .await;
        let paged = match low {
            Ok(_) => page.commit().await,
            Err(e) => Err(e),
        };

        let swept = sweep.await.expect("the sweep task");
        assert!(
            swept.is_ok() && paged.is_ok(),
            "the sweep and the page must not deadlock: sweep {:?}, page {:?}",
            swept.as_ref().err(),
            paged.as_ref().err()
        );
        assert_eq!(swept.expect("swept").deleted, 4, "every copy");
    }

    /// Issue #883's setup: `orders_copy`, a `catching_up` 1-1 copy of
    /// `public.orders` (ids 1 to 3), whose orders 1 and 3 are deleted with
    /// no CDC (a delete the definition skipped), so both copies are
    /// unbacked. A `Recompute` of order `id`, staged before the sweep's read
    /// (an earlier discharge's enumeration, say), waits in a sealed segment.
    /// Returns a same-crate pool, a raw connection, the copy's definition
    /// and that segment.
    async fn copy_with_a_pending_recompute(
        db: &testkit::TestDatabase,
        id: i64,
    ) -> (Pool, tokio_postgres::Client, Vec<i64>, i64) {
        let (pool, mut raw) = connect(db).await;
        raw.batch_execute(
            "create table public.orders (id bigint primary key, g text, a numeric); \
             insert into public.orders values (1, 'a', 1), (2, 'a', 2), (3, 'a', 3)",
        )
        .await
        .expect("seed orders");
        let columns = HashMap::from([
            ("id".to_string(), ValueType::Numeric),
            ("g".to_string(), ValueType::Text),
            ("a".to_string(), ValueType::Numeric),
        ]);
        crate::intake::markers::feed_from_a_test_definition(&raw, "public.orders")
            .await
            .expect("make orders read as another definition's target");
        crate::defs::catalog::install_definition(
            &pool,
            "TRANSFORM orders_copy FROM orders SELECT a AS a",
            &columns,
            "public",
        )
        .await
        .expect("register");
        crate::intake::markers::settle_builds(&pool).await;
        let ids = catching_up(&raw).await;
        assert_eq!(ids.len(), 1, "the build finished");
        raw.batch_execute("delete from public.orders where id in (1, 3)")
            .await
            .expect("unback orders 1 and 3");
        let txn = raw.transaction().await.expect("begin");
        crate::staging::append::append(
            &txn,
            &[crate::staging::append::StagedChange::Recompute {
                src_table: "public.orders".to_string(),
                key: id.to_string(),
                hop_gen: 0,
                src_changed: None,
                prior_image: None,
                origin_lsn: None,
            }],
        )
        .await
        .expect("stage the Recompute");
        txn.commit().await.expect("commit the Recompute");
        let seg = seal(&mut raw).await;
        (pool, raw, ids, seg)
    }

    /// Seals the active segment and returns it.
    async fn seal(client: &mut tokio_postgres::Client) -> i64 {
        let outcome = crate::staging::seal::seal_phase1(client)
            .await
            .expect("seal phase 1");
        crate::staging::seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
            .await
            .expect("seal phase 2");
        outcome.sealed_seg_seq
    }

    /// Drains sealed segment `seg` until no page is left: a bounded loop.
    async fn drain(pool: &Pool, seg: i64) -> Result<(), crate::staging::apply::ApplyError> {
        while crate::staging::apply::drain_once(pool, seg, "issue_883", 1, "wake")
            .await?
            .is_some()
        {}
        Ok(())
    }

    /// Re-inserts order `id` (`a = 100`) and stages its CDC in the same
    /// transaction, as a capture trigger does, so the Apply carries the
    /// insert's own transaction id. It lands in the active segment.
    async fn reinsert_order(writer: &mut tokio_postgres::Client, id: i64) {
        let txn = writer.transaction().await.expect("begin");
        txn.execute("insert into public.orders values ($1, 'a', 100)", &[&id])
            .await
            .expect("re-insert the order");
        let lsn: tokio_postgres::types::PgLsn = txn
            .query_one("select pg_current_wal_insert_lsn()", &[])
            .await
            .expect("read the WAL position")
            .get(0);
        crate::staging::append::append(
            &txn,
            &[crate::staging::append::StagedChange::Cdc {
                src_table: "public.orders".to_string(),
                key: id.to_string(),
                op: crate::staging::CdcOp::Insert,
                lsn: Some(lsn),
                old_image: None,
                new_image: Some(format!(r#"{{"id":"{id}","g":"a","a":"100"}}"#)),
                origin_lsn: None,
                src_changed: None,
                hop_gen: 0,
            }],
        )
        .await
        .expect("stage the re-insert's Apply");
        txn.commit().await.expect("commit the re-insert");
    }

    /// The copy's rows, `(id, a)` as text, by id.
    async fn copy_rows(client: &tokio_postgres::Client) -> Vec<(String, String)> {
        client
            .query(
                "select id::text, a::text from public.orders_copy order by id",
                &[],
            )
            .await
            .expect("read orders_copy")
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect()
    }

    /// Issue #883: the sweep doesn't delete a 1-1 row the source backs again
    /// by the time it deletes. The read judges order 1's copy unbacked, and
    /// order 1 is then re-inserted. A page drains the `Recompute` staged
    /// before the read, sees the re-insert, writes the row and stamps its
    /// entry with a basis the insert is visible in, and commits. The sweep
    /// then deletes.
    ///
    /// Deleting by key alone, the sweep removed the row the page wrote, and
    /// the insert's own Apply, drained after the discharge commits, is
    /// refused because the entry's basis already holds it (ADR-0002 I2): the
    /// copy lost order 1 for good. Re-checking the source, the sweep keeps
    /// it and still deletes order 3's.
    #[tokio::test]
    async fn a_sweep_keeps_a_one_to_one_row_a_page_rederived_after_its_read() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, mut raw, ids, pending) = copy_with_a_pending_recompute(&db, 1).await;

        let (_, mut sweeper) = connect(&db).await;
        let txn = sweeper.transaction().await.expect("begin the discharge");
        let mut sweep = Sweep::default();
        sweep
            .add(&txn, &ids, TransformStatus::CatchingUp)
            .await
            .expect("plan the sweep");
        assert!(
            super::super::markers::declare_read(&txn, None, &sweep)
                .await
                .expect("declare the read")
        );
        super::super::markers::fetch_read(&txn, "", &mut sweep)
            .await
            .expect("fetch the read");

        reinsert_order(&mut raw, 1).await;
        drain(&pool, pending).await.expect("drain the Recompute");
        assert_eq!(
            copy_rows(&raw).await,
            vec![
                ("1".to_string(), "100".to_string()),
                ("2".to_string(), "2".to_string()),
                ("3".to_string(), "3".to_string()),
            ],
            "the page re-derives order 1 from the re-insert"
        );

        let swept = sweep.finish(&txn).await.expect("finish the sweep");
        txn.commit().await.expect("commit the discharge");

        let active = seal(&mut raw).await;
        drain(&pool, active).await.expect("drain the re-insert");
        assert_eq!(
            copy_rows(&raw).await,
            vec![
                ("1".to_string(), "100".to_string()),
                ("2".to_string(), "2".to_string()),
            ],
            "the copy matches the source"
        );
        assert_eq!(swept.deleted, 1, "order 3's copy only");
    }

    /// Issue #883, with the page in flight when the sweep deletes: frozen
    /// before its commit, holding order 1's copy that it re-derived from the
    /// re-insert. The sweep's row lock queues on it, and once the page
    /// commits, the delete's snapshot sees the re-insert the page read, so
    /// the sweep keeps the row.
    #[tokio::test]
    async fn a_sweep_keeps_a_one_to_one_row_a_page_in_flight_rederived_after_its_read() {
        use crate::staging::interleave::{PausePoint, PauseScope};
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, mut raw, ids, pending) = copy_with_a_pending_recompute(&db, 1).await;

        let (_, mut sweeper) = connect(&db).await;
        let txn = sweeper.transaction().await.expect("begin the discharge");
        let mut sweep = Sweep::default();
        sweep
            .add(&txn, &ids, TransformStatus::CatchingUp)
            .await
            .expect("plan the sweep");
        assert!(
            super::super::markers::declare_read(&txn, None, &sweep)
                .await
                .expect("declare the read")
        );
        super::super::markers::fetch_read(&txn, "", &mut sweep)
            .await
            .expect("fetch the read");

        reinsert_order(&mut raw, 1).await;
        let (_, ctl) = connect(&db).await;
        ctl.execute("select pg_advisory_lock($1)", &[&PAGE_PAUSE])
            .await
            .expect("hold the pause lock");
        let scope = PauseScope::new();
        let reached = scope.arm(PausePoint::BeforeCommit, "public.orders_copy", PAGE_PAUSE);
        let page = {
            let pool = pool.clone();
            tokio::spawn(crate::staging::interleave::with_scope(scope, async move {
                drain(&pool, pending).await
            }))
        };
        let frozen = reached.await.expect("the page reaches its commit");

        let (swept, ()) = tokio::join!(sweep.finish(&txn), async {
            wait_blocked_behind(&ctl, frozen.backend_pid).await;
            ctl.execute("select pg_advisory_unlock($1)", &[&PAGE_PAUSE])
                .await
                .expect("release the page");
        });
        page.await
            .expect("the page task")
            .expect("drain the Recompute");
        let swept = swept.expect("finish the sweep");
        txn.commit().await.expect("commit the discharge");

        let active = seal(&mut raw).await;
        drain(&pool, active).await.expect("drain the re-insert");
        assert_eq!(
            copy_rows(&raw).await,
            vec![
                ("1".to_string(), "100".to_string()),
                ("2".to_string(), "2".to_string()),
            ],
            "the copy matches the source"
        );
        assert_eq!(swept.deleted, 1, "order 3's copy only");
    }

    /// Issue #883: the 1-1 delete's source re-check holds only because
    /// [`lock_statement`] runs, in a statement of its own, before
    /// [`delete_statement`] takes its snapshot. Another transaction holds
    /// order 1's copy, so the sweep's lock queues on it before it reaches
    /// order 3's. Order 3 is then re-inserted, and a page drains a
    /// `Recompute` of it staged before the read: it writes order 3's copy
    /// from the re-insert and stamps its entry with a basis the insert is
    /// visible in. Then the holder lets go.
    ///
    /// The delete's snapshot is taken only now, so it sees the re-insert and
    /// keeps the row. A delete that took its locks itself (without the
    /// separate lock, or with it folded into the delete) would have taken its
    /// snapshot before the re-insert, queued on order 1, then re-checked
    /// order 3's new version against that old snapshot and deleted it: the
    /// re-insert's Apply is refused, and the copy loses order 3 for good.
    #[tokio::test]
    async fn a_sweep_rechecks_the_source_on_a_snapshot_taken_after_its_locks() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, mut raw, ids, pending) = copy_with_a_pending_recompute(&db, 3).await;

        let (_, mut holder) = connect(&db).await;
        let held = holder.transaction().await.expect("begin the holder");
        held.execute(
            "select 1 from public.orders_copy where id = 1 for update",
            &[],
        )
        .await
        .expect("hold order 1's copy");
        let holder_pid: i32 = held
            .query_one("select pg_backend_pid()", &[])
            .await
            .expect("the holder's backend")
            .get(0);

        let (_, sweeper) = connect(&db).await;
        let sweep = spawn_sweep(sweeper, ids, crate::staging::interleave::PauseScope::new());
        let (_, ctl) = connect(&db).await;
        wait_blocked_behind(&ctl, holder_pid).await;

        reinsert_order(&mut raw, 3).await;
        drain(&pool, pending).await.expect("drain the Recompute");
        held.commit().await.expect("let go of order 1's copy");
        let swept = sweep
            .await
            .expect("the sweep task")
            .expect("the sweep commits");

        let active = seal(&mut raw).await;
        drain(&pool, active).await.expect("drain the re-insert");
        assert_eq!(
            copy_rows(&raw).await,
            vec![
                ("2".to_string(), "2".to_string()),
                ("3".to_string(), "100".to_string()),
            ],
            "the copy matches the source"
        );
        assert_eq!(swept.deleted, 1, "order 1's copy only");
    }

    /// The advisory lock key [`a_sweep_keeps_a_one_to_one_row_a_page_in_flight_rederived_after_its_read`]
    /// holds to freeze its page before the commit.
    const PAGE_PAUSE: i64 = 883;

    /// Issue #518: a definition row whose text doesn't parse is an error the
    /// discharge fails its marker on (and backs off, #407), not a panic that
    /// takes down the maintenance loop.
    #[tokio::test]
    async fn a_sweep_of_an_unparseable_definition_is_an_error() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (_pool, mut raw) = connect(&db).await;
        raw.batch_execute(
            "insert into source_table_versions (source_table, version) values ('public.t', 1)",
        )
        .await
        .expect("seed the source version");
        let id: i64 = raw
            .query_one(
                "insert into transform_definitions \
                 (target_table, source_table, source_version, definition_text, status) \
                 values ('public.d', 'public.t', 1, 'not a transform', 'catching_up') \
                 returning id",
                &[],
            )
            .await
            .expect("seed an unparseable definition")
            .get(0);

        let txn = raw.transaction().await.expect("begin");
        let mut sweep = Sweep::default();
        let err = sweep
            .add(&txn, &[id], TransformStatus::CatchingUp)
            .await
            .expect_err("an unparseable definition fails the sweep");
        assert!(
            matches!(&err, IntakeError::Catalog(e) if matches!(**e, CatalogError::Parse(_))),
            "the parse error is returned, got {err:?}"
        );
    }

    /// A 1-1 target over a source keyed by a nullable unique index has the
    /// same nullable key (issue #1141), and its delete's re-check of the
    /// source (issue #883) is `NULL`-safe: a `NULL`-keyed row the source
    /// still has a row of is kept, of every `NULL` pattern, and only the
    /// unbacked row goes. Its lock takes the rows in a drain page's order.
    #[tokio::test]
    async fn the_one_to_one_delete_keeps_a_null_keyed_row_its_source_backs() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (_pool, mut raw) = connect(&db).await;
        raw.batch_execute(
            "create table public.nk_src (g int, h int, unique nulls not distinct (g, h)); \
             create table public.nk (g int, h int, total int, unique nulls not distinct (g, h)); \
             insert into public.nk_src values (1, null), (null, null), (null, 3); \
             insert into public.nk values (1, null, 0), (null, null, 0), (null, 3, 0), \
                                          (2, 2, 0), (null, 4, 0); \
             insert into source_table_versions (source_table, version) values ('public.nk_src', 1)",
        )
        .await
        .expect("seed the target and its source");
        let table = "public.nk";
        let id: i64 = raw
            .query_one(
                "insert into transform_definitions \
                 (target_table, source_table, source_version, definition_text, status) \
                 values ($1, 'public.nk_src', 1, 'unused', 'catching_up') returning id",
                &[&table],
            )
            .await
            .expect("seed the definition")
            .get(0);
        let key_cols = ddl::identity_key_columns(&raw, table)
            .await
            .expect("identity");
        assert!(key_cols.iter().all(|c| c.nullable));
        let target = SweptTarget {
            id,
            status: TransformStatus::CatchingUp,
            lock_order: table.to_string(),
            target: table.to_string(),
            target_ident: ddl::qualified_target_table_ident(table),
            returning: ddl::pk_key_sql_expr(&key_cols, Some("t")),
            key_cols,
            branches: Vec::new(),
            has_image: false,
            source_guard: Some(ddl::qualified_source_table("public.nk_src")),
            page: Vec::new(),
            staged: StagedKeys::new("unused", String::new(), Vec::new()),
        };
        // Every target row's key: the sweep's anti-join named them all.
        let arrays: Vec<Vec<Option<String>>> = vec![
            vec![Some("1".into()), None, None, Some("2".into()), None],
            vec![
                None,
                None,
                Some("3".into()),
                Some("2".into()),
                Some("4".into()),
            ],
        ];
        // The lock takes the rows in the order a drain page's pre-lock
        // does: the keys with no `NULL` part, then each `NULL` pattern's
        // (`staging::apply`'s `MatchKeys`), not ascending with `NULL`s last,
        // which would take `(1, NULL)` before `(2, 2)` and deadlock against
        // a page holding `(2, 2)`.
        let status = target.status.as_str();
        let mut params: Vec<&(dyn ToSql + Sync)> =
            arrays.iter().map(|a| a as &(dyn ToSql + Sync)).collect();
        params.push(&target.id);
        params.push(&status);
        let locked: Vec<String> = raw
            .query(
                &lock_statement(&target, &arrays).replacen(
                    "select 1 ",
                    "select format('(%s,%s)', t.g, t.h) ",
                    1,
                ),
                &params,
            )
            .await
            .expect("the lock's order")
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        assert_eq!(
            locked,
            ["(2,2)", "(1,)", "(,3)", "(,4)", "(,)"],
            "a page's lock order"
        );
        let txn = raw.transaction().await.expect("begin");
        let mut deleted: Vec<String> = delete_keys(&txn, &target, &arrays)
            .await
            .expect("delete")
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        deleted.sort();
        txn.commit().await.expect("commit");
        assert_eq!(
            deleted,
            [
                ddl::join_pk_key([ddl::NULL_KEY_SENTINEL, "4"]),
                ddl::join_pk_key(["2", "2"]),
            ],
            "only the rows the source has no row of"
        );
    }

    /// Issue #790: [`delete_statement`] reads only its keys' rows of a
    /// single-column key's target whose statistics lag its size: analyzed at
    /// 100 rows, then grown to 400k with autovacuum off. Left to the keyset
    /// join alone, the planner hashed 5,000 keys against a sequential scan of
    /// the target; [`keyset_bound`] caps the target's side at the keys. The
    /// delete is explained the way [`Sweep::finish`] runs it, under
    /// `ENTRY_PLAN_SETTINGS`: with the bound alone, PostgreSQL 16 still
    /// scanned the target and filtered it (CI). [`lock_statement`] reads the
    /// same keyset the same way, and the scan count below covers it. The
    /// single-column target is a 1-1 one, whose delete re-checks its source
    /// (issue #883): the source's statistics lag the same way, and it is read
    /// through the keys too.
    ///
    /// A composite key isn't bounded. With fresh statistics, a four-column
    /// key at 2M rows must not be matched by comparing every target row with
    /// every key, which one bound per column made the planner do (2.6 s
    /// against 33 ms at 3M rows). A batch with `NULL`-bearing keys in more
    /// than one pattern isn't bounded either, and still probes the index per
    /// key while statistics lag. Every key must still be deleted.
    #[tokio::test]
    async fn the_sweep_delete_reads_only_its_keys_while_target_statistics_lag() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (_pool, mut raw) = connect(&db).await;
        raw.batch_execute(
            "create table public.single (id int primary key, total int) \
                 with (autovacuum_enabled = false); \
             create table public.composite (g int, h text, total int, \
                                            unique nulls not distinct (g, h)) \
                 with (autovacuum_enabled = false); \
             create table public.wide (a int, b text, c int, d text, total int, \
                                       unique nulls not distinct (a, b, c, d)) \
                 with (autovacuum_enabled = false); \
             insert into public.wide select i, 'k' || i, i, 'd' || i, i \
                 from generate_series(1, 2000000) i; \
             analyze public.wide; \
             create table public.single_src (id int primary key) \
                 with (autovacuum_enabled = false); \
             create table public.pair (g int, h text, total int, primary key (g, h)) \
                 with (autovacuum_enabled = false); \
             create table public.pair_src (g int, h text, primary key (g, h)) \
                 with (autovacuum_enabled = false); \
             insert into public.pair select i, 'k' || i, i from generate_series(1, 100) i; \
             insert into public.pair_src select i, 'k' || i from generate_series(1, 100) i \
                 where i % 79 <> 0; \
             analyze public.pair; analyze public.pair_src; \
             insert into public.pair select i, 'k' || i, i from generate_series(101, 400000) i; \
             insert into public.pair_src select i, 'k' || i from generate_series(101, 400000) i \
                 where i % 79 <> 0; \
             insert into public.single select i, i from generate_series(1, 100) i; \
             insert into public.single_src select i from generate_series(1, 100) i \
                 where i % 79 <> 0; \
             insert into public.composite select i, 'k' || i, i from generate_series(1, 100) i; \
             analyze public.single; analyze public.single_src; analyze public.composite; \
             insert into public.single select i, i from generate_series(101, 400000) i; \
             insert into public.single_src select i from generate_series(101, 400000) i \
                 where i % 79 <> 0; \
             insert into public.composite select i, 'k' || i, i \
                 from generate_series(101, 400000) i; \
             insert into public.composite values (null, 'k1', 0), (5, null, 0); \
             insert into source_table_versions (source_table, version) values ('public.t', 1)",
        )
        .await
        .expect("seed targets whose statistics lag");
        let cases = [
            (
                "public.single",
                "t.total % 79 = 0",
                true,
                Some("public.single_src"),
            ),
            ("public.wide", "t.total % 79 = 0", false, None),
            (
                "public.pair",
                "t.total % 79 = 0",
                false,
                Some("public.pair_src"),
            ),
            (
                "public.composite",
                "t.g is null or t.h is null or t.total % 79 = 0",
                false,
                None,
            ),
        ];
        for (table, batch, bounded, source) in cases {
            let id: i64 = raw
                .query_one(
                    "insert into transform_definitions \
                     (target_table, source_table, source_version, definition_text, status) \
                     values ($1, 'public.t', 1, 'unused', 'catching_up') \
                     on conflict (target_table) do update set status = excluded.status \
                     returning id",
                    &[&table],
                )
                .await
                .expect("seed the definition")
                .get(0);
            let key_cols = ddl::identity_key_columns(&raw, table)
                .await
                .expect("identity");
            let target = SweptTarget {
                id,
                status: TransformStatus::CatchingUp,
                lock_order: table.to_string(),
                target: table.to_string(),
                target_ident: ddl::qualified_target_table_ident(table),
                returning: ddl::pk_key_sql_expr(&key_cols, Some("t")),
                key_cols,
                branches: Vec::new(),
                has_image: false,
                source_guard: source.map(ddl::qualified_source_table),
                page: Vec::new(),
                staged: StagedKeys::new("unused", String::new(), Vec::new()),
            };
            let columns: Vec<String> = target
                .key_cols
                .iter()
                .map(|c| format!("t.{}::text", quote_ident(&c.name)))
                .collect();
            let key_sql = ddl::pk_key_sql_expr(&target.key_cols, Some("t"));
            let rows: Vec<(String, Vec<Option<String>>)> = raw
                .query(
                    &format!(
                        "select {key_sql}, {} from {table} t where {batch} \
                         order by t.total limit 5000",
                        columns.join(", ")
                    ),
                    &[],
                )
                .await
                .expect("keys")
                .into_iter()
                .map(|row| {
                    let parts = (1..=columns.len()).map(|i| row.get(i)).collect();
                    (row.get(0), parts)
                })
                .collect();
            assert_eq!(rows.len(), 5000);
            let arrays: Vec<Vec<Option<String>>> = (0..columns.len())
                .map(|j| rows.iter().map(|(_, parts)| parts[j].clone()).collect())
                .collect();
            let sql = delete_statement(&target, &arrays);
            assert_eq!(
                sql.contains("= any("),
                bounded,
                "{table}: bounded only for a single-column key with one NULL pattern:\n{sql}"
            );
            let status = target.status.as_str();
            let mut params: Vec<&(dyn ToSql + Sync)> =
                arrays.iter().map(|a| a as &(dyn ToSql + Sync)).collect();
            params.push(&target.id);
            params.push(&status);
            let mut txn = raw.transaction().await.expect("begin");
            let explain = txn.savepoint("explain").await.expect("savepoint");
            let plan: String = crate::staging::ledger::query_by_entry_key(
                &explain,
                &format!("explain (analyze, timing off) {sql}"),
                &params,
            )
            .await
            .expect("explain")
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>()
            .join("\n");
            explain
                .rollback()
                .await
                .expect("roll back the explain's delete");
            // The target, and a 1-1 target's source (issue #883), whose
            // statistics lag alike.
            for read in std::iter::once(table).chain(source) {
                let name = &read["public.".len()..];
                let scans: Vec<&str> = plan
                    .lines()
                    .filter(|line| line.contains(&format!(" on {name} ")))
                    .collect();
                assert!(!scans.is_empty(), "{table}: no scan of {read} in:\n{plan}");
                for scan in scans {
                    let figure = |label: &str, nth: usize| -> f64 {
                        scan.split(label)
                            .nth(nth)
                            .and_then(|rest| rest.split([' ', ')']).next())
                            .and_then(|n| n.parse().ok())
                            .unwrap_or_else(|| panic!("{label} in {scan}"))
                    };
                    // The estimate lags with the statistics, so the rows the
                    // scan actually read are checked too: a whole-index scan
                    // of a stale table is estimated as small as a probe.
                    let read_rows = figure("actual rows=", 1) * figure(" loops=", 1);
                    assert!(
                        !scan.contains("Seq Scan")
                            && figure("rows=", 1) <= rows.len() as f64
                            && read_rows <= rows.len() as f64,
                        "{table}: {read} must be read through the keys, got:\n{plan}"
                    );
                }
            }
            let filtered: u64 = plan
                .lines()
                .filter_map(|line| line.split("Rows Removed by Join Filter: ").nth(1))
                .map(|n| n.trim().parse::<u64>().expect("a row count"))
                .sum();
            assert!(
                filtered < rows.len() as u64,
                "{table}: the target must be matched to the keys without comparing \
                 every row with every key, got:\n{plan}"
            );
            let scanned = |txn| async move {
                let mut count = seq_scans_in_txn(txn, table).await;
                if let Some(source) = source {
                    count += seq_scans_in_txn(txn, source).await;
                }
                count
            };
            let before = scanned(&txn).await;
            let deleted: std::collections::HashSet<String> = delete_keys(&txn, &target, &arrays)
                .await
                .expect("delete")
                .into_iter()
                .map(|row| row.get(0))
                .collect();
            assert_eq!(
                scanned(&txn).await,
                before,
                "{table}: the sweep's lock and delete must not scan the target or its \
                 source, as the delete's plan above doesn't"
            );
            txn.rollback().await.expect("roll back");
            assert_eq!(
                deleted,
                rows.into_iter().map(|(key, _)| key).collect(),
                "{table}: every key is deleted"
            );
        }
    }

    /// How many sequential scans of `table` this transaction has started so
    /// far (`pg_stat_xact_user_tables` counts the open transaction's own).
    /// The plan above is explained through `query_by_entry_key`; this checks
    /// that [`delete_keys`] runs it that way too, which PostgreSQL 16 would
    /// otherwise plan as a scan of a stale target (#790).
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyset_match_renders_one_arm_per_null_pattern() {
        let cols = vec![r#"t."g""#.to_string(), r#"t."h""#.to_string()];
        let arrays = vec![
            vec![Some("1".to_string()), None, Some("2".to_string())],
            vec![Some("a".to_string()), Some("b".to_string()), None],
        ];
        assert_eq!(
            null_patterns(&arrays),
            vec![vec![false, false], vec![false, true], vec![true, false]]
        );
        assert_eq!(
            null_patterns(&[vec![Some("1".to_string()), Some("2".to_string())]]),
            vec![vec![false]],
            "a batch with no NULL key has the one all-false pattern"
        );

        assert_eq!(
            keyset_match_cols(&cols, &[vec![false, false]]),
            r#"t."g" = k.c0 and t."h" = k.c1"#
        );
        assert_eq!(
            keyset_match_cols(&cols, &[vec![false, true]]),
            r#"t."g" = k.c0 and t."h" is null and k.c1 is null"#
        );
        assert_eq!(
            keyset_match_cols(&cols, &null_patterns(&arrays)),
            r#"((t."g" = k.c0 and t."h" = k.c1) or "#.to_string()
                + r#"(t."g" = k.c0 and t."h" is null and k.c1 is null) or "#
                + r#"(t."g" is null and k.c0 is null and t."h" = k.c1))"#
        );
    }

    fn part(col: &str, nullable: bool) -> KeyPart {
        KeyPart {
            target_col: quote_ident(col),
            source_sql: format!("s.{}", quote_ident(col)),
            nullable,
        }
    }

    /// A `NOT NULL` key compares every column with `=`. `IS NOT DISTINCT
    /// FROM` anywhere would force a nested-loop anti-join.
    #[test]
    fn a_not_null_key_matches_with_plain_equality() {
        let matching = Match {
            parts: vec![part("a", false), part("b", false)],
        };
        assert_eq!(
            orphan_branch_sql(
                3,
                r#""public"."t""#,
                r#""public"."src""#,
                &matching,
                &[],
                OrphanPattern::Exactly(0),
            ),
            r#"select 3::int4, null::text, array[t."a"::text, t."b"::text]::text[] from "public"."t" as t where not exists (select 1 from "public"."src" as s where s."a" = t."a" and s."b" = t."b")"#
        );
    }

    /// A key over a nullable unique index (issue #1141) gets one branch per
    /// `NULL` pattern: `=` where the target row's value is present, `IS
    /// NULL` on the source side where it is `NULL`, never `IS NOT DISTINCT
    /// FROM`.
    #[test]
    fn a_null_pattern_matches_null_columns_with_is_null() {
        let matching = Match {
            parts: vec![part("g", true), part("h", true)],
        };
        let nullable = [0, 1];
        assert_eq!(
            orphan_branch_sql(
                0,
                "t",
                "src",
                &matching,
                &nullable,
                OrphanPattern::Exactly(0b10)
            ),
            r#"select 0::int4, null::text, array[t."g"::text, t."h"::text]::text[] from t as t where t."g" is not null and t."h" is null and not exists (select 1 from src as s where s."g" = t."g" and s."h" is null)"#
        );
        let none_null = orphan_branch_sql(
            0,
            "t",
            "src",
            &matching,
            &nullable,
            OrphanPattern::Exactly(0),
        );
        assert!(none_null.contains(r#"t."g" is not null and t."h" is not null and "#));
        assert!(!none_null.contains("distinct"), "{none_null}");
    }

    /// The catch-all branch covers every pattern the others don't, matching
    /// with `IS NOT DISTINCT FROM`.
    #[test]
    fn the_catch_all_covers_the_unlisted_patterns() {
        let matching = Match {
            parts: vec![part("g", true)],
        };
        assert_eq!(
            orphan_branch_sql(
                1,
                "t",
                "src",
                &matching,
                &[0],
                OrphanPattern::NoneOf(&[0, 1])
            ),
            r#"select 1::int4, null::text, array[t."g"::text]::text[] from t as t where ((case when t."g" is null then 1 else 0 end))::bigint <> all('{0,1}'::bigint[]) and not exists (select 1 from src as s where s."g" is not distinct from t."g")"#
        );
    }
}
