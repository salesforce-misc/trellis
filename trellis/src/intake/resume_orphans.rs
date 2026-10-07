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
//! Either way it is a 1-1 row whose source row was deleted, or an aggregate
//! group every one of whose source rows went away (or, for a
//! relationship-path `GROUP BY` key, moved to another group when the to-side
//! row changed). Nothing enumerates a key that isn't there.
//!
//! So the discharge also deletes those rows directly: every target row with
//! no source row that maps to its key, found by one anti-join per swept
//! target ([`Sweep`]). It is a target write like any other, so it goes
//! through the target-mutation seam (`staging::target_mutations`, issue
//! #315): each deleted key is recorded with its prior image and flushed as a
//! downstream `Recompute` in the discharge's own transaction. A chained
//! aggregate one hop down finds the group the deleted row belonged to from
//! its own ledger entry; the prior image is for relationship readers, which
//! take the row's old join value from it until #624.
//!
//! A target on the ledger is swept twice over. Each of its live entries the
//! source no longer backs is re-derived, which leaves a tombstone, takes the
//! entry's contribution out of its group and deletes a group it empties.
//! A group row with no live entry and no source row behind it is deleted
//! directly.
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
//! - **Unbacked at *S***: an aggregate group is deleted, whatever the source
//!   holds by the time the delete runs. A change after *S* that refills it
//!   drains after the commit and applies onto its row's ledger entry, which
//!   builds the group again from its entries (see "Pending deltas on a
//!   deleted group"). A 1-1 row is deleted only if the source still has no
//!   row of its key when the delete runs (see "A 1-1 row backed again").
//! - **Backed at *S***: kept, and the source row backing it at *S* is
//!   enumerated, so its `Recompute` re-derives the 1-1 row, or the
//!   aggregate row's ledger entry, from live state. A change after *S* that
//!   empties the group applies to its rows' entries, and the group row is
//!   deleted when its last member leaves.
//!
//! Neither case depends on when, between *S* and the drain, the source
//! changed, which is what closes the race #436 was filed for. Before it, the
//! anti-join was a `DELETE` of its own, one per target, before the cursor's
//! `DECLARE`. Its snapshot preceded *S*, by as much as every later target's
//! anti-join (#485), so a group still backed when its anti-join ran, emptied
//! before *S* and refilled after it, was neither deleted nor enumerated. It
//! kept its stale value, and the CDC for the emptying and the refill folded
//! onto it as deltas (#391 measured 103 where the source said 100). Moving
//! that `DELETE` after the watermark wait instead (as #330's spike did) opens
//! the mirror race: a group empty at *S* and refilled before the `DELETE`
//! survives at its stale value, since nothing enumerates it. Judging on *S*
//! and deleting later has neither.
//!
//! One kind of swept definition is outside that argument: one this discharge
//! dispatches to a chunked or direct build is `backfilling` once it commits,
//! so it skips the CDC after *S*. Its sweep here only removes early what its
//! target held from before the pause, so a reader doesn't see those rows for
//! the length of the build. The build's go-live catch-up sweeps again, and
//! that sweep is exact by the argument above.
//!
//! That argument needs a kept ledger target's group row to be the sum of its
//! live entries, and a direct build breaks it for a group whose rows left
//! between *S* and the build's read: the build empties the ledger, writes
//! only the groups the source has, and would leave that group's row at its
//! old value. A row that joins the group once the definition applies again
//! is then added on top, and the go-live sweep keeps the group, since it has
//! a live entry by then (issue #815). So the build itself deletes every
//! group its ledger has no live entry for, before it finishes, through the
//! target-mutation seam like this sweep's deletes (`defs::backfill`, "Groups
//! the ledger no longer has").
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
//! a drain page's order (ADR-0002 I5): the 1-1 targets, then the
//! aggregates, each by target name; within a ledger target its entries,
//! then its groups; and each class of row in one statement, in key order.
//! The cursor returns rows in no useful order, and a page of it holds some
//! of every target's, so [`Sweep::push`] only collects the keys and
//! [`Sweep::finish`] deletes them all at the end. Deleting each page of the
//! read as it comes would take a later page's low keys after an earlier
//! page's high ones, and one target's locks after another's, so a drain
//! holding the low key, or the other target, would wait on the discharge
//! while the discharge waits on it.
//!
//! One wait is still out of that order. A ledger target's group sweep comes
//! after its entry sweep, whose Re-derive locks the groups it takes entries
//! out of, so the sweep can hold such a group while it waits on a lower
//! group it deletes. A drain that holds the lower group and wants the
//! higher one closes a cycle. That drain applies a change into a group the
//! read found with no member and no source row, one the read's snapshot had
//! already undone. Postgres aborts one side of any such cycle. An aborted
//! apply is a transient error (40P01) with no quarantine charge, and an
//! aborted discharge rolls back with nothing deleted and retries after the
//! marker's backoff. Neither leaves anything stuck or wrong.
//!
//! Each delete also re-checks the definition's status (and a 1-1 delete the
//! source, below), which by then is after the watermark wait: a pause that
//! landed since the discharge read it (#331) leaves the target as the pause
//! found it, and its own resume comes back here. The dispatch and the flips
//! that move a swept definition out of the status it was read in come after
//! the fetch.
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
//! # Pending deltas on a deleted group
//!
//! A group the sweep deletes can still have deltas staged for it: CDC
//! committed before *S* that hasn't drained yet, which the definition
//! applies once it drains (it is `live` or `catching_up` by then). That is
//! routine for a `catching_up` definition, which has been applying since
//! its build finished. On the ledger (#623 D5) such a change applies to its
//! row's entry, not to the group row: the entry's basis says whether the
//! change is already counted, and the group is written from its live
//! entries, so a group refilled by then comes out as the source has it.
//! `a_group_swept_with_its_delete_still_staged_is_rederived_when_refilled`
//! pins this.
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
//! A catch-up on a relationship's to-side table enumerates that table, not
//! the definition's source, so a group its sweep keeps isn't re-derived by
//! it. Its source's own catch-up does that: the definition goes `live` only
//! once every table its build read has been re-read, and it applies every
//! change after each re-read.
//!
//! # Keeping the anti-join hashable
//!
//! A key column compared with `IS NOT DISTINCT FROM` forces a nested-loop
//! anti-join (each target row rescans the source), which took ~20 s for a
//! 50k-row 1-1 target in #330's spike. `=` lets Postgres hash it. A 1-1
//! target's key is its `PRIMARY KEY`, all `NOT NULL`, so it always gets `=`.
//! Every aggregate grouping column is nullable (`ddl::create_aggregate_target_table`,
//! issue #128), so the anti-join is split by which grouping columns are
//! `NULL` in the target row: within one such pattern a `NULL` column matches
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

use crate::defs::ast::{GroupByKey, KeySpace};
use crate::defs::catalog::{CatalogError, relationship_by_name_in};
use crate::defs::ddl::{self, PrimaryKeyColumn};
use crate::defs::model::RelationshipCardinality;
use crate::defs::oracle::{render_to_one_rel_expr_sql, to_one_join_clauses};
use crate::defs::{TransformStatus, parse};
use crate::pool::quote_ident;
use crate::staging::apply::bounds_keyset_by_array;
use crate::staging::target_mutations::TargetMutations;

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
    /// The ` left join ...` clauses a relationship-path `GROUP BY` key reads
    /// through; empty otherwise.
    joins: String,
    /// #623 D3: a ledger target's ledger (quoted). A group row with a live
    /// entry left is not an orphan here: the entry sweep re-derives the
    /// entry, which deletes the group once it is empty.
    ledger: Option<String>,
}

/// What a discharge's sweep deleted ([`Sweep::finish`]).
#[derive(Debug, Default)]
pub(super) struct Swept {
    /// Target rows deleted, across every swept target.
    pub(super) deleted: usize,
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
    /// target order (ADR-0002 I5). The 1-1 targets come before the
    /// aggregates, each by the definition's bare target name, the key
    /// `staging::apply` orders a page's targets by.
    lock_order: (bool, String),
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
    /// #623 D3: for a ledger target's group sweep, the ledger (quoted): a
    /// group is deleted only if it still has no live entry when the delete
    /// runs, since a Re-derive may have joined one since the read.
    ledger_guard: Option<String>,
    /// Issue #883: for a 1-1 target, its source (quoted): a row is deleted
    /// only if the source still has no row of its key when the delete runs,
    /// since a page may have re-derived it from a row inserted since the
    /// read ([`delete_statement`]).
    source_guard: Option<String>,
    /// #623 D3: `Some` for a ledger target's entry sweep. Its branch finds
    /// the ledger's live entries the source no longer backs, and
    /// [`Sweep::finish`] re-derives them through this plan instead of
    /// deleting target rows: the Re-derive finds no source row and takes
    /// each entry out of its group, which deleting the group here would
    /// leave counted in the ledger.
    ledger_rederive: Option<crate::staging::ledger::LedgerTargetPlan>,
    /// The unbacked keys the read has returned for this target so far, by
    /// column: `unbacked[j]` holds every key's `j`th column, as text.
    unbacked: Vec<Vec<Option<String>>>,
}

/// The targets one discharge sweeps, and what it has deleted from them so
/// far. See the module doc for the protocol: [`Sweep::add`] each set of
/// definitions, declare the read over [`Sweep::branches`] (with the
/// enumeration, if any, in the same statement), then pass each unbacked key
/// it fetches to [`Sweep::push`] and end with [`Sweep::finish`], which
/// deletes them.
#[derive(Default)]
pub(super) struct Sweep {
    targets: Vec<SweptTarget>,
    mutations: TargetMutations,
    swept: Swept,
}

impl Sweep {
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
                "select id, definition_text, target_table, source_table \
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
            let def = parse(&text).map_err(CatalogError::from)?;
            let target_ident = ddl::qualified_target_table_ident(&target);
            let key_cols = ddl::identity_key_columns(txn, &target).await?;
            if key_cols.is_empty() {
                // Only a target dropped since the catalog read above: every
                // target Trellis creates has a key.
                tracing::debug!(target = %target, "swept target is gone; nothing to sweep");
                continue;
            }
            // #623 D3: a target on the ledger is swept twice over. Its live
            // entries the source no longer backs are re-derived here (which
            // takes each out of its group, and deletes a group it empties),
            // and a group row with no live entry left and no source row is
            // deleted directly: a row a rebuild's build (which empties the
            // ledger) found no source rows for.
            let source_columns = crate::defs::catalog::source_columns_in(txn, id).await?;
            let relationships =
                crate::defs::catalog::resolve_relationships_in(txn, &def, &source_table).await?;
            let lock_order = (
                matches!(def.key_space, KeySpace::Aggregate { .. }),
                def.target.clone(),
            );
            let ledger = match crate::staging::ledger::route(&def, &source_columns, &relationships)
            {
                Some(shape) => {
                    let source_pk = ddl::identity_key_columns(txn, &source_table).await?;
                    let tag =
                        i32::try_from(self.targets.len()).expect("fewer than 2^31 swept targets");
                    self.targets.push(SweptTarget {
                        id,
                        status,
                        lock_order: lock_order.clone(),
                        target: target.clone(),
                        target_ident: target_ident.clone(),
                        key_cols: key_cols.clone(),
                        branches: vec![ledger_orphan_branch_sql(
                            tag,
                            &target,
                            &ddl::qualified_source_table(&source_table),
                            &source_pk,
                        )],
                        returning: String::new(),
                        has_image: false,
                        ledger_guard: None,
                        source_guard: None,
                        ledger_rederive: Some(crate::staging::ledger::LedgerTargetPlan::new(
                            &target,
                            &source_table,
                            source_pk,
                            key_cols.clone(),
                            shape,
                        )),
                        unbacked: Vec::new(),
                    });
                    Some(ddl::qualified_target_table_ident(
                        &crate::defs::ledger::ledger_table_name(&target),
                    ))
                }
                None => None,
            };
            let mut matching = match &def.key_space {
                KeySpace::OneToOne => one_to_one_match(&key_cols),
                KeySpace::Aggregate { group_by } => {
                    aggregate_match(txn, &source_table, &target, group_by, &key_cols).await?
                }
            };
            matching.ledger = ledger.clone();
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
                ledger_guard: ledger,
                source_guard,
                ledger_rederive: None,
                unbacked: Vec::new(),
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
    /// (its key columns as text, in key order), for [`Sweep::finish`] to
    /// delete.
    pub(super) fn push(&mut self, tag: i32, key: Vec<Option<String>>) {
        let target = usize::try_from(tag)
            .ok()
            .and_then(|i| self.targets.get_mut(i))
            .unwrap_or_else(|| panic!("the discharge read returned an unknown sweep tag {tag}"));
        if target.unbacked.is_empty() {
            target.unbacked = vec![Vec::new(); key.len()];
        }
        for (column, part) in target.unbacked.iter_mut().zip(key) {
            column.push(part);
        }
    }

    /// Deletes every key [`Sweep::push`] recorded from its target, if the
    /// target's definition is still in the status [`Sweep::add`] read it
    /// in, flushes each deleted row through the target-mutation seam, and
    /// returns what the sweep deleted.
    ///
    /// The read judged these rows unbacked on its snapshot, so this deletes
    /// an aggregate's by key, whatever the source holds by now: a group the
    /// source has since backed again is re-derived by the change that backed
    /// it, which drains after this discharge commits. A 1-1 row is deleted
    /// only if the source still doesn't back it (issue #883). See the module
    /// doc.
    ///
    /// It runs once the whole read is fetched, and takes its locks in a
    /// drain page's order (ADR-0002 I5, issue #716): the 1-1 targets, then
    /// the aggregates, each by target name, and each target's keys in one
    /// sorted statement per class (a ledger's entries, then its groups). See
    /// the module doc's "Why the delete comes last".
    pub(super) async fn finish(self, txn: &Transaction<'_>) -> Result<Swept, IntakeError> {
        let Sweep {
            mut targets,
            mut mutations,
            mut swept,
        } = self;
        // Stable, so a ledger target's entry sweep stays ahead of its group
        // sweep, which `add` pushed after it.
        targets.sort_by(|a, b| a.lock_order.cmp(&b.lock_order));
        for target in &mut targets {
            let unbacked = std::mem::take(&mut target.unbacked);
            if unbacked.is_empty() {
                continue;
            }
            swept.deleted += delete_unbacked(txn, target, unbacked, &mut mutations).await?;
        }
        mutations.flush(txn).await?;
        Ok(swept)
    }
}

/// [`Sweep::finish`]'s work on one target: deletes the keys `unbacked`
/// holds (by column, as [`SweptTarget::unbacked`] does) if `target`'s
/// definition is still in the status the sweep read it in, records each
/// deleted row in `mutations`, and returns how many rows it deleted.
async fn delete_unbacked(
    txn: &Transaction<'_>,
    target: &SweptTarget,
    unbacked: Vec<Vec<Option<String>>>,
    mutations: &mut TargetMutations,
) -> Result<usize, IntakeError> {
    if let Some(template) = &target.ledger_rederive {
        let applying: bool = txn
            .query_one(
                "select exists (select 1 from transform_definitions \
                 where id = $1 and status = $2)",
                &[&target.id, &target.status.as_str()],
            )
            .await?
            .get(0);
        if !applying {
            return Ok(0);
        }
        // The entry branch selects the entry's one key column.
        let keys = unbacked.into_iter().next().unwrap_or_default();
        let count = keys.len();
        let mut plan = template.clone();
        for key in keys.into_iter().flatten() {
            plan.push_rederive(key);
        }
        // A tombstone's `applied_seg`: the ring's latest segment, at or
        // above any a change for these keys can still be pending in.
        let seg_seq: Option<i64> = txn
            .query_one("select max(seg_seq) from segments", &[])
            .await?
            .get(0);
        // One call, so the entries are locked in one sorted statement and
        // the groups in another, as a page locks them.
        let (_, deleted) = crate::staging::ledger::apply_ledger_target(
            txn,
            &plan,
            seg_seq.unwrap_or(0),
            seg_seq.unwrap_or(0),
            mutations,
        )
        .await?;
        tracing::info!(
            target = %target.target,
            keys = count,
            groups_deleted = deleted,
            "re-derived ledger entries the source no longer backs"
        );
        return Ok(deleted);
    }
    let rows = delete_keys(txn, target, &unbacked).await?;
    if rows.is_empty() {
        return Ok(0);
    }
    tracing::info!(
        target = %target.target,
        deleted = rows.len(),
        "dropped target rows the source no longer backs"
    );
    for row in &rows {
        let prior = target.has_image.then(|| row.get::<_, String>(1));
        mutations.record(&target.target, row.get(0), prior, 0, None, None);
    }
    Ok(rows.len())
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
        joins: String::new(),
        ledger: None,
    }
}

/// An aggregate target's key is its grouping columns. A plain-column key
/// reads the source column; a relationship-path key (issue #137) reads the
/// to-side column through a `LEFT JOIN`, as the ledger's reads do, so a
/// source row
/// with no matching to-side row backs the `NULL` group, exactly as the build
/// and the live apply group it. Each value is cast to the target column's type
/// (the build writes it through the same assignment), which also keeps both
/// sides of the `=` one type, so it hashes.
async fn aggregate_match(
    txn: &Transaction<'_>,
    source_table: &str,
    target: &str,
    group_by: &[GroupByKey],
    key_cols: &[PrimaryKeyColumn],
) -> Result<Match, IntakeError> {
    let mut parts = Vec::with_capacity(group_by.len());
    let mut rels = BTreeSet::new();
    for key in group_by {
        let name = key.target_column_name();
        let col = key_cols.iter().find(|c| c.name == name).ok_or_else(|| {
            IntakeError::UnsweepableTarget {
                target: target.to_string(),
                reason: format!("its primary key has no grouping column {name:?}"),
            }
        })?;
        if let GroupByKey::RelationshipPath { rel, .. } = key {
            rels.insert(rel.as_str());
        }
        parts.push(KeyPart {
            target_col: quote_ident(name),
            source_sql: format!(
                "({})::{}",
                render_to_one_rel_expr_sql(&key.as_expr(), "s"),
                col.data_type
            ),
            nullable: col.nullable,
        });
    }

    let mut joins = Vec::with_capacity(rels.len());
    if !rels.is_empty() {
        let (from_schema, from_table) = source_table
            .split_once('.')
            .ok_or_else(|| IntakeError::InvalidTableName(source_table.to_string()))?;
        for rel in rels {
            // The catalog refuses to drop a relationship a definition still
            // reads, and the validator only admits a to-one path as a GROUP
            // BY key (a to-many join would fan out and match groups no row
            // backs), so either of these is a catalog edited by hand: an
            // error the marker backs off on (issue #518), not a panic.
            let unsweepable = |reason: String| IntakeError::UnsweepableTarget {
                target: target.to_string(),
                reason,
            };
            let reldef = relationship_by_name_in(txn, from_schema, from_table, rel)
                .await?
                .ok_or_else(|| {
                    unsweepable(format!(
                        "its GROUP BY reads relationship {rel:?}, which {source_table} doesn't declare"
                    ))
                })?;
            if reldef.cardinality != RelationshipCardinality::ToOne {
                return Err(unsweepable(format!(
                    "its GROUP BY reads relationship {rel:?}, which is not to-one"
                )));
            }
            // Joined by its recorded to-side (issue #372), not the bare
            // `to_table` re-resolved through this session's `search_path`.
            joins.push((rel.to_string(), reldef.qualified_to_table(), reldef.def));
        }
    }
    Ok(Match {
        parts,
        joins: to_one_join_clauses(
            joins.iter().map(|(rel, to_table, d)| {
                (
                    rel.as_str(),
                    to_table.as_str(),
                    d.to_col.as_str(),
                    d.from_col.as_str(),
                )
            }),
            "s",
        ),
        ledger: None,
    })
}

/// The read's `UNION ALL` branches that select `target_ident`'s unbacked
/// rows under `tag`: one per pattern of `NULL` grouping columns present in
/// the target now (see the module doc), and, for a target with a nullable
/// key column, one more for any other pattern.
///
/// The patterns are read here, before the read's snapshot, so a group with
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
    // The same match against a ledger entry's group columns (#623 D3).
    let mut ledger_matches = Vec::new();
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
                    ledger_matches.push(format!("l.{col} is null"));
                } else {
                    if part.nullable {
                        filter.push(format!("t.{col} is not null and "));
                    }
                    matches.push(format!("{} = t.{col}", part.source_sql));
                    ledger_matches.push(format!("l.{col} = t.{col}"));
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
                ledger_matches.push(format!("l.{0} is not distinct from t.{0}", part.target_col));
            }
        }
    }
    let key: Vec<String> = matching
        .parts
        .iter()
        .map(|p| format!("t.{}::text", p.target_col))
        .collect();
    // A ledger target's group row is the sum of its live entries (#623 D3),
    // so one with none is an orphan whatever the source holds: a source row
    // the build didn't see joins its group through its own Re-derive, which
    // starts the group again from zero.
    let unbacked = match &matching.ledger {
        Some(ledger) => {
            use crate::defs::ledger::{MEMBER_COLUMN, TOMBSTONE_COLUMN};
            format!(
                "not exists (select 1 from {ledger} as l where l.{} and not l.{} and {})",
                quote_ident(MEMBER_COLUMN),
                quote_ident(TOMBSTONE_COLUMN),
                ledger_matches.join(" and "),
            )
        }
        None => format!(
            "not exists (select 1 from {source_ident} as s{} where {})",
            matching.joins,
            matches.join(" and "),
        ),
    };
    format!(
        "select {tag}::int4, null::text, array[{}]::text[] from {target_ident} as t \
         where {}{unbacked}",
        key.join(", "),
        filter.concat(),
    )
}

/// A ledger target's branch of the read (#623 D3): `(tag, NULL, {key})` for
/// every live ledger entry whose key no source row has. The key is the
/// ring's encoding of the source key ([`ddl::pk_key_sql_expr`]), which the
/// anti-join hashes on.
fn ledger_orphan_branch_sql(
    tag: i32,
    target: &str,
    source_ident: &str,
    source_pk: &[PrimaryKeyColumn],
) -> String {
    use crate::defs::ledger::{KEY_COLUMN, MEMBER_COLUMN, TOMBSTONE_COLUMN, ledger_table_name};
    let key = quote_ident(KEY_COLUMN);
    format!(
        "select {tag}::int4, null::text, array[l.{key}]::text[] from {} as l \
         where l.{} and not l.{} \
           and not exists (select 1 from {source_ident} as s where {} = l.{key})",
        ddl::qualified_target_table_ident(&ledger_table_name(target)),
        quote_ident(MEMBER_COLUMN),
        quote_ident(TOMBSTONE_COLUMN),
        ddl::pk_key_sql_expr(source_pk, Some("s")),
    )
}

/// Runs [`lock_statement`] and then [`delete_statement`] for `arrays`, each
/// under `ENTRY_PLAN_SETTINGS` (through `ledger::query_by_entry_key`), and
/// returns the deleted rows: [`Sweep::finish`]'s statements for a target it
/// deletes rows of, as they run, which their plan test runs too.
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
    let status = target.status.as_str();
    let mut params: Vec<&(dyn ToSql + Sync)> =
        arrays.iter().map(|a| a as &(dyn ToSql + Sync)).collect();
    params.push(&target.id);
    params.push(&status);
    crate::staging::ledger::query_by_entry_key(txn, &lock_statement(target, arrays), &params)
        .await?;
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
    let cols: Vec<String> = (0..arity).map(keyset_col).collect();
    // Each value is cast back from its text on its own (rather than the
    // array as a whole), so any key type with a text input works, an
    // array-typed one included.
    let typed: Vec<String> = target
        .key_cols
        .iter()
        .zip(&cols)
        .map(|(c, k)| format!("{k}::{} as {k}", c.data_type))
        .collect();
    let arrays_sql: Vec<String> = (1..=arity).map(|i| format!("${i}::text[]")).collect();
    let target_cols = target_key_cols(target);
    let patterns = null_patterns(arrays);
    let bound = keyset_bound(&target_cols, &target.key_cols, &patterns);
    Keyset {
        relation: format!(
            "(select {} from unnest({}) as u({}))",
            typed.join(", "),
            arrays_sql.join(", "),
            cols.join(", "),
        ),
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

/// `target`'s key columns, qualified by its alias `t`, in key order.
fn target_key_cols(target: &SweptTarget) -> Vec<String> {
    target
        .key_cols
        .iter()
        .map(|c| format!("t.{}", quote_ident(&c.name)))
        .collect()
}

/// [`delete_keys`]'s lock: `target`'s rows of the keys in `arrays` (see
/// [`keyset`]), `for update` in key order. A drain page locks a 1-1
/// target's rows in that order (`staging::apply`'s pre-lock) and an
/// aggregate's groups in it (the group upsert's `order by`), both ascending
/// with `NULL`s last.
fn lock_statement(target: &SweptTarget, arrays: &[Vec<Option<String>>]) -> String {
    let Keyset {
        relation,
        condition,
    } = keyset(target, arrays);
    format!(
        "select 1 from {} as t, {relation} as k where {condition} \
         order by {} for update of t",
        target.target_ident,
        target_key_cols(target).join(", "),
    )
}

/// [`delete_keys`]'s delete: `target`'s rows of the keys in `arrays` (see
/// [`keyset`]) that are still unbacked when it runs, returning each deleted
/// row's key (and prior image). For a ledger target's group sweep, that is a
/// group with no live ledger entry left; for a 1-1 target, a row whose source
/// has no row of its key (issue #883). The 1-1 re-check relies on
/// [`lock_statement`] having run first, in its own statement: see the module
/// doc's "A 1-1 row backed again".
fn delete_statement(target: &SweptTarget, arrays: &[Vec<Option<String>>]) -> String {
    let Keyset {
        relation,
        condition,
    } = keyset(target, arrays);
    let mut guard = String::new();
    if let Some(ledger) = &target.ledger_guard {
        guard.push_str(&ledger_guard(
            ledger,
            &target.key_cols,
            &null_patterns(arrays),
        ));
    }
    if let Some(source) = &target.source_guard {
        // A 1-1 target's key is the source's own key, column for column, all
        // `NOT NULL` (`one_to_one_match`). The source's side is bounded by
        // the keys as the target's is ([`keyset_bound`]), so a source whose
        // statistics lag isn't scanned whole.
        let source_cols: Vec<String> = target
            .key_cols
            .iter()
            .map(|c| format!("s.{}", quote_ident(&c.name)))
            .collect();
        let matches: Vec<String> = source_cols
            .iter()
            .zip(target_key_cols(target))
            .map(|(s, t)| format!("{s} = {t}"))
            .collect();
        guard.push_str(&format!(
            " and not exists (select 1 from {source} as s where {}{})",
            matches.join(" and "),
            keyset_bound(&source_cols, &target.key_cols, &null_patterns(arrays)),
        ));
    }
    format!(
        "delete from {} as t using {relation} as k where {condition}{guard} returning {}",
        target.target_ident, target.returning,
    )
}

/// [`delete_statement`]'s guard for a ledger target's group sweep: it keeps
/// a group row (aliased `t`) that a live entry of `ledger` (quoted) has the
/// group of. It is one clause per `NULL` pattern of the batch's keys
/// (`patterns`, from [`null_patterns`]), each ` and (<the row has another
/// pattern> or not exists (<a live entry of the row's group>))`. The entry
/// matches with `=` on the pattern's non-`NULL` columns and `is null` on its
/// `NULL` ones, which together are `NULL`-safe equality for a row of that
/// pattern. Every row the delete reaches has one of the batch's patterns,
/// since [`keyset_match_cols`] matched it to a key.
///
/// So each `not exists` constrains every column of the ledger's `GROUP BY`
/// index (`defs::ledger::aggregate_ledger_index_ddl`). The planner either
/// probes the index once per key, for that key's group alone, or hashes the
/// ledger's live groups once. Whichever it picks, stale statistics included,
/// no key is compared with another group's entries. The `or` in front
/// runs the probe only for rows of its pattern. A row of another pattern
/// never reaches it, so it isn't compared with every entry that shares its
/// non-`NULL` columns.
///
/// The obvious alternatives are worse here:
/// - **A per-column match**, `(l.g = t.g or (l.g is null and t.g is null))`,
///   can't be an index condition, a hash key or a merge key. With the
///   ledger's statistics from before a reload, the planner compared every
///   key with every live entry. With current statistics, it probed the index
///   on the first column alone. For a 2-column key with 4 values in its
///   first column, a batch of 4,000 of 200k groups took 11 s with current
///   statistics and 23 s with stale ones. This guard takes 16 ms and 25 ms
///   (issue #819).
/// - **A record comparison** (`row(...)`), which the direct build's delete
///   of the groups its ledger lost uses (`defs::backfill`), merges or hashes
///   only against the whole ledger. It also needs nested loops off, which
///   turns the keyset's own join into a merge over the target's index.
///   That delete reads both tables whole anyway, but this one reads only its
///   keys.
fn ledger_guard(ledger: &str, key_cols: &[PrimaryKeyColumn], patterns: &[Vec<bool>]) -> String {
    use crate::defs::ledger::{MEMBER_COLUMN, TOMBSTONE_COLUMN};
    patterns
        .iter()
        .map(|pattern| {
            let mut other = Vec::with_capacity(key_cols.len());
            let mut matches = Vec::with_capacity(key_cols.len());
            for (c, &null) in key_cols.iter().zip(pattern) {
                let col = quote_ident(&c.name);
                if null {
                    other.push(format!("t.{col} is not null"));
                    matches.push(format!("l.{col} is null"));
                } else {
                    other.push(format!("t.{col} is null"));
                    matches.push(format!("l.{col} = t.{col}"));
                }
            }
            format!(
                " and ({} or not exists (select 1 from {ledger} as l where l.{} and not l.{} and {}))",
                other.join(" or "),
                quote_ident(MEMBER_COLUMN),
                quote_ident(TOMBSTONE_COLUMN),
                matches.join(" and "),
            )
        })
        .collect()
}

/// The `k`-alias column name for the `i`th `GROUP BY` column in a keyset
/// `unnest(...)`. Named `c0`, `c1`, … so they never
/// collide with the source/target's own (arbitrarily-named) grouping columns
/// when both appear in one query's join condition.
fn keyset_col(i: usize) -> String {
    format!("c{i}")
}

/// The distinct patterns of `NULL` `GROUP BY` columns among a keyset's groups
/// (`arrays[j]` holding column `j`'s value for each group): `pattern[i]` is whether
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
/// group matches only its own pattern's arm, so this is exactly `is not
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
    //!
    //! The aggregates here are relationship-fed ([`BY_G`]): every plain
    //! aggregate is built by the Re-derive build since #625 F5, which never
    //! reaches this sweep, and a relationship-fed one keeps the old build
    //! until #625 F9.

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

    /// `orders_by_g`, relationship-fed: `w` reads [`relate_orders`]'s to-one
    /// relationship, so it still takes the old build (and reaches this sweep)
    /// until #625 F9.
    const BY_G: &str = "TRANSFORM orders_by_g FROM orders GROUP BY g \
         SELECT max(a) AS total, max(gs.w) AS w";

    /// Creates `public.gs` (keyed by `orders.g`'s text values) and the to-one
    /// relationship `gs` from `orders.g` to it, which [`BY_G`] reads.
    async fn relate_orders(pool: &Pool, raw: &tokio_postgres::Client) {
        raw.batch_execute(
            "create table public.gs (g text primary key, w numeric); \
             insert into public.gs values ('a', 1), ('b', 2)",
        )
        .await
        .expect("seed gs");
        crate::defs::catalog::create_relationship(pool, "RELATIONSHIP gs FROM orders.g TO gs.g")
            .await
            .expect("create the to-one relationship");
    }

    /// Runs `ids`' sweep on its own: the discharge's read with nothing to
    /// enumerate.
    async fn sweep(txn: &Transaction<'_>, ids: &[i64]) -> Swept {
        let mut sweep = Sweep::default();
        sweep
            .add(txn, ids, TransformStatus::CatchingUp)
            .await
            .expect("plan the sweep");
        if super::super::markers::declare_read(txn, None, &sweep)
            .await
            .expect("declare the read")
        {
            super::super::markers::fetch_read(txn, "", &mut sweep)
                .await
                .expect("fetch the read");
        }
        sweep.finish(txn).await.expect("flush the sweep")
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

    /// A `catching_up` 1-1 and aggregate over a 20k-row source, with no
    /// orphans: the sweep deletes nothing, and each target's branch of the
    /// read plans as an anti-join, and each that reads the source plans as a
    /// set-based one (hash or merge), never a nested loop that probes the
    /// source once per target row. (The ledger guard probes the ledger's
    /// group index once per target group.) The aggregate's
    /// catch-all branch, for a `NULL` pattern that turned up after the sweep
    /// read the patterns, is the exception: it only scans rows no other
    /// branch covers. A planted orphan shows the sweep isn't vacuous.
    #[tokio::test]
    async fn a_sweep_with_no_orphans_deletes_nothing_through_an_anti_join() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, mut raw) = connect(&db).await;
        raw.batch_execute(
            "create table public.orders (id bigint primary key, g text, a numeric); \
             insert into public.orders \
               select n, 'g' || (n % 100), n from generate_series(1, 20000) n",
        )
        .await
        .expect("seed orders");
        relate_orders(&pool, &raw).await;
        let columns = HashMap::from([
            ("id".to_string(), ValueType::Numeric),
            ("g".to_string(), ValueType::Text),
            ("a".to_string(), ValueType::Numeric),
        ]);
        // The copy's build is the old one, with a go-live catch-up to
        // sweep, only while `orders` reads as another definition's target:
        // over a captured table a plain 1-1 is the Re-derive build's
        // (#625 F8a).
        crate::intake::markers::feed_from_a_test_definition(&raw, "public.orders")
            .await
            .expect("make orders read as another definition's target");
        for text in ["TRANSFORM orders_copy FROM orders SELECT a AS a", BY_G] {
            crate::defs::catalog::install_definition(&pool, text, &columns, "public")
                .await
                .expect("register");
        }
        crate::intake::markers::settle_builds(&pool).await;
        raw.batch_execute("analyze public.orders, public.orders_copy, public.orders_by_g")
            .await
            .expect("analyze");
        let ids = catching_up(&raw).await;
        assert_eq!(ids.len(), 2, "both builds finished");

        let txn = raw.transaction().await.expect("begin");
        let mut planned = Sweep::default();
        planned
            .add(&txn, &ids, TransformStatus::CatchingUp)
            .await
            .expect("plan the sweep");
        let (catch_all, hashable): (Vec<&str>, Vec<&str>) = planned
            .branches()
            .partition(|sql| sql.contains("is not distinct from"));
        // The copy's branch, and the aggregate's entry sweep and ledger
        // guard: a relationship-fed aggregate is on the ledger (#623 D5).
        assert_eq!(
            hashable.len(),
            3,
            "the copy's and the ledger's branches: {hashable:?}"
        );
        assert_eq!(catch_all.len(), 1, "the aggregate's catch-all");
        for sql in hashable {
            let plan: Vec<String> = txn
                .query(&format!("explain {sql}"), &[])
                .await
                .expect("explain the sweep")
                .into_iter()
                .map(|row| row.get(0))
                .collect();
            let plan = plan.join("\n");
            let reads_source = sql.contains(r#""public"."orders" as s"#);
            assert!(
                plan.contains("Anti Join") && !(reads_source && plan.contains("Nested Loop")),
                "the sweep must plan as a set-based anti-join:\n{plan}"
            );
        }
        // The catch-all's nested loop costs nothing while no target row has
        // an unlisted pattern: its filter rejects every row, so the side it
        // checks against, the ledger for a ledger-routed target, never runs.
        let plan: Vec<String> = txn
            .query(
                &format!(
                    "explain (analyze, costs off, timing off, summary off) {}",
                    catch_all[0]
                ),
                &[],
            )
            .await
            .expect("run the catch-all")
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        let ledger_scans: Vec<&String> = plan
            .iter()
            .filter(|l| l.contains(" on orders_by_g__ledger "))
            .collect();
        assert!(
            !ledger_scans.is_empty() && ledger_scans.iter().all(|l| l.contains("never executed")),
            "the catch-all must not scan the ledger when no row has a new pattern:\n{}",
            plan.join("\n")
        );
        let swept = sweep(&txn, &ids).await;
        assert_eq!(swept.deleted, 0, "nothing to delete");

        txn.batch_execute("insert into public.orders_copy (id, a) values (-1, 0)")
            .await
            .expect("plant an orphan");
        let swept = sweep(&txn, &ids).await;
        assert_eq!(
            swept.deleted, 1,
            "the planted orphan is the only row deleted"
        );
    }

    /// Issue #436: the sweep judges a row on its read's snapshot, and deletes
    /// what it judged whatever the source holds by the time it fetches. Group
    /// `a` has no source rows when the read is declared and gets one before
    /// the fetch: it is deleted (the refill's own change re-derives it once it
    /// drains). Group `b` is backed when the read is declared and loses its
    /// rows before the fetch: it is kept (the read's enumeration, or those
    /// deletes' changes, re-derive it). A group with a `NULL` pattern that
    /// appeared after the sweep read the patterns is still found.
    #[tokio::test]
    async fn a_sweep_judges_rows_on_its_reads_snapshot() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, mut raw) = connect(&db).await;
        raw.batch_execute(
            "create table public.orders (id bigint primary key, g text, a numeric); \
             insert into public.orders values (1, 'a', 1), (2, 'b', 2), (3, 'b', 3)",
        )
        .await
        .expect("seed orders");
        relate_orders(&pool, &raw).await;
        let columns = HashMap::from([
            ("id".to_string(), ValueType::Numeric),
            ("g".to_string(), ValueType::Text),
            ("a".to_string(), ValueType::Numeric),
        ]);
        crate::defs::catalog::install_definition(&pool, BY_G, &columns, "public")
            .await
            .expect("register");
        crate::intake::markers::settle_builds(&pool).await;
        let ids = catching_up(&raw).await;
        raw.batch_execute("delete from public.orders where g = 'a'")
            .await
            .expect("empty group a");
        let (_, other) = connect(&db).await;

        let txn = raw.transaction().await.expect("begin");
        let mut sweep = Sweep::default();
        sweep
            .add(&txn, &ids, TransformStatus::CatchingUp)
            .await
            .expect("plan the sweep");
        // A group whose `NULL` pattern the sweep didn't see, unbacked.
        other
            .batch_execute("insert into public.orders_by_g (g, total) values (null, 7)")
            .await
            .expect("plant a NULL group");
        assert!(
            super::super::markers::declare_read(&txn, None, &sweep)
                .await
                .expect("declare the read")
        );
        other
            .batch_execute(
                "insert into public.orders values (4, 'a', 100); \
                 delete from public.orders where g = 'b'",
            )
            .await
            .expect("refill group a and empty group b");
        super::super::markers::fetch_read(&txn, "", &mut sweep)
            .await
            .expect("fetch the read");
        let swept = sweep.finish(&txn).await.expect("flush the sweep");
        txn.commit().await.expect("commit");

        assert_eq!(swept.deleted, 2, "group a and the NULL group");
        let groups: Vec<Option<String>> = raw
            .query("select g from public.orders_by_g order by g", &[])
            .await
            .expect("read the target")
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        assert_eq!(groups, vec![Some("b".to_string())]);
    }

    /// The advisory lock key a test holds to freeze the sweep at a pause
    /// point (`crate::staging::interleave`).
    const SWEEP_PAUSE: i64 = 716;

    /// [`sweep`], committed, on a connection of its own and inside `scope`,
    /// so a test can freeze it at a pause point and drive another
    /// transaction against the locks it holds.
    fn spawn_sweep(
        mut client: tokio_postgres::Client,
        ids: Vec<i64>,
        scope: std::sync::Arc<crate::staging::interleave::PauseScope>,
    ) -> tokio::task::JoinHandle<Result<Swept, IntakeError>> {
        tokio::spawn(crate::staging::interleave::with_scope(scope, async move {
            let txn = client.transaction().await?;
            let mut sweep = Sweep::default();
            sweep.add(&txn, &ids, TransformStatus::CatchingUp).await?;
            if super::super::markers::declare_read(&txn, None, &sweep).await? {
                super::super::markers::fetch_read(&txn, "", &mut sweep).await?;
            }
            let swept = sweep.finish(&txn).await?;
            txn.commit().await?;
            Ok(swept)
        }))
    }

    /// Runs `statements` (each with one `text[]` parameter, or none) in one
    /// transaction on a connection of its own, and commits: a drain page's
    /// locks, taken in the order `staging::apply::apply_page` takes them.
    fn spawn_page(
        client: tokio_postgres::Client,
        statements: Vec<(String, Option<Vec<String>>)>,
    ) -> tokio::task::JoinHandle<Result<(), tokio_postgres::Error>> {
        tokio::spawn(async move {
            client.batch_execute("begin").await?;
            for (sql, keys) in &statements {
                match keys {
                    Some(keys) => client.execute(sql.as_str(), &[keys]).await?,
                    None => client.execute(sql.as_str(), &[]).await?,
                };
            }
            client.batch_execute("commit").await
        })
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

    /// The page's entry lock on [`BY_G`]'s ledger: `staging::ledger`'s
    /// `lock_statement`, every entry of `$1` in key order.
    fn by_g_entry_lock() -> String {
        let key = quote_ident(crate::defs::ledger::KEY_COLUMN);
        format!(
            "select 1 from public.orders_by_g__ledger \
             where {key} = any($1::text[]) order by {key} for update"
        )
    }

    /// Issue #716: the sweep locks a ledger target's entries in one sorted
    /// statement over every key its read found, as a drain page does
    /// (ADR-0002 I5), not one statement per page of the read. Every entry
    /// of [`BY_G`] is unbacked and its ledger is laid out in descending key
    /// order, so the read's first page holds the highest keys and its second
    /// the lowest. A page locks the lowest and the highest while the sweep
    /// is frozen after an entry lock.
    ///
    /// Locking per read page, the sweep held the highest key there and the
    /// page queued on it holding the lowest, which the sweep's second read
    /// page then wanted: a deadlock, which Postgres broke by aborting one of
    /// the two (40P01). Locking every key at once, the sweep holds both, the
    /// page waits for its commit, and both commit.
    #[tokio::test]
    async fn a_sweep_locks_entries_in_key_order_across_the_reads_pages() {
        use crate::staging::interleave::{PausePoint, PauseScope};
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
        relate_orders(&pool, &raw).await;
        let columns = HashMap::from([
            ("id".to_string(), ValueType::Numeric),
            ("g".to_string(), ValueType::Text),
            ("a".to_string(), ValueType::Numeric),
        ]);
        crate::defs::catalog::install_definition(&pool, BY_G, &columns, "public")
            .await
            .expect("register");
        crate::intake::markers::settle_builds(&pool).await;
        let ids = catching_up(&raw).await;
        assert_eq!(ids.len(), 1, "the build finished");
        let key = quote_ident(crate::defs::ledger::KEY_COLUMN);
        raw.batch_execute(&format!(
            "delete from public.orders; \
             create temp table entries as select * from public.orders_by_g__ledger; \
             delete from public.orders_by_g__ledger; \
             insert into public.orders_by_g__ledger select * from entries order by {key} desc; \
             analyze public.orders, public.orders_by_g__ledger"
        ))
        .await
        .expect("unback every entry, laid out in descending key order");
        let row = raw
            .query_one(
                &format!("select min({key}), max({key}) from public.orders_by_g__ledger"),
                &[],
            )
            .await
            .expect("the ledger's key range");
        let (lowest, highest): (String, String) = (row.get(0), row.get(1));

        // The precondition: the read returns the highest key first, so it
        // lands on the first of the read's two pages and the lowest on the
        // second.
        assert_eq!(
            first_unbacked(&db, &ids).await,
            vec![Some(highest.clone())],
            "the read must return the ledger in its heap order"
        );

        let (_, ctl) = connect(&db).await;
        ctl.execute("select pg_advisory_lock($1)", &[&SWEEP_PAUSE])
            .await
            .expect("hold the pause lock");
        let scope = PauseScope::new();
        let reached = scope.arm(
            PausePoint::AfterEntryLock,
            "public.orders_by_g",
            SWEEP_PAUSE,
        );
        let (_, sweeper) = connect(&db).await;
        let sweep = spawn_sweep(sweeper, ids, scope);
        let frozen = reached.await.expect("the sweep reaches its entry lock");

        let (_, pager) = connect(&db).await;
        let page = spawn_page(
            pager,
            vec![(by_g_entry_lock(), Some(vec![lowest, highest]))],
        );
        wait_blocked_behind(&ctl, frozen.backend_pid).await;
        ctl.execute("select pg_advisory_unlock($1)", &[&SWEEP_PAUSE])
            .await
            .expect("release the sweep");

        let swept = sweep.await.expect("the sweep task");
        let paged = page.await.expect("the page task");
        assert!(
            swept.is_ok() && paged.is_ok(),
            "the sweep and the page must not deadlock: sweep {:?}, page {:?}",
            swept.as_ref().err(),
            paged.as_ref().err()
        );
        assert_eq!(swept.expect("swept").deleted, 1, "the emptied group");
    }

    /// Issue #716's second shape: the sweep takes its targets in the order a
    /// drain page does (ADR-0002 I5), a 1-1 target's rows before an
    /// aggregate target's entries, whatever order it read them in. Order 1
    /// is unbacked in both [`BY_G`] (installed first, so read first) and a
    /// 1-1 copy. A page applying a change to order 1 locks the copy's row
    /// and then [`BY_G`]'s entry, while the sweep is frozen after its entry
    /// lock on [`BY_G`].
    ///
    /// Taking its targets in the order it read them, the sweep held the
    /// entry there and the page queued on it holding the copy's row, which
    /// the sweep then wanted: a deadlock (40P01). Taking the copy first, the
    /// sweep holds the row there, the page waits for its commit, and both
    /// commit.
    #[tokio::test]
    async fn a_sweep_takes_one_to_one_targets_before_ledger_entries() {
        use crate::staging::interleave::{PausePoint, PauseScope};
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, raw) = connect(&db).await;
        raw.batch_execute(
            "create table public.orders (id bigint primary key, g text, a numeric); \
             insert into public.orders values (1, 'a', 1), (2, 'b', 2)",
        )
        .await
        .expect("seed orders");
        relate_orders(&pool, &raw).await;
        let columns = HashMap::from([
            ("id".to_string(), ValueType::Numeric),
            ("g".to_string(), ValueType::Text),
            ("a".to_string(), ValueType::Numeric),
        ]);
        crate::intake::markers::feed_from_a_test_definition(&raw, "public.orders")
            .await
            .expect("make orders read as another definition's target");
        for text in [BY_G, "TRANSFORM orders_copy FROM orders SELECT a AS a"] {
            crate::defs::catalog::install_definition(&pool, text, &columns, "public")
                .await
                .expect("register");
        }
        crate::intake::markers::settle_builds(&pool).await;
        let ids = catching_up(&raw).await;
        assert_eq!(ids.len(), 2, "both builds finished");
        raw.batch_execute("delete from public.orders where id = 1")
            .await
            .expect("unback order 1");

        let (_, ctl) = connect(&db).await;
        ctl.execute("select pg_advisory_lock($1)", &[&SWEEP_PAUSE])
            .await
            .expect("hold the pause lock");
        let scope = PauseScope::new();
        let reached = scope.arm(
            PausePoint::AfterEntryLock,
            "public.orders_by_g",
            SWEEP_PAUSE,
        );
        let (_, sweeper) = connect(&db).await;
        let sweep = spawn_sweep(sweeper, ids, scope);
        let frozen = reached.await.expect("the sweep reaches its entry lock");

        let (_, pager) = connect(&db).await;
        let page = spawn_page(
            pager,
            vec![
                (
                    "select 1 from public.orders_copy where id = 1 for update".to_string(),
                    None,
                ),
                (by_g_entry_lock(), Some(vec!["1".to_string()])),
            ],
        );
        wait_blocked_behind(&ctl, frozen.backend_pid).await;
        ctl.execute("select pg_advisory_unlock($1)", &[&SWEEP_PAUSE])
            .await
            .expect("release the sweep");

        let swept = sweep.await.expect("the sweep task");
        let paged = page.await.expect("the page task");
        assert!(
            swept.is_ok() && paged.is_ok(),
            "the sweep and the page must not deadlock: sweep {:?}, page {:?}",
            swept.as_ref().err(),
            paged.as_ref().err()
        );
        assert_eq!(
            swept.expect("swept").deleted,
            2,
            "the copy's row and the emptied group"
        );
    }

    /// Issue #716: the sweep locks a target's rows in key order before it
    /// deletes them, in one statement over every key its read found, as a
    /// drain page's pre-lock takes them (ADR-0002 I5). Every row of a 1-1
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
                group_key: None,
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
        let watermark = crate::staging::watermark::StagedWatermark::saturated();
        while crate::staging::apply::drain_once(pool, seg, "issue_883", 1, "wake", &watermark)
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
                group_key: None,
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

    /// Issue #518: an aggregate target whose key no longer holds a grouping
    /// column (its primary key altered by hand) is an error the discharge
    /// backs off on, not a panic.
    #[tokio::test]
    async fn a_sweep_of_an_aggregate_missing_a_grouping_key_column_is_an_error() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, mut raw) = connect(&db).await;
        raw.batch_execute(
            "create table public.orders (id bigint primary key, g text, a numeric); \
             insert into public.orders values (1, 'a', 1)",
        )
        .await
        .expect("seed orders");
        relate_orders(&pool, &raw).await;
        let columns = HashMap::from([
            ("id".to_string(), ValueType::Numeric),
            ("g".to_string(), ValueType::Text),
            ("a".to_string(), ValueType::Numeric),
        ]);
        crate::defs::catalog::install_definition(&pool, BY_G, &columns, "public")
            .await
            .expect("register");
        crate::intake::markers::settle_builds(&pool).await;
        let ids = catching_up(&raw).await;
        raw.batch_execute(
            "do $$ declare c text; begin \
               for c in select conname from pg_constraint \
                        where conrelid = 'public.orders_by_g'::regclass and contype in ('p', 'u') \
               loop execute format('alter table public.orders_by_g drop constraint %I', c); \
               end loop; \
             end $$; \
             alter table public.orders_by_g add primary key (total)",
        )
        .await
        .expect("rekey the target by hand");

        let txn = raw.transaction().await.expect("begin");
        let mut sweep = Sweep::default();
        let err = sweep
            .add(&txn, &ids, TransformStatus::CatchingUp)
            .await
            .expect_err("a target missing its grouping column fails the sweep");
        assert!(
            matches!(&err, IntakeError::UnsweepableTarget { target, .. } if target == "public.orders_by_g"),
            "the mismatch is returned, got {err:?}"
        );
    }

    /// Issue #518: an aggregate grouped through a relationship its source no
    /// longer declares as to-one (its catalog row edited, then deleted, by
    /// hand) is an error the discharge backs off on, not a panic.
    #[tokio::test]
    async fn a_sweep_through_a_relationship_no_longer_to_one_is_an_error() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (pool, mut raw) = connect(&db).await;
        raw.batch_execute(
            "create table public.posts (id integer primary key, author text); \
             create table public.post_tags (id integer primary key, post integer, tag text); \
             create index on public.post_tags (post); \
             insert into public.posts values (1, 'alice'); \
             insert into public.post_tags values (10, 1, 'rust')",
        )
        .await
        .expect("seed posts and post_tags");
        crate::defs::catalog::create_relationship(
            &pool,
            "RELATIONSHIP post FROM post_tags.post TO posts.id",
        )
        .await
        .expect("create the to-one relationship");
        let columns = HashMap::from([
            ("id".to_string(), ValueType::Numeric),
            ("post".to_string(), ValueType::Numeric),
            ("tag".to_string(), ValueType::Text),
        ]);
        crate::defs::catalog::install_definition(
            &pool,
            "TRANSFORM author_tags FROM post_tags GROUP BY tag, post.author \
             SELECT count(*) AS n",
            &columns,
            "public",
        )
        .await
        .expect("register");
        crate::intake::markers::settle_builds(&pool).await;
        let ids = catching_up(&raw).await;
        assert_eq!(ids.len(), 1, "the build finished");

        for (edit, reason) in [
            (
                "update relationship_definitions set cardinality = 'many' where name = 'post'",
                "which is not to-one",
            ),
            (
                "delete from relationship_definitions where name = 'post'",
                "which public.post_tags doesn't declare",
            ),
        ] {
            raw.batch_execute(edit)
                .await
                .expect("edit the catalog by hand");
            let txn = raw.transaction().await.expect("begin");
            let mut sweep = Sweep::default();
            let err = sweep
                .add(&txn, &ids, TransformStatus::CatchingUp)
                .await
                .expect_err("the sweep can't join through the relationship");
            assert!(
                matches!(
                    &err,
                    IntakeError::UnsweepableTarget { target, reason: r }
                        if target == "public.author_tags" && r.ends_with(reason)
                ),
                "after {edit:?}, got {err:?}"
            );
        }
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
                lock_order: (false, table.to_string()),
                target: table.to_string(),
                target_ident: ddl::qualified_target_table_ident(table),
                returning: ddl::pk_key_sql_expr(&key_cols, Some("t")),
                key_cols,
                branches: Vec::new(),
                has_image: false,
                ledger_guard: None,
                source_guard: source.map(ddl::qualified_source_table),
                ledger_rederive: None,
                unbacked: Vec::new(),
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

    /// Issue #819: a ledger target's group delete ([`delete_statement`])
    /// never compares every key with every live entry of the ledger, for the
    /// key that made the per-column `OR` match quadratic (a 2-column key
    /// whose first column has 4 values) and with the ledger's statistics
    /// from before its reload, when every entry was in one group. The old
    /// match planned as a nested loop over the whole ledger, once per key.
    /// It covers a batch with one `NULL` pattern and one with four, and
    /// deletes exactly the batch's groups with no live entry, `NULL`s
    /// included.
    #[tokio::test]
    async fn the_sweep_group_delete_matches_the_ledger_by_whole_group_while_statistics_lag() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (_pool, mut raw) = connect(&db).await;
        raw.batch_execute(
            "create table public.grouped (a int, b int, n bigint, \
                                          unique nulls not distinct (a, b)) \
                 with (autovacuum_enabled = false); \
             create table public.grouped_ledger (k text primary key, a int, b int, \
                     __member boolean not null default true, \
                     __tombstone boolean not null default false) \
                 with (autovacuum_enabled = false); \
             create index on public.grouped_ledger (a, b) where __member and not __tombstone; \
             insert into public.grouped_ledger (k, a, b) \
                 select n::text, 0, 0 from generate_series(1, 1000) n; \
             analyze public.grouped_ledger; \
             truncate public.grouped_ledger; \
             insert into public.grouped (a, b, n) \
                 select n % 4, n / 4, n from generate_series(1, 20000) n \
                 union all values (null, 1, -1), (1, null, -2), (null, null, -3), (2, null, -4); \
             insert into public.grouped_ledger (k, a, b) \
                 select n::text, n % 4, n / 4 from generate_series(1, 20000) n \
                 where n % 100 <> 0 \
                 union all values ('x', null, 1), ('y', 1, null), ('z', null, null); \
             insert into public.grouped_ledger (k, a, b, __member, __tombstone) \
                 values ('gone', 0, 25, false, false), ('dead', 2, null, true, true); \
             analyze public.grouped; \
             insert into source_table_versions (source_table, version) values ('public.t', 1)",
        )
        .await
        .expect("seed a group target and a reloaded ledger");
        let id: i64 = raw
            .query_one(
                "insert into transform_definitions \
                 (target_table, source_table, source_version, definition_text, status) \
                 values ('public.grouped', 'public.t', 1, 'unused', 'catching_up') \
                 returning id",
                &[],
            )
            .await
            .expect("seed the definition")
            .get(0);
        let key_cols = ddl::identity_key_columns(&raw, "public.grouped")
            .await
            .expect("identity");
        let key_sql = ddl::pk_key_sql_expr(&key_cols, Some("t"));
        let target = SweptTarget {
            id,
            status: TransformStatus::CatchingUp,
            lock_order: (true, "public.grouped".to_string()),
            target: "public.grouped".to_string(),
            target_ident: ddl::qualified_target_table_ident("public.grouped"),
            returning: key_sql.clone(),
            key_cols,
            branches: Vec::new(),
            has_image: false,
            ledger_guard: Some("public.grouped_ledger".to_string()),
            source_guard: None,
            ledger_rederive: None,
            unbacked: Vec::new(),
        };
        let ledger_rows: f64 = raw
            .query_one("select count(*)::float8 from public.grouped_ledger", &[])
            .await
            .expect("count the ledger")
            .get(0);
        // Every 50th group, half of which (every 100th) have no live entry;
        // then those and every group with a `NULL`.
        let plain = "t.n % 50 = 0";
        for batch in [plain, &format!("{plain} or t.n < 0")] {
            let keys: Vec<(String, Option<String>, Option<String>)> = raw
                .query(
                    &format!(
                        "select {key_sql}, t.a::text, t.b::text from public.grouped t where {batch}"
                    ),
                    &[],
                )
                .await
                .expect("keys")
                .into_iter()
                .map(|row| (row.get(0), row.get(1), row.get(2)))
                .collect();
            let arrays = vec![
                keys.iter().map(|(_, a, _)| a.clone()).collect::<Vec<_>>(),
                keys.iter().map(|(_, _, b)| b.clone()).collect::<Vec<_>>(),
            ];
            let expected: std::collections::HashSet<String> = raw
                .query(
                    &format!(
                        "select {key_sql} from public.grouped t where ({batch}) \
                           and not exists (select 1 from public.grouped_ledger l \
                                           where l.__member and not l.__tombstone \
                                             and l.a is not distinct from t.a \
                                             and l.b is not distinct from t.b)"
                    ),
                    &[],
                )
                .await
                .expect("the batch's groups with no live entry")
                .into_iter()
                .map(|row| row.get(0))
                .collect();
            assert!(
                expected.len() >= 200 && expected.len() < keys.len(),
                "{batch}: the batch keeps some groups and deletes others"
            );
            let sql = delete_statement(&target, &arrays);
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
            let filtered: u64 = plan
                .lines()
                .filter_map(|line| line.split("Rows Removed by Join Filter: ").nth(1))
                .map(|n| n.trim().parse::<u64>().expect("a row count"))
                .sum();
            assert!(
                filtered < keys.len() as u64,
                "{batch}: the delete must not compare every key with every entry, got:\n{plan}"
            );
            // Read whole once, or probed by group: never whole once per key.
            let read: f64 = plan
                .lines()
                .filter(|line| line.contains(" on grouped_ledger "))
                .map(|scan| {
                    let figure = |label: &str| -> f64 {
                        scan.split(label)
                            .nth(1)
                            .and_then(|rest| rest.split([' ', ')']).next())
                            .and_then(|n| n.parse().ok())
                            .unwrap_or_else(|| panic!("{label} in {scan}"))
                    };
                    figure("actual rows=") * figure(" loops=")
                })
                .sum();
            assert!(
                read <= ledger_rows,
                "{batch}: the delete must read the ledger at most once, got:\n{plan}"
            );
            let deleted: std::collections::HashSet<String> = delete_keys(&txn, &target, &arrays)
                .await
                .expect("delete")
                .into_iter()
                .map(|row| row.get(0))
                .collect();
            txn.rollback().await.expect("roll back");
            assert_eq!(
                deleted, expected,
                "{batch}: the batch's groups with no live entry, and only those"
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

    /// Issue #819: the ledger guard has one clause per `NULL` pattern, each
    /// matching the entry on every grouping column, with `=` or `is null`.
    #[test]
    fn the_ledger_guard_matches_every_column_per_null_pattern() {
        let key_cols: Vec<PrimaryKeyColumn> = ["g", "h"]
            .into_iter()
            .map(|name| PrimaryKeyColumn {
                name: name.to_string(),
                data_type: "integer".to_string(),
                nullable: true,
                collation: None,
            })
            .collect();
        let live = r#"select 1 from led as l where l."__member" and not l."__tombstone""#;
        assert_eq!(
            ledger_guard("led", &key_cols, &[vec![false, false]]),
            format!(
                r#" and (t."g" is null or t."h" is null or not exists ({live} and l."g" = t."g" and l."h" = t."h"))"#
            )
        );
        assert_eq!(
            ledger_guard("led", &key_cols, &[vec![false, true], vec![true, true]]),
            format!(
                r#" and (t."g" is null or t."h" is not null or not exists ({live} and l."g" = t."g" and l."h" is null))"#
            ) + &format!(
                r#" and (t."g" is not null or t."h" is not null or not exists ({live} and l."g" is null and l."h" is null))"#
            )
        );
    }

    fn part(col: &str, source_sql: &str, nullable: bool) -> KeyPart {
        KeyPart {
            target_col: quote_ident(col),
            source_sql: source_sql.to_string(),
            nullable,
        }
    }

    /// A 1-1 key is all `NOT NULL`, so every column compares with `=`.
    /// `IS NOT DISTINCT FROM` anywhere would force a nested-loop anti-join.
    #[test]
    fn a_not_null_key_matches_with_plain_equality() {
        let matching = Match {
            parts: vec![part("a", r#"s."a""#, false), part("b", r#"s."b""#, false)],
            joins: String::new(),
            ledger: None,
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

    /// A nullable grouping column gets one branch per `NULL` pattern: `=`
    /// where the target row's value is present, `IS NULL` on the source side
    /// where it is `NULL`, never `IS NOT DISTINCT FROM`.
    #[test]
    fn a_null_pattern_matches_null_columns_with_is_null() {
        let matching = Match {
            parts: vec![
                part("g", r#"(s."g")::numeric"#, true),
                part("h", r#"(s."h")::text"#, true),
            ],
            joins: r#" left join "c" as "c" on "c"."id" = s."cid""#.to_string(),
            ledger: None,
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
            r#"select 0::int4, null::text, array[t."g"::text, t."h"::text]::text[] from t as t where t."g" is not null and t."h" is null and not exists (select 1 from src as s left join "c" as "c" on "c"."id" = s."cid" where (s."g")::numeric = t."g" and (s."h")::text is null)"#
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
            parts: vec![part("g", r#"(s."g")::numeric"#, true)],
            joins: String::new(),
            ledger: None,
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
            r#"select 1::int4, null::text, array[t."g"::text]::text[] from t as t where ((case when t."g" is null then 1 else 0 end))::bigint <> all('{0,1}'::bigint[]) and not exists (select 1 from src as s where (s."g")::numeric is not distinct from t."g")"#
        );
    }
}
