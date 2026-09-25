//! Issues #330, #485 and #436: the target rows a definition must drop because
//! its source no longer has them.
//!
//! A backfill marker's discharge ([`super::publication::run_pending_backfills`])
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
//! row changed). Nothing enumerates a key that isn't there, and an
//! aggregate's image-less `Recompute` for a vanished key can't even say which
//! group it left (`staging::apply_aggregate`'s module doc, "Image-less
//! changes").
//!
//! So the discharge also deletes those rows directly: every target row with
//! no source row that maps to its key, found by one anti-join per swept
//! target ([`Sweep`]). It is a target write like any other, so it goes
//! through the target-mutation seam (`staging::target_mutations`, issue
//! #315): each deleted key is recorded with its prior image and flushed as a
//! downstream `Recompute` in the discharge's own transaction. The prior image
//! is what lets a chained aggregate one hop down find the group the deleted
//! row belonged to, since the live re-read finds nothing.
//!
//! # One snapshot for the anti-join and the read (issue #436)
//!
//! The anti-joins are branches of the discharge's read: the one cursor that
//! also enumerates the marker's table
//! (`publication::declare_read`). One statement reads one snapshot, so a
//! row is judged unbacked on exactly the snapshot, call it *S*, whose keys
//! the enumeration stages. The cursor returns each unbacked row's key, and
//! [`Sweep::delete`] deletes by key after the intake wait, when the read is
//! fetched.
//!
//! Every commit visible to *S* is in both halves of the read. Every commit
//! after *S* reaches the ring's active segment, and the maintenance loop
//! running this pass is the only thing that seals one, so its CDC drains
//! only after this discharge commits, when a swept definition is `live` or
//! `catching_up` and applies it. For a key of a swept target:
//!
//! - **Unbacked at *S***: deleted, whatever the source holds by the time the
//!   delete runs. A change after *S* that backs the key again drains after
//!   the commit and re-derives it: a 1-1 upsert, or an aggregate delta onto a
//!   group with no row, which builds it from nothing or, at or below the
//!   extinct horizon, re-derives it (see "Pending deltas on a deleted group").
//! - **Backed at *S***: kept, and the source row backing it at *S* is
//!   enumerated, so its `Recompute` re-derives the row or group from live
//!   state. A change after *S* that empties the group folds with that
//!   `Recompute` and still forces the re-derive (issue #392, #493), and
//!   apply's existence check then deletes the group.
//!
//! Neither case depends on when, between *S* and the drain, the source
//! changed, which is what closes the race #436 was filed for. Before it, the
//! anti-join was a `DELETE` of its own, one per target, before the cursor's
//! `DECLARE`. Its snapshot preceded *S*, by as much as every later target's
//! anti-join (#485), so a group still backed when its anti-join ran, emptied
//! before *S* and refilled after it, was neither deleted nor enumerated. It
//! kept its stale value, and the CDC for the emptying and the refill folded
//! onto it as deltas (#391 measured 103 where the source said 100). Moving
//! that `DELETE` after the intake wait instead (as #330's spike did) opens
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
//! # Why the delete comes last (issue #503)
//!
//! The delete runs as the read is fetched, after the intake wait, and the
//! enumeration branch comes first, so it runs at the end of the fetch. A
//! target row the sweep deletes stays locked only from there to the
//! discharge's commit, not across the intake wait, where a drain of an
//! already-sealed segment that touched it used to wait for the discharge.
//!
//! That tail can still deadlock with such a drain: the sweep locks its rows
//! in the cursor's order, not apply's ascending key order, so a drain that
//! touches two of them there (a bulk delete's CDC for a `catching_up`
//! definition, say) can wait on the discharge while the discharge waits on
//! it. Postgres aborts one side. An aborted apply is a transient error
//! (40P01) with no quarantine charge, and an aborted discharge rolls back
//! with nothing deleted and retries after the marker's backoff. Neither
//! leaves anything stuck or wrong.
//!
//! Each delete also re-checks the definition's status, which by then is
//! after the intake wait: a pause that landed since the discharge read it
//! (#331) leaves the target as the pause found it, and its own resume comes
//! back here. The dispatch and the flips that move a swept definition out of
//! the status it was read in come after the fetch.
//!
//! # Pending deltas on a deleted group
//!
//! A group the sweep deletes can still have deltas staged for it: CDC
//! committed before *S* that hasn't drained yet, which the definition
//! applies once it drains (it is `live` or `catching_up` by then). That is
//! routine for a `catching_up` definition, which has been applying since
//! its build finished. Such a delta lands on a group with no row, and if
//! the group has been refilled by then, apply's existence check keeps it,
//! so a delete the sweep already accounted for is subtracted from nothing
//! (the group is left at a count of 0 and a `NULL` total, with a row in the
//! source).
//!
//! This is the situation issue #321's extinct horizon exists for: a live
//! read (here, the anti-join) found a group empty while deltas for commits
//! it saw may still be in flight. So the discharge raises the extinct
//! horizon of every aggregate target the sweep deleted from
//! ([`raise_extinct_horizons`]), in the same transaction, and a delta on a
//! group with no row at or below it re-derives the group from the source.
//! `a_group_swept_with_its_delete_still_staged_is_rederived_when_refilled`
//! pins this. A direct-build rebuild raises it again after its read, which
//! is at least as high.
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
//! fetches it at once: there is no intake wait to hold it.
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
//! `=`. That's the same rule `apply_aggregate::keyset_match` applies per
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
use crate::staging::apply_aggregate;
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
}

/// What a discharge's sweep deleted ([`Sweep::finish`]).
#[derive(Debug, Default)]
pub(super) struct Swept {
    /// Target rows deleted, across every swept target.
    pub(super) deleted: usize,
    /// The aggregate targets it deleted at least one group from, whose
    /// extinct horizons the caller must raise before it commits
    /// ([`raise_extinct_horizons`]).
    pub(super) emptied_aggregates: BTreeSet<String>,
}

/// The tag the discharge read's enumeration branch selects. Each swept
/// target's branches select its index in [`Sweep`] instead.
pub(super) const ENUMERATION_TAG: i32 = -1;

/// One target a [`Sweep`] covers.
struct SweptTarget {
    /// The definition, and the status the discharge read it in.
    id: i64,
    status: TransformStatus,
    target: String,
    target_ident: String,
    key_cols: Vec<PrimaryKeyColumn>,
    aggregate: bool,
    /// The read's `UNION ALL` branches that find this target's unbacked
    /// rows ([`orphan_branch_sql`]).
    branches: Vec<String>,
    /// The deleted row's encoded key, then its prior image if the seam wants
    /// one.
    returning: String,
    has_image: bool,
}

/// The targets one discharge sweeps, and what it has deleted from them so
/// far. See the module doc for the protocol: [`Sweep::add`] each set of
/// definitions, declare the read over [`Sweep::branches`] (with the
/// enumeration, if any, in the same statement), then pass each fetched page
/// of unbacked keys to [`Sweep::delete`] and end with [`Sweep::finish`].
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
    /// its catch-up may flip `live`. [`Sweep::delete`] checks it again, so a
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
            let matching = match &def.key_space {
                KeySpace::OneToOne => one_to_one_match(&key_cols),
                KeySpace::Aggregate { group_by } => {
                    aggregate_match(txn, &source_table, &target, group_by, &key_cols).await?
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
            let mut returning = ddl::pk_key_sql_expr(&key_cols, Some("t"));
            if let Some(expr) = &image_expr {
                returning.push_str(&format!(", ({expr})::text"));
            }
            self.targets.push(SweptTarget {
                id,
                status,
                target,
                target_ident,
                key_cols,
                aggregate: matches!(def.key_space, KeySpace::Aggregate { .. }),
                branches,
                returning,
                has_image: image_expr.is_some(),
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
    /// (for [`Sweep::delete`]), and the text array holds the unbacked row's
    /// key columns in key order.
    pub(super) fn branches(&self) -> impl Iterator<Item = &str> {
        self.targets
            .iter()
            .flat_map(|t| t.branches.iter().map(String::as_str))
    }

    /// Deletes `keys` (key-column values as text, in key order) from the
    /// target `tag` names, if its definition is still in the status
    /// [`Sweep::add`] read it in, and records each deleted row through the
    /// target-mutation seam.
    ///
    /// The read judged these rows unbacked on its snapshot, so this deletes
    /// them by key, whatever the source holds by now: a key the source has
    /// since backed again is re-derived by the change that backed it, which
    /// drains after this discharge commits (see the module doc).
    pub(super) async fn delete(
        &mut self,
        txn: &Transaction<'_>,
        tag: i32,
        keys: &[Vec<Option<String>>],
    ) -> Result<(), IntakeError> {
        if keys.is_empty() {
            return Ok(());
        }
        let target = usize::try_from(tag)
            .ok()
            .and_then(|i| self.targets.get(i))
            .unwrap_or_else(|| panic!("the discharge read returned an unknown sweep tag {tag}"));
        let arity = target.key_cols.len();
        let arrays: Vec<Vec<Option<String>>> = (0..arity)
            .map(|j| keys.iter().map(|key| key[j].clone()).collect())
            .collect();
        let cols: Vec<String> = (0..arity).map(apply_aggregate::keyset_col).collect();
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
        let target_cols: Vec<String> = target
            .key_cols
            .iter()
            .map(|c| format!("t.{}", quote_ident(&c.name)))
            .collect();
        let sql = format!(
            "delete from {} as t using (select {} from unnest({}) as u({})) as k \
             where {} and exists ( \
                 select 1 from transform_definitions where id = ${} and status = ${} \
             ) \
             returning {}",
            target.target_ident,
            typed.join(", "),
            arrays_sql.join(", "),
            cols.join(", "),
            apply_aggregate::keyset_match_cols(
                &target_cols,
                &apply_aggregate::null_patterns(&arrays)
            ),
            arity + 1,
            arity + 2,
            target.returning,
        );
        let status = target.status.as_str();
        let mut params: Vec<&(dyn ToSql + Sync)> =
            arrays.iter().map(|a| a as &(dyn ToSql + Sync)).collect();
        params.push(&target.id);
        params.push(&status);
        let rows = txn.query(&sql, &params).await?;
        if rows.is_empty() {
            return Ok(());
        }
        tracing::info!(
            target = %target.target,
            deleted = rows.len(),
            "dropped target rows the source no longer backs"
        );
        for row in &rows {
            let prior = target.has_image.then(|| row.get::<_, String>(1));
            self.mutations
                .record(&target.target, row.get(0), prior, 0, None, None);
        }
        if target.aggregate {
            self.swept.emptied_aggregates.insert(target.target.clone());
        }
        self.swept.deleted += rows.len();
        Ok(())
    }

    /// Flushes every deleted row through the target-mutation seam, in `txn`,
    /// and returns what the sweep deleted. The caller passes
    /// [`Swept::emptied_aggregates`] to [`raise_extinct_horizons`] in the
    /// same transaction ("Pending deltas on a deleted group").
    pub(super) async fn finish(self, txn: &Transaction<'_>) -> Result<Swept, IntakeError> {
        self.mutations.flush(txn).await?;
        Ok(self.swept)
    }
}

/// Raises the extinct horizon (issue #321, `aggregate_extinct_horizon`) of
/// each of `targets` to the current WAL insert position: the sweep's
/// counterpart of the raise apply makes after a live read finds a group
/// empty. See "Pending deltas on a deleted group" in the module doc. The
/// discharge calls it last, just before it commits, so it takes each
/// horizon row's lock only for the transaction's final statements rather
/// than across the intake wait; a later position is still at or above every
/// commit the sweep saw.
pub(super) async fn raise_extinct_horizons(
    txn: &Transaction<'_>,
    targets: &BTreeSet<String>,
) -> Result<(), IntakeError> {
    for target in targets {
        txn.execute(
            "insert into aggregate_extinct_horizon (target_table, lsn) \
             values ($1, pg_current_wal_insert_lsn()) \
             on conflict (target_table) do update \
             set lsn = greatest(aggregate_extinct_horizon.lsn, excluded.lsn)",
            &[target],
        )
        .await?;
    }
    Ok(())
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
    }
}

/// An aggregate target's key is its grouping columns. A plain-column key
/// reads the source column; a relationship-path key (issue #137) reads the
/// to-side column through the same `LEFT JOIN` every other source probe of an
/// aggregate uses (`apply_aggregate::probe_group_exists`), so a source row
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
    format!(
        "select {tag}::int4, null::text, array[{}]::text[] from {target_ident} as t \
         where {}not exists (select 1 from {source_ident} as s{} where {})",
        key.join(", "),
        filter.concat(),
        matching.joins,
        matches.join(" and "),
    )
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

    /// Runs `ids`' sweep on its own: the discharge's read with nothing to
    /// enumerate.
    async fn sweep(txn: &Transaction<'_>, ids: &[i64]) -> Swept {
        let mut sweep = Sweep::default();
        sweep
            .add(txn, ids, TransformStatus::CatchingUp)
            .await
            .expect("plan the sweep");
        if super::super::publication::declare_read(txn, None, &sweep)
            .await
            .expect("declare the read")
        {
            super::super::publication::fetch_read(txn, "", &mut sweep)
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
    /// read plans as a set-based anti-join (hash or merge), never a nested
    /// loop that probes the source once per target row. The aggregate's
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
             alter table public.orders replica identity full; \
             insert into public.orders \
               select n, 'g' || (n % 100), n from generate_series(1, 20000) n",
        )
        .await
        .expect("seed orders");
        let columns = HashMap::from([
            ("id".to_string(), ValueType::Numeric),
            ("g".to_string(), ValueType::Text),
            ("a".to_string(), ValueType::Numeric),
        ]);
        for text in [
            "TRANSFORM orders_copy FROM orders SELECT a AS a",
            "TRANSFORM orders_by_g FROM orders GROUP BY g SELECT sum(a) AS total",
        ] {
            crate::defs::catalog::install_definition(&pool, text, &columns, "public")
                .await
                .expect("register");
        }
        crate::intake::publication::settle_builds(&pool).await;
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
        assert_eq!(hashable.len(), 2, "one branch per target: {hashable:?}");
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
            assert!(
                plan.contains("Anti Join") && !plan.contains("Nested Loop"),
                "the sweep must plan as a set-based anti-join:\n{plan}"
            );
        }
        // The catch-all's nested loop costs nothing while no target row has
        // an unlisted pattern: its filter rejects every row, so the source
        // side never runs.
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
        let source_scans: Vec<&String> =
            plan.iter().filter(|l| l.contains(" on orders ")).collect();
        assert!(
            !source_scans.is_empty() && source_scans.iter().all(|l| l.contains("never executed")),
            "the catch-all must not scan the source when no row has a new pattern:\n{}",
            plan.join("\n")
        );
        let swept = sweep(&txn, &ids).await;
        assert_eq!(swept.deleted, 0, "nothing to delete");
        assert!(swept.emptied_aggregates.is_empty());

        txn.batch_execute("insert into public.orders_copy (id, a) values (-1, 0)")
            .await
            .expect("plant an orphan");
        let swept = sweep(&txn, &ids).await;
        assert_eq!(
            swept.deleted, 1,
            "the planted orphan is the only row deleted"
        );
        assert!(
            swept.emptied_aggregates.is_empty(),
            "a 1-1 target has no extinct horizon"
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
             alter table public.orders replica identity full; \
             insert into public.orders values (1, 'a', 1), (2, 'b', 2), (3, 'b', 3)",
        )
        .await
        .expect("seed orders");
        let columns = HashMap::from([
            ("id".to_string(), ValueType::Numeric),
            ("g".to_string(), ValueType::Text),
            ("a".to_string(), ValueType::Numeric),
        ]);
        crate::defs::catalog::install_definition(
            &pool,
            "TRANSFORM orders_by_g FROM orders GROUP BY g SELECT sum(a) AS total",
            &columns,
            "public",
        )
        .await
        .expect("register");
        crate::intake::publication::settle_builds(&pool).await;
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
            super::super::publication::declare_read(&txn, None, &sweep)
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
        super::super::publication::fetch_read(&txn, "", &mut sweep)
            .await
            .expect("fetch the read");
        let swept = sweep.finish(&txn).await.expect("flush the sweep");
        txn.commit().await.expect("commit");

        assert_eq!(swept.deleted, 2, "group a and the NULL group");
        assert!(swept.emptied_aggregates.contains("public.orders_by_g"));
        let groups: Vec<Option<String>> = raw
            .query("select g from public.orders_by_g order by g", &[])
            .await
            .expect("read the target")
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        assert_eq!(groups, vec![Some("b".to_string())]);
    }

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
             alter table public.orders replica identity full; \
             insert into public.orders values (1, 'a', 1)",
        )
        .await
        .expect("seed orders");
        let columns = HashMap::from([
            ("id".to_string(), ValueType::Numeric),
            ("g".to_string(), ValueType::Text),
            ("a".to_string(), ValueType::Numeric),
        ]);
        crate::defs::catalog::install_definition(
            &pool,
            "TRANSFORM orders_by_g FROM orders GROUP BY g SELECT sum(a) AS total",
            &columns,
            "public",
        )
        .await
        .expect("register");
        crate::intake::publication::settle_builds(&pool).await;
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
             alter table public.posts replica identity full; \
             alter table public.post_tags replica identity full; \
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
        crate::intake::publication::settle_builds(&pool).await;
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
