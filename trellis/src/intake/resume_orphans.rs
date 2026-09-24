//! Issue #330: the target rows a rebuilt definition must drop because its
//! source no longer has them.
//!
//! A resumed definition (ADR-0014) rebuilds from a backfill marker's
//! discharge ([`super::publication::run_pending_backfills`]), which enumerates
//! the source's *current* keys as image-less `Recompute`s. That re-derives
//! every target row the source still backs, but it can never reach a row the
//! source stopped backing while the definition was frozen: a 1-1 row whose
//! source row was deleted, or an aggregate group every one of whose source
//! rows went away (or, for a relationship-path `GROUP BY` key, moved to
//! another group when the to-side row changed). Nothing enumerates a key that
//! isn't there, and an aggregate's image-less `Recompute` for a vanished key
//! can't even say which group it left (`staging::apply_aggregate`'s module
//! doc, "Image-less changes").
//!
//! So the discharge also deletes those rows directly, one anti-join `DELETE`
//! per rebuilt target: every target row with no source row that maps to its
//! key. It is a target write like any other, so it goes through the
//! target-mutation seam (`staging::target_mutations`, issue #315): each
//! deleted key is recorded with its prior image and flushed as a downstream
//! `Recompute` in the discharge's own transaction. The prior image is what
//! lets a chained aggregate one hop down find the group the deleted row
//! belonged to, since the live re-read finds nothing.
//!
//! # Ordering: why this runs before the enumeration's `DECLARE`
//!
//! [`delete_orphaned_target_rows`] runs inside the discharge transaction
//! before the enumeration cursor is declared, so its snapshot is at or before
//! the cursor's. Each statement in that `READ COMMITTED` transaction reads
//! its own snapshot, so there is a short gap between the two, and a source
//! change can commit in it. What makes CDC committed during the pass safe to
//! lean on: its ring rows land in the active segment, and the maintenance
//! loop running this pass is the only thing that seals one, so they drain
//! only after the pass has flipped the definition `live` and are never
//! skipped for it.
//!
//! - **A key deleted in the gap** is not orphaned yet when this runs and is
//!   not enumerated either. Its CDC delete removes it. For an aggregate, the
//!   delete's apply probes the group, finds no source row and deletes it
//!   (`apply_aggregate_target`'s existence check), provided nothing has
//!   repopulated the group by then (see the limits below).
//! - **A key re-inserted in the gap** (after this deleted it) is enumerated,
//!   and its `Recompute` re-derives it from live state.
//! - **Anything after the `DECLARE`** is ordinary CDC, applied once the
//!   definition is `live`, and ordered against the enumeration by the #312
//!   catch-up gate.
//!
//! Running it after the intake wait (as #330's spike did) is wrong for
//! aggregates over a window as long as the wait: a group empty at the
//! cursor's snapshot and repopulated before the anti-join survives at its
//! pre-pause value, nothing enumerates it, and the new rows' CDC folds into
//! that stale value as deltas. Deleting the group first means those deltas
//! build it from nothing. `a_group_repopulated_after_the_enumeration_snapshot_holds_only_its_new_rows`
//! pins this.
//!
//! Deleting a row the source *does* still back (a group repopulated in the
//! gap) is harmless: the enumeration or CDC re-derives it. So the anti-join
//! only has to be exact in one direction. It must never keep a row that no
//! source row backs.
//!
//! ## What no placement closes (aggregates only)
//!
//! The anti-join and the cursor read different snapshots, so *either* order
//! leaves a short race. The two races mirror each other:
//!
//! - Immediately after `DECLARE`: a group empty at the cursor's snapshot and
//!   repopulated before the anti-join keeps its stale pre-pause value.
//! - Before `DECLARE` (this code): a group still backed when the anti-join
//!   runs, whose last rows are deleted before `DECLARE` and which is
//!   repopulated before those deletes drain, keeps its stale value, and the
//!   deletes and inserts fold onto it as deltas.
//!
//! This order was chosen because its race needs a group to both empty inside
//! the gap and refill before the drain. Reading both on one snapshot would
//! close it.
//!
//! A wider window remains that has nothing to do with this module, and it
//! also hits a fresh deferred definition's discharge. Take an aggregate
//! group whose every row in the cursor's snapshot is deleted after `DECLARE`
//! (during the intake wait, say) and which is refilled before the drain. It
//! never gets a forced recompute: each enumerated key's `Recompute` either
//! finds its row gone and is dropped, or folds with that row's CDC delete
//! into a plain delta. So the group ends up as its target value (stale, or
//! absent) plus the CDC deltas, not as its live contents.
//!
//! # Only the definitions this marker rebuilds
//!
//! The caller passes exactly the `waiting_to_backfill` definitions the
//! discharge is dispatching (a resumed definition, or a new one whose target
//! is still empty). A `live` sibling on the same source is not rebuilt, and
//! its target is none of this pass's business.
//!
//! ## A chunked rebuild (issue #418)
//!
//! Everything above assumes the ring rebuild, which goes `live` in the
//! discharge's own transaction. A plain 1-1 definition is rebuilt by
//! `backfill_chunks` instead (ADR-0016's dispatch by shape), which drain
//! threads run after this pass commits, and it stays `backfilling`, so its
//! CDC is skipped, until the last chunk finishes. A source row deleted after
//! this anti-join ran is then not removed by anything: its chunk either
//! copied it before the delete or never saw it, its CDC delete is skipped,
//! and the go-live catch-up only enumerates keys that still exist. A fresh
//! chunked build has the same window for a row deleted after its chunk
//! copied it. Issue #436 closes the race on the new path.
//!
//! # Keeping the anti-join hashable
//!
//! A key column compared with `IS NOT DISTINCT FROM` forces a nested-loop
//! anti-join (each target row rescans the source), which took ~20 s for a
//! 50k-row 1-1 target in #330's spike. `=` lets Postgres hash it. A 1-1
//! target's key is its `PRIMARY KEY`, all `NOT NULL`, so it always gets `=`.
//! Every aggregate grouping column is nullable (`ddl::create_aggregate_target_table`,
//! issue #128), so the delete is split by which grouping columns are `NULL`
//! in the target row: within one such pattern a `NULL` column matches a
//! source row whose key is `IS NULL`, and every other column matches with
//! `=`. That's the same rule `apply_aggregate::keyset_match` applies per
//! batch. There is one statement per pattern actually present in the target,
//! usually one.

use std::collections::BTreeSet;

use tokio_postgres::Transaction;

use crate::defs::ast::{GroupByKey, KeySpace};
use crate::defs::catalog::relationship_by_name_in;
use crate::defs::ddl::{self, PrimaryKeyColumn};
use crate::defs::model::RelationshipCardinality;
use crate::defs::oracle::{render_to_one_rel_expr_sql, to_one_join_clauses};
use crate::defs::{TransformStatus, parse};
use crate::pool::quote_ident;
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

/// Deletes, from each target of `ids` that is still `waiting_to_backfill`,
/// every row no row of `source_table` backs any more, and reports each
/// deleted key through the target-mutation seam in `txn`. See the module doc
/// for why this must run before the discharge's enumeration is declared.
pub(super) async fn delete_orphaned_target_rows(
    txn: &Transaction<'_>,
    source_table: &str,
    ids: &[i64],
) -> Result<(), IntakeError> {
    if ids.is_empty() {
        return Ok(());
    }
    // Still `waiting_to_backfill`: a pause that landed since the discharge
    // read these (#331) leaves the target as the pause found it, and its own
    // resume comes back here.
    let defs = txn
        .query(
            "select definition_text, target_table from transform_definitions \
             where id = any($1) and status = $2 order by id",
            &[&ids, &TransformStatus::WaitingToBackfill.as_str()],
        )
        .await?;
    let source_ident = ddl::qualified_source_table(source_table);
    let mut mutations = TargetMutations::new();
    for row in defs {
        let text: String = row.get(0);
        let target: String = row.get(1);
        let def =
            parse(&text).unwrap_or_else(|e| panic!("persisted definition failed to parse: {e}"));
        // Quoted: `identity_key_columns` resolves its argument with
        // `to_regclass`, which would case-fold a bare mixed-case name.
        let key_cols =
            ddl::identity_key_columns(txn, &ddl::qualified_target_table_ident(&target)).await?;
        if key_cols.is_empty() {
            // Only a target dropped since the catalog read above: every
            // target Trellis creates has a key.
            tracing::debug!(target = %target, "rebuilt target is gone; nothing to sweep");
            continue;
        }
        let matching = match &def.key_space {
            KeySpace::OneToOne => one_to_one_match(&key_cols),
            KeySpace::Aggregate { group_by } => {
                aggregate_match(txn, source_table, &target, group_by, &key_cols).await?
            }
        };
        let deleted = delete_unbacked_rows(
            txn,
            &source_ident,
            &target,
            &key_cols,
            &matching,
            &mut mutations,
        )
        .await?;
        if deleted > 0 {
            tracing::info!(
                target = %target,
                deleted,
                "rebuild dropped target rows the source no longer backs"
            );
        }
    }
    mutations.flush(txn).await?;
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
        let col = key_cols.iter().find(|c| c.name == name).unwrap_or_else(|| {
            panic!("aggregate target {target} has no grouping column {name:?} in its key")
        });
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
            let reldef = relationship_by_name_in(txn, from_schema, from_table, rel)
                .await?
                // The catalog refuses to drop a relationship a definition
                // still reads, so this is a broken invariant, not a race.
                .unwrap_or_else(|| {
                    panic!("{target}'s GROUP BY reads relationship {rel:?}, which {source_table} doesn't declare")
                });
            // The validator only admits a to-one path as a GROUP BY key; a
            // to-many join would fan out and match groups no row backs.
            assert_eq!(
                reldef.cardinality,
                RelationshipCardinality::ToOne,
                "{target}'s GROUP BY reads relationship {rel:?}, which is not to-one"
            );
            joins.push((rel.to_string(), reldef.def));
        }
    }
    Ok(Match {
        parts,
        joins: to_one_join_clauses(
            joins.iter().map(|(rel, d)| {
                (
                    rel.as_str(),
                    d.to_table.as_str(),
                    d.to_col.as_str(),
                    d.from_col.as_str(),
                )
            }),
            "s",
        ),
    })
}

/// Runs the anti-join `DELETE` for one target, once per pattern of `NULL`
/// grouping columns present in it (see the module doc), recording every
/// deleted key into `mutations`. Returns how many rows it deleted.
async fn delete_unbacked_rows(
    txn: &Transaction<'_>,
    source_ident: &str,
    target: &str,
    key_cols: &[PrimaryKeyColumn],
    matching: &Match,
    mutations: &mut TargetMutations,
) -> Result<usize, IntakeError> {
    let target_ident = ddl::qualified_target_table_ident(target);
    let nullable: Vec<usize> = (0..matching.parts.len())
        .filter(|&i| matching.parts[i].nullable)
        .collect();
    // Bit `j` set: the `j`th nullable column is `NULL`.
    let patterns: Vec<u64> = if nullable.is_empty() {
        vec![0]
    } else {
        let mask = nullable
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
            .join(" + ");
        txn.query(
            &format!("select distinct ({mask})::bigint from {target_ident} as t"),
            &[],
        )
        .await?
        .into_iter()
        .map(|row| row.get::<_, i64>(0) as u64)
        .collect()
    };

    let key_expr = ddl::pk_key_sql_expr(key_cols, Some("t"));
    let image_expr = mutations.image_sql(txn, target, "t").await?;
    let image_select = match &image_expr {
        Some(expr) => format!(", ({expr})::text"),
        None => String::new(),
    };
    let returning = format!("{key_expr}{image_select}");
    let mut deleted = 0;
    for pattern in patterns {
        let rows = txn
            .query(
                &orphan_delete_sql(
                    &target_ident,
                    source_ident,
                    matching,
                    &nullable,
                    pattern,
                    &returning,
                ),
                &[],
            )
            .await?;
        deleted += rows.len();
        for row in rows {
            let prior = image_expr.as_ref().map(|_| row.get::<_, String>(1));
            mutations.record(target, row.get(0), prior, 0, None, None);
        }
    }
    Ok(deleted)
}

/// The anti-join `DELETE` for the target rows whose nullable key columns are
/// `NULL` exactly where `pattern` says (bit `j` for `nullable[j]`). Every
/// other column matches with `=`, which is what lets Postgres hash the
/// anti-join (see the module doc).
fn orphan_delete_sql(
    target_ident: &str,
    source_ident: &str,
    matching: &Match,
    nullable: &[usize],
    pattern: u64,
    returning: &str,
) -> String {
    let is_null = |i: usize| {
        nullable
            .iter()
            .position(|&n| n == i)
            .is_some_and(|j| pattern & (1 << j) != 0)
    };
    let mut filter = Vec::new();
    let mut matches = Vec::new();
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
    format!(
        "delete from {target_ident} as t \
         where {}not exists (select 1 from {source_ident} as s{} where {}) \
         returning {returning}",
        filter.concat(),
        matching.joins,
        matches.join(" and "),
    )
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
            orphan_delete_sql(
                r#""public"."t""#,
                r#""public"."src""#,
                &matching,
                &[],
                0,
                "k"
            ),
            r#"delete from "public"."t" as t where not exists (select 1 from "public"."src" as s where s."a" = t."a" and s."b" = t."b") returning k"#
        );
    }

    /// A nullable grouping column gets one statement per `NULL` pattern: `=`
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
            orphan_delete_sql("t", "src", &matching, &nullable, 0b10, "k"),
            r#"delete from t as t where t."g" is not null and t."h" is null and not exists (select 1 from src as s left join "c" as "c" on "c"."id" = s."cid" where (s."g")::numeric = t."g" and (s."h")::text is null) returning k"#
        );
        let none_null = orphan_delete_sql("t", "src", &matching, &nullable, 0, "k");
        assert!(none_null.contains(r#"t."g" is not null and t."h" is not null and "#));
        assert!(!none_null.contains("distinct"), "{none_null}");
    }
}
