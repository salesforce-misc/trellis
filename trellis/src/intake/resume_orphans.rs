//! Issues #330 and #485: the target rows a definition must drop because its
//! source no longer has them.
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
//! So the discharge also deletes those rows directly, one anti-join `DELETE`
//! per swept target: every target row with no source row that maps to its
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
//! A wider window used to remain here, unrelated to this module's ordering
//! choice, and it also hit a fresh deferred definition's discharge: take an
//! aggregate group whose every row in the cursor's snapshot is deleted after
//! `DECLARE` (during the intake wait, say) and which is refilled before the
//! drain. Each enumerated key's `Recompute` either found its row gone and
//! was dropped, or folded with that row's CDC delete into a plain delta, so
//! the group ended up as its target value (stale, or absent) plus the CDC
//! deltas, never a forced recompute of its live contents. Issue #392 (#493)
//! closed it: the fold now carries `has_recompute` through every merge, and
//! `accumulate_changes` forces every group such a record names onto the
//! full-recompute path, exactly as it already did for an image-less change —
//! so an enumerated `Recompute`'s intent survives whatever it folds with.
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
//! ## The go-live sweep (issue #485)
//!
//! Everything above is written for the ring rebuild, which goes `live` in
//! the discharge's own transaction. A chunked or direct build runs on drain
//! threads after the dispatching pass commits, and its definition stays
//! `backfilling`, so its CDC is skipped, until the build finishes. It then
//! moves to `catching_up` (it applies from then on) and parks its go-live
//! catch-ups (#476). A source row deleted while it was `backfilling` is
//! removed by nothing else: its chunk either copied it before the delete or
//! never saw it, its CDC delete was skipped, and the catch-up's re-read only
//! enumerates keys that still exist. So the sweep runs again at the
//! discharge of each go-live catch-up, before its `DECLARE`, as above.
//!
//! The ordering argument carries over. Changes committed after the sweep
//! drain after it, since this pass is the only sealer, and the definition
//! applies them because it is already `catching_up`. For a 1-1 target the
//! sweep is exact. For an aggregate the two-snapshot race described above
//! remains (#436).
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

/// Deletes, from the target of each of `ids` that is still in `status`,
/// every row no row of its source backs any more, and reports each deleted
/// key through the target-mutation seam in `txn`. Returns how many rows it
/// deleted in all. See the module doc for why this must run before the
/// discharge's enumeration is declared.
///
/// `status` is the status the caller read `ids` in: `waiting_to_backfill`
/// for the definitions a discharge dispatches, `catching_up` for the ones
/// its catch-up may flip `live`. A pause that landed since (#331) leaves the
/// target as the pause found it, and its own resume comes back here.
pub(super) async fn delete_orphaned_target_rows(
    txn: &Transaction<'_>,
    ids: &[i64],
    status: TransformStatus,
) -> Result<usize, IntakeError> {
    if ids.is_empty() {
        return Ok(0);
    }
    let defs = txn
        .query(
            "select definition_text, target_table, source_table from transform_definitions \
             where id = any($1) and status = $2 order by id",
            &[&ids, &status.as_str()],
        )
        .await?;
    let mut mutations = TargetMutations::new();
    let mut total = 0;
    for row in defs {
        let text: String = row.get(0);
        let target: String = row.get(1);
        let source_table: String = row.get(2);
        let source_ident = ddl::qualified_source_table(&source_table);
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
                aggregate_match(txn, &source_table, &target, group_by, &key_cols).await?
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
                "dropped target rows the source no longer backs"
            );
        }
        total += deleted;
    }
    mutations.flush(txn).await?;
    Ok(total)
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

    /// The anti-join `DELETE` the sweep runs for `id`'s target, for the
    /// pattern with no `NULL` key column.
    async fn orphan_delete_for(txn: &Transaction<'_>, id: i64) -> String {
        let row = txn
            .query_one(
                "select definition_text, target_table, source_table \
                 from transform_definitions where id = $1",
                &[&id],
            )
            .await
            .expect("read the definition");
        let def = parse(row.get::<_, &str>(0)).expect("parse");
        let target: String = row.get(1);
        let source: String = row.get(2);
        let key_cols = ddl::identity_key_columns(txn, &ddl::qualified_target_table_ident(&target))
            .await
            .expect("read the target's key");
        let matching = match &def.key_space {
            KeySpace::OneToOne => one_to_one_match(&key_cols),
            KeySpace::Aggregate { group_by } => {
                aggregate_match(txn, &source, &target, group_by, &key_cols)
                    .await
                    .expect("match the grouping columns")
            }
        };
        let nullable: Vec<usize> = (0..matching.parts.len())
            .filter(|&i| matching.parts[i].nullable)
            .collect();
        orphan_delete_sql(
            &ddl::qualified_target_table_ident(&target),
            &ddl::qualified_source_table(&source),
            &matching,
            &nullable,
            0,
            "1",
        )
    }

    /// A `catching_up` 1-1 and aggregate over a 20k-row source, with no
    /// orphans: the sweep deletes nothing, and each target's `DELETE` plans
    /// as a set-based anti-join (hash or merge), never a nested loop that
    /// probes the source once per target row. A planted orphan shows the
    /// sweep isn't vacuous.
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
        let ids: Vec<i64> = raw
            .query(
                "select id from transform_definitions where status = 'catching_up' order by id",
                &[],
            )
            .await
            .expect("read the built definitions")
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        assert_eq!(ids.len(), 2, "both builds finished");

        let txn = raw.transaction().await.expect("begin");
        for &id in &ids {
            let sql = orphan_delete_for(&txn, id).await;
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
        let deleted = delete_orphaned_target_rows(&txn, &ids, TransformStatus::CatchingUp)
            .await
            .expect("sweep");
        assert_eq!(deleted, 0, "nothing to delete");

        txn.batch_execute("insert into public.orders_copy (id, a) values (-1, 0)")
            .await
            .expect("plant an orphan");
        let deleted = delete_orphaned_target_rows(&txn, &ids, TransformStatus::CatchingUp)
            .await
            .expect("sweep");
        assert_eq!(deleted, 1, "the planted orphan is the only row deleted");
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
