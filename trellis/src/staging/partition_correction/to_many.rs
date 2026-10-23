//! The correction engine for a to-many relationship whose to-side is a
//! partitioned source (ADR-0017, P4b of epic #897).
//!
//! A definition reading a partitioned root through a to-many relationship
//! keeps one target row per from-side row (a parent), each computed from the
//! parent's to-side rows (its children) read live at Apply. A lifecycle
//! operation on the root moves children in or out without a write any
//! capture trigger sees, so the parents of those children read stale counts
//! until something recomputes them.
//!
//! # What a correction does
//!
//! It **recomputes the routed parents** through the normal Recompute path:
//! for each parent whose join value routes to the node, it stages the
//! image-less [`StagedChange::Recompute`] a child's change stages for its
//! parent (`staging::apply`'s reverse path), and the drain re-reads the
//! parent and its children live and rewrites the target row. A parent whose
//! last child went away keeps its row and reads a count of 0. No target row is
//! ever deleted.
//!
//! The staged recomputes need no fence against the ring: a child's change
//! staged before the operation and drained after the correction recomputes its
//! parents from the live rows too, so it cannot bring a removed child back.
//!
//! # When it is done
//!
//! Staging a recompute does not write the target: the drain applies it. So
//! the walk's last page moves the progress to the `drain` phase, recording as
//! `drain_seg` the segment active at its commit, read under the share lock on
//! `segment_pointer` that keeps the seal from moving it until the page
//! commits (as a build's start records `build_seg`). Every recompute the
//! correction staged is then in a batch at or below `drain_seg` (or, a
//! straggler of the one before, claimed by its fenced read), so once every
//! segment through `drain_seg` is drained or retired, every one is applied,
//! and the reader is done. Until then its row waits, and the definition reads
//! `backfilling`. A `drain_seg` that is still the active segment and empty,
//! with no straggler of the one before, holds none of them (the last page
//! staged nothing after the ring sealed the earlier pages'), so only the
//! segments before it are waited for: the staging worker seals an empty
//! segment only while its predecessor's slot holds rows that need a
//! successor read, so an idle ring would otherwise never let it finish.
//!
//! # Routing
//!
//! A parent routes to a node when the node's constraint text holds over its
//! join value, presented under the to-side's join column (the partition key
//! column the text names) and cast to that column's recorded type and
//! collation, under the pinned settings, as an aggregate's group is routed
//! ([`super::route_predicate`]). Every row of a node holds the text, so every
//! child of the node, gone or new, has a parent the walk finds: a node in the
//! tree needs no read of its own rows, unlike an aggregate's.
//!
//! The walk reads the from-side in primary-key order, in pages of
//! [`super::Options::page_keys`] parents. Each page stages its recomputes and
//! commits its progress (`parents` phase, the last key as the cursor) in one
//! transaction, so a restart resumes after the last committed page, and
//! `keys_total` (the routed parents, counted on the first page) less
//! `keys_done` is the keys remaining.
//!
//! A node with no usable text (a gone node whose text was never recorded)
//! routes no parent. Its rows are read instead, as an aggregate's correction
//! reads them (the `node` phase): the parents of each page of its rows are
//! recomputed. A dropped one has no rows to read, and the every-pass sweep
//! (P5) finds its parents.

use tokio_postgres::types::ToSql;
use tokio_postgres::{GenericClient, IsolationLevel};

use crate::capture::partitions::{self, PendingCorrection, Recorded};
use crate::defs::chunk_queue::ClaimFence;
use crate::defs::model::{
    APPLYING_SQL, Definition, RelationshipCardinality, RelationshipDefinition, TransformStatus,
};
use crate::defs::{backfill, catalog, ddl};
use crate::pool::{Pool, quote_ident};
use crate::staging::append::{self, StagedChange};

use super::{
    Ancestry, CHUNK_APPLICATION_NAME, CorrectionError, Options, Outcome, Phase, Progress, Route,
    RouteColumn, Step, ancestry, budget_spent, covered_by_parent, detach_pending, mark_done,
    progress, route_of, route_predicate, u32_param,
};
use crate::staging::apply::ApplyError;

/// A definition that reads the partitioned source through one or more
/// to-many relationships, all from its own source (the from-side).
pub(super) struct Reader {
    definition: Definition,
    relationships: Vec<RelationshipDefinition>,
}

/// The to-many relationships whose to-side is `source_table` and whose
/// from-side is another table (a definition over the partitioned root itself
/// is an aggregate's reader).
async fn relationships_to(
    pool: &Pool,
    source_table: &str,
) -> Result<Vec<RelationshipDefinition>, CorrectionError> {
    Ok(catalog::relationships_to_table(pool, source_table)
        .await?
        .into_iter()
        .filter(|r| {
            r.cardinality == RelationshipCardinality::ToMany
                && r.qualified_from_table() != source_table
        })
        .collect())
}

/// The relationships of `relationships` that `definition` reads, from its
/// own source.
fn read_through(
    definition: &Definition,
    relationships: &[RelationshipDefinition],
) -> Vec<RelationshipDefinition> {
    let read = crate::defs::eval::relationship_references(&definition.def);
    relationships
        .iter()
        .filter(|r| {
            r.qualified_from_table() == definition.source_table
                && read.iter().any(|(name, _)| *name == r.def.name)
        })
        .cloned()
        .collect()
}

/// `definition` as a to-many reader of `source_table`, or `None` when it
/// reads it through no to-many relationship.
pub(super) async fn reader(
    pool: &Pool,
    definition: &Definition,
    source_table: &str,
) -> Result<Option<Reader>, CorrectionError> {
    let relationships = read_through(definition, &relationships_to(pool, source_table).await?);
    Ok((!relationships.is_empty()).then(|| Reader {
        definition: definition.clone(),
        relationships,
    }))
}

/// The ids of the definitions that read `source_table` through a to-many
/// relationship and apply, and whether one of them is under an old-path build
/// (`backfilling` without a Re-derive build), which holds the corrections in
/// the queue as an aggregate's does.
pub(super) async fn reader_ids(
    pool: &Pool,
    source_table: &str,
) -> Result<(Vec<i64>, bool), CorrectionError> {
    let relationships = relationships_to(pool, source_table).await?;
    if relationships.is_empty() {
        return Ok((Vec::new(), false));
    }
    let mut from_tables: Vec<String> = relationships
        .iter()
        .map(RelationshipDefinition::qualified_from_table)
        .collect();
    from_tables.sort();
    from_tables.dedup();
    let rows = {
        let client = pool.get().await?;
        client
            .query(
                &format!(
                    "select id, {APPLYING_SQL}, \
                            status = $2 and build is distinct from 'rederive' \
                     from transform_definitions where source_table = any($1) order by id"
                ),
                &[&from_tables, &TransformStatus::Backfilling.as_str()],
            )
            .await?
    };
    let mut out = Vec::new();
    let mut held = false;
    for row in rows {
        let (id, applying, old_build): (i64, bool, bool) = (row.get(0), row.get(1), row.get(2));
        if !applying && !old_build {
            continue;
        }
        let Some(definition) = catalog::definition_by_id(pool, id).await? else {
            continue;
        };
        if read_through(&definition, &relationships).is_empty() {
            continue;
        }
        if applying {
            out.push(id);
        } else {
            held = true;
        }
    }
    Ok((out, held))
}

/// Runs `correction` for `reader` page by page, resuming from its progress,
/// each write under `fence`, then waits for the ring to apply what it staged
/// (the module doc's "When it is done").
pub(super) async fn correct(
    pool: &Pool,
    reader: &Reader,
    correction: &PendingCorrection,
    options: &Options,
    outcome: &mut Outcome,
    fence: &ClaimFence<'_>,
) -> Result<Step, CorrectionError> {
    let definition = &reader.definition;
    if covered_by_parent(pool, correction).await? {
        return Ok(if mark_done(pool, fence, correction, definition).await? {
            Step::Done
        } else {
            Step::Superseded
        });
    }
    let recorded = {
        let client = pool.get().await?;
        partitions::recorded(&**client, &correction.table).await?
    };
    match ancestry(pool, correction, recorded.as_ref()).await? {
        Ancestry::Intact => {}
        Ancestry::Moved => return Ok(Step::Waiting),
        Ancestry::Gone => {
            return Ok(if mark_done(pool, fence, correction, definition).await? {
                Step::Done
            } else {
                Step::Superseded
            });
        }
    }
    let route = route_of(correction, recorded.as_ref());
    if matches!(route, Route::Text(_)) && !join_types_match(pool, reader, recorded.as_ref()).await?
    {
        return Ok(Step::Waiting);
    }
    let from_pk = ddl::source_primary_key(pool, &definition.source_table)
        .await
        .map_err(ApplyError::from)?;

    loop {
        if budget_spent(options, outcome) {
            return Ok(Step::Stopped);
        }
        let current = {
            let client = pool.get().await?;
            progress(&**client, correction.id, definition.id).await?
        };
        let Some(current) = current else {
            match start(pool, definition, correction, fence).await? {
                Some(true) => continue,
                Some(false) => return Ok(Step::Waiting),
                None => return Ok(Step::Superseded),
            }
        };
        let staged = match Phase::parse(current.phase) {
            Phase::Done => return Ok(Step::Done),
            Phase::Drain => {
                return match drained(pool, fence, correction, definition).await? {
                    Some(true) => Ok(Step::Done),
                    Some(false) => Ok(Step::Waiting),
                    None => Ok(Step::Superseded),
                };
            }
            Phase::Parents => {
                parents_page(
                    pool,
                    reader,
                    &from_pk,
                    &route,
                    recorded.as_ref(),
                    &current,
                    correction,
                    options.page_keys,
                    fence,
                )
                .await?
            }
            Phase::Node => {
                node_page(
                    pool,
                    reader,
                    &from_pk,
                    &current,
                    correction,
                    options.page_keys,
                    fence,
                )
                .await?
            }
            Phase::Fence | Phase::Ledger => {
                return Err(CorrectionError::Unroutable(format!(
                    "a to-many correction {} is in an aggregate's phase",
                    correction.id
                )));
            }
        };
        let Some(staged) = staged else {
            return Ok(Step::Superseded);
        };
        outcome.pages += 1;
        outcome.keys += staged;
    }
}

/// Moves the progress from `drain` to `done` once every segment through its
/// `drain_seg` is drained or retired, under `fence`. Returns whether it
/// moved, or `None`, with nothing written, when the fence no longer holds.
///
/// `drain_seg` can still be the active segment with nothing in it: the last
/// page staged nothing (the parents filled the pages before it exactly, or
/// none routes to the node) after the ring sealed the earlier pages'
/// segment. The staging worker seals an empty segment only while its
/// predecessor's slot holds rows that need a successor read (`staging::seal`'s
/// idle and straggler seals), so on an idle ring it might never drain. Every
/// row the correction staged committed before this check, so an empty active
/// slot holds none of them, and when its
/// predecessor holds no row its own fence left out (a straggler the active
/// segment's read would claim, `staging::seal`'s phase-gap
/// straggler), the segments before it are all there is to wait for. The
/// pointer, the slots and the registry are read in the one statement, so
/// against one snapshot.
async fn drained(
    pool: &Pool,
    fence: &ClaimFence<'_>,
    correction: &PendingCorrection,
    definition: &Definition,
) -> Result<Option<bool>, CorrectionError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    if !fence.hold(&*txn).await? {
        txn.rollback().await?;
        return Ok(None);
    }
    let moved = txn
        .execute(&drained_sql()?, &[&correction.id, &definition.id])
        .await?;
    txn.commit().await?;
    Ok(Some(moved > 0))
}

/// [`drained`]'s statement: the ring's state, then the move.
fn drained_sql() -> Result<String, CorrectionError> {
    let mut active_rows = String::from("case p.ring_slot");
    let mut stragglers = String::from("case s.ring_slot");
    for slot in 0..append::RING_SIZE {
        let table = append::ring_table_name(slot)?;
        active_rows.push_str(&format!(" when {slot} then exists (select 1 from {table})"));
        stragglers.push_str(&format!(
            " when {slot} then exists (select 1 from {table} \
                                       where not pg_visible_in_snapshot(row_txid, s.fence_snapshot))"
        ));
    }
    active_rows.push_str(" end");
    stragglers.push_str(" end");
    Ok(format!(
        "with ring as ( \
             select p.active_seq, {active_rows} as active_rows, \
                    coalesce((select {stragglers} from segments s \
                              where s.seg_seq = p.active_seq - 1), false) as stragglers \
             from segment_pointer p) \
         update partition_correction_progress pr set phase = 'done', drain_seg = null \
         from ring r \
         where pr.correction_id = $1 and pr.definition_id = $2 and pr.phase = 'drain' \
           and not exists (select 1 from segments s \
                           where s.seg_seq <= pr.drain_seg and s.seg_seq < r.active_seq \
                             and s.state <> 'drained') \
           and (pr.drain_seg < r.active_seq or not (r.active_rows or r.stragglers))"
    ))
}

/// The segment active now, read under a share lock on `segment_pointer` that
/// keeps the seal from moving it until `txn` commits: every ring row `txn`
/// wrote is in a batch at or below it.
async fn active_seg(txn: &impl GenericClient) -> Result<i64, CorrectionError> {
    Ok(txn
        .query_one("select active_seq from segment_pointer for share", &[])
        .await?
        .get(0))
}

/// Whether each relationship's from-side join column still has the type of
/// the to-side's recorded partition key, the type routing casts its values to.
///
/// Define and resume require the two join columns to have the same type, so
/// the cast is the identity, and the reconcile pass pauses a definition whose
/// join column is re-typed. Between the re-type and that pass, a from-side
/// value the cast refuses (an `integer` column made `bigint`) would fail every
/// run of the row. The reader waits instead, and the pass's pause then takes
/// it off the readers.
async fn join_types_match(
    pool: &Pool,
    reader: &Reader,
    recorded: Option<&Recorded>,
) -> Result<bool, CorrectionError> {
    let Some(recorded) = recorded else {
        return Ok(true);
    };
    let client = pool.get().await?;
    for rel in &reader.relationships {
        let Some(key) = recorded
            .keys
            .iter()
            .find(|k| k.column.as_deref() == Some(rel.def.to_col.as_str()))
        else {
            continue;
        };
        let from_type: Option<String> = client
            .query_opt(
                "select pg_catalog.format_type(a.atttypid, a.atttypmod) \
                 from pg_catalog.pg_attribute a \
                 where a.attrelid = pg_catalog.to_regclass($1) and a.attname = $2 \
                   and a.attnum > 0 and not a.attisdropped",
                &[
                    &ddl::regclass_arg(&reader.definition.source_table),
                    &rel.def.from_col,
                ],
            )
            .await?
            .map(|row| row.get(0));
        if from_type.as_deref() != Some(key.pg_type.as_str()) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Starts `correction` for `definition` once a pending detach has finished:
/// records the progress row in the `parents` phase, under `fence`. Nothing
/// is stamped and no transaction is waited for: every recompute reads the
/// source live. Returns `false` while the detach is pending, and `None`, with
/// nothing written, when the fence no longer holds.
async fn start(
    pool: &Pool,
    definition: &Definition,
    correction: &PendingCorrection,
    fence: &ClaimFence<'_>,
) -> Result<Option<bool>, CorrectionError> {
    let mut client = pool.get().await?;
    if detach_pending(&**client, correction).await? {
        return Ok(Some(false));
    }
    let txn = client.transaction().await?;
    fence.arm(&*txn).await?;
    // The definition's row before the claim's (`ClaimFence`): the insert's
    // foreign key takes it.
    txn.execute(
        "select 1 from transform_definitions where id = $1 for key share",
        &[&definition.id],
    )
    .await?;
    if !fence.hold(&*txn).await? {
        txn.rollback().await?;
        return Ok(None);
    }
    txn.execute(
        "insert into partition_correction_progress (correction_id, definition_id, phase) \
         values ($1, $2, 'parents') on conflict do nothing",
        &[&correction.id, &definition.id],
    )
    .await?;
    txn.commit().await?;
    Ok(Some(true))
}

/// `pk`'s ordering, `not null` and after-the-cursor SQL over `alias`, with
/// the cursor's parts to bind from `$1`.
struct KeyWalk {
    key: String,
    order: String,
    not_null: String,
    after: String,
    binds: Vec<String>,
}

impl KeyWalk {
    fn new(
        pk: &[ddl::PrimaryKeyColumn],
        table: &str,
        alias: &str,
        cursor: Option<&str>,
    ) -> Result<KeyWalk, CorrectionError> {
        let idents: Vec<String> = pk
            .iter()
            .map(|c| format!("{alias}.{}", quote_ident(&c.name)))
            .collect();
        let binds: Option<Vec<String>> = match cursor {
            Some(cursor) => Some(
                ddl::split_pk_key(pk, table, cursor)
                    .map_err(|e| CorrectionError::Unroutable(format!("an unreadable cursor: {e}")))?
                    .into_iter()
                    .map(|part| part.map(|c| c.into_owned()).unwrap_or_default())
                    .collect(),
            ),
            None => None,
        };
        Ok(KeyWalk {
            key: ddl::pk_key_sql_expr(pk, Some(alias)),
            order: pk
                .iter()
                .zip(&idents)
                .map(|(c, ident)| c.ordered(ident))
                .collect::<Vec<_>>()
                .join(", "),
            not_null: backfill::key_not_null(&idents),
            after: if binds.is_some() {
                backfill::pk_row_cmp(&idents, pk, ">", 0)
            } else {
                "true".to_string()
            },
            binds: binds.unwrap_or_default(),
        })
    }

    /// The parameter number of the page's limit.
    fn limit_param(&self) -> usize {
        self.binds.len() + 1
    }
}

/// Reads one page of the routed parents and stages their recomputes,
/// committing the progress with them under `fence`. Returns the parents
/// staged, or `None`, with nothing written, when the fence no longer holds.
#[allow(clippy::too_many_arguments)]
async fn parents_page(
    pool: &Pool,
    reader: &Reader,
    from_pk: &[ddl::PrimaryKeyColumn],
    route: &Route,
    recorded: Option<&Recorded>,
    current: &Progress,
    correction: &PendingCorrection,
    page_keys: usize,
    fence: &ClaimFence<'_>,
) -> Result<Option<i64>, CorrectionError> {
    let definition = &reader.definition;
    let from = ddl::qualified_source_table(&definition.source_table);
    let walk = KeyWalk::new(
        from_pk,
        &definition.source_table,
        "f",
        current.cursor.as_deref(),
    )?;
    let routed = routed_predicate(reader, route, recorded);
    let limit = i64::try_from(page_keys.max(1)).unwrap_or(i64::MAX);
    let select = format!(
        "select {key} from {from} f where {after} and {not_null} and {routed} \
         order by {order} limit ${limit_param}",
        key = walk.key,
        after = walk.after,
        not_null = walk.not_null,
        order = walk.order,
        limit_param = walk.limit_param(),
    );
    let count = format!(
        "select count(*) from {from} f where {not_null} and {routed}",
        not_null = walk.not_null,
    );

    // Read under the pinned settings, in a transaction of its own: they
    // change what the constraint text means, and the staging below runs
    // under the session's own.
    let mut client = pool.get().await?;
    let (found, total) = {
        let txn = client
            .build_transaction()
            .isolation_level(IsolationLevel::ReadCommitted)
            .start()
            .await?;
        txn.batch_execute(&partitions::pinned_settings()).await?;
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
        for bind in &walk.binds {
            params.push(bind);
        }
        params.push(&limit);
        let found: Vec<String> = txn
            .query(&select, &params)
            .await?
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        let total: Option<i64> = if current.keys_total.is_none() {
            Some(txn.query_one(&count, &[]).await?.get(0))
        } else {
            None
        };
        txn.commit().await?;
        (found, total)
    };
    let finished = found.len() < page_keys.max(1);

    let Some(txn) = write_txn(&mut client, fence).await? else {
        return Ok(None);
    };
    stage(&txn, &definition.source_table, &found).await?;
    let (phase, cursor) = if !finished {
        (Phase::Parents, found.last().cloned())
    } else if *route == Route::Unusable {
        (Phase::Node, None)
    } else {
        (Phase::Drain, None)
    };
    let estimate = if phase == Phase::Node {
        node_estimate(&txn, correction).await?
    } else {
        0
    };
    let staged = i64::try_from(found.len()).unwrap_or(i64::MAX);
    // Last: the share lock is held only for the commit.
    let drain_seg = if phase == Phase::Drain {
        Some(active_seg(&*txn).await?)
    } else {
        None
    };
    txn.execute(
        "update partition_correction_progress set phase = $3, cursor = $4, \
             keys_done = keys_done + $5::bigint, \
             keys_total = coalesce(keys_total, 0) + coalesce($6::bigint, 0) + $7::bigint, \
             drain_seg = $8 \
         where correction_id = $1 and definition_id = $2",
        &[
            &correction.id,
            &reader.definition.id,
            &phase.as_str(),
            &cursor,
            &staged,
            &total,
            &estimate,
            &drain_seg,
        ],
    )
    .await?;
    txn.commit().await?;
    Ok(Some(staged))
}

/// A page's write transaction, holding `fence`, or `None` when it no longer
/// holds.
async fn write_txn<'c>(
    client: &'c mut crate::pool::Client,
    fence: &ClaimFence<'_>,
) -> Result<Option<deadpool_postgres::Transaction<'c>>, CorrectionError> {
    let txn = client
        .build_transaction()
        .isolation_level(IsolationLevel::ReadCommitted)
        .start()
        .await?;
    txn.batch_execute(&format!(
        "set local application_name = '{CHUNK_APPLICATION_NAME}'"
    ))
    .await?;
    if !fence.hold(&*txn).await? {
        txn.rollback().await?;
        return Ok(None);
    }
    Ok(Some(txn))
}

/// SQL for a boolean over the from-side row `f`: whether one of the reader's
/// relationships routes it to `route`, its join value presented as the
/// to-side's join column.
fn routed_predicate(reader: &Reader, route: &Route, recorded: Option<&Recorded>) -> String {
    let routes: Vec<String> = reader
        .relationships
        .iter()
        .map(|rel| {
            let column = RouteColumn {
                name: rel.def.to_col.clone(),
                value: format!("f.{}", quote_ident(&rel.def.from_col)),
                key: recorded.and_then(|r| {
                    r.keys
                        .iter()
                        .find(|k| k.column.as_deref() == Some(rel.def.to_col.as_str()))
                        .cloned()
                }),
            };
            route_predicate(route, std::slice::from_ref(&column))
        })
        .collect();
    format!("({})", routes.join(" or "))
}

/// Stages a recompute of each of `keys` of `from_table`, as a child's change
/// stages one for its parent.
async fn stage(
    txn: &tokio_postgres::Transaction<'_>,
    from_table: &str,
    keys: &[String],
) -> Result<(), CorrectionError> {
    let changes: Vec<StagedChange> = keys
        .iter()
        .map(|key| StagedChange::Recompute {
            src_table: from_table.to_string(),
            key: key.clone(),
            hop_gen: 0,
            group_key: None,
            src_changed: None,
            prior_image: None,
            origin_lsn: None,
        })
        .collect();
    append::append(txn, &changes).await?;
    Ok(())
}

/// The rows the node phase expects, by the planner's estimate (`reltuples`,
/// zero for a table nothing has analyzed or a node that is gone).
async fn node_estimate(
    txn: &tokio_postgres::Transaction<'_>,
    correction: &PendingCorrection,
) -> Result<i64, CorrectionError> {
    let estimate: Option<f32> = txn
        .query_opt(
            "select c.reltuples from pg_catalog.pg_class c where c.oid = $1::bigint::oid",
            &[&u32_param(correction.node_oid)],
        )
        .await?
        .map(|row| row.get(0));
    #[allow(clippy::cast_possible_truncation)]
    Ok(estimate.map_or(0, |e| e.max(0.0) as i64))
}

/// Reads one page of a node's rows (a node with no usable text) and stages
/// a recompute of each parent they name, committing the progress with them
/// under `fence`. Returns the parents staged, or `None`, with nothing
/// written, when the fence no longer holds.
async fn node_page(
    pool: &Pool,
    reader: &Reader,
    from_pk: &[ddl::PrimaryKeyColumn],
    current: &Progress,
    correction: &PendingCorrection,
    page_keys: usize,
    fence: &ClaimFence<'_>,
) -> Result<Option<i64>, CorrectionError> {
    let definition = &reader.definition;
    let node_pk = ddl::source_primary_key(pool, &correction.table)
        .await
        .map_err(ApplyError::from)?;
    let mut client = pool.get().await?;
    let Some(txn) = write_txn(&mut client, fence).await? else {
        return Ok(None);
    };
    // The node by oid, so a rename doesn't lose it; none once it is dropped.
    let node: Option<String> = txn
        .query_opt(
            "select pg_catalog.format('%I.%I', n.nspname, c.relname) \
             from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
             where c.oid = $1::bigint::oid and c.relkind in ('r', 'p')",
            &[&u32_param(correction.node_oid)],
        )
        .await?
        .map(|row| row.get(0));
    let Some(node) = node else {
        record(&txn, correction, definition, 0, None, Phase::Drain).await?;
        txn.commit().await?;
        return Ok(Some(0));
    };
    let walk = KeyWalk::new(&node_pk, &correction.table, "n", current.cursor.as_deref())?;
    let limit = i64::try_from(page_keys.max(1)).unwrap_or(i64::MAX);
    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
    for bind in &walk.binds {
        params.push(bind);
    }
    params.push(&limit);
    let rows: Vec<String> = txn
        .query(
            &format!(
                "select {key} from {node} n where {after} and {not_null} \
                 order by {order} limit ${limit_param}",
                key = walk.key,
                after = walk.after,
                not_null = walk.not_null,
                order = walk.order,
                limit_param = walk.limit_param(),
            ),
            &params,
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    let finished = rows.len() < page_keys.max(1);
    let mut parents: Vec<String> = Vec::new();
    if let Some(last) = rows.last() {
        // The page's rows are the node's rows after the cursor up to `last`;
        // their parents, through each relationship.
        let upto = KeyWalk::new(&node_pk, &correction.table, "n", Some(last))?;
        let upto_cmp = backfill::pk_row_cmp(
            &node_pk
                .iter()
                .map(|c| format!("n.{}", quote_ident(&c.name)))
                .collect::<Vec<_>>(),
            &node_pk,
            "<=",
            walk.binds.len(),
        );
        let from = ddl::qualified_source_table(&definition.source_table);
        let from_walk = KeyWalk::new(from_pk, &definition.source_table, "f", None)?;
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
        for bind in walk.binds.iter().chain(&upto.binds) {
            params.push(bind);
        }
        for rel in &reader.relationships {
            let sql = format!(
                "select distinct {key} from {node} n join {from} f \
                     on f.{from_col} = n.{to_col} \
                 where {after} and {upto_cmp} and {not_null}",
                key = from_walk.key,
                from_col = quote_ident(&rel.def.from_col),
                to_col = quote_ident(&rel.def.to_col),
                after = walk.after,
                not_null = from_walk.not_null,
            );
            for row in txn.query(&sql, &params).await? {
                parents.push(row.get(0));
            }
        }
        parents.sort();
        parents.dedup();
    }
    stage(&txn, &definition.source_table, &parents).await?;
    let (phase, cursor) = if finished {
        (Phase::Drain, None)
    } else {
        (Phase::Node, rows.last().cloned())
    };
    record(
        &txn,
        correction,
        definition,
        i64::try_from(rows.len()).unwrap_or(i64::MAX),
        cursor.as_deref(),
        phase,
    )
    .await?;
    txn.commit().await?;
    Ok(Some(i64::try_from(parents.len()).unwrap_or(i64::MAX)))
}

/// Commits a node-phase page's progress: `done` rows more, the cursor and
/// the phase, and `drain_seg` when it moves to `drain`. Last in the page's
/// transaction (see [`active_seg`]).
async fn record(
    txn: &tokio_postgres::Transaction<'_>,
    correction: &PendingCorrection,
    definition: &Definition,
    done: i64,
    cursor: Option<&str>,
    phase: Phase,
) -> Result<(), CorrectionError> {
    let drain_seg = if phase == Phase::Drain {
        Some(active_seg(txn).await?)
    } else {
        None
    };
    txn.execute(
        "update partition_correction_progress set phase = $3, cursor = $4, \
             keys_done = keys_done + $5::bigint, drain_seg = $6 \
         where correction_id = $1 and definition_id = $2",
        &[
            &correction.id,
            &definition.id,
            &phase.as_str(),
            &cursor,
            &done,
            &drain_seg,
        ],
    )
    .await?;
    Ok(())
}
