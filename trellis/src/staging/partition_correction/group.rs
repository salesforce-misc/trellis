//! The correction of an aggregate on the group basis over a partitioned
//! source (#1103, ADR-0002 "Aggregates: a basis per group, kept by deltas";
//! the parent module's "On the group basis").
//!
//! Every group key contains every partition-key column (ADR-0017), so each
//! group routes to exactly one node of the tree. A node's operation is
//! corrected by **recomputing the groups that route to it**, never by
//! zeroing them (#1043 F4a: a `TRUNCATE` and an `INSERT` into the node in one
//! transaction leaves a row a zero would erase). The set is, in one
//! statement ([`routed_groups`], #1043 F4b):
//!
//! - the target's groups that route to the node: every row of its hidden
//!   table, zero groups included;
//! - the groups of the images of every change of the source still in the
//!   ring that route to it: a change committed before the operation and not
//!   yet applied may name a group the target doesn't have, which its page
//!   would otherwise create with a member the operation removed;
//! - the groups of the node's current rows, for a node in the tree (a new,
//!   attached or truncated node, or a `DEFAULT` whose siblings changed):
//!   rows written into it directly before its capture existed are counted
//!   only here. A gone node without a usable text has its table read the
//!   same way while it still exists, as the ledger's node phase does: the
//!   region it left is covered by the complement sweep, a `DEFAULT`'s
//!   correction or the node that took it (#1129), with no full routing form.
//!
//! One statement, because a page that applies a ring change and commits
//! between two reads, its ring rows retired after it, would otherwise be in
//! neither. Whatever commits after it is either in a group of the set, which
//! the recompute's snapshot counts and its basis then refuses, or in a group
//! outside it, which the operation didn't touch.
//!
//! The recompute ([`group_basis::recompute_routed`]) then locks the set in
//! one sorted statement and rewrites each group from a `GROUP BY` of its
//! rows, with that statement's snapshot as the group's basis, in one
//! transaction with the correction's progress. The rows are read through the
//! root, one index probe per group, or, for a node in a tree that is still
//! exactly as recorded, by one `GROUP BY` of the node ([`node_to_read`]).
//! Every change the operation removed was committed before the pass queued
//! the correction (the operation's lock on the node waits for its writers; a
//! detach that is still pending is waited for), so the basis sees its
//! transaction, and its page refuses it. No `build_seg` stamp and no fence
//! are needed. The definition reports `backfilling` from the pass that
//! schedules the correction until the transaction commits, as under the
//! ledger's engine, with no group deltas for the build's completion to wait
//! on.

use tokio_postgres::IsolationLevel;

use crate::capture::partitions::{self, PendingCorrection};
use crate::defs::ddl;
use crate::defs::model::Definition;
use crate::pool::{Pool, quote_ident, quote_literal};
use crate::staging::append::{RING_SIZE, ring_table_name};
use crate::staging::group_basis::{self, GroupShape};

use super::{
    Ancestry, CHUNK_APPLICATION_NAME, CorrectionError, Outcome, Phase, Route, Step, ancestry,
    budget_spent, complement_unevaluable, covered_by_parent, detach_pending, mark_done,
    pause_before_commit, progress, reads_node, route_columns_named, route_of, route_predicate,
};
use crate::defs::chunk_queue::ClaimFence;

/// Runs correction `correction` for `definition`, which the group basis
/// takes with `shape`: one transaction, under `fence`.
pub(super) async fn correct(
    pool: &Pool,
    definition: &Definition,
    shape: &GroupShape,
    correction: &PendingCorrection,
    options: &super::Options,
    outcome: &mut Outcome,
    fence: &ClaimFence<'_>,
) -> Result<Step, CorrectionError> {
    if covered_by_parent(pool, correction).await?
        || complement_unevaluable(pool, correction).await?
    {
        return Ok(if mark_done(pool, fence, correction, definition).await? {
            Step::Done
        } else {
            Step::Superseded
        });
    }
    {
        let client = pool.get().await?;
        if progress(&**client, correction.id, definition.id)
            .await?
            .is_some_and(|p| p.phase() == Phase::Done.as_str())
        {
            return Ok(Step::Done);
        }
        if detach_pending(&**client, correction).await? {
            return Ok(Step::Waiting);
        }
    }
    if budget_spent(options, outcome) {
        return Ok(Step::Stopped);
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
    let groups = routed_groups(
        pool,
        definition,
        shape,
        correction,
        &route,
        recorded.as_ref(),
    )
    .await?;

    let mut client = pool.get().await?;
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
        return Ok(Step::Superseded);
    }
    // Test-only pause point (#1103): between the enumeration and the
    // recompute. See `super::super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    crate::staging::interleave::pause_at(
        &*txn,
        crate::staging::interleave::PausePoint::BeforeCorrectionRecompute,
        &definition.target_table,
    )
    .await?;
    if !groups.is_empty() {
        let node = node_to_read(&txn, correction, recorded.as_ref()).await?;
        group_basis::recompute_routed(&txn, pool.schema(), definition, &groups, node.as_deref())
            .await?;
    }
    let count = i64::try_from(groups.len()).unwrap_or(i64::MAX);
    txn.execute(
        "insert into partition_correction_progress \
             (correction_id, definition_id, phase, keys_done, keys_total) \
         values ($1, $2, 'done', $3, $3) \
         on conflict (correction_id, definition_id) do update \
         set phase = 'done', cursor = null, keys_done = $3, keys_total = $3",
        &[&correction.id, &definition.id, &count],
    )
    .await?;
    pause_before_commit(&txn, definition).await?;
    txn.commit().await?;
    outcome.pages += 1;
    outcome.keys += count;
    Ok(Step::Done)
}

/// The groups `correction` recomputes for `definition` (see the module
/// doc), each an object of the group columns as text, read in one statement
/// under the pinned settings (`partitions::pinned_settings`), which the
/// constraint text's meaning depends on.
async fn routed_groups(
    pool: &Pool,
    definition: &Definition,
    shape: &GroupShape,
    correction: &PendingCorrection,
    route: &Route,
    recorded: Option<&partitions::Recorded>,
) -> Result<Vec<String>, CorrectionError> {
    let schema = quote_ident(pool.schema());
    let names: Vec<&str> = shape.group_columns().iter().map(String::as_str).collect();
    let source_row = ddl::qualified_source_table(&definition.source_table);
    // The group columns of a row aliased `alias`, each as its hidden
    // column's type, so the parts' rows compare and deduplicate as values.
    let values = |alias: &str| -> String {
        names
            .iter()
            .zip(shape.group_types())
            .map(|(n, t)| format!("{alias}.{c}::{t} as {c}", c = quote_ident(n)))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let object = format!(
        "jsonb_build_object({})",
        names
            .iter()
            .map(|n| format!("{}, u.{}", quote_literal(n), quote_ident(n)))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let mut parts: Vec<String> = Vec::new();
    if *route != Route::Unusable {
        let hidden = group_basis::hidden_table_ident(pool.schema(), definition.id);
        let predicate = route_predicate(route, &route_columns_named(&names, recorded, "h"));
        parts.push(format!(
            "select {} from {hidden} h where {predicate}",
            values("h")
        ));
        let predicate = route_predicate(route, &route_columns_named(&names, recorded, "s"));
        for slot in 0..RING_SIZE {
            let ring = ring_table_name(slot).map_err(CorrectionError::Staging)?;
            parts.push(format!(
                "select {o} from {schema}.{ring} r \
                 cross join lateral (values (r.old_image), (r.new_image), \
                     (r.transition_images -> 'old'), (r.transition_images -> 'new')) as i(img) \
                 cross join lateral jsonb_populate_record(null::{source_row}, i.img) as s \
                 where r.src_table = $1 and jsonb_typeof(i.img) = 'object' and {predicate}",
                o = values("s"),
            ));
        }
    }
    let mut client = pool.get().await?;
    let txn = client
        .build_transaction()
        .isolation_level(IsolationLevel::ReadCommitted)
        .start()
        .await?;
    txn.batch_execute(&partitions::pinned_settings()).await?;
    // The node is read under the session's own lock wait: an operation on it
    // still running holds it only until it commits.
    txn.batch_execute("set local lock_timeout to default")
        .await?;
    // A gone node routed by its text needs no read of its rows: every row
    // it held satisfied the text, so its group is the target's or a ring
    // image's (`super::reads_node`).
    if reads_node(correction, route) {
        // By oid, so a rename doesn't lose it; none once it is dropped.
        let node: Option<String> = txn
            .query_opt(
                "select pg_catalog.format('%I.%I', n.nspname, c.relname) \
                 from pg_catalog.pg_class c \
                 join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
                 where c.oid = $1::bigint::oid and c.relkind in ('r', 'p')",
                &[&i64::from(correction.node_oid)],
            )
            .await?
            .map(|row| row.get(0));
        if let Some(node) = node {
            parts.push(format!("select {} from {node} n", values("n")));
        }
    }
    if parts.is_empty() {
        txn.commit().await?;
        return Ok(Vec::new());
    }
    let sql = format!(
        "select {object}::text from ({}) as u order by 1",
        parts.join(" union ")
    );
    let rows = if *route == Route::Unusable {
        txn.query(&sql, &[]).await?
    } else {
        txn.query(&sql, &[&correction.table]).await?
    };
    txn.commit().await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

/// The node a correction of a node in the tree (a new, attached or
/// truncated one) reads its groups' rows from, quoted and qualified, locked
/// `ACCESS SHARE` for the rest of `txn`; `None` to read them through the
/// root. Every group in the set routes to the node, and the group key holds
/// every partition-key column, so the node holds every row of every one of
/// them while the whole tree is as recorded: every node under the same
/// parent, by the same `pg_inherits` row, none detach-pending and none
/// added. The node's own row is not enough: a sibling attached beside a
/// `DEFAULT`, or an ancestor re-attached under another bound, moves groups
/// the set routed to the node into another node, whose rows a read of this
/// node alone would zero. The check runs after the lock, so it reads the
/// catalog as the lock left it, and the lock keeps the node's region from
/// narrowing after it: a `DROP`, `DETACH` or `TRUNCATE` of the node, an
/// attach beside it or beside an ancestor `DEFAULT`, and an ancestor's
/// re-attach all lock it `ACCESS EXCLUSIVE`. A tree that changed since the
/// record is read through the root: the pass queues the change's own
/// corrections.
async fn node_to_read(
    txn: &tokio_postgres::Transaction<'_>,
    correction: &PendingCorrection,
    recorded: Option<&partitions::Recorded>,
) -> Result<Option<String>, CorrectionError> {
    if correction.kind != partitions::Kind::NewLeaf.as_str()
        && correction.kind != partitions::Kind::Truncated.as_str()
    {
        return Ok(None);
    }
    let Some(recorded) = recorded else {
        return Ok(None);
    };
    if !recorded.nodes[1..]
        .iter()
        .any(|n| n.oid == correction.node_oid)
    {
        return Ok(None);
    }
    let name: Option<String> = txn
        .query_opt(
            "select pg_catalog.format('%I.%I', n.nspname, c.relname) \
             from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
             where c.oid = $1::bigint::oid and c.relkind in ('r', 'p')",
            &[&i64::from(correction.node_oid)],
        )
        .await?
        .map(|row| row.get(0));
    let Some(name) = name else {
        return Ok(None);
    };
    txn.batch_execute(&format!("lock table {name} in access share mode"))
        .await?;
    let mut oids = Vec::new();
    let mut parents = Vec::new();
    let mut xmins = Vec::new();
    for node in &recorded.nodes[1..] {
        let (Some(parent), Some(xmin)) = (node.parent, node.inherits_xmin.as_ref()) else {
            return Ok(None);
        };
        oids.push(i64::from(node.oid));
        parents.push(i64::from(parent));
        xmins.push(xmin.clone());
    }
    let as_recorded: bool = txn
        .query_one(
            "with recursive t as ( \
                 select i.inhrelid, i.inhparent, i.xmin::text as x, i.inhdetachpending as p \
                 from pg_catalog.pg_inherits i where i.inhparent = $1::bigint::oid \
                 union all \
                 select i.inhrelid, i.inhparent, i.xmin::text, i.inhdetachpending \
                 from pg_catalog.pg_inherits i join t on i.inhparent = t.inhrelid), \
             now as (select inhrelid::bigint, inhparent::bigint, x from t where not p), \
             was as (select * from unnest($2::bigint[], $3::bigint[], $4::text[])) \
             select not exists (select * from now except select * from was) \
                and not exists (select * from was except select * from now)",
            &[&i64::from(recorded.nodes[0].oid), &oids, &parents, &xmins],
        )
        .await?
        .get(0);
    Ok(as_recorded.then_some(name))
}
