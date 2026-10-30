//! The correction of an aggregate on the group basis over a partitioned
//! source (ADR-0002 "Aggregates: a basis per group, kept by deltas";
//! the parent module's "What a correction does"), and of a `JOIN` whose side is
//! the partitioned source (ADR-0019), over that side's partial cells
//! ([`Cells`]): below, a `JOIN`'s cells read as groups, its `P` or `M` as
//! the hidden table, and the side's table as the source.
//!
//! Every group key contains every partition-key column (ADR-0017), so each
//! group routes to exactly one node of the tree. A node's operation is
//! corrected by **recomputing the groups that route to it**, never by
//! zeroing them (a `TRUNCATE` and an `INSERT` into the node in one
//! transaction leaves a row a zero would erase). The set is, in one
//! statement ([`routed_groups`]):
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
//!   same way while it still exists: the
//!   region it left is covered by the complement sweep, a `DEFAULT`'s
//!   correction or the node that took it, with no full routing form.
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
//! transaction that records the correction done. The rows are read through the
//! root, one index probe per group, or, for a node in a tree that is still
//! exactly as recorded, by one `GROUP BY` of the node ([`node_to_read`]).
//! Every change the operation removed was committed before the reader
//! recorded its start ([`record_started`], before the enumeration: an
//! operation the pass detects after it queues an entry of its own), as the
//! operation's lock on the node waits for its writers and a detach that is
//! still pending is waited for, so the basis sees its
//! transaction, and its page refuses it. No `build_seg` stamp and no fence
//! are needed. The definition reports `backfilling` from the pass that
//! schedules the correction until the transaction commits.

use tokio_postgres::IsolationLevel;

use crate::capture::partitions::{self, PendingCorrection};
use crate::defs::ddl;
use crate::defs::join::{self as join_spec, HiddenTable, Side};
use crate::defs::model::Definition;
use crate::pool::{Pool, jsonb_build_object_sql, quote_ident, quote_literal};
use crate::staging::append::{RING_SIZE, ring_table_name};
use crate::staging::group_basis::{self, GroupShape};

use super::{
    Ancestry, CHUNK_APPLICATION_NAME, CorrectionError, Outcome, Phase, Route, Step, ancestry,
    budget_spent, complement_unevaluable, covered_by_parent, detach_pending, mark_done,
    pause_before_commit, progress, reads_node, route_columns_named, route_of, route_predicate,
};
use crate::defs::chunk_queue::ClaimFence;

/// What a correction recomputes: a group-basis aggregate's groups, or one
/// side's partial cells of a `JOIN`.
#[derive(Clone, Copy)]
pub(super) enum Cells<'a> {
    /// The aggregate's groups, which the group basis takes with this shape.
    Groups(&'a GroupShape),
    /// The `P` (`FROM`) or `M` (joined) cells of a `JOIN` whose side is the
    /// correction's table. Each cell's key holds every partition-key column
    /// (ADR-0019), so it routes to exactly one node as a group does, and its
    /// recompute carries its change into `T` under its `J` lock
    /// (`crate::staging::join::recompute_routed`).
    Join(Side),
}

/// Runs correction `correction` for `definition`'s `cells`: one
/// transaction, under `fence`.
pub(super) async fn correct(
    pool: &Pool,
    definition: &Definition,
    cells: Cells<'_>,
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
    if !record_started(pool, fence, correction, definition).await? {
        return Ok(Step::Superseded);
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
        cells,
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
    // Test-only pause point: between the enumeration and the
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
        match cells {
            Cells::Groups(_) => {
                group_basis::recompute_routed(
                    &txn,
                    pool.schema(),
                    definition,
                    &groups,
                    node.as_deref(),
                )
                .await?;
            }
            Cells::Join(side) => {
                crate::staging::join::recompute_routed(
                    &txn,
                    pool.schema(),
                    definition,
                    side,
                    &groups,
                    node.as_deref(),
                )
                .await?;
            }
        }
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

/// Records that `definition` has started `correction`, under `fence`, before
/// it reads anything of the node. The pass and the seal let a queued entry
/// absorb a later operation on its node only while no reader has a progress
/// row of it (ADR-0017), and this reader's enumeration may already miss what
/// that operation changed: once the row is there, the operation queues an
/// entry of its own. Returns `false`, with nothing written, when the fence no
/// longer holds.
async fn record_started(
    pool: &Pool,
    fence: &ClaimFence<'_>,
    correction: &PendingCorrection,
    definition: &Definition,
) -> Result<bool, CorrectionError> {
    let mut client = pool.get().await?;
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
        return Ok(false);
    }
    txn.execute(
        "insert into partition_correction_progress (correction_id, definition_id, phase) \
         values ($1, $2, $3) on conflict do nothing",
        &[&correction.id, &definition.id, &Phase::Fence.as_str()],
    )
    .await?;
    txn.commit().await?;
    Ok(true)
}

/// The groups (or cells) `correction` recomputes for `definition` (see the
/// module doc), each an object of the group columns as text, read in one
/// statement under the pinned settings (`partitions::pinned_settings`),
/// which the constraint text's meaning depends on.
async fn routed_groups(
    pool: &Pool,
    definition: &Definition,
    cells: Cells<'_>,
    correction: &PendingCorrection,
    route: &Route,
    recorded: Option<&partitions::Recorded>,
) -> Result<Vec<String>, CorrectionError> {
    let schema = quote_ident(pool.schema());
    // The group (or cell) columns, each with its hidden column's type; the
    // hidden table; and the source whose rows and ring images they group.
    let (names, types, hidden, source_row): (Vec<String>, Vec<String>, String, String) = match cells
    {
        Cells::Groups(shape) => (
            shape.group_columns().to_vec(),
            shape.group_types().to_vec(),
            group_basis::hidden_table_ident(pool.schema(), definition.id),
            ddl::qualified_source_table(&definition.source_table),
        ),
        Cells::Join(side) => {
            let names = join_spec::side_key_columns(&definition.def, side);
            let bare = HiddenTable::partials(side).name(definition.id);
            let types = {
                let client = pool.get().await?;
                hidden_column_types(&**client, pool.schema(), &bare, &names).await?
            };
            (
                names,
                types,
                format!("{schema}.{}", quote_ident(&bare)),
                ddl::qualified_source_table(&correction.table),
            )
        }
    };
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    // The group columns of a row aliased `alias`, each as its hidden
    // column's type, so the parts' rows compare and deduplicate as values.
    let values = |alias: &str| -> String {
        names
            .iter()
            .zip(&types)
            .map(|(n, t)| format!("{alias}.{c}::{t} as {c}", c = quote_ident(n)))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let object = jsonb_build_object_sql(
        &names
            .iter()
            .map(|n| format!("{}, u.{}", quote_literal(n), quote_ident(n)))
            .collect::<Vec<_>>(),
    );
    let mut parts: Vec<String> = Vec::new();
    if *route != Route::Unusable {
        let predicate = route_predicate(route, &route_columns_named(&names, recorded, "h"));
        parts.push(format!(
            "select {} from {hidden} h where {predicate}",
            values("h")
        ));
        let predicate = route_predicate(route, &route_columns_named(&names, recorded, "s"));
        // Only a change's images are rows of the source. A marker's object
        // (a node truncate's `{node, oid}`, a schema change's `{missing,
        // key_missing}`) is not, and populated as one it would fill a source
        // column of that name.
        for slot in 0..RING_SIZE {
            let ring = ring_table_name(slot).map_err(CorrectionError::Staging)?;
            parts.push(format!(
                "select {o} from {schema}.{ring} r \
                 cross join lateral (values (r.old_image), (r.new_image), \
                     (r.transition_images -> 'old'), (r.transition_images -> 'new')) as i(img) \
                 cross join lateral jsonb_populate_record(null::{source_row}, i.img) as s \
                 where r.src_table = $1 and r.op in ('insert', 'update', 'delete', 'recompute') \
                   and jsonb_typeof(i.img) = 'object' and {predicate}",
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
    // The node is read under the session's own lock wait (its `lock_timeout`
    // cap, which a `set local ... to default` would drop): an operation on it
    // still running holds it only until it commits.
    let session_lock_timeout: String = txn
        .query_one("select current_setting('lock_timeout')", &[])
        .await?
        .get(0);
    txn.batch_execute(&partitions::pinned_settings()).await?;
    txn.execute(
        "select set_config('lock_timeout', $1, true)",
        &[&session_lock_timeout],
    )
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

/// The exact type (`format_type`) of each of `columns` of the hidden table
/// `bare` in `schema`, in `columns`' order.
async fn hidden_column_types(
    client: &impl tokio_postgres::GenericClient,
    schema: &str,
    bare: &str,
    columns: &[String],
) -> Result<Vec<String>, CorrectionError> {
    let rows = client
        .query(
            &format!(
                "select {} \
                 from unnest($3::text[]) with ordinality as c(name, ord) \
                 join pg_catalog.pg_attribute a \
                   on a.attrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', $1::text, $2::text)) \
                  and a.attname = c.name and not a.attisdropped \
                 order by c.ord",
                crate::defs::copy_type::type_sql("a.atttypid")
            ),
            &[&schema, &bare, &columns],
        )
        .await?;
    if rows.len() != columns.len() {
        return Err(CorrectionError::Unroutable(format!(
            "the hidden table {schema}.{bare} lacks a column of {columns:?}"
        )));
    }
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
    Ok(lock_recorded_node(txn, correction.node_oid, recorded)
        .await?
        .filter(|node| node.as_recorded)
        .map(|node| node.name))
}

/// A node of the recorded tree, locked by [`lock_recorded_node`].
pub(crate) struct LockedNode {
    /// Quoted and qualified.
    pub name: String,
    /// Whether the whole tree is exactly as recorded (see [`node_to_read`]),
    /// as of the lock.
    pub as_recorded: bool,
}

/// Locks the node `node_oid` of `recorded` `ACCESS SHARE` for the rest of
/// `txn` and checks the tree against the record (see [`node_to_read`]).
/// `None` when the node is gone.
///
/// The node's recorded ancestors are locked first, root down, each itself
/// only (an ancestor that is gone is skipped): a user's `TRUNCATE` or DDL of
/// a partitioned table locks it, then the tables under it, and a later
/// statement of this transaction names the root's row type, or reads
/// through the root, which locks the ancestors. Locking the node first would
/// deadlock with the user's statement, and Postgres would fail it.
pub(crate) async fn lock_recorded_node(
    txn: &tokio_postgres::Transaction<'_>,
    node_oid: u32,
    recorded: &partitions::Recorded,
) -> Result<Option<LockedNode>, tokio_postgres::Error> {
    let mut ancestors = vec![recorded.nodes[0].oid];
    let mut at = node_oid;
    while let Some(parent) = recorded
        .nodes
        .iter()
        .find(|n| n.oid == at)
        .and_then(|n| n.parent)
    {
        if ancestors.contains(&parent) {
            break;
        }
        ancestors.insert(1, parent);
        at = parent;
    }
    let mut name = None;
    for (oid, only) in ancestors
        .iter()
        .map(|oid| (*oid, "only "))
        .chain([(node_oid, "")])
    {
        name = txn
            .query_opt(
                "select pg_catalog.format('%I.%I', n.nspname, c.relname) \
                 from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
                 where c.oid = $1::bigint::oid and c.relkind in ('r', 'p')",
                &[&i64::from(oid)],
            )
            .await?
            .map(|row| row.get::<_, String>(0));
        if let Some(name) = &name {
            txn.batch_execute(&format!("lock table {only}{name} in access share mode"))
                .await?;
        }
    }
    let Some(name) = name else {
        return Ok(None);
    };
    let mut oids = Vec::new();
    let mut parents = Vec::new();
    let mut xmins = Vec::new();
    for node in &recorded.nodes[1..] {
        let (Some(parent), Some(xmin)) = (node.parent, node.inherits_xmin.as_ref()) else {
            return Ok(Some(LockedNode {
                name,
                as_recorded: false,
            }));
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
    Ok(Some(LockedNode { name, as_recorded }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capture::partitions::{Node, Recorded};

    fn node(oid: u32, parent: Option<u32>) -> Node {
        Node {
            oid,
            parent,
            name: String::new(),
            partitioned: parent.is_none(),
            is_default: false,
            bound: None,
            inherits_xmin: None,
            constraint: None,
        }
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

    /// A user's `TRUNCATE` (or any DDL) of a partitioned table locks it,
    /// then the tables under it. A node lock that took the leaf first would
    /// hold it while a later statement of the same transaction (the
    /// enumeration's `jsonb_populate_record(null::root, ..)`, or a read
    /// through the root) waited for the user's table: a deadlock Postgres
    /// resolves by failing the user's statement. The node's ancestors are
    /// locked first, root down, so the lock waits behind the user's
    /// statement holding nothing under it, and the statement still gets the
    /// leaf: at once, which a `NOWAIT` checks, whichever ancestor the user
    /// holds.
    #[tokio::test]
    async fn locking_a_node_takes_its_ancestors_first() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_empty_database().await;
        let admin = connect(db.dsn()).await;
        admin
            .batch_execute(
                "create table public.r (id int, c int, d int, primary key (id, c, d)) \
                     partition by list (c); \
                 create table public.r_m partition of public.r for values in (0) \
                     partition by list (d); \
                 create table public.r_l partition of public.r_m for values in (0);",
            )
            .await
            .expect("a two-level partitioned table");
        let mut oids = Vec::new();
        for name in ["public.r", "public.r_m", "public.r_l"] {
            let oid: u32 = admin
                .query_one(&format!("select '{name}'::regclass::oid"), &[])
                .await
                .expect("oid")
                .get(0);
            oids.push(oid);
        }
        let (root, mid, leaf) = (oids[0], oids[1], oids[2]);
        let recorded = Recorded {
            nodes: vec![
                node(root, None),
                node(mid, Some(root)),
                node(leaf, Some(mid)),
            ],
            keys: Vec::new(),
        };

        for held in ["public.r", "public.r_m"] {
            // The user's transaction holds `held`, as `TRUNCATE held` does
            // first.
            let mut user = connect(db.dsn()).await;
            let user_txn = user.transaction().await.expect("begin");
            user_txn
                .batch_execute(&format!("lock table only {held} in access exclusive mode"))
                .await
                .expect("the user's table");

            let mut worker = connect(db.dsn()).await;
            let pid: i32 = worker
                .query_one("select pg_catalog.pg_backend_pid()", &[])
                .await
                .expect("pid")
                .get(0);
            let (locked_tx, mut locked_rx) = tokio::sync::oneshot::channel();
            let (commit_tx, commit_rx) = tokio::sync::oneshot::channel::<()>();
            let recorded = recorded.clone();
            let task = tokio::spawn(async move {
                let txn = worker.transaction().await.expect("begin");
                lock_recorded_node(&txn, leaf, &recorded)
                    .await
                    .expect("lock the node")
                    .expect("the node exists");
                let _ = locked_tx.send(());
                let _ = commit_rx.await;
                txn.commit().await.expect("commit");
            });

            // Until the worker waits on a lock, or (locking the leaf first)
            // has its locks.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            loop {
                let waiting: bool = admin
                    .query_one(
                        "select coalesce((select wait_event_type = 'Lock' \
                             from pg_catalog.pg_stat_activity where pid = $1), false)",
                        &[&pid],
                    )
                    .await
                    .expect("the worker's wait")
                    .get(0);
                if waiting || locked_rx.try_recv().is_ok() {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "the worker neither waited nor locked"
                );
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }

            user_txn
                .batch_execute("lock table only public.r_l in access exclusive mode nowait")
                .await
                .unwrap_or_else(|err| {
                    panic!("holding {held}, the leaf is free: the worker holds nothing under it ({err})")
                });
            user_txn.commit().await.expect("commit");
            let _ = commit_tx.send(());
            task.await.expect("the worker");
        }
    }
}
