//! What the reconcile pass records about a partitioned source, and how it
//! detects partition maintenance (ADR-0017, P3 of epic #897).
//!
//! A statement trigger fires only for the table a statement names, so
//! `ATTACH`, `DETACH`, `DROP` and a new leaf change a partitioned root's rows
//! without a write any trigger sees. The pass finds them by catalog state.
//!
//! # Two fingerprints, every pass
//!
//! One statement per pass hashes two catalog views of the source, on the
//! server (about 2-3 ms at 1,000 leaves, `tests/partition_fingerprints.rs`
//! records it):
//!
//! - the **lifecycle** fingerprint covers every node reached from the root
//!   through `pg_inherits`: its oid, its parent, `inhdetachpending`, the
//!   `pg_inherits` row's `xmin` and its `relpartbound`. The `xmin` is what
//!   shows a leaf detached, modified and re-attached under the same bound
//!   within one pass.
//! - the **compliance** fingerprint covers the root's own identity (oid, kind,
//!   whether it is a partition, schema and name) and its `pg_inherits` rows
//!   in both directions. It sees the root joining a hierarchy or being
//!   renamed, which the lifecycle fingerprint, walking down from the root,
//!   cannot.
//!
//! It does not cover the root's `pg_class.xmin`, which moves on `GRANT`, an
//! owner change, a reloption, `VACUUM FULL` and `CLUSTER` and would force a
//! deep check, and a correction, for each. (A `VACUUM FREEZE` of `pg_inherits`
//! itself leaves the `xmin` column as it was: it reports the raw value, and
//! freezing only sets a flag.)
//!
//! # The deep check
//!
//! A **deep check** runs when either fingerprint differs from the stored one,
//! when none is stored, once after the process starts (the memory of having
//! checked is per process), and every pass while the source is recorded as
//! non-compliant. It reads the tree, then
//!
//! 1. pauses every reader the source no longer complies for (the P1 checks:
//!    [`PartitionTree::refusal`]), recording why in `capture_failure`; a
//!    `RESUME` refuses until it complies, and rebuilds;
//! 2. classifies each node against the recorded ones ([`plan`]): created,
//!    attached or re-attached (a new `pg_inherits.xmin`, parent or bound),
//!    a `DEFAULT` whose sibling set changed (treated as new), gone (dropped,
//!    detached, or a detach pending: a pending detach already stages nothing,
//!    so it is gone here), or the whole source renamed away and back;
//! 3. fetches `pg_get_partition_constraintdef` for the new or changed nodes
//!    only, in short transactions under a short `lock_timeout` and pinned
//!    settings, retrying on a lock timeout (reading 1,000 texts holds 1,005
//!    locks and blocks behind uncommitted DDL). A fetch that stays blocked
//!    leaves the check for the next pass and records nothing.
//!
//! The pass then installs capture on every node (`capture::install`), and
//! only when that completes does [`commit`] record the nodes, the
//! fingerprints and the corrections in one transaction. The fingerprints are
//! therefore stored once the corrections are queued, not once they have
//! drained, and a restart finds the queue. A correction that is only
//! queued is a request: the correction engine (P4) consumes
//! [`pending_corrections`] and calls [`complete_correction`] once a node's
//! groups are corrected, and only after the ring is folded through the
//! row's `queued_lsn`. A `gone_node` whose `pg_inherits` row is still
//! detach-pending needs more: a writer with an older snapshot can still
//! write through the parent into the leaf, and the root's trigger stages
//! that after `queued_lsn`, until the detach completes. So P4 corrects such
//! a node only once that row is gone, folding through a WAL position read
//! after that. A `gone_node`'s text is `None` for a `DEFAULT` with no
//! siblings (its constraint is empty: every group routes to it) and for a
//! node detached between the tree read and the fetch.
//!
//! A node queued again before P4 takes it adds nothing, unless it left the
//! tree under another recorded text: its groups route by each.
//!
//! # A renamed root
//!
//! Capture's guard names the root, so a renamed root stages nothing under the
//! old name. The recorded root oid says what happened: when the pass finds
//! the recorded oid alive under another name it marks the state `stale`, and
//! the deep check that follows (the name resolves again, or to a new root)
//! queues every leaf of an unchanged root, or classifies a replaced root as
//! gone plus created. Capture's functions never need regenerating: they name
//! the source, and the source is again what the name resolves to.
//!
//! No event trigger is used, and nothing here needs a superuser.

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, PoisonError};

use serde::{Deserialize, Serialize};
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, GenericClient};

use super::CaptureError;
use super::columns::{CaptureCatalog, CaptureReader, reads_through};
use crate::defs::ast::{GroupByKey, KeySpace};
use crate::defs::ddl::regclass_arg;
use crate::defs::model::RelationshipCardinality;
use crate::defs::partition_tree::{
    self, KeyRole, PartitionRefusal, PartitionTree, read_partition_tree, server_version_num,
};

/// Constraint texts fetched in one transaction. A transaction holds one lock
/// per leaf it reads, so this keeps it far below the lock table's limit and
/// short.
const FETCH_BATCH: usize = 100;

/// Attempts at one batch of texts before the check is left for the next pass.
const FETCH_ATTEMPTS: usize = 3;

/// The `lock_timeout` of a batch of texts.
const FETCH_LOCK_TIMEOUT: &str = "250ms";

/// One recorded table of the tree. `nodes[0]` of a [`Recorded`] is the root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    pub oid: u32,
    /// The parent's oid, `None` for the root.
    pub parent: Option<u32>,
    /// Unquoted `schema.table` when recorded.
    pub name: String,
    pub partitioned: bool,
    pub is_default: bool,
    /// `pg_get_expr(relpartbound)`, `None` for the root.
    pub bound: Option<String>,
    /// The node's `pg_inherits.xmin`. `None` for the root.
    pub inherits_xmin: Option<String>,
    /// `pg_get_partition_constraintdef`, fetched under pinned settings, for
    /// every node but the root. `None` until fetched.
    pub constraint: Option<String>,
}

/// One partition-key column of a partitioned node, as a correction casts a
/// group to it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Key {
    /// The partitioned node's oid.
    pub node: u32,
    pub column: Option<String>,
    /// `format_type(atttypid, atttypmod)`.
    pub pg_type: String,
    /// The key's collation, schema-qualified, empty when the type has none.
    pub collation: String,
}

/// The state recorded for one source: the tree as the pass last saw it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Recorded {
    pub nodes: Vec<Node>,
    pub keys: Vec<Key>,
}

impl Recorded {
    fn root_oid(&self) -> u32 {
        self.nodes[0].oid
    }
}

/// What a queued correction asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Correct the groups of this leaf's current rows: it was created,
    /// attached or re-attached, or its text changed with its siblings.
    NewLeaf,
    /// Correct the groups that route to this node by its recorded constraint
    /// text: it was dropped or detached, and every node under it with it.
    GoneNode,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::NewLeaf => "new_leaf",
            Kind::GoneNode => "gone_node",
        }
    }
}

/// Why a node is corrected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cause {
    /// A node the record doesn't have: a new leaf, or one attached.
    Created,
    /// A recorded node whose parent, bound or `pg_inherits` row changed: it
    /// was detached and attached again.
    Reattached,
    /// A `DEFAULT` whose sibling set changed, so its text did.
    DefaultSiblings,
    /// The root was renamed away while recorded and is back: nothing staged
    /// meanwhile.
    Renamed,
    /// A recorded node that is gone.
    Gone,
}

impl Cause {
    pub fn as_str(self) -> &'static str {
        match self {
            Cause::Created => "created",
            Cause::Reattached => "reattached",
            Cause::DefaultSiblings => "default_siblings",
            Cause::Renamed => "renamed",
            Cause::Gone => "gone",
        }
    }
}

/// One correction a deep check queues.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Correction {
    pub kind: Kind,
    pub node: u32,
    pub name: String,
    pub cause: Cause,
    /// For a [`Kind::GoneNode`], the constraint text recorded while the node
    /// was in the tree.
    pub constraint: Option<String>,
}

/// What a deep check works out from the recorded and the live tree.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Plan {
    pub corrections: Vec<Correction>,
    /// The live nodes whose constraint text must be fetched: the new or
    /// changed ones, a `DEFAULT` whose siblings changed, and any whose text
    /// was never recorded.
    pub fetch: Vec<u32>,
}

/// Classifies the live tree against the recorded one. `prev` is `None` the
/// first time a source is seen: the definition's build covers what is there,
/// so there is nothing to correct and every text is fetched. `renamed` says
/// the recorded root was renamed away and back, so nothing staged meanwhile.
///
/// Pure: the decision over two catalog snapshots, tested without a database.
pub fn plan(prev: Option<&Recorded>, live: &Recorded, renamed: bool) -> Plan {
    let Some(prev) = prev else {
        return Plan {
            corrections: Vec::new(),
            fetch: live.nodes.iter().skip(1).map(|n| n.oid).collect(),
        };
    };
    let prev_by: HashMap<u32, &Node> = prev.nodes.iter().map(|n| (n.oid, n)).collect();
    let live_by: HashMap<u32, &Node> = live.nodes.iter().map(|n| (n.oid, n)).collect();

    // Each live node's own cause.
    let mut own: HashMap<u32, Cause> = HashMap::new();
    for node in live.nodes.iter().skip(1) {
        match prev_by.get(&node.oid) {
            None => {
                own.insert(node.oid, Cause::Created);
            }
            Some(old)
                if old.parent != node.parent
                    || old.bound != node.bound
                    || old.inherits_xmin != node.inherits_xmin =>
            {
                own.insert(node.oid, Cause::Reattached);
            }
            Some(_) => {}
        }
    }
    // A `DEFAULT`'s text is every sibling's complement.
    for node in live.nodes.iter().skip(1).filter(|n| n.is_default) {
        if own.contains_key(&node.oid) {
            continue;
        }
        let siblings = |nodes: &[Node]| -> HashSet<(u32, Option<String>)> {
            nodes
                .iter()
                .filter(|n| n.parent == node.parent && n.oid != node.oid && n.parent.is_some())
                .map(|n| (n.oid, n.bound.clone()))
                .collect()
        };
        if siblings(&live.nodes) != siblings(&prev.nodes) {
            own.insert(node.oid, Cause::DefaultSiblings);
        }
    }

    // Every node under a new or re-attached one is as new as it is. Nodes
    // are ordered root first, then by depth, so a parent is settled before
    // its children.
    let rename_all = renamed && prev.root_oid() == live.root_oid();
    let mut settled: HashMap<u32, Option<Cause>> = HashMap::new();
    let mut corrections = Vec::new();
    let mut fetch = Vec::new();
    for node in live.nodes.iter().skip(1) {
        let inherited = node.parent.and_then(|p| settled.get(&p).copied().flatten());
        let mut cause = own.get(&node.oid).copied().or(inherited);
        if cause.is_none() && rename_all && !node.partitioned {
            cause = Some(Cause::Renamed);
        }
        settled.insert(node.oid, cause);
        let text_unknown = prev_by
            .get(&node.oid)
            .is_none_or(|old| old.constraint.is_none());
        if cause.is_some() || text_unknown {
            fetch.push(node.oid);
        }
        if let Some(cause) = cause
            && !node.partitioned
        {
            corrections.push(Correction {
                kind: Kind::NewLeaf,
                node: node.oid,
                name: node.name.clone(),
                cause,
                constraint: None,
            });
        }
    }

    // The topmost node of each gone subtree, routed by the text recorded
    // while it was in the tree.
    for old in &prev.nodes {
        if live_by.contains_key(&old.oid) {
            continue;
        }
        if old.parent.is_some_and(|p| !live_by.contains_key(&p)) {
            continue;
        }
        corrections.push(Correction {
            kind: Kind::GoneNode,
            node: old.oid,
            name: old.name.clone(),
            cause: Cause::Gone,
            constraint: old.constraint.clone(),
        });
    }
    Plan { corrections, fetch }
}

/// Carries the recorded constraint text onto each live node the plan doesn't
/// fetch.
fn carry_constraints(prev: Option<&Recorded>, live: &mut Recorded, fetch: &[u32]) {
    let Some(prev) = prev else { return };
    let fetching: HashSet<u32> = fetch.iter().copied().collect();
    let by: HashMap<u32, &Node> = prev.nodes.iter().map(|n| (n.oid, n)).collect();
    for node in &mut live.nodes {
        if !fetching.contains(&node.oid)
            && let Some(old) = by.get(&node.oid)
        {
            node.constraint = old.constraint.clone();
        }
    }
}

/// The recorded form of a read tree: every node reachable from the root
/// without crossing a detach-pending link (a pending detach stages nothing
/// already, so the node has left the tree), with its `pg_inherits` row and
/// its partitioned nodes' keys.
///
/// `inherits` maps a node's oid to (detach pending, `xmin`).
pub fn live_from(
    tree: &PartitionTree,
    inherits: &HashMap<u32, (bool, String)>,
    keys: Vec<Key>,
) -> Recorded {
    let mut reachable: Vec<bool> = Vec::with_capacity(tree.nodes.len());
    let mut nodes = Vec::new();
    for (i, node) in tree.nodes.iter().enumerate() {
        let pending = inherits.get(&node.oid).is_some_and(|(p, _)| *p);
        let ok = match node.parent {
            None => i == 0,
            Some(p) => reachable[p] && !pending,
        };
        reachable.push(ok);
        if !ok {
            continue;
        }
        nodes.push(Node {
            oid: node.oid,
            parent: node.parent.map(|p| tree.nodes[p].oid),
            name: node.name.clone(),
            partitioned: node.partitioned,
            is_default: node.is_default,
            bound: node.bound.clone(),
            inherits_xmin: inherits.get(&node.oid).map(|(_, x)| x.clone()),
            constraint: None,
        });
    }
    let kept: HashSet<u32> = nodes.iter().map(|n| n.oid).collect();
    Recorded {
        keys: keys
            .into_iter()
            .filter(|k| kept.contains(&k.node))
            .collect(),
        nodes,
    }
}

/// The readers of `table` the tree no longer complies for, each with the
/// first reason (the P1 checks, [`PartitionTree::refusal`]): an aggregate
/// whose `GROUP BY` lacks a partition-key column, a to-many relationship's
/// to-side whose join column isn't the key, a 1-1 transform's `FROM` or a
/// to-one to-side. A definition already paused counts too: one the pass
/// paused for this tree keeps the source held until the tree complies.
pub fn reader_refusals(
    catalog: &CaptureCatalog,
    table: &str,
    tree: &PartitionTree,
    server_version_num: i32,
) -> Vec<(i64, PartitionRefusal)> {
    let mut out = Vec::new();
    for reader in &catalog.definitions {
        if let Some(refusal) = refusal_for(catalog, reader, table, tree, server_version_num) {
            out.push((reader.id, refusal));
        }
    }
    out
}

fn refusal_for(
    catalog: &CaptureCatalog,
    reader: &CaptureReader,
    table: &str,
    tree: &PartitionTree,
    server_version_num: i32,
) -> Option<PartitionRefusal> {
    if reader.source == table {
        let refusal = match &reader.def.key_space {
            KeySpace::OneToOne => Some(PartitionRefusal::OneToOneFrom {
                table: table.to_string(),
            }),
            KeySpace::Aggregate { group_by } => {
                let columns: Vec<String> = group_by
                    .iter()
                    .filter_map(|key| match key {
                        GroupByKey::Column(column) => Some(column.clone()),
                        GroupByKey::RelationshipPath { .. } => None,
                    })
                    .collect();
                tree.refusal(server_version_num, KeyRole::GroupBy, &columns)
            }
        };
        if refusal.is_some() {
            return refusal;
        }
    }
    for rel in &catalog.relationships {
        if rel.qualified_to_table() != table
            || rel.qualified_from_table() != reader.source
            || !reads_through(&reader.def, rel)
        {
            continue;
        }
        let refusal = match rel.cardinality {
            RelationshipCardinality::ToOne => Some(PartitionRefusal::ToOneToSide {
                table: table.to_string(),
            }),
            RelationshipCardinality::ToMany => tree.refusal(
                server_version_num,
                KeyRole::JoinKey,
                std::slice::from_ref(&rel.def.to_col),
            ),
        };
        if refusal.is_some() {
            return refusal;
        }
    }
    None
}

/// What the fingerprint statement reads of a source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fingerprints {
    pub root_oid: u32,
    /// Whether the table is a partitioned root that is itself no partition
    /// and sits in no inheritance hierarchy.
    pub servable: bool,
    pub lifecycle: String,
    pub compliance: String,
}

/// Reads both fingerprints of `table` (an unquoted `schema.table`) in one
/// statement. `None` when the table doesn't exist.
pub async fn read_fingerprints(
    client: &impl GenericClient,
    table: &str,
) -> Result<Option<Fingerprints>, tokio_postgres::Error> {
    let row = client
        .query_opt(
            "with recursive root as ( \
                 select c.oid, c.relkind, c.relispartition, c.relnamespace, c.relname \
                 from pg_catalog.pg_class c where c.oid = pg_catalog.to_regclass($1)), \
             t(oid, parent, pending, ix) as ( \
                 select oid, null::oid, false, ''::text from root where relkind = 'p' \
                 union all \
                 select i.inhrelid, i.inhparent, i.inhdetachpending, \
                        i.xmin::text \
                 from t join pg_catalog.pg_inherits i on i.inhparent = t.oid) \
             select r.oid::bigint, \
                    r.relkind = 'p' and not r.relispartition \
                        and not exists (select 1 from pg_catalog.pg_inherits h \
                                        where h.inhrelid = r.oid) \
                        and not exists (select 1 from pg_catalog.pg_inherits h \
                                        join pg_catalog.pg_class k on k.oid = h.inhrelid \
                                        where h.inhparent = r.oid and not k.relispartition), \
                    (select pg_catalog.md5(coalesce(pg_catalog.string_agg( \
                         pg_catalog.concat_ws('|', t.oid::text, coalesce(t.parent::text, ''), \
                             t.pending::text, t.ix, coalesce(c.relpartbound::text, '')), \
                         ',' order by t.oid), '')) \
                     from t join pg_catalog.pg_class c on c.oid = t.oid), \
                    pg_catalog.md5(pg_catalog.concat_ws('|', r.oid::text, r.relkind::text, \
                         r.relispartition::text, r.relnamespace::text, r.relname::text, \
                         coalesce((select pg_catalog.string_agg( \
                                       pg_catalog.concat_ws(':', h.inhrelid::text, \
                                           h.inhparent::text, h.inhdetachpending::text, \
                                           h.xmin::text), \
                                       ',' order by h.inhrelid, h.inhparent) \
                                   from pg_catalog.pg_inherits h \
                                   where h.inhrelid = r.oid or h.inhparent = r.oid), ''))) \
             from root r",
            &[&regclass_arg(table)],
        )
        .await?;
    Ok(row.map(|row| Fingerprints {
        root_oid: row.get::<_, i64>(0) as u32,
        servable: row.get(1),
        lifecycle: row.get(2),
        compliance: row.get(3),
    }))
}

/// What is stored for a source.
#[derive(Debug, Clone)]
struct Stored {
    root_oid: u32,
    lifecycle: String,
    compliance: String,
    recorded: Recorded,
    stale: bool,
    noncompliant: bool,
}

async fn load_state(
    client: &impl GenericClient,
    table: &str,
) -> Result<Option<Stored>, CaptureError> {
    let Some(row) = client
        .query_opt(
            "select root_oid, lifecycle_fp, compliance_fp, nodes::text, stale, noncompliant \
             from partition_sources where table_name = $1",
            &[&table],
        )
        .await?
    else {
        return Ok(None);
    };
    let text: String = row.get(3);
    // A row this binary can't read is as good as none: the deep check records
    // the tree again.
    let Ok(recorded) = serde_json::from_str::<Recorded>(&text) else {
        return Ok(None);
    };
    Ok(Some(Stored {
        root_oid: row.get::<_, i64>(0) as u32,
        lifecycle: row.get(1),
        compliance: row.get(2),
        recorded,
        stale: row.get(4),
        noncompliant: row.get(5),
    }))
}

/// The sources whose deep check ran in this process, per instance. A deep
/// check also runs after a restart, which this memory forgets.
static CHECKED: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());

fn was_checked(instance: &str, table: &str) -> bool {
    CHECKED
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .any(|(i, t)| i == instance && t == table)
}

fn mark_checked(instance: &str, table: &str) {
    let mut checked = CHECKED.lock().unwrap_or_else(PoisonError::into_inner);
    if !checked.iter().any(|(i, t)| i == instance && t == table) {
        checked.push((instance.to_string(), table.to_string()));
    }
}

/// Forgets which sources this process has deep-checked for the instance in
/// `database` and `schema`, as a process restart would.
pub fn forget_checked(database: &str, schema: &str) {
    let instance = super::reconcile::instance_key(database, schema);
    CHECKED
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .retain(|(i, _)| *i != instance);
}

/// What [`begin`] decided about a table.
pub(crate) enum Begin {
    /// Not a partitioned root the pass can serve (a plain table, a partition,
    /// an inheritance table, a table that doesn't exist): the hierarchy check
    /// and the plain-table path decide.
    NotPartitioned,
    /// The source stopped complying and every reader is paused, or the tree
    /// changed under the read: install nothing, leave its capture as it is.
    /// `deep` says whether a deep check ran.
    Held { deep: bool },
    /// Install capture, then [`commit`] what the check found.
    Proceed(Proceed),
}

pub(crate) struct Proceed {
    /// Whether a deep check ran.
    pub deep: bool,
    table: String,
    record: Option<Record>,
}

struct Record {
    fingerprints: Fingerprints,
    live: Recorded,
    corrections: Vec<Correction>,
}

/// The first half of the pass for `table`: both fingerprints, and a deep
/// check when they say so.
pub(crate) async fn begin(
    client: &mut Client,
    instance: &str,
    catalog: &CaptureCatalog,
    table: &str,
) -> Result<Begin, CaptureError> {
    // Until the gate opens, a partitioned root in `desired` is a table in a
    // hierarchy, which the pass pauses its readers for.
    if !crate::defs::catalog::PARTITIONED_SOURCES_CAPTURED
        && !partition_tree::gate_opened_for_tests()
    {
        return Ok(Begin::NotPartitioned);
    }
    let Some(fingerprints) = read_fingerprints(&*client, table).await? else {
        observe_missing(&*client, table).await?;
        return Ok(Begin::NotPartitioned);
    };
    if !fingerprints.servable {
        // Another table may hold the name while the recorded root is away.
        observe_missing(&*client, table).await?;
        return Ok(Begin::NotPartitioned);
    }
    let stored = load_state(&*client, table).await?;
    let unchanged = stored.as_ref().is_some_and(|s| {
        s.lifecycle == fingerprints.lifecycle
            && s.compliance == fingerprints.compliance
            && s.root_oid == fingerprints.root_oid
            && !s.stale
            && !s.noncompliant
    }) && was_checked(instance, table);
    if unchanged {
        return Ok(Begin::Proceed(Proceed {
            deep: false,
            table: table.to_string(),
            record: None,
        }));
    }

    let deferred = || {
        Begin::Proceed(Proceed {
            deep: true,
            table: table.to_string(),
            record: None,
        })
    };
    let Some(read) = read_tree(client, table).await? else {
        // A lock held by uncommitted DDL; the next pass tries again.
        return Ok(deferred());
    };
    let Some((tree, version, live)) = read else {
        // Raced a DDL; the next pass reads it again.
        return Ok(Begin::Held { deep: false });
    };
    // Every reader the tree doesn't comply for, paused or not: one paused by
    // an earlier pass (or by this check before a crash) still holds the
    // source, so the pass keeps checking and installs nothing.
    let refusals = reader_refusals(catalog, table, &tree, version);
    let unpaused: HashSet<i64> = crate::staging::schema_change::unpaused_readers(catalog, table)
        .into_iter()
        .collect();
    let to_pause: Vec<(i64, PartitionRefusal)> = refusals
        .iter()
        .filter(|(id, _)| unpaused.contains(id))
        .cloned()
        .collect();
    if !to_pause.is_empty() {
        pause_noncompliant(client, table, &to_pause).await?;
    }
    let refused: HashSet<i64> = refusals.iter().map(|(id, _)| *id).collect();
    let remaining = unpaused.iter().filter(|id| !refused.contains(id)).count();

    let prev = stored.as_ref().map(|s| &s.recorded);
    let renamed = stored.as_ref().is_some_and(|s| s.stale);
    let mut plan = plan(prev, &live, renamed);
    let mut live = live;
    carry_constraints(prev, &mut live, &plan.fetch);
    if remaining == 0 && !refusals.is_empty() {
        // Nothing reads the source until a resume rebuilds it, which covers
        // whatever changed: record the tree and its fingerprints, queue
        // nothing, and look again on every pass.
        plan.corrections.clear();
        let record = Record {
            fingerprints,
            live,
            corrections: Vec::new(),
        };
        write_state(client, table, &record, true).await?;
        mark_checked(instance, table);
        return Ok(Begin::Held { deep: true });
    }
    let Some(fetched) = fetch_constraints(client, &plan.fetch).await? else {
        return Ok(deferred());
    };
    for node in &mut live.nodes {
        if let Some(text) = fetched.get(&node.oid) {
            node.constraint = text.clone();
        }
    }
    Ok(Begin::Proceed(Proceed {
        deep: true,
        table: table.to_string(),
        record: Some(Record {
            fingerprints,
            live,
            corrections: plan.corrections,
        }),
    }))
}

/// The second half: records what [`begin`] found, with its corrections,
/// in one transaction. Call it once capture is installed on every node.
/// Returns how many corrections were queued.
pub(crate) async fn commit(
    client: &mut Client,
    instance: &str,
    proceed: Proceed,
) -> Result<usize, CaptureError> {
    let Some(record) = proceed.record else {
        return Ok(0);
    };
    let queued = record.corrections.len();
    write_state(client, &proceed.table, &record, false).await?;
    mark_checked(instance, &proceed.table);
    Ok(queued)
}

async fn write_state(
    client: &mut Client,
    table: &str,
    record: &Record,
    noncompliant: bool,
) -> Result<(), CaptureError> {
    let nodes = serde_json::to_string(&record.live).expect("a recorded tree serializes");
    let txn = client.transaction().await?;
    txn.execute(
        "insert into partition_sources as s \
             (table_name, root_oid, lifecycle_fp, compliance_fp, nodes, stale, noncompliant) \
         values ($1, $2, $3, $4, $5::text::jsonb, false, $6) \
         on conflict (table_name) do update set \
             root_oid = excluded.root_oid, lifecycle_fp = excluded.lifecycle_fp, \
             compliance_fp = excluded.compliance_fp, nodes = excluded.nodes, \
             stale = false, noncompliant = excluded.noncompliant",
        &[
            &table,
            &i64::from(record.fingerprints.root_oid),
            &record.fingerprints.lifecycle,
            &record.fingerprints.compliance,
            &nodes,
            &noncompliant,
        ],
    )
    .await?;
    for correction in &record.corrections {
        txn.execute(
            "insert into partition_corrections \
                 (table_name, kind, node_oid, node_name, cause, constraint_text) \
             values ($1, $2, $3, $4, $5, $6) \
             on conflict (table_name, kind, node_oid, \
                          (md5(coalesce(constraint_text, '')))) do nothing",
            &[
                &table,
                &correction.kind.as_str(),
                &i64::from(correction.node),
                &correction.name,
                &correction.cause.as_str(),
                &correction.constraint,
            ],
        )
        .await?;
    }
    txn.commit().await?;
    Ok(())
}

/// Pauses the readers in `refusals`, recording each one's reason.
async fn pause_noncompliant(
    client: &mut Client,
    table: &str,
    refusals: &[(i64, PartitionRefusal)],
) -> Result<(), CaptureError> {
    let txn = client.transaction().await?;
    for (id, refusal) in refusals {
        let error = format!(
            "{refusal}; fix the table, then resume the definition to rebuild it, or drop the \
             definition"
        );
        if crate::defs::lifecycle::pause_for_capture_failure(&txn, *id, table, &[], &error, None)
            .await?
        {
            crate::instance_log::warn!(transform_id = *id, table = %table, "definition paused: {error}");
        }
    }
    txn.commit().await?;
    Ok(())
}

/// A source whose name doesn't resolve to a servable root (to nothing, or to
/// another table): if the recorded root is alive under another name, it was
/// renamed, and nothing staged under its old name meanwhile, so the next deep
/// check corrects every leaf. A recorded root that is still the table the
/// name resolves to (it joined a hierarchy) is not marked.
async fn observe_missing(
    client: &impl GenericClient,
    table: &str,
) -> Result<(), tokio_postgres::Error> {
    client
        .execute(
            "update partition_sources set stale = true \
             where table_name = $1 and not stale \
               and pg_catalog.to_regclass($2) is distinct from root_oid::oid \
               and exists (select 1 from pg_catalog.pg_class c where c.oid = root_oid::oid)",
            &[&table, &regclass_arg(table)],
        )
        .await?;
    Ok(())
}

/// Reads the tree, the server version and the tree's recorded form, in one
/// short transaction under the pinned settings and a short `lock_timeout`
/// (the catalog read of a bound waits behind uncommitted DDL on the node).
/// `Some(None)` when the table stopped being a partitioned root meanwhile;
/// `None` when the read stayed blocked.
#[allow(clippy::type_complexity)]
async fn read_tree(
    client: &mut Client,
    table: &str,
) -> Result<Option<Option<(PartitionTree, i32, Recorded)>>, CaptureError> {
    let mut attempts = 0;
    loop {
        attempts += 1;
        match read_tree_once(client, table).await {
            Ok(read) => return Ok(Some(read)),
            Err(err) if err.code() == Some(&SqlState::LOCK_NOT_AVAILABLE) => {
                if attempts >= FETCH_ATTEMPTS {
                    return Ok(None);
                }
            }
            Err(err) => return Err(err.into()),
        }
    }
}

async fn read_tree_once(
    client: &mut Client,
    table: &str,
) -> Result<Option<(PartitionTree, i32, Recorded)>, tokio_postgres::Error> {
    let txn = client.transaction().await?;
    txn.batch_execute(&pinned_settings()).await?;
    let Some(tree) = read_partition_tree(&txn, table).await? else {
        return Ok(None);
    };
    let version = server_version_num(&txn).await?;
    let live = read_live(&txn, &tree).await?;
    txn.commit().await?;
    Ok(Some((tree, version, live)))
}

/// The `SET LOCAL`s of every catalog read of a node's bound.
fn pinned_settings() -> String {
    format!(
        "set local lock_timeout = '{FETCH_LOCK_TIMEOUT}'; \
         set local search_path = pg_catalog, pg_temp; \
         set local datestyle = 'ISO, YMD'; \
         set local timezone = 'UTC'"
    )
}

/// Reads the live tree's recorded form: the `pg_inherits` rows and the key
/// columns' types and collations, around the tree already read.
async fn read_live(
    client: &impl GenericClient,
    tree: &PartitionTree,
) -> Result<Recorded, tokio_postgres::Error> {
    let oids: Vec<i64> = tree.nodes.iter().map(|n| i64::from(n.oid)).collect();
    let rows = client
        .query(
            "select i.inhrelid::bigint, i.inhdetachpending, \
                    i.xmin::text \
             from pg_catalog.pg_inherits i where i.inhrelid = any($1::bigint[]::oid[])",
            &[&oids],
        )
        .await?;
    let inherits: HashMap<u32, (bool, String)> = rows
        .iter()
        .map(|r| (r.get::<_, i64>(0) as u32, (r.get(1), r.get(2))))
        .collect();
    let partitioned: Vec<i64> = tree
        .nodes
        .iter()
        .filter(|n| n.partitioned)
        .map(|n| i64::from(n.oid))
        .collect();
    let rows = client
        .query(
            "select p.partrelid::bigint, a.attname::text, \
                    coalesce(pg_catalog.format_type(a.atttypid, a.atttypmod), ''), \
                    case when k.collid = 0 then '' else coalesce( \
                        (select n.nspname::text || '.' || co.collname::text \
                         from pg_catalog.pg_collation co \
                         join pg_catalog.pg_namespace n on n.oid = co.collnamespace \
                         where co.oid = k.collid), '') end \
             from pg_catalog.pg_partitioned_table p \
             cross join lateral unnest(p.partattrs::int2[], p.partcollation::oid[]) \
                 with ordinality as k(attnum, collid, ord) \
             left join pg_catalog.pg_attribute a \
                 on a.attrelid = p.partrelid and a.attnum = k.attnum and k.attnum > 0 \
             where p.partrelid = any($1::bigint[]::oid[]) \
             order by 1, k.ord",
            &[&partitioned],
        )
        .await?;
    let keys = rows
        .iter()
        .map(|r| Key {
            node: r.get::<_, i64>(0) as u32,
            column: r.get(1),
            pg_type: r.get(2),
            collation: r.get(3),
        })
        .collect();
    Ok(live_from(tree, &inherits, keys))
}

/// Fetches `pg_get_partition_constraintdef` of each of `oids`, `Some(map)`
/// when every batch was read. A batch runs in its own short transaction
/// under pinned settings (`search_path`, `DateStyle`, `TimeZone`: the text
/// of a bound depends on them) and a short `lock_timeout`; a lock timeout
/// retries the batch, and `None` is returned when it stays blocked. A node
/// dropped meanwhile reads as `None` text.
async fn fetch_constraints(
    client: &mut Client,
    oids: &[u32],
) -> Result<Option<HashMap<u32, Option<String>>>, CaptureError> {
    let mut out: HashMap<u32, Option<String>> = HashMap::with_capacity(oids.len());
    for batch in oids.chunks(FETCH_BATCH) {
        let ids: Vec<i64> = batch.iter().map(|o| i64::from(*o)).collect();
        let mut attempts = 0;
        loop {
            attempts += 1;
            match fetch_batch(client, &ids).await {
                Ok(rows) => {
                    out.extend(rows);
                    break;
                }
                Err(err) if err.code() == Some(&SqlState::LOCK_NOT_AVAILABLE) => {
                    if attempts >= FETCH_ATTEMPTS {
                        return Ok(None);
                    }
                }
                Err(err) => return Err(err.into()),
            }
        }
    }
    Ok(Some(out))
}

async fn fetch_batch(
    client: &mut Client,
    ids: &[i64],
) -> Result<Vec<(u32, Option<String>)>, tokio_postgres::Error> {
    let txn = client.transaction().await?;
    txn.batch_execute(&pinned_settings()).await?;
    let rows = txn
        .query(
            "select o::bigint, pg_get_partition_constraintdef(o) \
             from unnest($1::bigint[]::oid[]) as o",
            &[&ids],
        )
        .await?;
    txn.commit().await?;
    Ok(rows
        .iter()
        .map(|r| (r.get::<_, i64>(0) as u32, r.get(1)))
        .collect())
}

/// Deletes the state and corrections of every source not in `desired`:
/// nothing reads it any more.
pub(crate) async fn forget_except(
    client: &impl GenericClient,
    desired: &[String],
) -> Result<(), tokio_postgres::Error> {
    client
        .execute(
            "delete from partition_corrections where table_name <> all($1)",
            &[&desired],
        )
        .await?;
    client
        .execute(
            "delete from partition_sources where table_name <> all($1)",
            &[&desired],
        )
        .await?;
    Ok(())
}

/// One queued correction, as the correction engine (P4) reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingCorrection {
    pub id: i64,
    pub table: String,
    pub kind: String,
    pub node_oid: u32,
    pub node_name: String,
    pub cause: String,
    pub constraint: Option<String>,
    /// The WAL position when the pass queued it, `X/X`.
    pub queued_lsn: String,
}

/// Every queued correction of `table`, oldest first.
pub async fn pending_corrections(
    client: &impl GenericClient,
    table: &str,
) -> Result<Vec<PendingCorrection>, tokio_postgres::Error> {
    let rows = client
        .query(
            "select id, table_name, kind, node_oid, node_name, cause, constraint_text, \
                    queued_lsn::text \
             from partition_corrections where table_name = $1 order by id",
            &[&table],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|r| PendingCorrection {
            id: r.get(0),
            table: r.get(1),
            kind: r.get(2),
            node_oid: r.get::<_, i64>(3) as u32,
            node_name: r.get(4),
            cause: r.get(5),
            constraint: r.get(6),
            queued_lsn: r.get(7),
        })
        .collect())
}

/// Removes a correction once its node is corrected.
pub async fn complete_correction(
    client: &impl GenericClient,
    id: i64,
) -> Result<(), tokio_postgres::Error> {
    client
        .execute("delete from partition_corrections where id = $1", &[&id])
        .await?;
    Ok(())
}

/// A source's recorded tree, for tests and the correction engine.
pub async fn recorded(
    client: &impl GenericClient,
    table: &str,
) -> Result<Option<Recorded>, CaptureError> {
    Ok(load_state(client, table).await?.map(|s| s.recorded))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(oid: u32, parent: Option<u32>, bound: &str, xmin: &str) -> Node {
        Node {
            oid,
            parent,
            name: format!("public.n{oid}"),
            partitioned: false,
            is_default: bound == "DEFAULT",
            bound: Some(bound.to_string()),
            inherits_xmin: Some(xmin.to_string()),
            constraint: Some(format!("text{oid}")),
        }
    }

    fn root(oid: u32) -> Node {
        Node {
            oid,
            parent: None,
            name: "public.r".to_string(),
            partitioned: true,
            is_default: false,
            bound: None,
            inherits_xmin: None,
            constraint: None,
        }
    }

    fn tree(nodes: Vec<Node>) -> Recorded {
        Recorded {
            nodes,
            keys: Vec::new(),
        }
    }

    fn base() -> Recorded {
        tree(vec![
            root(1),
            node(2, Some(1), "FOR VALUES IN (1)", "10"),
            node(3, Some(1), "FOR VALUES IN (2)", "11"),
        ])
    }

    fn pairs(plan: &Plan) -> Vec<(Kind, u32, Cause)> {
        plan.corrections
            .iter()
            .map(|c| (c.kind, c.node, c.cause))
            .collect()
    }

    #[test]
    fn the_first_sight_records_every_text_and_corrects_nothing() {
        let plan = plan(None, &base(), false);
        assert!(plan.corrections.is_empty());
        assert_eq!(plan.fetch, vec![2, 3]);
    }

    #[test]
    fn an_unchanged_tree_plans_nothing() {
        let plan = plan(Some(&base()), &base(), false);
        assert_eq!(plan, Plan::default());
    }

    #[test]
    fn a_new_leaf_is_corrected_and_its_text_fetched() {
        let mut live = base();
        live.nodes.push(node(4, Some(1), "FOR VALUES IN (3)", "12"));
        let plan = plan(Some(&base()), &live, false);
        assert_eq!(pairs(&plan), vec![(Kind::NewLeaf, 4, Cause::Created)]);
        assert_eq!(plan.fetch, vec![4]);
    }

    #[test]
    fn a_detached_leaf_corrects_its_gone_node_by_the_recorded_text() {
        let mut live = base();
        live.nodes.pop();
        let plan = plan(Some(&base()), &live, false);
        assert_eq!(pairs(&plan), vec![(Kind::GoneNode, 3, Cause::Gone)]);
        assert_eq!(plan.corrections[0].constraint.as_deref(), Some("text3"));
        assert!(plan.fetch.is_empty());
    }

    #[test]
    fn a_leaf_detached_and_attached_again_under_the_same_bound_is_reattached() {
        let mut live = base();
        live.nodes[2].inherits_xmin = Some("99".to_string());
        let plan = plan(Some(&base()), &live, false);
        assert_eq!(pairs(&plan), vec![(Kind::NewLeaf, 3, Cause::Reattached)]);
        assert_eq!(plan.fetch, vec![3]);
    }

    #[test]
    fn a_leaf_re_attached_under_another_bound_is_reattached() {
        let mut live = base();
        live.nodes[2].bound = Some("FOR VALUES IN (7)".to_string());
        let plan = plan(Some(&base()), &live, false);
        assert_eq!(pairs(&plan), vec![(Kind::NewLeaf, 3, Cause::Reattached)]);
    }

    fn with_middle() -> Recorded {
        let mut middle = node(2, Some(1), "FOR VALUES IN (1)", "10");
        middle.partitioned = true;
        tree(vec![
            root(1),
            middle,
            node(5, Some(2), "FOR VALUES IN (1)", "20"),
            node(6, Some(2), "FOR VALUES IN (2)", "21"),
            node(3, Some(1), "FOR VALUES IN (2)", "11"),
        ])
    }

    #[test]
    fn a_detached_middle_corrects_only_its_topmost_gone_node() {
        let mut live = with_middle();
        live.nodes.retain(|n| ![2, 5, 6].contains(&n.oid));
        let plan = plan(Some(&with_middle()), &live, false);
        assert_eq!(pairs(&plan), vec![(Kind::GoneNode, 2, Cause::Gone)]);
    }

    #[test]
    fn a_re_attached_middle_corrects_every_leaf_under_it() {
        let mut live = with_middle();
        live.nodes[1].inherits_xmin = Some("77".to_string());
        let plan = plan(Some(&with_middle()), &live, false);
        assert_eq!(
            pairs(&plan),
            vec![
                (Kind::NewLeaf, 5, Cause::Reattached),
                (Kind::NewLeaf, 6, Cause::Reattached)
            ]
        );
        // The middle's text and its leaves' (which carry its qual) refetch.
        assert_eq!(plan.fetch, vec![2, 5, 6]);
    }

    fn with_default() -> Recorded {
        tree(vec![
            root(1),
            node(2, Some(1), "FOR VALUES IN (1)", "10"),
            node(9, Some(1), "DEFAULT", "11"),
        ])
    }

    #[test]
    fn a_default_whose_siblings_changed_is_treated_as_new() {
        let mut live = with_default();
        live.nodes.push(node(4, Some(1), "FOR VALUES IN (3)", "12"));
        let plan = plan(Some(&with_default()), &live, false);
        assert_eq!(
            pairs(&plan),
            vec![
                (Kind::NewLeaf, 9, Cause::DefaultSiblings),
                (Kind::NewLeaf, 4, Cause::Created)
            ]
        );
        assert!(plan.fetch.contains(&9) && plan.fetch.contains(&4));
    }

    #[test]
    fn a_default_whose_sibling_left_is_treated_as_new() {
        let mut live = with_default();
        live.nodes.remove(1);
        let plan = plan(Some(&with_default()), &live, false);
        let mut got = pairs(&plan);
        got.sort_by_key(|(_, node, _)| *node);
        assert_eq!(
            got,
            vec![
                (Kind::GoneNode, 2, Cause::Gone),
                (Kind::NewLeaf, 9, Cause::DefaultSiblings)
            ]
        );
    }

    #[test]
    fn a_default_whose_siblings_are_unchanged_is_left_alone() {
        assert_eq!(
            plan(Some(&with_default()), &with_default(), false),
            Plan::default()
        );
    }

    #[test]
    fn a_node_whose_text_was_never_recorded_is_fetched_without_a_correction() {
        let mut prev = base();
        prev.nodes[1].constraint = None;
        let plan = plan(Some(&prev), &base(), false);
        assert!(plan.corrections.is_empty());
        assert_eq!(plan.fetch, vec![2]);
    }

    #[test]
    fn a_replaced_root_is_gone_with_every_new_leaf_created() {
        let mut live = base();
        live.nodes[0].oid = 100;
        for (i, n) in live.nodes.iter_mut().enumerate().skip(1) {
            n.oid = 100 + i as u32;
            n.parent = Some(100);
        }
        let plan = plan(Some(&base()), &live, false);
        let got = pairs(&plan);
        assert!(got.contains(&(Kind::GoneNode, 1, Cause::Gone)));
        assert_eq!(
            got.iter().filter(|g| g.0 == Kind::NewLeaf).count(),
            2,
            "{got:?}"
        );
        // Only the topmost gone node.
        assert_eq!(got.iter().filter(|g| g.0 == Kind::GoneNode).count(), 1);
    }

    #[test]
    fn a_root_renamed_away_and_back_corrects_every_leaf() {
        let plan = plan(Some(&base()), &base(), true);
        assert_eq!(
            pairs(&plan),
            vec![
                (Kind::NewLeaf, 2, Cause::Renamed),
                (Kind::NewLeaf, 3, Cause::Renamed)
            ]
        );
    }

    #[test]
    fn a_pending_detach_leaves_the_live_tree() {
        use crate::defs::partition_tree::PartitionNode;
        let n = |oid, parent: Option<usize>, partitioned| PartitionNode {
            oid,
            name: format!("public.n{oid}"),
            parent,
            depth: 0,
            partitioned,
            is_default: false,
            bound: Some("FOR VALUES IN (1)".to_string()),
        };
        let tree = PartitionTree {
            nodes: vec![n(1, None, true), n(2, Some(0), true), n(3, Some(1), false)],
            levels: Vec::new(),
        };
        let mut inherits = HashMap::new();
        inherits.insert(2, (true, "5".to_string()));
        inherits.insert(3, (false, "6".to_string()));
        let live = live_from(&tree, &inherits, Vec::new());
        // The pending middle, and the leaf under it, are no longer in the tree.
        assert_eq!(live.nodes.iter().map(|n| n.oid).collect::<Vec<_>>(), [1]);
    }
}
