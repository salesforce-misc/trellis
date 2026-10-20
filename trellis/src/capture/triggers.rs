//! Disruption of a captured table's capture that Trellis didn't make (#707,
//! with #999's table-gone cases).
//!
//! A statement trigger only captures what it sees. An operator who removes,
//! swaps, disables or recreates Trellis's capture triggers, or replaces a
//! capture function by hand, leaves writes uncaptured that Trellis can't
//! recover from the table's current state, and a repair the pass makes by
//! itself would hide that. So the pass pauses every definition that reads
//! the table, with a `capture_failure` naming what it found, and waits for a
//! `RESUME`, which re-validates the definition against the table now at that
//! name, reinstalls capture, re-records it and rebuilds. Nothing here
//! reinstalls or rebuilds on its own.
//!
//! # What is recorded
//!
//! `capture_triggers` keeps, for each of a plain table's five capture
//! triggers (a partitioned source's: of every node), its `pg_trigger` oid,
//! `xmin` and enabled mode, and each
//! capture function's comment keeps a hash of the source Trellis generated
//! (`sql::comment_text`). An install ([`Mode::Overwrite`]) writes the
//! trigger rows in its own transaction, with the triggers, so a crash
//! mid-install leaves both or neither; a widen or narrow ([`Mode::IfAbsent`])
//! changes no trigger, so it keeps the rows it finds and never absorbs a
//! disruption it didn't see. A table captured with no rows (its capture
//! restored from a dump; the rows are runtime state a dump leaves out) gets
//! them from the next pass.
//!
//! # What pauses
//!
//! The pass compares the live triggers and functions with the record
//! ([`findings`]) and pauses on any of:
//!
//! - **the table is gone** at that name: dropped, renamed away, moved to
//!   another schema ([`pause_readers_of_gone_table`]);
//! - **a recorded trigger is gone** from the table at that name: a swap, a
//!   drop and recreate, the triggers dropped;
//! - **a trigger's oid changed**: recreated by hand or by a schema tool;
//! - **a trigger isn't `ENABLE ALWAYS`**: disabled, `ORIGIN` (what
//!   `ENABLE TRIGGER ALL|USER` leaves) or `REPLICA`;
//! - **a trigger's `xmin` moved** with the same oid: altered by name;
//! - **a capture function's source differs from the hash its own comment
//!   records**: replaced by hand. A source that still matches its hash but
//!   isn't what this build generates is a function an older Trellis left; the
//!   ordinary widen replaces it, with no pause.
//!
//! The record of a restored database is re-recorded instead. A dump and
//! restore, or `pg_upgrade`, recreates every trigger under a new oid, so the
//! rows say which they are: the cluster's `system_identifier` and the oid of
//! `capture_triggers` itself, both written with each row. A stored value that
//! differs from the live one means the rows came from another database, and
//! the pass forgets them. New trigger oids under the same values mean the
//! triggers were recreated in this one: that pauses.
//!
//! # A partitioned source
//!
//! A partitioned root's tree has a set of five triggers on every node, and the
//! rows say which node each sits on (`node_oid`; a plain table's rows keep 0,
//! because the table at a name is judged by its name). Each install operation
//! (`capture::install`) records the nodes it installed in its own transaction:
//! a tree of fewer than `SINGLE_TRANSACTION_TABLES` nodes in one, a larger one
//! root first and then node by node, so a pass that stops between two of them
//! leaves a record for exactly the nodes that carry triggers.
//!
//! The pass checks each recorded node that is still in the tree, with the
//! rules above, and the root's capture functions once. Partition maintenance
//! pauses nothing, because it never changes a recorded node: a leaf that is
//! attached, created (`DEFAULT` and `HASH` re-splits included), detached or
//! dropped is not one the record holds, or no longer is in the tree
//! ([`partitions`](super::partitions) owns the tree and its corrections). The
//! pass forgets the rows of a node that left the tree, installs capture on a
//! node new to it, which records it, and records a node that comes back
//! carrying all five triggers `ENABLE ALWAYS` as it finds it. A recorded node
//! that is still in the tree and lacks its triggers is a disruption, as is a
//! root's.
//!
//! A name held by the other kind of table than the rows were recorded for
//! (a plain table's rows, a partitioned root now) is forgotten, not acted on.

use std::collections::BTreeMap;

use tokio_postgres::types::Type;
use tokio_postgres::{Client, GenericClient};

use super::CaptureError;
use super::columns::CaptureCatalog;
use super::sql::{self, CaptureEvent};

/// How [`record`] treats rows already there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    /// An install (or repair): the triggers are what Trellis just made them.
    Overwrite,
    /// A widen or narrow: no trigger changes, so a table with rows keeps
    /// them.
    IfAbsent,
}

/// Which of a table's capture triggers [`record`] covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Covers<'a> {
    /// The plain table's, or a partitioned root's and every node's.
    Whole,
    /// A partitioned root's own.
    Root,
    /// One node's, a `schema.table` text.
    Node(&'a str),
}

async fn oid_of(client: &impl GenericClient, name: &str) -> Result<Option<u32>, CaptureError> {
    Ok(client
        .query_typed(
            "select pg_catalog.to_regclass($1)::oid",
            &[(&crate::defs::ddl::regclass_arg(name), Type::TEXT)],
        )
        .await?
        .first()
        .and_then(|row| row.get(0)))
}

/// Records `table`'s capture triggers as they are now, in the caller's
/// transaction (see the module doc), on the tables `covers` names. A table
/// that is neither a plain table nor a partitioned root gets no rows, and
/// loses any it had.
pub(crate) async fn record(
    client: &impl GenericClient,
    schema: &str,
    table: &str,
    mode: Mode,
    covers: Covers<'_>,
) -> Result<(), CaptureError> {
    if mode == Mode::IfAbsent {
        let has: bool = client
            .query_typed(
                "select exists (select 1 from capture_triggers where table_name = $1)",
                &[(&table, Type::TEXT)],
            )
            .await?
            .first()
            .is_some_and(|row| row.get(0));
        if has {
            return Ok(());
        }
    }
    // The tables to read, and whether the table is a plain one (its rows
    // keep node 0).
    let (tables, plain): (Vec<u32>, bool) = match covers {
        Covers::Whole => {
            let tree = super::install::tree_nodes(client, table).await?;
            if tree.is_empty() {
                (oid_of(client, table).await?.into_iter().collect(), true)
            } else {
                (tree.into_iter().map(|(oid, _)| oid).collect(), false)
            }
        }
        Covers::Root => (oid_of(client, table).await?.into_iter().collect(), false),
        Covers::Node(node) => (oid_of(client, node).await?.into_iter().collect(), false),
    };
    if mode == Mode::Overwrite {
        if covers == Covers::Whole {
            forget(client, table).await?;
        } else {
            client
                .execute_typed(
                    "delete from capture_triggers where table_name = $1 and node_oid = any($2)",
                    &[(&table, Type::TEXT), (&tables, Type::OID_ARRAY)],
                )
                .await?;
        }
    }
    let names: Vec<String> = CaptureEvent::ALL
        .into_iter()
        .map(|event| sql::trigger_name(schema, event))
        .collect();
    client
        .execute_typed(
            "insert into capture_triggers \
                 (table_name, node_oid, trigger_name, trigger_oid, trigger_xmin, enabled, \
                  system_identifier, catalog_oid) \
             select $1, case when $4 then 0::oid else g.tgrelid end, g.tgname::text, g.oid, \
                    g.xmin::text, g.tgenabled, \
                    (select system_identifier::text from pg_catalog.pg_control_system()), \
                    'capture_triggers'::pg_catalog.regclass::pg_catalog.oid \
             from pg_catalog.pg_trigger g \
             join pg_catalog.pg_class c on c.oid = g.tgrelid \
             where g.tgrelid = any($2) and c.relkind in ('r', 'p') \
               and g.tgname = any($3) and not g.tgisinternal \
             on conflict (table_name, node_oid, trigger_name) do nothing",
            &[
                (&table, Type::TEXT),
                (&tables, Type::OID_ARRAY),
                (&names, Type::TEXT_ARRAY),
                (&plain, Type::BOOL),
            ],
        )
        .await?;
    Ok(())
}

/// Forgets `table`'s trigger record: its capture is uninstalled, or a resume
/// asks for a fresh one.
pub(crate) async fn forget(
    client: &impl GenericClient,
    table: &str,
) -> Result<(), tokio_postgres::Error> {
    client
        .execute_typed(
            "delete from capture_triggers where table_name = $1",
            &[(&table, Type::TEXT)],
        )
        .await?;
    Ok(())
}

/// Forgets the trigger record of `node`, one table of `table`'s tree: its
/// triggers are uninstalled.
pub(crate) async fn forget_node(
    client: &impl GenericClient,
    table: &str,
    node: &str,
) -> Result<(), CaptureError> {
    if let Some(oid) = oid_of(client, node).await? {
        forget_nodes(client, table, &[oid]).await?;
    }
    Ok(())
}

async fn forget_nodes(
    client: &impl GenericClient,
    table: &str,
    nodes: &[u32],
) -> Result<(), tokio_postgres::Error> {
    client
        .execute_typed(
            "delete from capture_triggers where table_name = $1 and node_oid = any($2)",
            &[(&table, Type::TEXT), (&nodes, Type::OID_ARRAY)],
        )
        .await?;
    Ok(())
}

/// Forgets the trigger record of every table but `keep`.
pub(crate) async fn forget_except(
    client: &impl GenericClient,
    keep: &[&str],
) -> Result<(), tokio_postgres::Error> {
    client
        .execute_typed(
            "delete from capture_triggers where table_name <> all($1)",
            &[(&keep, Type::TEXT_ARRAY)],
        )
        .await?;
    Ok(())
}

/// One recorded or live capture trigger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Trigger {
    pub name: String,
    pub oid: u32,
    pub xmin: String,
    /// `pg_trigger.tgenabled`: `A` is `ENABLE ALWAYS`.
    pub enabled: String,
}

/// One live capture function: its source, and the hash its comment records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Function {
    pub name: String,
    pub source: String,
    /// `None` for a function whose comment records no hash.
    pub recorded_hash: Option<String>,
}

/// One way the live capture differs from what Trellis recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Finding {
    /// A recorded trigger isn't on the table at that name.
    Gone(String),
    /// A trigger has an oid other than the recorded one.
    Recreated(String),
    /// A trigger isn't `ENABLE ALWAYS`, with its mode.
    Mode(String, String),
    /// A trigger's `xmin` moved under the recorded oid.
    Altered(String),
    /// A capture function's source isn't the one its comment hashes.
    FunctionReplaced(String),
}

impl Finding {
    /// The finding on one node of a partition tree.
    fn describe_on(&self, node: &str) -> String {
        format!("{} (on {node})", self.describe())
    }

    fn describe(&self) -> String {
        match self {
            Finding::Gone(t) => format!("the capture trigger {t} is gone"),
            Finding::Recreated(t) => {
                format!("the capture trigger {t} was dropped and created again")
            }
            Finding::Mode(t, mode) => format!(
                "the capture trigger {t} is {} (it must be ENABLE ALWAYS)",
                match mode.as_str() {
                    "D" => "disabled",
                    "O" => "enabled in ORIGIN mode",
                    "R" => "enabled in REPLICA mode",
                    _ => "not enabled ALWAYS",
                }
            ),
            Finding::Altered(t) => format!("the capture trigger {t} was altered"),
            Finding::FunctionReplaced(f) => {
                format!("the capture function {f} was replaced")
            }
        }
    }
}

/// What the live `triggers` and `functions` show against the `recorded`
/// triggers. Pure. A trigger is reported once, by the first thing wrong with
/// it: missing, recreated, not `ENABLE ALWAYS`, altered.
pub(crate) fn findings(
    recorded: &[Trigger],
    triggers: &[Trigger],
    functions: &[Function],
) -> Vec<Finding> {
    let live: BTreeMap<&str, &Trigger> = triggers.iter().map(|t| (t.name.as_str(), t)).collect();
    let mut found = Vec::new();
    for was in recorded {
        let Some(now) = live.get(was.name.as_str()) else {
            found.push(Finding::Gone(was.name.clone()));
            continue;
        };
        if now.oid != was.oid {
            found.push(Finding::Recreated(was.name.clone()));
        } else if now.enabled != "A" {
            found.push(Finding::Mode(was.name.clone(), now.enabled.clone()));
        } else if now.xmin != was.xmin {
            found.push(Finding::Altered(was.name.clone()));
        }
    }
    for function in functions {
        if let Some(hash) = &function.recorded_hash
            && *hash != sql::body_hash(&function.source)
        {
            found.push(Finding::FunctionReplaced(function.name.clone()));
        }
    }
    found
}

/// What a partitioned source's pass found in its tree.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct TreeReport {
    /// One text per disruption, on the recorded nodes still in the tree.
    pub found: Vec<String>,
    /// Recorded nodes that left the tree (detached, dropped, replaced): their
    /// rows go.
    pub departed: Vec<u32>,
    /// Nodes of the tree with no rows that carry all five triggers `ENABLE
    /// ALWAYS` (a detached leaf attached again): recorded as they are.
    pub unrecorded: Vec<(u32, String)>,
}

/// Compares the live `tree` (oid and name of the root and every node) with
/// the `recorded` triggers by node, and the capture `functions` once. Pure.
/// Nodes absent from the record are not disruptions (a leaf partition
/// maintenance added carries no triggers until the pass installs them); nodes
/// absent from the tree are not either.
pub(crate) fn tree_findings(
    tree: &[(u32, String)],
    recorded: &BTreeMap<u32, Vec<Trigger>>,
    live: &BTreeMap<u32, Vec<Trigger>>,
    functions: &[Function],
) -> TreeReport {
    let mut report = TreeReport::default();
    let none = Vec::new();
    for (oid, name) in tree {
        let now = live.get(oid).unwrap_or(&none);
        match recorded.get(oid) {
            Some(was) => report.found.extend(
                findings(was, now, &[])
                    .iter()
                    .map(|finding| finding.describe_on(name)),
            ),
            None if now.len() == CaptureEvent::ALL.len()
                && now.iter().all(|t| t.enabled == "A") =>
            {
                report.unrecorded.push((*oid, name.clone()));
            }
            None => {}
        }
    }
    report.departed = recorded
        .keys()
        .filter(|oid| !tree.iter().any(|(n, _)| n == *oid))
        .copied()
        .collect();
    report
        .found
        .extend(findings(&[], &[], functions).iter().map(Finding::describe));
    report
}

/// How many findings a `capture_failure` lists before it counts the rest.
const LISTED_FINDINGS: usize = 5;

/// The `capture_failure` sentence for `what` (one text per finding) on
/// `table`.
fn disrupted_error(table: &str, what: &[String]) -> String {
    let mut listed = what
        .iter()
        .take(LISTED_FINDINGS)
        .cloned()
        .collect::<Vec<_>>()
        .join("; ");
    if what.len() > LISTED_FINDINGS {
        listed.push_str(&format!("; and {} more", what.len() - LISTED_FINDINGS));
    }
    format!(
        "Trellis's capture of {table} was changed outside Trellis: {listed}. Changes made to the \
         table meanwhile may be missing from this definition, so Trellis can't guarantee it is \
         correct. Resume the definition to reinstall capture and rebuild it, or drop the \
         definition"
    )
}

/// The `capture_failure` sentence for a definition whose `table` is gone.
fn gone_error(table: &str) -> String {
    format!(
        "the table {table} no longer exists at that name (it was dropped, renamed away, moved \
         to another schema, or replaced by something that isn't a table). Changes made to it meanwhile may be missing from this definition, \
         so Trellis can't guarantee it is correct. Put a table there again and resume the \
         definition to re-validate it against that table and rebuild it (a resume refuses while \
         the table is missing), or drop the definition"
    )
}

/// Pauses each definition that reads `table` and isn't paused for a capture
/// failure yet, with `error`.
async fn pause_readers(
    client: &mut Client,
    catalog: &CaptureCatalog,
    table: &str,
    error: &str,
) -> Result<(), CaptureError> {
    let readers = crate::staging::schema_change::unpaused_readers(catalog, table);
    if readers.is_empty() {
        return Ok(());
    }
    let txn = client.transaction().await?;
    for id in readers {
        if crate::defs::lifecycle::pause_for_disruption(&txn, id, table, error).await? {
            crate::instance_log::warn!(transform_id = id, table = %table, "definition paused: {error}");
        }
    }
    txn.commit().await?;
    Ok(())
}

/// The capture pass's check that `table`, which some definition reads, still
/// exists at that name as a table (plain or partitioned), and, for a
/// partitioned source the pass has recorded, is still the root it recorded.
/// When it doesn't, pauses every definition that reads it and isn't paused
/// yet (#999, #1200) and returns `true`, and the caller leaves the table
/// alone: its capture can't be installed, and a table swapped in meanwhile is
/// for a resume to re-validate. A view, materialized view or foreign table put
/// at the name (a table renamed away behind a compatibility view, say) is no
/// table: the table is gone from the name all the same.
///
/// A partitioned root renamed away, moved to another schema, swapped in by
/// renames, or dropped and recreated (empty, or with leaves attached) is table
/// maintenance, not partition maintenance: the readers pause and nothing is
/// corrected or reinstalled until a resume. A source whose recorded state
/// came from another database (a restore, or `pg_upgrade`) is not judged
/// here: [`super::partitions`] classifies it.
pub(crate) async fn pause_readers_of_gone_table(
    client: &mut Client,
    catalog: &CaptureCatalog,
    table: &str,
) -> Result<bool, CaptureError> {
    let live: Option<(u32, String)> = client
        .query_typed(
            "select c.oid, c.relkind::text from pg_catalog.pg_class c \
             where c.oid = pg_catalog.to_regclass($1) and c.relkind in ('r', 'p')",
            &[(&crate::defs::ddl::regclass_arg(table), Type::TEXT)],
        )
        .await?
        .first()
        .map(|row| (row.get(0), row.get(1)));
    // The recorded root, if the recorded state is this database's own.
    let recorded_root: Option<u32> = client
        .query_typed(
            "select s.root_oid::oid from partition_sources s \
             where s.table_name = $1 \
               and s.catalog_oid = pg_catalog.to_regclass('partition_sources')::oid \
               and s.system_identifier = \
                   (select system_identifier::text from pg_catalog.pg_control_system())",
            &[(&table, Type::TEXT)],
        )
        .await?
        .first()
        .map(|row| row.get(0));
    let error = match (&live, recorded_root) {
        (None, _) => gone_error(table),
        (Some((oid, _)), Some(root)) if *oid != root => {
            disrupted_error(table, &[root_replaced(table)])
        }
        _ => return Ok(false),
    };
    pause_readers(client, catalog, table, &error).await?;
    Ok(true)
}

/// Pauses every reader of the partitioned source `table` that isn't paused
/// yet, because the root at its name is not the recorded one: what
/// [`pause_readers_of_gone_table`] does, for a swap that commits after it
/// ran ([`super::partitions`]).
pub(crate) async fn pause_readers_of_replaced_root(
    client: &mut Client,
    catalog: &CaptureCatalog,
    table: &str,
) -> Result<(), CaptureError> {
    pause_readers(
        client,
        catalog,
        table,
        &disrupted_error(table, &[root_replaced(table)]),
    )
    .await
}

/// The finding for a partitioned root that isn't the recorded one.
fn root_replaced(table: &str) -> String {
    format!(
        "the partitioned table {table} at that name is not the one Trellis captured (it was \
         dropped and created again, or swapped in by renames)"
    )
}

/// Whether the rows of `capture_triggers` for `table` came from another
/// database: a stored `system_identifier` or `catalog_oid` that isn't the
/// live one.
async fn restored(client: &impl GenericClient, table: &str) -> Result<bool, CaptureError> {
    Ok(client
        .query_typed(
            "select exists (select 1 from capture_triggers \
                 where table_name = $1 \
                   and (system_identifier is distinct from \
                            (select system_identifier::text from pg_catalog.pg_control_system()) \
                        or catalog_oid is distinct from \
                            'capture_triggers'::pg_catalog.regclass::pg_catalog.oid))",
            &[(&table, Type::TEXT)],
        )
        .await?
        .first()
        .is_some_and(|row| row.get(0)))
}

/// The `relkind` of the relation at `table`'s name: `r` plain, `p`
/// partitioned, `None` for nothing there.
async fn relkind(client: &impl GenericClient, table: &str) -> Result<Option<String>, CaptureError> {
    Ok(client
        .query_typed(
            "select c.relkind::text from pg_catalog.pg_class c \
             where c.oid = pg_catalog.to_regclass($1)",
            &[(&crate::defs::ddl::regclass_arg(table), Type::TEXT)],
        )
        .await?
        .first()
        .map(|row| row.get(0)))
}

/// The recorded triggers of `table`, by node (0 for a plain table).
async fn recorded_triggers(
    client: &impl GenericClient,
    table: &str,
) -> Result<BTreeMap<u32, Vec<Trigger>>, CaptureError> {
    let mut by_node: BTreeMap<u32, Vec<Trigger>> = BTreeMap::new();
    for row in client
        .query_typed(
            "select node_oid, trigger_name, trigger_oid, trigger_xmin, enabled::text \
             from capture_triggers where table_name = $1 order by node_oid, trigger_name",
            &[(&table, Type::TEXT)],
        )
        .await?
    {
        by_node.entry(row.get(0)).or_default().push(Trigger {
            name: row.get(1),
            oid: row.get(2),
            xmin: row.get(3),
            enabled: row.get(4),
        });
    }
    Ok(by_node)
}

/// The live capture triggers on each of the tables `nodes`, by table.
async fn live_triggers(
    client: &impl GenericClient,
    schema: &str,
    nodes: &[u32],
) -> Result<BTreeMap<u32, Vec<Trigger>>, CaptureError> {
    let names: Vec<String> = CaptureEvent::ALL
        .into_iter()
        .map(|event| sql::trigger_name(schema, event))
        .collect();
    let mut by_node: BTreeMap<u32, Vec<Trigger>> = BTreeMap::new();
    for row in client
        .query_typed(
            "select g.tgrelid, g.tgname::text, g.oid, g.xmin::text, g.tgenabled::text \
             from pg_catalog.pg_trigger g \
             where g.tgrelid = any($1) and g.tgname = any($2) and not g.tgisinternal \
             order by g.tgrelid, g.tgname",
            &[(&nodes, Type::OID_ARRAY), (&names, Type::TEXT_ARRAY)],
        )
        .await?
    {
        by_node.entry(row.get(0)).or_default().push(Trigger {
            name: row.get(1),
            oid: row.get(2),
            xmin: row.get(3),
            enabled: row.get(4),
        });
    }
    Ok(by_node)
}

/// `table`'s capture functions that exist, with the hash each comment records.
async fn live_functions(
    client: &impl GenericClient,
    schema: &str,
    table: &str,
) -> Result<Vec<Function>, CaptureError> {
    let names: Vec<String> = CaptureEvent::ALL
        .into_iter()
        .map(|event| sql::function_name(table, event))
        .collect::<Result<_, _>>()?;
    Ok(client
        .query_typed(
            "select p.proname::text, p.prosrc, \
                    case when d.description like '{\"trellis_capture\":1,%' \
                         then d.description::jsonb ->> 'body' end \
             from pg_catalog.pg_proc p \
             left join pg_catalog.pg_description d \
               on d.objoid = p.oid \
              and d.classoid = 'pg_catalog.pg_proc'::pg_catalog.regclass and d.objsubid = 0 \
             where p.pronamespace = (select oid from pg_catalog.pg_namespace where nspname = $1) \
               and p.pronargs = 0 and p.proname = any($2) \
             order by p.proname",
            &[(&schema, Type::TEXT), (&names, Type::TEXT_ARRAY)],
        )
        .await?
        .into_iter()
        .map(|row| Function {
            name: row.get(0),
            source: row.get(1),
            recorded_hash: row.get(2),
        })
        .collect())
}

/// The capture pass's check of `table`'s capture against what Trellis
/// recorded (see the module doc). Returns whether it found a disruption, in
/// which case it has paused the table's readers and the caller installs
/// nothing on the table.
///
/// With no record, a complete capture is recorded as it is, for the next pass
/// to compare; a record from another database is forgotten the same way.
pub(crate) async fn check(
    client: &mut Client,
    schema: &str,
    catalog: &CaptureCatalog,
    table: &str,
) -> Result<bool, CaptureError> {
    let mut recorded = recorded_triggers(&*client, table).await?;
    let kind = relkind(&*client, table).await?;
    let partitioned = kind.as_deref() == Some("p");
    // Rows recorded for the other kind of table than the one at the name now
    // (a plain table's, a partitioned root holds the name; or the reverse), or
    // for nothing: nothing recorded applies.
    let other_kind = recorded.keys().any(|node| (*node == 0) == partitioned)
        || !matches!(kind.as_deref(), Some("r" | "p"));
    if !recorded.is_empty() && other_kind {
        forget(&*client, table).await?;
        return Ok(false);
    }
    if !recorded.is_empty() && restored(&*client, table).await? {
        forget(&*client, table).await?;
        recorded.clear();
    }
    if recorded.is_empty() {
        if matches!(
            super::install::installed(&*client, schema, table).await?,
            super::install::Installed::Complete { .. }
        ) {
            record(&*client, schema, table, Mode::Overwrite, Covers::Whole).await?;
        }
        return Ok(false);
    }
    let functions = live_functions(&*client, schema, table).await?;
    let found = if partitioned {
        let tree = super::install::tree_nodes(&*client, table).await?;
        let oids: Vec<u32> = tree.iter().map(|(oid, _)| *oid).collect();
        let live = live_triggers(&*client, schema, &oids).await?;
        let report = tree_findings(&tree, &recorded, &live, &functions);
        if !report.departed.is_empty() {
            forget_nodes(&*client, table, &report.departed).await?;
        }
        if report.found.is_empty() {
            for (_, name) in &report.unrecorded {
                record(&*client, schema, table, Mode::Overwrite, Covers::Node(name)).await?;
            }
        }
        report.found
    } else {
        let live = live_triggers(
            &*client,
            schema,
            &[oid_of(&*client, table).await?.unwrap_or(0)],
        )
        .await?;
        let now: Vec<Trigger> = live.into_values().next().unwrap_or_default();
        let was = recorded.remove(&0).unwrap_or_default();
        findings(&was, &now, &functions)
            .iter()
            .map(Finding::describe)
            .collect()
    };
    if found.is_empty() {
        return Ok(false);
    }
    pause_readers(client, catalog, table, &disrupted_error(table, &found)).await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trigger(name: &str, oid: u32, xmin: &str, enabled: &str) -> Trigger {
        Trigger {
            name: name.to_string(),
            oid,
            xmin: xmin.to_string(),
            enabled: enabled.to_string(),
        }
    }

    fn recorded() -> Vec<Trigger> {
        vec![
            trigger("t_a", 10, "100", "A"),
            trigger("t_b", 11, "100", "A"),
        ]
    }

    fn function(source: &str, hash_of: &str) -> Function {
        Function {
            name: "f".to_string(),
            source: source.to_string(),
            recorded_hash: Some(sql::body_hash(hash_of)),
        }
    }

    #[test]
    fn untouched_capture_finds_nothing() {
        assert_eq!(
            findings(&recorded(), &recorded(), &[function("body", "body")]),
            vec![]
        );
    }

    #[test]
    fn each_kind_of_disruption_is_found_once_per_trigger() {
        let found = findings(
            &recorded(),
            &[trigger("t_a", 99, "200", "D")],
            &[function("edited", "body")],
        );
        assert_eq!(
            found,
            vec![
                Finding::Recreated("t_a".into()),
                Finding::Gone("t_b".into()),
                Finding::FunctionReplaced("f".into()),
            ]
        );
        assert_eq!(
            findings(
                &recorded(),
                &[
                    trigger("t_a", 10, "100", "O"),
                    trigger("t_b", 11, "300", "A")
                ],
                &[]
            ),
            vec![
                Finding::Mode("t_a".into(), "O".into()),
                Finding::Altered("t_b".into()),
            ]
        );
    }

    #[test]
    fn a_function_with_no_recorded_hash_is_not_judged() {
        let f = Function {
            name: "f".into(),
            source: "anything".into(),
            recorded_hash: None,
        };
        assert_eq!(findings(&[], &[], &[f]), vec![]);
    }

    #[test]
    fn the_error_says_what_was_found_and_that_correctness_is_not_guaranteed() {
        let text = disrupted_error(
            "public.t",
            &[Finding::Mode("trellis_capture_insert".into(), "O".into()).describe()],
        );
        assert!(text.contains("ORIGIN"), "{text}");
        assert!(text.contains("can't guarantee"), "{text}");
        assert!(text.contains("Resume"), "{text}");
    }

    fn five(node: u32, oid_base: u32, enabled: &str) -> Vec<Trigger> {
        (0..5)
            .map(|i| trigger(&format!("t{i}"), oid_base + i, &format!("{node}"), enabled))
            .collect()
    }

    fn by_node(nodes: &[(u32, Vec<Trigger>)]) -> BTreeMap<u32, Vec<Trigger>> {
        nodes.iter().cloned().collect()
    }

    fn tree(nodes: &[u32]) -> Vec<(u32, String)> {
        nodes.iter().map(|n| (*n, format!("public.n{n}"))).collect()
    }

    #[test]
    fn partition_maintenance_is_no_disruption() {
        let recorded = by_node(&[(1, five(1, 100, "A")), (2, five(2, 200, "A"))]);
        // Node 2 detached or dropped; node 3 (a new leaf, a re-split, a
        // DEFAULT) has no triggers yet; node 4 (re-created leaf) likewise.
        let live = by_node(&[(1, five(1, 100, "A"))]);
        let report = tree_findings(&tree(&[1, 3, 4]), &recorded, &live, &[]);
        assert_eq!(report.found, Vec::<String>::new());
        assert_eq!(report.departed, vec![2]);
        assert!(report.unrecorded.is_empty());
    }

    #[test]
    fn a_node_attached_again_with_its_triggers_is_recorded_as_found() {
        let live = by_node(&[(1, five(1, 100, "A")), (5, five(5, 500, "A"))]);
        let recorded = by_node(&[(1, five(1, 100, "A"))]);
        let report = tree_findings(&tree(&[1, 5]), &recorded, &live, &[]);
        assert_eq!(report.unrecorded, vec![(5, "public.n5".to_string())]);
        // Triggers that aren't all ENABLE ALWAYS, or not all there, are the
        // install's to repair, not recorded.
        let live = by_node(&[(1, five(1, 100, "A")), (5, five(5, 500, "O"))]);
        assert!(
            tree_findings(&tree(&[1, 5]), &recorded, &live, &[])
                .unrecorded
                .is_empty()
        );
    }

    #[test]
    fn a_disrupted_recorded_node_or_root_is_found_naming_the_node() {
        let recorded = by_node(&[(1, five(1, 100, "A")), (2, five(2, 200, "A"))]);
        let mut live = recorded.clone();
        live.get_mut(&2).unwrap()[0].enabled = "D".into();
        live.remove(&1);
        let report = tree_findings(&tree(&[1, 2]), &recorded, &live, &[]);
        assert_eq!(report.found.len(), 6, "{:?}", report.found);
        assert!(
            report.found[0].contains("(on public.n1)"),
            "{:?}",
            report.found
        );
        assert!(report.found[5].contains("disabled"), "{:?}", report.found);
        assert!(
            report.found[5].contains("(on public.n2)"),
            "{:?}",
            report.found
        );
    }

    #[test]
    fn a_long_list_of_findings_is_cut_short() {
        let many: Vec<String> = (0..9).map(|i| format!("f{i}")).collect();
        let text = disrupted_error("public.t", &many);
        assert!(text.contains("f4; and 4 more"), "{text}");
        assert!(!text.contains("f5"), "{text}");
    }
}
