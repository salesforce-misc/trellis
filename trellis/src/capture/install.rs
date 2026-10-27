//! Installs, widens, narrows and uninstalls one table's capture triggers,
//! and reads back what is installed.
//!
//! The staging worker's reconcile pass ([`super::reconcile`], C5) computes
//! each captured table's [`CaptureSpec`] from the catalog
//! ([`super::columns::capture_spec`]) and hands it to [`reconcile`], one
//! table at a time, so a table whose lock is held doesn't hold back another
//! table's install.
//!
//! # What each operation does
//!
//! | Operation | When ([`plan`]) | Table lock | What it records |
//! |---|---|---|---|
//! | [`install`] | nothing, or a partial install, is there | `SHARE ROW EXCLUSIVE` | the table's capture gate, and a rebuild of each of the table's applying readers ([`park_table_catch_ups`]) |
//! | [`widen`] | the table must image a column, key or group-key column it doesn't yet, or a function body is stale | `SHARE ROW EXCLUSIVE` | the table's capture gate |
//! | [`narrow`] | the table images columns no reader needs any more | none | nothing |
//! | [`uninstall`] | nothing reads the table any more | `ACCESS EXCLUSIVE` (`DROP TRIGGER`'s) | nothing |
//!
//! [`park_table_catch_ups`]: crate::intake::catch_up::park_table_catch_ups
//!
//! # Never blocking a writer (ADR-0002 I6)
//!
//! `CREATE TRIGGER`, `ALTER TABLE … ENABLE ALWAYS TRIGGER` and `LOCK TABLE …
//! IN SHARE ROW EXCLUSIVE MODE` all conflict with a writer's `ROW
//! EXCLUSIVE`, and every writer that arrives while one of them waits queues
//! behind it. So each attempt is one short transaction whose table lock waits
//! at most [`USER_TABLE_DDL_LOCK_TIMEOUT`] (50 ms), and
//! [`crate::locks::DdlRetry`] retries it until it lands or, with a deadline,
//! until the next attempt couldn't end by it (see "Waiting, never
//! cancelling" for what it returns then). The attempt locks the definitions
//! it rebuilds *before* it asks for the table lock, under the session's
//! `lock_timeout`: waiting for a row lock of Trellis's own while holding the
//! table lock would queue the application's writers behind a Trellis
//! transaction. Once the attempt holds the table lock, nothing in it waits
//! for anything else, and the whole stretch runs under the 50 ms timeout
//! anyway.
//!
//! `DROP TRIGGER` takes `ACCESS EXCLUSIVE`, so an uninstall's attempt also
//! queues readers of the table for up to 50 ms. A late drop only costs
//! capture writes nobody reads, so it retries the same way.
//!
//! # Waiting, never cancelling
//!
//! Every lock holder is waited out, an autovacuum included: nothing here
//! cancels or terminates a backend. (Postgres itself cancels an autovacuum
//! that holds a lock a waiter wants, but only from the waiter's deadlock
//! check after `deadlock_timeout`, 1 s by default, which a 50 ms attempt
//! never reaches. So an install on a table under a long vacuum waits the
//! whole vacuum out.)
//!
//! Waiting is kept off `apply`'s path: defining a transform only registers
//! it, and the staging worker's background reconcile does the install. So a wait has to be
//! visible some other way, and that is the [`LockWait`] report. The
//! `deadline` an operation takes bounds one pass. When the retries run out
//! of it, the operation returns [`Progress::Waiting`], having changed
//! nothing, with the table, the operation, the lock it asks for, when the
//! pass began waiting, and every session that holds or is queued for a
//! conflicting lock ([`Blocker`]: its pid, or a prepared transaction's gid;
//! its backend type; its lock mode; since when; and the start of its query).
//! The reconcile pass retries on its next pass and records the report in
//! `capture_holdups`, where `Trellis::status` reports it as a waiting
//! definition's `capture_wait`. Without a deadline an operation waits until it
//! lands. While it retries, it logs the blockers at most every five seconds.
//!
//! `pg_stat_activity` shows another role's backend only to a superuser or a
//! member of `pg_read_all_stats`. To any other role such a blocker reads as
//! `unknown`, with no transaction start and `<insufficient privilege>` for
//! its query. Its pid, lock mode, and (for a queued request) how long it has
//! waited still show.
//!
//! # The install is the fence
//!
//! An install creates the triggers in one transaction under the table lock,
//! so the install commits exactly when capture starts. A write committed
//! before the install is visible to the snapshot of any build that starts
//! after it; one after it runs the trigger. The table lock makes that split
//! exact: no writer of the table is in flight when the triggers commit.
//!
//! # Widening and the capture gate
//!
//! A table's capture functions image only the columns its readers need
//! ([`super::columns`]). A new reader can need a column the functions don't
//! image yet, so the functions are replaced with wider ones. Two things can
//! then put a row without that column in front of the new reader:
//!
//! 1. **A writer already running the old body.** `CREATE OR REPLACE
//!    FUNCTION` takes no table lock, so a writer mid-transaction keeps
//!    committing rows the old function imaged. A widen therefore takes the
//!    same `SHARE ROW EXCLUSIVE` lock a join takes: once it holds it, no
//!    writer of the table is in flight, every row the old body staged has
//!    committed, and every later statement on the table runs the new body.
//! 2. **Rows the old body staged that haven't drained yet.** A definition
//!    applies every ring row drained after its build starts, and a
//!    row staged before the widen lacks the new column: the reader fails it
//!    for want of the column and the key is quarantined. The rows
//!    can be in the ring. A row held in `poison_held` never reaches
//!    a reader with its images: its release re-stages an image-less
//!    `Recompute` that re-derives the key from its live row.
//!
//! **The capture gate closes the second gap.** Holding the table lock, the
//! install or widen reads `pg_current_wal_insert_lsn()` and records it as the
//! table's gate (`capture_gates`, [`super::gate`]). A capture
//! function reads the insert position before it inserts and commits, so
//! every row the old body staged has an `origin_lsn` below the gate, and
//! every row the new body stages, which can only run after the widen
//! commits, has one above it. No waiting definition's build starts while any
//! change to the table at or below the gate is still pending ([`crate::staging::converge::table_changes_pending_through`]).
//! Those rows drain through the definitions that were already applying,
//! which read only columns the old body imaged. Once they have, nothing
//! staged by the old body is left for the new reader to meet: a drained row
//! is never applied again, and a released or re-staged one keeps its origin,
//! so it would have held the gate.
//!
//! Why "below" holds for every old-body row: a statement that fires a
//! capture function holds `ROW EXCLUSIVE` on the table (`ACCESS EXCLUSIVE`
//! for a `TRUNCATE`) from before the trigger runs until its transaction
//! ends, and a prepared transaction keeps it until `COMMIT PREPARED`. A
//! subtransaction that rolls back releases its lock, but the rows it staged
//! roll back with it. So once the widen holds `SHARE ROW EXCLUSIVE`, every
//! statement that ran the old body has read its insert position and
//! committed or aborted, and the insert position only moves forward. Other
//! tables' WAL only moves the gate later, which old rows are still below.
//! The gate must be read *after* `LOCK TABLE`: read at the start of the
//! attempt, a writer already holding the table could stage and commit an
//! old-body row while the attempt waits (the test
//! `a_row_staged_while_the_widen_waits_for_its_lock_is_below_the_gate`).
//!
//! **The gate only holds definitions sourced from the gated table.** A build
//! start checks the gate of its definition's `source_table`. A widen of a
//! `JOIN`'s joined table `T` for a new `JOIN` sourced from `U` (it reads a new
//! column of `T`) gates `T`, but the new definition is sourced from `U`,
//! which no gate holds. C5 closes this in the reconcile pass
//! ([`super::reconcile`], rule 3): such a definition isn't started while `T`
//! has a gate a pending change holds.
//!
//! Why not the alternatives:
//!
//! - **Re-deriving rows below the new reader's build horizon** instead of
//!   evaluating them would be the cheaper wait, but apply had no
//!   per-definition horizon, and every consumer path (1-1, aggregate,
//!   `JOIN`) would need one.
//!   That is D's ledger, not C's.
//! - **A seal after the widen** would bound the pre-widen rows to the
//!   batches up to the widen's, but a batch drained doesn't mean every row in
//!   it did (`poison_held`, re-staged reverses), and a phase-gap straggler
//!   belongs to its successor's batch. The origin predicate covers each of
//!   those, and it is the one [`crate::staging::converge`] already keeps
//!   exact.
//!
//! What the gate costs: a new definition waits for the table's pending
//! changes to drain before its build starts, normally one seal and drain.
//! A key held in `poison_held` doesn't hold the gate: it is held for
//! the definitions that hold it, never for the new one, and its release
//! re-derives it from the live row rather than replaying its images.
//!
//! Every install and widen sets the gate, not only a widen: an install can
//! follow an uninstall whose last rows haven't drained, and those were imaged
//! for a column set that may not cover the new reader.
//!
//! **The reconcile runs before the build starts**, on the same connection: a
//! definition registered on a captured table must not start before the widen
//! its columns need. On one staging worker the two run in sequence.
//!
//! # Narrowing
//!
//! A narrow replaces the functions with ones that image fewer columns, with
//! no table lock and no gate. A writer still running the old body stages
//! rows with more columns than anyone reads, which harms nothing. The
//! reconcile pass narrows only from a catalog read after the drop of the
//! definition that read the dropped columns committed, since the next rows
//! lack them.
//!
//! # Partitioned tables (ADR-0017)
//!
//! A statement trigger fires only for the table a statement names, so a
//! partitioned root's capture needs the five triggers on the root, on each
//! middle and on each leaf ([`descendants`] lists them). All of them call the
//! *root's* one function set, so every write stages under the root's
//! identity. The generated bodies name columns instead of numbering them, so
//! one function set serves leaves whose attribute numbers differ (dropped or
//! reordered columns). A statement trigger isn't copied from a partitioned
//! table to its partitions, so the nodes' triggers are created explicitly,
//! and an install is idempotent per node (`CREATE OR REPLACE TRIGGER`).
//!
//! A tree of up to [`SINGLE_TRANSACTION_TABLES`] tables (root included)
//! installs, and uninstalls, in one transaction under one `LOCK TABLE` of
//! the root, which locks every table under it. A larger tree installs the
//! root first and then one table per transaction, so no writer waits behind
//! a long hold; the gate and the rebuilds are recorded in the last
//! transaction, the one that completes capture. [`installed`] reports a root whose tree is missing any
//! node's triggers as [`Installed::Partial`], so a restart mid-install runs
//! the install again and it adds only the missing nodes. An uninstall of a
//! large tree drops the nodes' triggers first and the root's last, so an
//! interrupted one is found again the same way.
//!
//! A table attached to the tree later gets no triggers from `ATTACH
//! PARTITION`, which clones only row triggers, so it reads as a bare node
//! and the next install adds its five. A table detached from the tree keeps
//! its triggers, which still call the root's functions: [`uninstall`] drops
//! them along with the nodes' ([`bearers`]), and the reconcile pass lists
//! such a table as installed in its own right, so it drops them while the
//! root stays captured (ADR-0017's cleanup). The cleanup waits for a
//! `DETACH CONCURRENTLY` to complete: a writer with an older snapshot can
//! still write through the parent into the pending leaf.
//!
//! Until the pass drops them, the detached table's triggers stage nothing:
//! a partitioned source's functions begin by comparing
//! `pg_partition_root(tg_relid)` with the root ([`sql`]'s "A table that left
//! the tree stages nothing"). [`installed`] marks the spec it reads back as
//! partitioned from the root's `relkind`, so the comparison of generated and
//! installed function bodies covers the check.
//!
//! # What is installed
//!
//! The catalog is the only record: the triggers in
//! `pg_trigger`, and each function's spec in its `COMMENT ON FUNCTION`
//! ([`sql::comment_text`]). `pg_dump` carries both, and there is no
//! Trellis table to disagree with them. [`installed`] reads them back, and
//! also compares each function's source with what this build generates, so
//! a function an older generator left behind is replaced.
//!
//! Nothing else this module writes is installed state. The capture gate is
//! a row of its own ([`super::gate`]) that goes when no pending change holds
//! it. A [`LockWait`] is returned;
//! the reconcile pass records the latest one per table in `capture_holdups`,
//! which describes other sessions, not what is installed.
//!
//! # The Trellis role
//!
//! One role does everything Trellis does: it owns the ring (and the
//! instance schema, unless a DBA pre-created it, below), runs the migrations
//! and the staging worker, and installs and owns the capture functions.
//! There is no separate capture role. What that role needs:
//!
//! - **On each captured table, ownership or membership in the owning
//!   role.** `CREATE TRIGGER` needs the `TRIGGER` privilege, but `ALTER TABLE
//!   … ENABLE ALWAYS TRIGGER` needs ownership, and
//!   membership in the owner counts as ownership.
//! - **No superuser.** Nothing here signals a backend, and reading the
//!   blockers needs no privilege (`pg_read_all_stats` only makes other
//!   roles' blockers less `unknown`).
//!
//! The functions are `SECURITY DEFINER`, so they run as their owner, the
//! Trellis role, whichever application role writes the table, and the
//! application needs no privilege on Trellis's schema. What they write is the
//! ring (`seg_*`, the `change_id` sequence and `ring_slot_mirror`), so their
//! owner is the ring's owner, the role that ran the migrations. A session
//! that isn't that role itself but a member of it (a login role granted the
//! Trellis role, say) hands the functions to it with `ALTER FUNCTION … OWNER
//! TO` in the same transaction, and a session that isn't a member fails
//! there, loudly, instead of installing functions that can't write.
//!
//! The schema's owner is not a stand-in for the ring's. A DBA
//! can pre-create the schema as one role and have a login role that is a
//! member of it run the migrations: `create schema if not exists` keeps the
//! DBA's role as the schema's owner, while the ring belongs to the login
//! role, and the schema's owner has no privilege on it. Functions owned by
//! the schema's owner would fail every captured write.

use std::time::{Duration, Instant, SystemTime};

use tokio_postgres::types::{PgLsn, Type};
use tokio_postgres::{Client, GenericClient};

use super::CaptureError;
use super::sql::{self, CaptureEvent, CaptureSpec};
use crate::locks::{self, DdlRetry, USER_TABLE_DDL_LOCK_TIMEOUT};

/// What the catalog says is installed for one table ([`installed`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Installed {
    /// No trigger and no function of this instance for the table.
    Absent,
    /// All five triggers exist, enabled `ALWAYS`, each calling its event's
    /// function, and all five functions' comments record `spec`. `current`
    /// says whether every function's source is what this build generates for
    /// `spec`.
    Complete { spec: CaptureSpec, current: bool },
    /// Some of the pieces exist and some don't, or don't agree: an install
    /// that a crash or a hand edit left incomplete. Each fault is one line
    /// for a log.
    Partial { faults: Vec<String> },
}

/// What [`reconcile`] does to bring a table's capture to a spec ([`plan`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureAction {
    /// Installed and current already.
    Unchanged,
    /// [`install`]: create or repair the functions and triggers.
    Install,
    /// [`widen`]: replace the functions under the table lock.
    Widen,
    /// [`narrow`]: replace the functions without a lock.
    Narrow,
}

/// What it takes to bring `installed` to `desired`.
///
/// A narrow is only for a spec that images a subset of what is installed,
/// with the same key, from current functions. Anything else that differs
/// widens: a new column, a changed key, a new group-key column, or a
/// function this build wouldn't generate (whose images can't be trusted to
/// be a superset of anything).
pub fn plan(installed: &Installed, desired: &CaptureSpec) -> CaptureAction {
    match installed {
        Installed::Absent | Installed::Partial { .. } => CaptureAction::Install,
        Installed::Complete { spec, current } => {
            if *current && spec == desired {
                CaptureAction::Unchanged
            } else if *current && narrows(spec, desired) {
                CaptureAction::Narrow
            } else {
                CaptureAction::Widen
            }
        }
    }
}

/// Whether `desired` images nothing `installed` doesn't: the same table and
/// key, and a subset of its image columns.
fn narrows(installed: &CaptureSpec, desired: &CaptureSpec) -> bool {
    installed.table() == desired.table()
        && installed.key() == desired.key()
        && desired
            .columns()
            .iter()
            .all(|c| installed.columns().contains(c))
}

/// Reads what instance `schema` has installed for `table` (an unquoted
/// `schema.table` identity) from `pg_trigger`, `pg_proc` and the functions'
/// comments.
///
/// One statement reads all five events and, for a partitioned root, every
/// node's triggers, so a read racing a one-transaction install or uninstall
/// sees it either whole or not at all, never a spurious
/// [`Installed::Partial`]. (A node-by-node one is partial in between.)
pub async fn installed(
    client: &impl GenericClient,
    schema: &str,
    table: &str,
) -> Result<Installed, CaptureError> {
    let regclass = crate::defs::ddl::regclass_arg(table);
    let mut functions = Vec::with_capacity(CaptureEvent::ALL.len());
    let mut triggers = Vec::with_capacity(CaptureEvent::ALL.len());
    for event in CaptureEvent::ALL {
        functions.push(sql::function_name(table, event)?);
        triggers.push(sql::trigger_name(schema, event));
    }
    // A comment this generator didn't write reads as no spec, rather than
    // failing the cast.
    let rows = client
        .query_typed(
            &format!(
                "select p.oid is not null, p.prosrc, \
                    c.j ->> 'table', c.j ->> 'event', \
                    case when c.j is not null then array( \
                        select pg_catalog.jsonb_array_elements_text(c.j -> 'key')) end, \
                    case when c.j is not null then array( \
                        select pg_catalog.jsonb_array_elements_text(c.j -> 'columns')) end, \
                    t.oid is not null, t.tgenabled::text, t.tgfoid = p.oid, \
                    case when e.ord = 1 and exists ( \
                        select 1 from pg_catalog.pg_class r \
                        where r.oid = pg_catalog.to_regclass($3) and r.relkind = 'p') \
                    then array({NODES_MISSING_CAPTURE}) end, \
                    exists (select 1 from pg_catalog.pg_class r \
                        where r.oid = pg_catalog.to_regclass($3) and r.relkind = 'p') \
             from unnest($2::text[], $4::text[]) with ordinality as e(function, trigger, ord) \
             left join pg_catalog.pg_proc p \
               on p.proname = e.function and p.pronargs = 0 \
              and p.pronamespace = ( \
                  select oid from pg_catalog.pg_namespace where nspname = $1) \
             left join lateral ( \
                 select case when d.description like '{{\"trellis_capture\":1,%' \
                             then d.description::jsonb end as j \
                 from pg_catalog.pg_description d \
                 where d.objoid = p.oid \
                   and d.classoid = 'pg_catalog.pg_proc'::pg_catalog.regclass \
                   and d.objsubid = 0) c on true \
             left join pg_catalog.pg_trigger t \
               on t.tgrelid = pg_catalog.to_regclass($3) and t.tgname = e.trigger \
              and not t.tgisinternal \
             order by e.ord"
            ),
            &[
                (&schema, Type::TEXT),
                (&functions, Type::TEXT_ARRAY),
                (&regclass, Type::TEXT),
                (&triggers, Type::TEXT_ARRAY),
            ],
        )
        .await?;
    let partitioned: bool = rows.first().is_some_and(|row| row.get(10));
    let mut present = false;
    let mut current = true;
    let mut faults = Vec::new();
    let mut specs = Vec::new();
    for (((event, function), trigger), row) in CaptureEvent::ALL
        .into_iter()
        .zip(&functions)
        .zip(&triggers)
        .zip(&rows)
    {
        let has_function: bool = row.get(0);
        let has_trigger: bool = row.get(6);
        present |= has_function || has_trigger;
        let what = event.as_str();
        if !has_function {
            faults.push(format!(
                "the {what} function {schema}.{function} is missing"
            ));
        } else {
            let recorded =
                recorded_spec(table, what, row.get(2), row.get(3), row.get(4), row.get(5))
                    .map(|spec| spec.with_partitioned(partitioned));
            match recorded {
                Some(spec) => {
                    let source: String = row.get(1);
                    current &= source == sql::function_source(schema, &spec, event);
                    specs.push(spec);
                }
                None => faults.push(format!(
                    "the {what} function {schema}.{function} has no capture comment for {table}"
                )),
            }
        }
        if !has_trigger {
            faults.push(format!(
                "the {what} trigger {trigger} on {table} is missing"
            ));
        } else {
            let enabled: String = row.get(7);
            if enabled != "A" {
                faults.push(format!(
                    "the {what} trigger {trigger} on {table} is not ENABLE ALWAYS \
                     (tgenabled = {enabled})"
                ));
            }
            if row.get::<_, Option<bool>>(8) != Some(true) {
                faults.push(format!(
                    "the {what} trigger {trigger} on {table} doesn't call {schema}.{function}"
                ));
            }
        }
    }
    if !present {
        return Ok(Installed::Absent);
    }
    // Read by the same statement, so a racing install or uninstall can't
    // show the root whole and a node bare.
    let missing = rows
        .first()
        .and_then(|row| row.get::<_, Option<Vec<String>>>(9))
        .unwrap_or_default();
    if !missing.is_empty() {
        faults.push(format!(
            "{} of {table}'s partition tree lack(s) the capture triggers: {}",
            missing.len(),
            missing.join(", ")
        ));
    }
    if faults.is_empty() && specs.windows(2).any(|pair| pair[0] != pair[1]) {
        faults.push(format!(
            "{table}'s capture functions record different column sets"
        ));
    }
    match (faults.is_empty(), specs.pop()) {
        (true, Some(spec)) => Ok(Installed::Complete { spec, current }),
        _ => Ok(Installed::Partial { faults }),
    }
}

/// The partitioned root `$root` (a SQL regclass expression)'s tree below it as
/// `(relid, level)` rows, one level per step of `pg_inherits`. Read from the
/// catalogs alone: `pg_partition_tree` takes `ACCESS SHARE` on every node, so
/// a leaf held under `ACCESS EXCLUSIVE` would stall the caller, which no
/// `lock_timeout` bounds. Like it, this omits a child whose detach is pending,
/// and yields nothing for a root that isn't partitioned.
macro_rules! partition_nodes {
    ($root:literal) => {
        concat!(
            "(with recursive t(relid, level) as ( \
                 select i.inhrelid, 1 from pg_catalog.pg_inherits i \
                 where i.inhparent = ",
            $root,
            " and not i.inhdetachpending \
                   and exists (select 1 from pg_catalog.pg_class r \
                               where r.oid = ",
            $root,
            " and r.relkind = 'p') \
                 union all \
                 select i.inhrelid, t.level + 1 \
                 from t join pg_catalog.pg_inherits i on i.inhparent = t.relid \
                 where not i.inhdetachpending) \
             select relid, level from t)"
        )
    };
}

/// The tables under the partitioned root `table` (an unquoted `schema.table`),
/// every level, shallowest first and by name within a level; empty for a table
/// that isn't partitioned or doesn't exist. Each carries its own five capture
/// triggers, because a statement trigger fires only for the table a
/// statement names (ADR-0017).
pub async fn descendants(
    client: &impl GenericClient,
    table: &str,
) -> Result<Vec<String>, CaptureError> {
    Ok(client
        .query(
            concat!(
                "select n.nspname::text || '.' || c.relname::text from ",
                partition_nodes!("pg_catalog.to_regclass($1)"),
                " d join pg_catalog.pg_class c on c.oid = d.relid \
                 join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
                 order by d.level, 1"
            ),
            &[&crate::defs::ddl::regclass_arg(table)],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect())
}

/// The partitioned root `table` and the tables under it, by oid, with each
/// one's `schema.table` text; empty for a table that isn't partitioned or
/// doesn't exist. A node whose detach is pending is left out, as in
/// [`descendants`]. The trigger record keeps one set of rows per entry
/// (`super::triggers`).
pub(crate) async fn tree_nodes(
    client: &impl GenericClient,
    table: &str,
) -> Result<Vec<(u32, String)>, CaptureError> {
    Ok(client
        .query(
            concat!(
                "select d.relid, n.nspname::text || '.' || c.relname::text from (",
                "select r.oid as relid from pg_catalog.pg_class r \
                 where r.oid = pg_catalog.to_regclass($1) and r.relkind = 'p' \
                 union all select p.relid from ",
                partition_nodes!("pg_catalog.to_regclass($1)"),
                " p) d join pg_catalog.pg_class c on c.oid = d.relid \
                 join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
                 order by 2"
            ),
            &[&crate::defs::ddl::regclass_arg(table)],
        )
        .await?
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect())
}

/// The query behind [`nodes_missing_capture`], which [`installed`] also
/// embeds: `$1` the instance schema, `$2` the root's functions, `$3` the
/// root's regclass text and `$4` the triggers, in [`CaptureEvent::ALL`] order.
const NODES_MISSING_CAPTURE: &str = concat!(
    "select n.nspname::text || '.' || c.relname::text from ",
    partition_nodes!("pg_catalog.to_regclass($3)"),
    " d join pg_catalog.pg_class c on c.oid = d.relid \
     join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
     where exists ( \
         select 1 from unnest($2::text[], $4::text[]) as e(function, trigger) \
         where not exists ( \
             select 1 from pg_catalog.pg_trigger t \
             join pg_catalog.pg_proc p on p.oid = t.tgfoid \
             where t.tgrelid = d.relid and t.tgname = e.trigger \
               and not t.tgisinternal and t.tgenabled = 'A' \
               and p.proname = e.function \
               and p.pronamespace = ( \
                   select oid from pg_catalog.pg_namespace where nspname = $1))) \
     order by d.level, 1"
);

/// [`NODES_MISSING_CAPTURE`]'s nodes by oid (`bigint`), unordered.
const NODE_OIDS_MISSING_CAPTURE: &str = concat!(
    "select d.relid::bigint from ",
    partition_nodes!("pg_catalog.to_regclass($3)"),
    " d where exists ( \
         select 1 from unnest($2::text[], $4::text[]) as e(function, trigger) \
         where not exists ( \
             select 1 from pg_catalog.pg_trigger t \
             join pg_catalog.pg_proc p on p.oid = t.tgfoid \
             where t.tgrelid = d.relid and t.tgname = e.trigger \
               and not t.tgisinternal and t.tgenabled = 'A' \
               and p.proname = e.function \
               and p.pronamespace = ( \
                   select oid from pg_catalog.pg_namespace where nspname = $1)))"
);

/// The tables under the partitioned root `table` that lack any of the five
/// capture triggers: missing, not `ENABLE ALWAYS`, or not calling the root's
/// function for the event. `functions` and `triggers` are the root's names,
/// in [`CaptureEvent::ALL`] order. Same order as [`descendants`].
async fn nodes_missing_capture(
    client: &impl GenericClient,
    schema: &str,
    table: &str,
    functions: &[String],
    triggers: &[String],
) -> Result<Vec<String>, CaptureError> {
    Ok(client
        .query(
            NODES_MISSING_CAPTURE,
            &[
                &schema,
                &functions,
                &crate::defs::ddl::regclass_arg(table),
                &triggers,
            ],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect())
}

/// Whether installing `spec`'s capture only adds nodes new to its table's
/// recorded tree to a root already captured: the table is a partitioned root
/// whose own five triggers are in place, calling its functions, which
/// already are `spec`'s, and every node that lacks any of its triggers is
/// absent, by oid, from the tree the reconcile pass recorded
/// (`super::partitions::recorded`). A new or attached leaf is such a node,
/// and the pass queues its correction
/// (`crate::staging::partition_correction`), which a reader that engine
/// serves takes instead of a rebuild and a whole-table catch-up
/// (`crate::intake::catch_up::park_table_catch_ups`).
///
/// A recorded node whose triggers were dropped by hand is not. Neither is an
/// install that also changes the functions (a reader registered in the same
/// pass needs a column they don't image): its capture gate holds that
/// reader's build until every row the old functions staged has drained (see
/// "Widening and the capture gate").
async fn adds_only_new_nodes(
    client: &impl GenericClient,
    schema: &str,
    spec: &CaptureSpec,
) -> Result<bool, CaptureError> {
    let table = spec.table();
    let Some(recorded) = super::partitions::recorded(client, table).await? else {
        return Ok(false);
    };
    let functions = function_names(table)?;
    let triggers = trigger_names(schema);
    let sources: Vec<String> = CaptureEvent::ALL
        .into_iter()
        .map(|event| sql::function_source(schema, spec, event))
        .collect();
    let root = crate::defs::ddl::regclass_arg(table);
    let row = client
        .query_one(
            &format!(
                "select not exists ( \
                     select 1 from unnest($2::text[], $4::text[], $5::text[]) \
                         as e(function, trigger, source) \
                     where not exists ( \
                         select 1 from pg_catalog.pg_trigger t \
                         join pg_catalog.pg_proc p on p.oid = t.tgfoid \
                         where t.tgrelid = pg_catalog.to_regclass($3) and t.tgname = e.trigger \
                           and not t.tgisinternal and t.tgenabled = 'A' \
                           and p.proname = e.function and p.prosrc = e.source \
                           and p.pronamespace = ( \
                               select oid from pg_catalog.pg_namespace where nspname = $1))), \
                        array({NODE_OIDS_MISSING_CAPTURE})"
            ),
            &[&schema, &functions, &root, &triggers, &sources],
        )
        .await?;
    let root_captured: bool = row.get(0);
    let bare: Vec<i64> = row.get(1);
    let known: std::collections::HashSet<i64> =
        recorded.nodes.iter().map(|n| i64::from(n.oid)).collect();
    Ok(root_captured && !bare.is_empty() && bare.iter().all(|oid| !known.contains(oid)))
}

/// The spec a function's comment records, if it records one for `table`'s
/// `event` that [`CaptureSpec::new`] accepts.
fn recorded_spec(
    table: &str,
    event: &str,
    recorded_table: Option<String>,
    recorded_event: Option<String>,
    key: Option<Vec<String>>,
    columns: Option<Vec<String>>,
) -> Option<CaptureSpec> {
    if recorded_table.as_deref() != Some(table) || recorded_event.as_deref() != Some(event) {
        return None;
    }
    CaptureSpec::new(table, key?, columns?).ok()
}

/// How far one pass of a locking operation got: it landed, or it ran out of
/// its deadline waiting for the table lock and changed nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub enum Progress<T> {
    /// The operation landed (or had nothing to do), with its result.
    Done(T),
    /// The deadline passed while the table lock was held against it. Nothing
    /// changed. The report says who holds it (see "Waiting, never
    /// cancelling").
    Waiting(Box<LockWait>),
}

impl<T> Progress<T> {
    /// The result, if the operation landed.
    #[cfg(any(test, feature = "test-util"))]
    pub fn done(self) -> Option<T> {
        match self {
            Progress::Done(value) => Some(value),
            Progress::Waiting(_) => None,
        }
    }

    /// The same progress, with `f` applied to a landed result.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Progress<U> {
        match self {
            Progress::Done(value) => Progress::Done(f(value)),
            Progress::Waiting(wait) => Progress::Waiting(wait),
        }
    }
}

/// Brings `desired.table()`'s capture to `desired` ([`plan`]) and returns
/// what it did. `deadline` bounds the retries of a locked table, as
/// [`DdlRetry::new`]'s does: past it, the result is [`Progress::Waiting`]
/// and nothing has changed.
pub async fn reconcile(
    client: &mut Client,
    schema: &str,
    desired: &CaptureSpec,
    deadline: Option<Instant>,
) -> Result<Progress<CaptureAction>, CaptureError> {
    let action = plan(
        &installed(&*client, schema, desired.table()).await?,
        desired,
    );
    let progress = match action {
        CaptureAction::Unchanged => Progress::Done(()),
        CaptureAction::Install => install_tree(client, schema, desired, deadline).await?,
        CaptureAction::Widen => run(client, schema, Op::Widen(desired), deadline).await?,
        CaptureAction::Narrow => Progress::Done(narrow(client, schema, desired).await?),
    };
    Ok(progress.map(|()| action))
}

/// Installs `spec`'s capture, and records the table's capture gate, in one
/// transaction under the table's `SHARE ROW EXCLUSIVE` lock, retried as the
/// module doc describes. Lands `false`, having done nothing, when `spec` is
/// already installed and current: a repeat install takes no lock and records
/// no gate.
#[cfg(any(test, feature = "test-util"))]
pub async fn install(
    client: &mut Client,
    schema: &str,
    spec: &CaptureSpec,
    deadline: Option<Instant>,
) -> Result<Progress<bool>, CaptureError> {
    if plan(&installed(&*client, schema, spec.table()).await?, spec) == CaptureAction::Unchanged {
        return Ok(Progress::Done(false));
    }
    Ok(install_tree(client, schema, spec, deadline)
        .await?
        .map(|()| true))
}

/// The most tables of one partition tree, the root included, whose capture
/// installs or uninstalls in a single transaction. Holding the locks of one
/// transaction stalls a writer about 0.3 ms per table (34-44 ms at 100,
/// 271-305 ms at 1,000), and the 50 ms lock timeout bounds the wait for a
/// lock, not the time the transaction holds it. A larger tree goes root
/// first and then one table per transaction: a writer's worst commit stays
/// at 7-56 ms at any size, for about twice the total time.
pub const SINGLE_TRANSACTION_TABLES: usize = 100;

/// Installs `spec`'s capture on its table and, for a partitioned root, on
/// every table under it (see "Partitioned tables"). The gate is recorded in
/// the transaction that completes the install.
async fn install_tree(
    client: &mut Client,
    schema: &str,
    spec: &CaptureSpec,
    deadline: Option<Instant>,
) -> Result<Progress<()>, CaptureError> {
    let nodes = descendants(&*client, spec.table()).await?;
    let new_nodes = adds_only_new_nodes(&*client, schema, spec).await?;
    if nodes.len() < SINGLE_TRANSACTION_TABLES {
        let op = Op::Install {
            spec,
            scope: Scope::All,
            last: true,
            new_nodes,
        };
        return run(client, schema, op, deadline).await;
    }
    // A resumed install skips the tables that already have their triggers.
    let functions = function_names(spec.table())?;
    let triggers = trigger_names(schema);
    let missing =
        nodes_missing_capture(&*client, schema, spec.table(), &functions, &triggers).await?;
    let root = Op::Install {
        spec,
        scope: Scope::Root,
        last: missing.is_empty(),
        new_nodes,
    };
    if let waiting @ Progress::Waiting(_) = run(client, schema, root, deadline).await? {
        return Ok(waiting);
    }
    for (i, node) in missing.iter().enumerate() {
        let op = Op::Install {
            spec,
            scope: Scope::Node(node),
            last: i + 1 == missing.len(),
            new_nodes,
        };
        if let waiting @ Progress::Waiting(_) = run(client, schema, op, deadline).await? {
            return Ok(waiting);
        }
    }
    Ok(Progress::Done(()))
}

/// Installs `spec`'s capture on its whole tree in one transaction, whatever
/// the tree's size: the shape [`install`] gives a tree of fewer than
/// [`SINGLE_TRANSACTION_TABLES`] tables. For measuring what that shape would
/// stall writers for on a larger tree (ADR-0017 P10).
#[cfg(any(test, feature = "internals"))]
pub async fn install_in_one_transaction(
    client: &mut Client,
    schema: &str,
    spec: &CaptureSpec,
    deadline: Option<Instant>,
) -> Result<Progress<()>, CaptureError> {
    let op = Op::Install {
        spec,
        scope: Scope::All,
        last: true,
        new_nodes: false,
    };
    run(client, schema, op, deadline).await
}

fn function_names(table: &str) -> Result<Vec<String>, CaptureError> {
    CaptureEvent::ALL
        .into_iter()
        .map(|event| sql::function_name(table, event))
        .collect()
}

fn trigger_names(schema: &str) -> Vec<String> {
    CaptureEvent::ALL
        .into_iter()
        .map(|event| sql::trigger_name(schema, event))
        .collect()
}

/// Replaces an installed table's capture functions with `spec`'s under the
/// table's `SHARE ROW EXCLUSIVE` lock, and records the table's capture gate
/// (see "Widening and the capture gate"), retried as the module doc
/// describes.
#[cfg(any(test, feature = "internals"))]
pub async fn widen(
    client: &mut Client,
    schema: &str,
    spec: &CaptureSpec,
    deadline: Option<Instant>,
) -> Result<Progress<()>, CaptureError> {
    run(client, schema, Op::Widen(spec), deadline).await
}

/// Replaces an installed table's capture functions with `spec`'s, which
/// image a subset of the installed columns, in one transaction with no table
/// lock and no gate (see "Narrowing").
pub async fn narrow(
    client: &mut Client,
    schema: &str,
    spec: &CaptureSpec,
) -> Result<(), CaptureError> {
    let owner = foreign_ring_owner(&*client, schema).await?;
    let txn = client.transaction().await?;
    for statement in sql::function_statements(schema, spec)? {
        txn.batch_execute(&statement).await?;
    }
    if let Some(owner) = owner {
        for statement in sql::owner_statements(schema, spec.table(), &owner)? {
            txn.batch_execute(&statement).await?;
        }
    }
    super::baseline::record(&txn, spec.table(), spec.columns()).await?;
    super::triggers::record(
        &txn,
        schema,
        spec.table(),
        super::triggers::Mode::IfAbsent,
        super::triggers::Covers::Whole,
    )
    .await?;
    txn.commit().await?;
    Ok(())
}

/// Drops `table`'s capture triggers and functions, in one transaction under
/// the table's `ACCESS EXCLUSIVE` lock (`DROP TRIGGER`'s), retried as the
/// module doc describes. A partitioned root's nodes, and any table detached
/// from its tree that still carries its triggers, lose theirs too (see
/// "Partitioned tables"). A table the application already dropped takes its
/// triggers with it, so only the functions go. Lands `false`, having done
/// nothing, when nothing is installed.
pub async fn uninstall(
    client: &mut Client,
    schema: &str,
    table: &str,
    deadline: Option<Instant>,
) -> Result<Progress<bool>, CaptureError> {
    if installed(&*client, schema, table).await? == Installed::Absent {
        return Ok(Progress::Done(false));
    }
    let mut nodes = descendants(&*client, table).await?;
    for bearer in bearers(&*client, schema, table).await? {
        if !nodes.contains(&bearer) {
            nodes.push(bearer);
        }
    }
    if nodes.len() < SINGLE_TRANSACTION_TABLES {
        return Ok(run(
            client,
            schema,
            Op::Uninstall {
                table,
                scope: Scope::All,
            },
            deadline,
        )
        .await?
        .map(|()| true));
    }
    // The tables under the root first and the root last, so an interrupted
    // uninstall still finds the root's triggers and resumes.
    for node in nodes.iter().rev() {
        let op = Op::Uninstall {
            table,
            scope: Scope::Node(node),
        };
        if let waiting @ Progress::Waiting(_) = run(client, schema, op, deadline).await? {
            return Ok(waiting.map(|()| true));
        }
    }
    let root = Op::Uninstall {
        table,
        scope: Scope::Root,
    };
    Ok(run(client, schema, root, deadline).await?.map(|()| true))
}

/// Which tables of a partition tree one attempt covers.
#[derive(Clone, Copy)]
enum Scope<'a> {
    /// The table and, if it is a partitioned root, every table under it.
    /// `LOCK TABLE` on a partitioned table locks its partitions too.
    All,
    /// The root alone: its functions (an install) and its own triggers.
    Root,
    /// One table under the root: its triggers only.
    Node(&'a str),
}

/// One of the operations that lock the table, for [`run`].
#[derive(Clone, Copy)]
enum Op<'a> {
    /// `last` is whether this attempt completes the install, and so records
    /// the gate (see "Partitioned tables"). `new_nodes` is whether
    /// the install only adds nodes new to the recorded tree to a captured
    /// root ([`adds_only_new_nodes`]).
    Install {
        spec: &'a CaptureSpec,
        scope: Scope<'a>,
        last: bool,
        new_nodes: bool,
    },
    Widen(&'a CaptureSpec),
    Uninstall {
        table: &'a str,
        scope: Scope<'a>,
    },
}

impl Op<'_> {
    /// The captured table, the root of a partition tree.
    fn table(&self) -> &str {
        match self {
            Op::Install { spec, .. } | Op::Widen(spec) => spec.table(),
            Op::Uninstall { table, .. } => table,
        }
    }

    fn scope(&self) -> Scope<'_> {
        match self {
            Op::Install { scope, .. } | Op::Uninstall { scope, .. } => *scope,
            Op::Widen(_) => Scope::All,
        }
    }

    /// The table the attempt locks first.
    fn locked(&self) -> &str {
        match self.scope() {
            Scope::Node(node) => node,
            _ => self.table(),
        }
    }

    /// The `LOCK TABLE` statement for the attempt.
    fn lock_statement(&self) -> Result<String, CaptureError> {
        let only = if matches!(self.scope(), Scope::All) {
            ""
        } else {
            "only "
        };
        Ok(format!(
            "lock table {only}{} in {} mode",
            sql::quoted_table(self.locked())?,
            self.lock().0
        ))
    }

    fn operation(&self) -> LockingOperation {
        match self {
            Op::Install { .. } => LockingOperation::Install,
            Op::Widen(_) => LockingOperation::Widen,
            Op::Uninstall { .. } => LockingOperation::Uninstall,
        }
    }

    /// The lock the operation takes on the table, as `LOCK TABLE` spells it,
    /// and the lock modes others hold that conflict with it, as `pg_locks`
    /// spells them.
    fn lock(&self) -> (&'static str, &'static [&'static str]) {
        const SHARE_ROW_EXCLUSIVE_CONFLICTS: &[&str] = &[
            "RowExclusiveLock",
            "ShareUpdateExclusiveLock",
            "ShareLock",
            "ShareRowExclusiveLock",
            "ExclusiveLock",
            "AccessExclusiveLock",
        ];
        const ACCESS_EXCLUSIVE_CONFLICTS: &[&str] = &[
            "AccessShareLock",
            "RowShareLock",
            "RowExclusiveLock",
            "ShareUpdateExclusiveLock",
            "ShareLock",
            "ShareRowExclusiveLock",
            "ExclusiveLock",
            "AccessExclusiveLock",
        ];
        match self {
            Op::Install { .. } | Op::Widen(_) => {
                ("share row exclusive", SHARE_ROW_EXCLUSIVE_CONFLICTS)
            }
            Op::Uninstall { .. } => ("access exclusive", ACCESS_EXCLUSIVE_CONFLICTS),
        }
    }
}

/// Runs `op` in attempts of one transaction each, under
/// [`USER_TABLE_DDL_LOCK_TIMEOUT`], until one lands or `deadline` stops the
/// retries (see [`DdlRetry`]). After a timed-out attempt it logs who holds
/// the table ([`BlockerLog`]), and once `deadline` stops the retries it
/// reads them afresh for the [`LockWait`] it returns.
async fn run(
    client: &mut Client,
    schema: &str,
    op: Op<'_>,
    deadline: Option<Instant>,
) -> Result<Progress<()>, CaptureError> {
    let operation = op.operation();
    let started = Instant::now();
    let mut retry = DdlRetry::new(operation.what(), USER_TABLE_DDL_LOCK_TIMEOUT, deadline);
    let mut log = BlockerLog::new(operation);
    let mut attempts = 0;
    loop {
        attempts += 1;
        match attempt(client, schema, op, retry.lock_timeout()).await {
            Err(err) if locks::is_lock_not_available(&err) => {
                if retry.again(&err).await {
                    log.after_timeout(&*client, op, started, attempts).await;
                    continue;
                }
                let wait = lock_wait(&*client, op, started, attempts).await?;
                log.at_deadline(&wait);
                return Ok(Progress::Waiting(Box::new(wait)));
            }
            Err(err) => return Err(err),
            Ok(()) => return Ok(Progress::Done(())),
        }
    }
}

/// One attempt at `op`: the readers it rebuilds (if any) under the session's
/// `lock_timeout`, then the table lock under `lock_timeout` and the DDL.
async fn attempt(
    client: &mut Client,
    schema: &str,
    op: Op<'_>,
    lock_timeout: Duration,
) -> Result<(), CaptureError> {
    let table = op.table();
    let owner = match op {
        Op::Install {
            scope: Scope::Node(_),
            ..
        } => None,
        Op::Install { .. } | Op::Widen(_) => foreign_ring_owner(&*client, schema).await?,
        Op::Uninstall { .. } => None,
    };
    let txn = client.transaction().await?;
    let table_exists: bool = txn
        .query_one(
            "select pg_catalog.to_regclass($1) is not null",
            &[&crate::defs::ddl::regclass_arg(op.locked())],
        )
        .await?
        .get(0);
    // The definitions an install rebuilds, for the build segment
    // below.
    let mut rebuilt = Vec::new();
    // An install that only adds nodes new to a captured root's tree leaves the
    // functions as they are, so no row staged by an older body is in flight.
    let records_gate = matches!(
        op,
        Op::Install {
            last: true,
            new_nodes: false,
            ..
        } | Op::Widen(_)
    );
    match op {
        Op::Install {
            last: true,
            new_nodes,
            ..
        } => {
            rebuilt =
                crate::intake::catch_up::park_table_catch_ups(&txn, &[table.to_string()], new_nodes)
                    .await?
        }
        Op::Install { .. } | Op::Widen(_) | Op::Uninstall { .. } => {}
    }
    // A node dropped since the tree was read has nothing to lock or change.
    let locks = match op.scope() {
        Scope::Node(_) => table_exists,
        _ => table_exists || !matches!(op, Op::Uninstall { .. }),
    };
    if locks {
        // From here on the transaction holds the table, so nothing in it may
        // wait longer than the attempt's timeout.
        locks::set_local_lock_timeout(&txn, lock_timeout).await?;
        txn.batch_execute(&op.lock_statement()?).await?;
    }
    let gate: PgLsn = txn
        .query_one("select pg_catalog.pg_current_wal_insert_lsn()", &[])
        .await?
        .get(0);
    // After the lock, so a whole-tree attempt covers the tables it holds.
    let statements = match op {
        Op::Install { spec, scope, .. } => {
            let mut statements = Vec::new();
            if !matches!(scope, Scope::Node(_)) {
                statements.extend(sql::install_statements(schema, spec)?);
            }
            for node in nodes_in_scope(&txn, scope, table, table_exists).await? {
                statements.extend(sql::node_trigger_statements(schema, spec, &node)?);
            }
            statements
        }
        Op::Widen(spec) => sql::function_statements(schema, spec)?,
        Op::Uninstall { table, scope } => {
            let mut statements = Vec::new();
            let mut nodes = nodes_in_scope(&txn, scope, table, table_exists).await?;
            if !matches!(scope, Scope::Node(_)) {
                // A table that left the tree keeps triggers that call the
                // root's functions, which can't be dropped while it does.
                for bearer in bearers(&txn, schema, table).await? {
                    if !nodes.contains(&bearer) {
                        nodes.push(bearer);
                    }
                }
            }
            for node in nodes {
                statements.extend(sql::drop_node_trigger_statements(schema, &node)?);
            }
            if !matches!(scope, Scope::Node(_)) {
                statements.extend(sql::uninstall_statements(schema, table, table_exists)?);
            }
            statements
        }
    };
    for statement in statements {
        txn.batch_execute(&statement).await?;
    }
    if let Some(owner) = owner {
        for statement in sql::owner_statements(schema, table, &owner)? {
            txn.batch_execute(&statement).await?;
        }
    }
    // The columns capture images from here on, as they are now.
    match op {
        Op::Install {
            spec, scope, last, ..
        } => {
            if last {
                super::baseline::record(&txn, table, spec.columns()).await?;
            }
            // The triggers of the tables this install made them
            // on, as it made them, whether or not it is the last of several.
            let covers = match scope {
                Scope::All => super::triggers::Covers::Whole,
                Scope::Root => super::triggers::Covers::Root,
                Scope::Node(node) => super::triggers::Covers::Node(node),
            };
            super::triggers::record(
                &txn,
                schema,
                table,
                super::triggers::Mode::Overwrite,
                covers,
            )
            .await?;
        }
        Op::Widen(spec) => {
            super::baseline::record(&txn, table, spec.columns()).await?;
            super::triggers::record(
                &txn,
                schema,
                table,
                super::triggers::Mode::IfAbsent,
                super::triggers::Covers::Whole,
            )
            .await?;
        }
        Op::Uninstall {
            scope: Scope::All | Scope::Root,
            ..
        } => {
            super::baseline::forget(&txn, table).await?;
            super::triggers::forget(&txn, table).await?;
        }
        Op::Uninstall {
            scope: Scope::Node(node),
            ..
        } => super::triggers::forget_node(&txn, table, node).await?,
    }
    if records_gate {
        super::gate::record(&txn, table, gate).await?;
    }
    // Last, so the share lock on the active segment is held only for the
    // commit, not for the table lock's wait.
    crate::staging::build::stamp_rebuild_seg(&txn, &rebuilt).await?;
    txn.commit().await?;
    Ok(())
}

/// The tables of the tree rooted at `root` that `scope` names: the tree under
/// the root for [`Scope::All`], the one table for [`Scope::Node`] and none for
/// [`Scope::Root`].
async fn nodes_in_scope(
    client: &impl GenericClient,
    scope: Scope<'_>,
    root: &str,
    exists: bool,
) -> Result<Vec<String>, CaptureError> {
    match scope {
        Scope::All => descendants(client, root).await,
        Scope::Root => Ok(Vec::new()),
        Scope::Node(node) if exists => Ok(vec![node.to_string()]),
        Scope::Node(_) => Ok(Vec::new()),
    }
}

/// Every table other than `root` that carries a capture trigger calling one
/// of `root`'s functions: its tree's nodes and any table detached from the
/// tree since its install. An uninstall drops their triggers before the
/// functions they depend on. Ordered by name.
async fn bearers(
    client: &impl GenericClient,
    schema: &str,
    root: &str,
) -> Result<Vec<String>, CaptureError> {
    let functions = function_names(root)?;
    let triggers = trigger_names(schema);
    Ok(client
        .query(
            "select distinct n.nspname::text || '.' || c.relname::text \
             from pg_catalog.pg_trigger t \
             join pg_catalog.pg_proc p on p.oid = t.tgfoid \
             join pg_catalog.pg_class c on c.oid = t.tgrelid \
             join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
             where t.tgname = any($3) and not t.tgisinternal \
               and p.proname = any($2) \
               and p.pronamespace = ( \
                   select oid from pg_catalog.pg_namespace where nspname = $1) \
               and t.tgrelid is distinct from pg_catalog.to_regclass($4) \
             order by 1",
            &[
                &schema,
                &functions,
                &triggers,
                &crate::defs::ddl::regclass_arg(root),
            ],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect())
}

/// A scalar subquery for the oid of the role that owns the ring of the
/// instance schema bound to `$1`, the role the capture functions belong to
/// (see "The Trellis role"); `null` when the schema has no ring. `seg_0`
/// stands for the ring: the migrations create every ring table as one role,
/// and `self_check`'s capture audit, which expects the functions to belong
/// to this same role, also checks that it holds every privilege their body
/// uses on the other ring objects.
pub(crate) const RING_OWNER: &str = "(select c.relowner from pg_catalog.pg_class c \
     join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
     where n.nspname = $1 and c.relname = 'seg_0')";

/// The role that owns instance schema `schema`'s ring ([`RING_OWNER`]), when
/// the session's role isn't it: the functions are handed to it (see "The
/// Trellis role").
async fn foreign_ring_owner(
    client: &impl GenericClient,
    schema: &str,
) -> Result<Option<String>, CaptureError> {
    let row = client
        .query_one(
            &format!(
                "select case when o.owner = r.oid then null \
                             else pg_catalog.pg_get_userbyid(o.owner)::text end \
                 from (select {RING_OWNER} as owner) o, pg_catalog.pg_roles r \
                 where r.rolname = current_user"
            ),
            &[&schema],
        )
        .await?;
    Ok(row.get(0))
}

/// The longest query text a [`Blocker`] carries, in characters.
const BLOCKER_QUERY_CHARS: i32 = 200;

/// A capture operation that takes the table lock, as a [`LockWait`] names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockingOperation {
    Install,
    Widen,
    Uninstall,
}

impl LockingOperation {
    /// `install`, `widen` or `uninstall`.
    pub fn as_str(self) -> &'static str {
        match self {
            LockingOperation::Install => "install",
            LockingOperation::Widen => "widen",
            LockingOperation::Uninstall => "uninstall",
        }
    }

    /// The lock the operation takes on the table, as `pg_locks` spells it.
    pub fn lock_mode(self) -> &'static str {
        match self {
            LockingOperation::Install | LockingOperation::Widen => "ShareRowExclusiveLock",
            LockingOperation::Uninstall => "AccessExclusiveLock",
        }
    }

    fn what(self) -> &'static str {
        match self {
            LockingOperation::Install => "capture install",
            LockingOperation::Widen => "capture widen",
            LockingOperation::Uninstall => "capture uninstall",
        }
    }
}

/// What a capture operation that ran out of its deadline was waiting for
/// (see "Waiting, never cancelling"). Every field is plain data (text,
/// integers, timestamps and a three-way enum), so C5 can store one as it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockWait {
    /// The table the operation locks, as its spec names it.
    pub table: String,
    pub operation: LockingOperation,
    /// The lock the operation asks for, as `pg_locks` spells it
    /// (`ShareRowExclusiveLock`, `AccessExclusiveLock`).
    pub lock_mode: String,
    /// When this pass began waiting: its first attempt. A caller that
    /// retries in passes keeps the first pass's value.
    pub waiting_since: SystemTime,
    /// How many attempts this call made at the lock: the first always runs,
    /// and a retry only if it could end by the call's deadline.
    pub attempts: u32,
    /// When the blockers were read (Postgres's `clock_timestamp()`).
    pub observed_at: SystemTime,
    /// Every other session holding, or queued for, a lock on the table that
    /// conflicts with `lock_mode`, in pid order, prepared transactions last.
    /// One entry per lock mode, so a session holding two conflicting modes
    /// (an insert, then `SHARE UPDATE EXCLUSIVE`) or holding one and queued
    /// for another appears twice. Empty if they all let go between the last
    /// attempt and the read.
    pub blockers: Vec<Blocker>,
}

impl LockWait {
    /// How long this pass waited, up to [`Self::observed_at`].
    pub fn waited(&self) -> Duration {
        self.observed_at
            .duration_since(self.waiting_since)
            .unwrap_or_default()
    }
}

impl std::fmt::Display for LockWait {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "capture {} on {} waiting {:.1?} for {}",
            self.operation.as_str(),
            self.table,
            self.waited(),
            self.lock_mode,
        )?;
        if self.blockers.is_empty() {
            return write!(f, "; no conflicting lock is held now");
        }
        write!(f, ", held against it by")?;
        for (i, blocker) in self.blockers.iter().enumerate() {
            let sep = if i == 0 { " " } else { "; " };
            write!(f, "{sep}{}", blocker.describe(self.observed_at))?;
        }
        Ok(())
    }
}

/// One session's hold on, or queued request for, a lock mode that conflicts
/// with the one a capture operation asks for (a `pg_locks` row).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Blocker {
    /// `None` for a prepared transaction, which has no backend.
    pub pid: Option<i32>,
    /// A prepared transaction's `gid`; `None` for a backend.
    pub prepared_gid: Option<String>,
    /// `pg_stat_activity.backend_type`: `client backend`, `autovacuum
    /// worker`, …; `prepared transaction` for one; or `unknown` for a
    /// backend this role may not see (another role's, without
    /// `pg_read_all_stats`), whose query reads `<insufficient privilege>`.
    pub backend_type: String,
    /// The lock mode it holds or asks for, as `pg_locks` spells it.
    pub mode: String,
    /// Whether it holds the lock (`true`) or is queued for it ahead of the
    /// next attempt (`false`).
    pub granted: bool,
    /// For a queued request, when it began waiting (`pg_locks.waitstart`).
    /// For a holder, when its transaction began, or a prepared
    /// transaction was prepared: it has held the lock at most since then.
    /// `None` where this role may not see it.
    pub since: Option<SystemTime>,
    /// The first 200 characters of `pg_stat_activity.query`, as far as this
    /// role may see it; empty for a prepared transaction.
    pub query: String,
}

impl Blocker {
    /// One line for a log or a status, with how long it has held or waited
    /// as of `at`.
    pub fn describe(&self, at: SystemTime) -> String {
        let who = match (self.pid, &self.prepared_gid) {
            (Some(pid), _) => format!("pid {pid}"),
            (None, Some(gid)) => format!("prepared transaction {gid:?}"),
            (None, None) => "no pid".to_string(),
        };
        let state = if self.granted { "holds" } else { "waits for" };
        let age = match self.since {
            Some(since) => format!(" for {:.1?}", at.duration_since(since).unwrap_or_default()),
            None => String::new(),
        };
        let mut line = format!("{who} ({}, {state} {}{age})", self.backend_type, self.mode);
        if !self.query.is_empty() {
            line.push_str(": ");
            line.push_str(&self.query);
        }
        line
    }
}

/// Who, other than this session, holds or is queued for a lock on `table`
/// that conflicts with `conflicts` (lock modes as `pg_locks` spells them).
#[cfg(any(test, feature = "internals"))]
pub async fn blockers(
    client: &impl GenericClient,
    table: &str,
    conflicts: &[&str],
) -> Result<Vec<Blocker>, tokio_postgres::Error> {
    blockers_of(client, table, false, conflicts).await
}

/// [`blockers`], and with `tree` also those on any table of `table`'s
/// partition tree, which a `LOCK TABLE` without `ONLY` waits for too: a
/// writer that names a leaf locks only the leaf.
async fn blockers_of(
    client: &impl GenericClient,
    table: &str,
    tree: bool,
    conflicts: &[&str],
) -> Result<Vec<Blocker>, tokio_postgres::Error> {
    // A prepared transaction's locks have no pid. Its own `transactionid`
    // lock shares their `virtualtransaction` and names the xid that
    // `pg_prepared_xacts` lists its gid under. A session holding several of
    // the tree's tables is listed once per mode.
    Ok(client
        .query(
            concat!(
                "select distinct on (l.pid, p.gid, l.mode, l.granted) \
                    l.pid, p.gid, \
                    case when l.pid is null then 'prepared transaction' \
                         else coalesce(a.backend_type, 'unknown') end, \
                    l.mode, l.granted, \
                    case when not l.granted then l.waitstart \
                         when l.pid is null then p.prepared \
                         else a.xact_start end, \
                    coalesce(pg_catalog.left(a.query, $3), '') \
             from pg_catalog.pg_locks l \
             left join pg_catalog.pg_stat_activity a on a.pid = l.pid \
             left join lateral ( \
                 select p.gid, p.prepared \
                 from pg_catalog.pg_locks x \
                 join pg_catalog.pg_prepared_xacts p on p.transaction = x.transactionid \
                 where x.pid is null and x.locktype = 'transactionid' \
                   and x.virtualtransaction = l.virtualtransaction \
                 limit 1) p on l.pid is null \
             where l.locktype = 'relation' \
               and l.database = (select oid from pg_catalog.pg_database \
                                 where datname = pg_catalog.current_database()) \
               and (l.relation = pg_catalog.to_regclass($1) \
                    or ($4 and l.relation in ( \
                        select relid from ",
                partition_nodes!("pg_catalog.to_regclass($1)"),
                " nodes))) \
               and l.pid is distinct from pg_catalog.pg_backend_pid() \
               and l.mode = any($2) \
             order by l.pid, p.gid, l.mode, l.granted"
            ),
            &[
                &crate::defs::ddl::regclass_arg(table),
                &conflicts,
                &BLOCKER_QUERY_CHARS,
                &tree,
            ],
        )
        .await?
        .into_iter()
        .map(|row| Blocker {
            pid: row.get(0),
            prepared_gid: row.get(1),
            backend_type: row.get(2),
            mode: row.get(3),
            granted: row.get(4),
            since: row.get(5),
            query: row.get(6),
        })
        .collect())
}

/// Reads the [`LockWait`] for `op`, waiting since `started`.
async fn lock_wait(
    client: &impl GenericClient,
    op: Op<'_>,
    started: Instant,
    attempts: u32,
) -> Result<LockWait, tokio_postgres::Error> {
    let conflicts = op.lock().1;
    let observed_at: SystemTime = client
        .query_one("select pg_catalog.clock_timestamp()", &[])
        .await?
        .get(0);
    let tree = matches!(op.scope(), Scope::All);
    let blockers = blockers_of(client, op.locked(), tree, conflicts).await?;
    Ok(LockWait {
        table: op.table().to_string(),
        operation: op.operation(),
        lock_mode: op.operation().lock_mode().to_string(),
        waiting_since: observed_at
            .checked_sub(started.elapsed())
            .unwrap_or(observed_at),
        attempts,
        observed_at,
        blockers,
    })
}

/// Logs what keeps a capture operation from its table lock, at most once
/// per [`BLOCKER_LOG_INTERVAL`] of one call. (A caller that runs passes with
/// short deadlines gets `debug` lines only at its deadlines; the reconcile
/// pass rate-limits its own `info` report of a wait across passes.)
struct BlockerLog {
    operation: LockingOperation,
    last_logged: Option<Instant>,
}

/// How often [`BlockerLog`] logs the blockers of an operation that keeps
/// timing out.
const BLOCKER_LOG_INTERVAL: Duration = Duration::from_secs(5);

impl BlockerLog {
    fn new(operation: LockingOperation) -> Self {
        Self {
            operation,
            last_logged: None,
        }
    }

    /// Whether a line is due, and if so, marks one logged now.
    fn due(&mut self) -> bool {
        let now = Instant::now();
        if self
            .last_logged
            .is_some_and(|logged| now.duration_since(logged) < BLOCKER_LOG_INTERVAL)
        {
            return false;
        }
        self.last_logged = Some(now);
        true
    }

    /// After a timed-out attempt that will be retried: reads and logs the
    /// blockers if a line is due. A failed read is logged and otherwise
    /// ignored: the retry goes on either way.
    async fn after_timeout(
        &mut self,
        client: &impl GenericClient,
        op: Op<'_>,
        started: Instant,
        attempts: u32,
    ) {
        if !self.due() {
            return;
        }
        match lock_wait(client, op, started, attempts).await {
            Ok(wait) => self.log(&wait, "retrying"),
            Err(error) => crate::instance_log::debug!(
                what = self.operation.what(),
                table = op.table(),
                %error,
                "reading lock blockers failed"
            ),
        }
    }

    /// Once the deadline stops the retries: logs `wait` if a line is due.
    fn at_deadline(&mut self, wait: &LockWait) {
        // The reconcile pass reports a wait that outlasts its passes at
        // `info`, rate-limited across passes (`super::reconcile`).
        crate::instance_log::debug!(
            what = self.operation.what(),
            table = %wait.table,
            "{wait}; leaving it for the next pass"
        );
    }

    fn log(&self, wait: &LockWait, next: &str) {
        crate::instance_log::info!(what = self.operation.what(), table = %wait.table, "{wait}; {next}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(columns: &[&str]) -> CaptureSpec {
        CaptureSpec::new(
            "public.orders",
            vec!["id".to_string()],
            columns.iter().map(|c| c.to_string()),
        )
        .expect("valid spec")
    }

    fn complete(spec: CaptureSpec) -> Installed {
        Installed::Complete {
            spec,
            current: true,
        }
    }

    #[test]
    fn nothing_or_a_partial_install_installs() {
        let desired = spec(&["a"]);
        assert_eq!(plan(&Installed::Absent, &desired), CaptureAction::Install);
        let partial = Installed::Partial {
            faults: vec!["the insert trigger is missing".to_string()],
        };
        assert_eq!(plan(&partial, &desired), CaptureAction::Install);
    }

    #[test]
    fn the_same_spec_from_current_functions_is_unchanged() {
        let desired = spec(&["a", "b"]);
        assert_eq!(
            plan(&complete(desired.clone()), &desired),
            CaptureAction::Unchanged
        );
    }

    #[test]
    fn a_new_column_widens() {
        let installed = complete(spec(&["a"]));
        assert_eq!(plan(&installed, &spec(&["a", "b"])), CaptureAction::Widen);
        // One column added and another removed still needs the lock.
        assert_eq!(plan(&installed, &spec(&["b"])), CaptureAction::Widen);
    }

    #[test]
    fn a_changed_key_widens() {
        let installed = complete(spec(&["a"]));
        let rekeyed = CaptureSpec::new(
            "public.orders",
            vec!["a".to_string()],
            ["id".to_string(), "a".to_string()],
        )
        .expect("valid spec");
        assert_eq!(plan(&installed, &rekeyed), CaptureAction::Widen);
    }

    #[test]
    fn fewer_columns_narrow() {
        let installed = complete(spec(&["a", "b", "c"]));
        assert_eq!(plan(&installed, &spec(&["a"])), CaptureAction::Narrow);
        assert_eq!(plan(&installed, &spec(&[])), CaptureAction::Narrow);
    }

    #[test]
    fn a_stale_function_is_replaced_under_the_lock() {
        let desired = spec(&["a"]);
        let stale = Installed::Complete {
            spec: desired.clone(),
            current: false,
        };
        assert_eq!(plan(&stale, &desired), CaptureAction::Widen);
        // Even for a narrower spec: a stale body's images aren't known to
        // cover anything.
        let stale_wide = Installed::Complete {
            spec: spec(&["a", "b"]),
            current: false,
        };
        assert_eq!(plan(&stale_wide, &desired), CaptureAction::Widen);
    }

    fn blocker(backend_type: &str, query: &str) -> Blocker {
        Blocker {
            pid: Some(42),
            prepared_gid: None,
            backend_type: backend_type.to_string(),
            mode: "ShareUpdateExclusiveLock".to_string(),
            granted: true,
            since: None,
            query: query.to_string(),
        }
    }

    #[test]
    fn a_blocker_names_its_pid_or_gid_and_how_long_it_has_held() {
        let at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let vacuum = Blocker {
            since: Some(at - Duration::from_millis(2_500)),
            ..blocker("autovacuum worker", "autovacuum: VACUUM public.orders")
        };
        assert_eq!(
            vacuum.describe(at),
            "pid 42 (autovacuum worker, holds ShareUpdateExclusiveLock for 2.5s): \
             autovacuum: VACUUM public.orders"
        );
        let prepared = Blocker {
            pid: None,
            prepared_gid: Some("tx-1".to_string()),
            mode: "RowExclusiveLock".to_string(),
            ..blocker("prepared transaction", "")
        };
        assert_eq!(
            prepared.describe(at),
            "prepared transaction \"tx-1\" (prepared transaction, holds RowExclusiveLock)"
        );
        let queued = Blocker {
            granted: false,
            mode: "AccessExclusiveLock".to_string(),
            since: Some(at - Duration::from_secs(1)),
            ..blocker("unknown", "")
        };
        assert_eq!(
            queued.describe(at),
            "pid 42 (unknown, waits for AccessExclusiveLock for 1.0s)"
        );
    }

    #[test]
    fn a_lock_wait_says_what_waits_how_long_and_on_whom() {
        let at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let mut wait = LockWait {
            table: "public.orders".to_string(),
            operation: LockingOperation::Install,
            lock_mode: LockingOperation::Install.lock_mode().to_string(),
            waiting_since: at - Duration::from_millis(300),
            attempts: 1,
            observed_at: at,
            blockers: vec![
                blocker("client backend", "update orders set a = 1"),
                Blocker {
                    pid: Some(43),
                    ..blocker("autovacuum worker", "autovacuum: VACUUM public.orders")
                },
            ],
        };
        assert_eq!(wait.waited(), Duration::from_millis(300));
        assert_eq!(
            wait.to_string(),
            "capture install on public.orders waiting 300.0ms for ShareRowExclusiveLock, \
             held against it by pid 42 (client backend, holds ShareUpdateExclusiveLock): \
             update orders set a = 1; pid 43 (autovacuum worker, holds \
             ShareUpdateExclusiveLock): autovacuum: VACUUM public.orders"
        );
        wait.blockers.clear();
        assert!(
            wait.to_string()
                .ends_with("; no conflicting lock is held now"),
            "{wait}"
        );
    }
}
