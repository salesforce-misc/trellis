//! Installs, widens, narrows and uninstalls one table's capture triggers
//! (#622 C3), and reads back what is installed.
//!
//! The staging worker's reconcile pass ([`super::reconcile`], C5) computes
//! each captured table's [`CaptureSpec`] from the catalog
//! ([`super::columns::capture_spec`]) and hands it to [`reconcile`], one
//! table at a time, so a table whose lock is held doesn't hold back another
//! table's join.
//!
//! # What each operation does
//!
//! | Operation | When ([`plan`]) | Table lock | Marker it parks |
//! |---|---|---|---|
//! | [`install`] | nothing, or a partial install, is there | `SHARE ROW EXCLUSIVE` | the join marker, with a rebuild or a catch-up for each of the table's applying readers ([`park_table_catch_ups`]) |
//! | [`widen`] | the table must image a column, key or group-key column it doesn't yet, or a function body is stale | `SHARE ROW EXCLUSIVE` | a registration marker for the new reader |
//! | [`narrow`] | the table images columns no reader needs any more | none | none |
//! | [`uninstall`] | nothing reads the table any more | `ACCESS EXCLUSIVE` (`DROP TRIGGER`'s) | none |
//!
//! [`park_table_catch_ups`]: crate::intake::markers::park_table_catch_ups
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
//! cancelling" for what it returns then). The attempt parks its marker
//! *before* it asks for the table lock, under the session's `lock_timeout`:
//! the marker's row locks are Trellis's own, and waiting for one of them
//! while holding the table lock would queue the application's writers behind
//! a Trellis transaction. Once the attempt holds the table lock, nothing in
//! it waits for anything else, and the whole stretch runs under the 50 ms
//! timeout anyway.
//!
//! `DROP TRIGGER` takes `ACCESS EXCLUSIVE`, so an uninstall's attempt also
//! queues readers of the table for up to 50 ms. A late drop only costs
//! capture writes nobody reads, so it retries the same way.
//!
//! # Waiting, never cancelling (#622 plan Q1)
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
//! definition's `capture_wait` (Q5, #687). Without a deadline an operation waits until it
//! lands. While it retries, it logs the blockers at most every five seconds.
//!
//! `pg_stat_activity` shows another role's backend only to a superuser or a
//! member of `pg_read_all_stats`. To any other role such a blocker reads as
//! `unknown`, with no transaction start and `<insufficient privilege>` for
//! its query. Its pid, lock mode, and (for a queued request) how long it has
//! waited still show.
//!
//! # The join fence
//!
//! An install parks the table's join marker in the transaction that creates
//! the triggers, so the marker commits exactly when capture starts. The
//! discharge takes the marker's fence only after reading it committed (issue
//! #431, [`crate::intake::markers::park_marker`]), so the fence postdates
//! the install, and a writer still open then is behind it. A write committed
//! before the install is in the enumeration; one after it runs the trigger.
//! The table lock makes that split exact: no writer of the table is in
//! flight when the triggers commit.
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
//!    applies every ring row drained after its discharge dispatches it, and a
//!    row staged before the widen lacks the new column: the reader fails it
//!    with `EvalError::MissingColumn` and the key is quarantined. The rows
//!    can be in the ring or re-staged by a drain (a deferred relationship
//!    reverse keeps its images). A row held in `poison_held` never reaches
//!    a reader with its images: its release re-stages an image-less
//!    `Recompute` that re-derives the key from its live row.
//!
//! **The capture gate closes the second gap.** Holding the table lock, the
//! install or widen reads `pg_current_wal_insert_lsn()` and records it on
//! the marker it parks (`pending_backfill.capture_gate_lsn`, V55). A capture
//! function reads the insert position before it inserts and commits, so
//! every row the old body staged has an `origin_lsn` below the gate, and
//! every row the new body stages, which can only run after the widen
//! commits, has one above it. The discharge doesn't dispatch the marker's
//! waiting definitions while any change to the table at or below the gate is
//! still pending ([`crate::staging::converge::table_changes_pending_through`]).
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
//! **The gate only holds definitions sourced from the gated table.** The
//! discharge dispatches a marker's `waiting_to_backfill` definitions whose
//! `source_table` is the marker's table. A widen of a relationship's to-side
//! `T` for a new definition sourced from the from-side `U` (it reads a new
//! column of `T` through the relationship) gates `T`'s marker, but the new
//! definition waits on `U`'s marker, which no gate holds. A pre-widen `T` row
//! then reaches it through the reverse path, as could a deferred reverse
//! (`rel_reverse_deferred`, staged under a synthetic `src_table`) carrying
//! `T`'s old images. C5 closes this in the reconcile pass
//! ([`super::reconcile`], rule 3): such a definition isn't dispatched while
//! `T` has a gated marker pending, and
//! [`crate::staging::converge::table_changes_pending_through`] counts the
//! deferred reverses whose relationship's to-side is `T`. A to-side widen's
//! marker also refreshes `T`'s settled projections
//! (`intake::markers::park_widen_marker`).
//!
//! Why not the alternatives:
//!
//! - **Re-deriving rows below the new reader's build horizon** instead of
//!   evaluating them would be the cheaper wait, but apply had no
//!   per-definition horizon, and every consumer path (1-1, aggregate,
//!   relationship forward and reverse, settled projections) would need one.
//!   That is D's ledger (#623), not C's.
//! - **A seal after the widen** would bound the pre-widen rows to the
//!   batches up to the widen's, but a batch drained doesn't mean every row in
//!   it did (`poison_held`, re-staged reverses), and a phase-gap straggler
//!   belongs to its successor's batch. The origin predicate covers each of
//!   those, and it is the one [`crate::staging::converge`] already keeps
//!   exact.
//!
//! What the gate costs: a new definition waits for the table's pending
//! changes to drain before its build starts, normally one seal and drain.
//! A key held in `poison_held` doesn't hold the gate (#799): it is held for
//! the definitions that hold it, never for the new one, and its release
//! re-derives it from the live row rather than replaying its images.
//!
//! Every install and widen sets the gate, not only a widen: an install can
//! follow an uninstall whose last rows haven't drained, and those were imaged
//! for a column set that may not cover the new reader.
//!
//! **The reconcile runs before the discharge**, on the same connection: a definition registered on a
//! captured table must not be dispatched by a discharge that runs before the
//! widen its columns need. On one staging worker the two run in sequence.
//!
//! # Narrowing
//!
//! A narrow replaces the functions with ones that image fewer columns, with
//! no table lock and no marker. A writer still running the old body stages
//! rows with more columns than anyone reads, which harms nothing. The
//! reconcile pass narrows only from a catalog read after the drop of the
//! definition that read the dropped columns committed, since the next rows
//! lack them.
//!
//! # What is installed
//!
//! The catalog is the only record (#622 plan Q9): the triggers in
//! `pg_trigger`, and each function's spec in its `COMMENT ON FUNCTION`
//! ([`sql::comment_text`]). `pg_dump` carries both (#644), and there is no
//! Trellis table to disagree with them. [`installed`] reads them back, and
//! also compares each function's source with what this build generates, so
//! a function an older generator left behind is replaced.
//!
//! Nothing else this module writes is installed state. The capture gate
//! belongs to a pending marker and goes with it. A [`LockWait`] is returned;
//! the reconcile pass records the latest one per table in `capture_holdups`
//! (#687), which describes other sessions, not what is installed.
//!
//! # The Trellis role (#622 plan Q3)
//!
//! One role does everything Trellis does: it owns the ring (and the
//! instance schema, unless a DBA pre-created it, below), runs the migrations
//! and the staging worker, and installs and owns the capture functions.
//! There is no separate capture role. What that role needs:
//!
//! - **On each captured table, ownership or membership in the owning
//!   role.** `CREATE TRIGGER` needs the `TRIGGER` privilege, but `ALTER TABLE
//!   … ENABLE ALWAYS TRIGGER` needs ownership (#622 plan finding 3), and
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
//! The schema's owner is not a stand-in for the ring's (issue #701). A DBA
//! can pre-create the schema as one role and have a login role that is a
//! member of it run the migrations: `create schema if not exists` keeps the
//! DBA's role as the schema's owner, while the ring belongs to the login
//! role, and the schema's owner has no privilege on it. Functions owned by
//! the schema's owner would fail every captured write.

use std::time::{Duration, Instant, SystemTime};

use tokio_postgres::types::PgLsn;
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
/// key, and a subset of its image and group-key columns.
fn narrows(installed: &CaptureSpec, desired: &CaptureSpec) -> bool {
    installed.table() == desired.table()
        && installed.key() == desired.key()
        && desired
            .columns()
            .iter()
            .all(|c| installed.columns().contains(c))
        && desired
            .group_key()
            .iter()
            .all(|c| installed.group_key().contains(c))
}

/// Reads what instance `schema` has installed for `table` (an unquoted
/// `schema.table` identity) from `pg_trigger`, `pg_proc` and the functions'
/// comments.
///
/// One statement reads all four events, so a read racing an install or
/// uninstall sees it either whole or not at all, never a spurious
/// [`Installed::Partial`].
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
        .query(
            "select p.oid is not null, p.prosrc, \
                    c.j ->> 'table', c.j ->> 'event', \
                    case when c.j is not null then array( \
                        select pg_catalog.jsonb_array_elements_text(c.j -> 'key')) end, \
                    case when c.j is not null then array( \
                        select pg_catalog.jsonb_array_elements_text(c.j -> 'columns')) end, \
                    case when c.j is not null then array( \
                        select pg_catalog.jsonb_array_elements_text(c.j -> 'group_key')) end, \
                    t.oid is not null, t.tgenabled::text, t.tgfoid = p.oid \
             from unnest($2::text[], $4::text[]) with ordinality as e(function, trigger, ord) \
             left join pg_catalog.pg_proc p \
               on p.proname = e.function and p.pronargs = 0 \
              and p.pronamespace = ( \
                  select oid from pg_catalog.pg_namespace where nspname = $1) \
             left join lateral ( \
                 select case when d.description like '{\"trellis_capture\":1,%' \
                             then d.description::jsonb end as j \
                 from pg_catalog.pg_description d \
                 where d.objoid = p.oid \
                   and d.classoid = 'pg_catalog.pg_proc'::pg_catalog.regclass \
                   and d.objsubid = 0) c on true \
             left join pg_catalog.pg_trigger t \
               on t.tgrelid = pg_catalog.to_regclass($3) and t.tgname = e.trigger \
              and not t.tgisinternal \
             order by e.ord",
            &[&schema, &functions, &regclass, &triggers],
        )
        .await?;
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
        let has_trigger: bool = row.get(7);
        present |= has_function || has_trigger;
        let what = event.as_str();
        if !has_function {
            faults.push(format!(
                "the {what} function {schema}.{function} is missing"
            ));
        } else {
            let recorded = recorded_spec(
                table,
                what,
                row.get(2),
                row.get(3),
                row.get(4),
                row.get(5),
                row.get(6),
            );
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
            let enabled: String = row.get(8);
            if enabled != "A" {
                faults.push(format!(
                    "the {what} trigger {trigger} on {table} is not ENABLE ALWAYS \
                     (tgenabled = {enabled})"
                ));
            }
            if row.get::<_, Option<bool>>(9) != Some(true) {
                faults.push(format!(
                    "the {what} trigger {trigger} on {table} doesn't call {schema}.{function}"
                ));
            }
        }
    }
    if !present {
        return Ok(Installed::Absent);
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

/// The spec a function's comment records, if it records one for `table`'s
/// `event` that [`CaptureSpec::new`] accepts.
fn recorded_spec(
    table: &str,
    event: &str,
    recorded_table: Option<String>,
    recorded_event: Option<String>,
    key: Option<Vec<String>>,
    columns: Option<Vec<String>>,
    group_key: Option<Vec<String>>,
) -> Option<CaptureSpec> {
    if recorded_table.as_deref() != Some(table) || recorded_event.as_deref() != Some(event) {
        return None;
    }
    CaptureSpec::new(table, key?, columns?, group_key?).ok()
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
        CaptureAction::Install => run(client, schema, Op::Install(desired), deadline).await?,
        CaptureAction::Widen => run(client, schema, Op::Widen(desired), deadline).await?,
        CaptureAction::Narrow => Progress::Done(narrow(client, schema, desired).await?),
    };
    Ok(progress.map(|()| action))
}

/// Installs `spec`'s capture, and parks the table's join marker, in one
/// transaction under the table's `SHARE ROW EXCLUSIVE` lock, retried as the
/// module doc describes. Lands `false`, having done nothing, when `spec` is
/// already installed and current: a repeat install takes no lock and parks
/// no marker.
pub async fn install(
    client: &mut Client,
    schema: &str,
    spec: &CaptureSpec,
    deadline: Option<Instant>,
) -> Result<Progress<bool>, CaptureError> {
    if plan(&installed(&*client, schema, spec.table()).await?, spec) == CaptureAction::Unchanged {
        return Ok(Progress::Done(false));
    }
    Ok(run(client, schema, Op::Install(spec), deadline)
        .await?
        .map(|()| true))
}

/// Replaces an installed table's capture functions with `spec`'s under the
/// table's `SHARE ROW EXCLUSIVE` lock, and parks a marker gated on the widen
/// (see "Widening and the capture gate"), retried as the module doc
/// describes.
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
/// lock and no marker (see "Narrowing").
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
    txn.commit().await?;
    Ok(())
}

/// Drops `table`'s capture triggers and functions, in one transaction under
/// the table's `ACCESS EXCLUSIVE` lock (`DROP TRIGGER`'s), retried as the
/// module doc describes. A table the application already dropped takes its
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
    Ok(run(client, schema, Op::Uninstall(table), deadline)
        .await?
        .map(|()| true))
}

/// One of the operations that lock the table, for [`run`].
#[derive(Clone, Copy)]
enum Op<'a> {
    Install(&'a CaptureSpec),
    Widen(&'a CaptureSpec),
    Uninstall(&'a str),
}

impl Op<'_> {
    fn table(&self) -> &str {
        match self {
            Op::Install(spec) | Op::Widen(spec) => spec.table(),
            Op::Uninstall(table) => table,
        }
    }

    fn operation(&self) -> LockingOperation {
        match self {
            Op::Install(_) => LockingOperation::Install,
            Op::Widen(_) => LockingOperation::Widen,
            Op::Uninstall(_) => LockingOperation::Uninstall,
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
            Op::Install(_) | Op::Widen(_) => ("share row exclusive", SHARE_ROW_EXCLUSIVE_CONFLICTS),
            Op::Uninstall(_) => ("access exclusive", ACCESS_EXCLUSIVE_CONFLICTS),
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
    loop {
        match attempt(client, schema, op, retry.lock_timeout()).await {
            Err(err) if locks::is_lock_not_available(&err) => {
                if retry.again(&err).await {
                    log.after_timeout(&*client, op, started).await;
                    continue;
                }
                let wait = lock_wait(&*client, op, started).await?;
                log.at_deadline(&wait);
                return Ok(Progress::Waiting(Box::new(wait)));
            }
            Err(err) => return Err(err),
            Ok(()) => return Ok(Progress::Done(())),
        }
    }
}

/// One attempt at `op`: the marker (if any) under the session's
/// `lock_timeout`, then the table lock under `lock_timeout` and the DDL.
async fn attempt(
    client: &mut Client,
    schema: &str,
    op: Op<'_>,
    lock_timeout: Duration,
) -> Result<(), CaptureError> {
    let table = op.table();
    let owner = match op {
        Op::Install(_) | Op::Widen(_) => foreign_ring_owner(&*client, schema).await?,
        Op::Uninstall(_) => None,
    };
    let txn = client.transaction().await?;
    let table_exists: bool = txn
        .query_one(
            "select pg_catalog.to_regclass($1) is not null",
            &[&crate::defs::ddl::regclass_arg(table)],
        )
        .await?
        .get(0);
    // The definitions an install rebuilds (#625 F7), for the build segment
    // below.
    let mut rebuilt = Vec::new();
    match op {
        Op::Install(_) => {
            rebuilt =
                crate::intake::markers::park_table_catch_ups(&txn, &[table.to_string()]).await?
        }
        Op::Widen(_) => crate::intake::markers::park_widen_marker(&txn, table).await?,
        Op::Uninstall(_) => {}
    }
    let statements = match op {
        Op::Install(spec) => sql::install_statements(schema, spec)?,
        Op::Widen(spec) => sql::function_statements(schema, spec)?,
        Op::Uninstall(table) => sql::uninstall_statements(schema, table, table_exists)?,
    };
    if table_exists || !matches!(op, Op::Uninstall(_)) {
        // From here on the transaction holds the table, so nothing in it may
        // wait longer than the attempt's timeout.
        locks::set_local_lock_timeout(&txn, lock_timeout).await?;
        txn.batch_execute(&format!(
            "lock table {} in {} mode",
            sql::quoted_table(table)?,
            op.lock().0
        ))
        .await?;
    }
    let gate: PgLsn = txn
        .query_one("select pg_catalog.pg_current_wal_insert_lsn()", &[])
        .await?
        .get(0);
    for statement in statements {
        txn.batch_execute(&statement).await?;
    }
    if let Some(owner) = owner {
        for statement in sql::owner_statements(schema, table, &owner)? {
            txn.batch_execute(&statement).await?;
        }
    }
    if matches!(op, Op::Install(_) | Op::Widen(_)) {
        // The marker row is this transaction's own since the park above, so
        // this takes no new lock.
        txn.execute(
            "update pending_backfill \
             set capture_gate_lsn = greatest(capture_gate_lsn, $2) \
             where table_name = $1",
            &[&table, &gate],
        )
        .await?;
    }
    // Last, so the share lock on the active segment is held only for the
    // commit, not for the table lock's wait.
    crate::staging::build::stamp_rebuild_seg(&txn, &rebuilt).await?;
    txn.commit().await?;
    Ok(())
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
pub async fn blockers(
    client: &impl GenericClient,
    table: &str,
    conflicts: &[&str],
) -> Result<Vec<Blocker>, tokio_postgres::Error> {
    // A prepared transaction's locks have no pid. Its own `transactionid`
    // lock shares their `virtualtransaction` and names the xid that
    // `pg_prepared_xacts` lists its gid under.
    Ok(client
        .query(
            "select l.pid, p.gid, \
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
               and l.relation = pg_catalog.to_regclass($1) \
               and l.pid is distinct from pg_catalog.pg_backend_pid() \
               and l.mode = any($2) \
             order by l.pid, p.gid, l.mode",
            &[
                &crate::defs::ddl::regclass_arg(table),
                &conflicts,
                &BLOCKER_QUERY_CHARS,
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
) -> Result<LockWait, tokio_postgres::Error> {
    let conflicts = op.lock().1;
    let observed_at: SystemTime = client
        .query_one("select pg_catalog.clock_timestamp()", &[])
        .await?
        .get(0);
    let blockers = blockers(client, op.table(), conflicts).await?;
    Ok(LockWait {
        table: op.table().to_string(),
        operation: op.operation(),
        lock_mode: op.operation().lock_mode().to_string(),
        waiting_since: observed_at
            .checked_sub(started.elapsed())
            .unwrap_or(observed_at),
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
    async fn after_timeout(&mut self, client: &impl GenericClient, op: Op<'_>, started: Instant) {
        if !self.due() {
            return;
        }
        match lock_wait(client, op, started).await {
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

    fn spec(columns: &[&str], group_key: &[&str]) -> CaptureSpec {
        CaptureSpec::new(
            "public.orders",
            vec!["id".to_string()],
            columns.iter().map(|c| c.to_string()),
            group_key.iter().map(|c| c.to_string()).collect(),
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
        let desired = spec(&["a"], &[]);
        assert_eq!(plan(&Installed::Absent, &desired), CaptureAction::Install);
        let partial = Installed::Partial {
            faults: vec!["the insert trigger is missing".to_string()],
        };
        assert_eq!(plan(&partial, &desired), CaptureAction::Install);
    }

    #[test]
    fn the_same_spec_from_current_functions_is_unchanged() {
        let desired = spec(&["a", "b"], &["a"]);
        assert_eq!(
            plan(&complete(desired.clone()), &desired),
            CaptureAction::Unchanged
        );
    }

    #[test]
    fn a_new_column_or_group_key_column_widens() {
        let installed = complete(spec(&["a"], &[]));
        assert_eq!(
            plan(&installed, &spec(&["a", "b"], &[])),
            CaptureAction::Widen
        );
        assert_eq!(
            plan(&installed, &spec(&["a"], &["a"])),
            CaptureAction::Widen
        );
        // One column added and another removed still needs the lock.
        assert_eq!(plan(&installed, &spec(&["b"], &[])), CaptureAction::Widen);
    }

    #[test]
    fn a_changed_key_widens() {
        let installed = complete(spec(&["a"], &[]));
        let rekeyed = CaptureSpec::new(
            "public.orders",
            vec!["a".to_string()],
            ["id".to_string(), "a".to_string()],
            vec![],
        )
        .expect("valid spec");
        assert_eq!(plan(&installed, &rekeyed), CaptureAction::Widen);
    }

    #[test]
    fn fewer_columns_narrow() {
        let installed = complete(spec(&["a", "b", "c"], &["a", "b"]));
        assert_eq!(
            plan(&installed, &spec(&["a"], &["a"])),
            CaptureAction::Narrow
        );
        assert_eq!(plan(&installed, &spec(&[], &[])), CaptureAction::Narrow);
    }

    #[test]
    fn a_stale_function_is_replaced_under_the_lock() {
        let desired = spec(&["a"], &[]);
        let stale = Installed::Complete {
            spec: desired.clone(),
            current: false,
        };
        assert_eq!(plan(&stale, &desired), CaptureAction::Widen);
        // Even for a narrower spec: a stale body's images aren't known to
        // cover anything.
        let stale_wide = Installed::Complete {
            spec: spec(&["a", "b"], &[]),
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
