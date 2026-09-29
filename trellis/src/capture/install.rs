//! Installs, widens, narrows and uninstalls one table's capture triggers
//! (#622 C3), and reads back what is installed.
//!
//! Nothing at runtime calls this yet. C5's reconcile pass computes each
//! captured table's [`CaptureSpec`] from the catalog
//! ([`super::columns::capture_spec`]) and hands it to [`reconcile`], one
//! table at a time, so a table whose lock is held doesn't hold back another
//! table's join.
//!
//! # What each operation does
//!
//! | Operation | When ([`plan`]) | Table lock | Marker it parks |
//! |---|---|---|---|
//! | [`install`] | nothing, or a partial install, is there | `SHARE ROW EXCLUSIVE` | the join marker, with catch-ups for the table's applying readers ([`park_table_catch_ups`]) |
//! | [`widen`] | the table must image a column, key or group-key column it doesn't yet, or a function body is stale | `SHARE ROW EXCLUSIVE` | a registration marker for the new reader |
//! | [`narrow`] | the table images columns no reader needs any more | none | none |
//! | [`uninstall`] | nothing reads the table any more | `ACCESS EXCLUSIVE` (`DROP TRIGGER`'s) | none |
//!
//! [`park_table_catch_ups`]: crate::intake::publication::park_table_catch_ups
//!
//! # Never blocking a writer (ADR-0002 I6)
//!
//! `CREATE TRIGGER`, `ALTER TABLE … ENABLE ALWAYS TRIGGER` and `LOCK TABLE …
//! IN SHARE ROW EXCLUSIVE MODE` all conflict with a writer's `ROW
//! EXCLUSIVE`, and every writer that arrives while one of them waits queues
//! behind it. So each attempt is one short transaction whose table lock waits
//! at most [`USER_TABLE_DDL_LOCK_TIMEOUT`] (50 ms), and
//! [`crate::locks::DdlRetry`] retries it until it lands or, with a deadline,
//! until the next attempt couldn't end by it. The attempt parks its marker
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
//! **A blocking autovacuum (#622 plan Q1, provisional).** Postgres cancels
//! an autovacuum that holds a lock a waiter wants only once the waiter runs
//! its deadlock check, after `deadlock_timeout` (1 s by default). A 50 ms
//! attempt never gets that far, so the join would wait out the whole vacuum.
//! After a timed-out attempt, [`Blockers`] reads who holds a conflicting
//! lock on the table and logs them, pid and query, at most every five
//! seconds. If one is an autovacuum worker that isn't preventing wraparound,
//! it cancels it with `pg_cancel_backend`, which works only where the role
//! may signal an autovacuum worker (superuser on PostgreSQL 17 and earlier,
//! `pg_signal_autovacuum_worker` from 18). Where Postgres refuses, it logs
//! that once and the retries wait the vacuum out. An application backend is
//! never cancelled. The worker must also be visible to the role in
//! `pg_stat_activity` (superuser or `pg_read_all_stats`): to any other role
//! it reads as an `unknown` blocker and is left alone. The cancelling
//! statement checks the pid, its lock and its query again
//! ([`cancel_autovacuum`]), so a pid reused since the read is never hit.
//!
//! # The join fence
//!
//! An install parks the table's join marker in the transaction that creates
//! the triggers, so the marker commits exactly when capture starts. The
//! discharge takes the marker's fence only after reading it committed (issue
//! #431, [`crate::intake::publication::park_marker`]), so the fence postdates
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
//!    can be in the ring, in `poison_held` waiting for release, or re-staged
//!    by a drain (a deferred relationship reverse keeps its images).
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
//! then reaches it through the reverse path, and a deferred reverse
//! (`rel_reverse_deferred`, staged under a synthetic `src_table`) carrying
//! `T`'s old images isn't counted by
//! [`crate::staging::converge::table_changes_pending_through`] either. C5
//! must close this before it wires widening to to-side columns: for example,
//! by gating the marker of every table a waiting reader of `T` is sourced
//! from on `T`'s pending changes, counting deferred reverses whose
//! relationship's to-side is `T`.
//!
//! Why not the alternatives:
//!
//! - **Re-deriving rows below the new reader's build horizon** instead of
//!   evaluating them would be the cheaper wait, but apply has no
//!   per-definition horizon today (only aggregates' per-group recompute
//!   horizon, #442), and every consumer path (1-1, aggregate, relationship
//!   forward and reverse, settled projections) would need it. That is D's
//!   ledger (#623), not C's.
//! - **A seal after the widen** would bound the pre-widen rows to the
//!   batches up to the widen's, but a batch drained doesn't mean every row in
//!   it did (`poison_held`, re-staged reverses), and a phase-gap straggler
//!   belongs to its successor's batch. The origin predicate covers each of
//!   those, and it is the one [`crate::staging::converge`] already keeps
//!   exact.
//!
//! What the gate costs: a new definition waits for the table's pending
//! changes to drain before its build starts, normally one seal and drain.
//! A key of the table held in `poison_held` from before the widen holds the
//! gate until it is released or discarded, which is an operator's call; the
//! alternative is that the release poisons the key again for every reader,
//! because the new one can't evaluate it.
//!
//! Every install and widen sets the gate, not only a widen: an install can
//! follow an uninstall whose last rows haven't drained, and those were imaged
//! for a column set that may not cover the new reader.
//!
//! **C5 must also order the reconcile before the discharge**, as today's
//! publication reconcile runs before it: a definition registered on a
//! captured table must not be dispatched by a discharge that runs before the
//! widen its columns need. On one staging worker the two run in sequence.
//!
//! # Narrowing
//!
//! A narrow replaces the functions with ones that image fewer columns, with
//! no table lock and no marker. A writer still running the old body stages
//! rows with more columns than anyone reads, which harms nothing. C5 must
//! narrow only after the definition that read the dropped columns has
//! stopped applying (its drop committed), since the next rows lack them.
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
//! # Who owns the functions
//!
//! The role that owns the instance schema (#622 plan Q3, provisional): the
//! migration role, which already owns the ring the functions write to. The
//! functions are `SECURITY DEFINER`, so they run as that role whoever writes
//! the table. An install by another role (it must be a member of the owning
//! role) hands the functions over with `ALTER FUNCTION … OWNER TO`. The
//! installing role must own the captured table, as `ENABLE ALWAYS TRIGGER`
//! requires (#622 plan finding 3).

use std::time::{Duration, Instant};

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
    /// All four triggers exist, enabled `ALWAYS`, each calling its event's
    /// function, and all four functions' comments record `spec`. `current`
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
pub async fn installed(
    client: &impl GenericClient,
    schema: &str,
    table: &str,
) -> Result<Installed, CaptureError> {
    let regclass = crate::defs::ddl::regclass_arg(table);
    let mut present = false;
    let mut current = true;
    let mut faults = Vec::new();
    let mut specs = Vec::new();
    for event in CaptureEvent::ALL {
        let function = sql::function_name(table, event)?;
        let trigger = sql::trigger_name(schema, event);
        // A comment this generator didn't write reads as no spec, rather
        // than failing the cast.
        let row = client
            .query_one(
                "select p.oid is not null, p.prosrc, \
                        c.j ->> 'table', c.j ->> 'event', \
                        case when c.j is not null then array( \
                            select pg_catalog.jsonb_array_elements_text(c.j -> 'key')) end, \
                        case when c.j is not null then array( \
                            select pg_catalog.jsonb_array_elements_text(c.j -> 'columns')) end, \
                        case when c.j is not null then array( \
                            select pg_catalog.jsonb_array_elements_text(c.j -> 'group_key')) end, \
                        t.oid is not null, t.tgenabled::text, t.tgfoid = p.oid \
                 from (select 1) one \
                 left join pg_catalog.pg_proc p \
                   on p.proname = $2 and p.pronargs = 0 \
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
                   on t.tgrelid = pg_catalog.to_regclass($3) and t.tgname = $4 \
                  and not t.tgisinternal",
                &[&schema, &function, &regclass, &trigger],
            )
            .await?;
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

/// Brings `desired.table()`'s capture to `desired` ([`plan`]) and returns
/// what it did. `deadline` bounds the retries of a locked table, as
/// [`DdlRetry::new`]'s does: past it, the lock timeout comes back as the
/// error and nothing has changed.
pub async fn reconcile(
    client: &mut Client,
    schema: &str,
    desired: &CaptureSpec,
    deadline: Option<Instant>,
) -> Result<CaptureAction, CaptureError> {
    let action = plan(
        &installed(&*client, schema, desired.table()).await?,
        desired,
    );
    match action {
        CaptureAction::Unchanged => {}
        CaptureAction::Install => run(client, schema, Op::Install(desired), deadline).await?,
        CaptureAction::Widen => run(client, schema, Op::Widen(desired), deadline).await?,
        CaptureAction::Narrow => narrow(client, schema, desired).await?,
    }
    Ok(action)
}

/// Installs `spec`'s capture, and parks the table's join marker, in one
/// transaction under the table's `SHARE ROW EXCLUSIVE` lock, retried as the
/// module doc describes. Returns `false`, having done nothing, when `spec` is
/// already installed and current: a repeat install takes no lock and parks
/// no marker.
pub async fn install(
    client: &mut Client,
    schema: &str,
    spec: &CaptureSpec,
    deadline: Option<Instant>,
) -> Result<bool, CaptureError> {
    if plan(&installed(&*client, schema, spec.table()).await?, spec) == CaptureAction::Unchanged {
        return Ok(false);
    }
    run(client, schema, Op::Install(spec), deadline).await?;
    Ok(true)
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
) -> Result<(), CaptureError> {
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
    let owner = foreign_schema_owner(&*client, schema).await?;
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
/// triggers with it, so only the functions go. Returns `false`, having done
/// nothing, when nothing is installed.
pub async fn uninstall(
    client: &mut Client,
    schema: &str,
    table: &str,
    deadline: Option<Instant>,
) -> Result<bool, CaptureError> {
    if installed(&*client, schema, table).await? == Installed::Absent {
        return Ok(false);
    }
    run(client, schema, Op::Uninstall(table), deadline).await?;
    Ok(true)
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

    fn what(&self) -> &'static str {
        match self {
            Op::Install(_) => "capture install",
            Op::Widen(_) => "capture widen",
            Op::Uninstall(_) => "capture uninstall",
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
/// retries (see [`DdlRetry`]). After each timed-out attempt, [`Blockers`]
/// reports who holds the table and cancels a blocking autovacuum where it
/// may.
async fn run(
    client: &mut Client,
    schema: &str,
    op: Op<'_>,
    deadline: Option<Instant>,
) -> Result<(), CaptureError> {
    let mut retry = DdlRetry::new(op.what(), USER_TABLE_DDL_LOCK_TIMEOUT, deadline);
    let mut blockers = Blockers::new(op.what());
    loop {
        match attempt(client, schema, op, retry.lock_timeout()).await {
            Err(err) if locks::is_lock_not_available(&err) => {
                blockers.inspect(&*client, op.table(), op.lock().1).await;
                if retry.again(&err).await {
                    continue;
                }
                return Err(err);
            }
            other => return other,
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
        Op::Install(_) | Op::Widen(_) => foreign_schema_owner(&*client, schema).await?,
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
    match op {
        Op::Install(_) => {
            crate::intake::publication::park_table_catch_ups(&txn, &[table.to_string()]).await?
        }
        Op::Widen(_) => crate::intake::publication::park_marker(&txn, table).await?,
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
    txn.commit().await?;
    Ok(())
}

/// The role that owns instance schema `schema`, when the session's role
/// isn't it: the functions are handed to it (see "Who owns the functions").
async fn foreign_schema_owner(
    client: &impl GenericClient,
    schema: &str,
) -> Result<Option<String>, CaptureError> {
    let row = client
        .query_opt(
            "select case when n.nspowner = r.oid then null \
                         else pg_catalog.pg_get_userbyid(n.nspowner)::text end \
             from pg_catalog.pg_namespace n, pg_catalog.pg_roles r \
             where n.nspname = $1 and r.rolname = current_user",
            &[&schema],
        )
        .await?;
    Ok(row.and_then(|row| row.get(0)))
}

/// How often [`Blockers::inspect`] reads `pg_locks` while an operation keeps
/// timing out, and how often it logs what it read.
const BLOCKER_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const BLOCKER_LOG_INTERVAL: Duration = Duration::from_secs(5);

/// The longest query text a blocker's log line carries.
const BLOCKER_QUERY_CHARS: usize = 200;

/// One session holding a lock that conflicts with the one a capture
/// operation waits for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Blocker {
    /// `None` for a prepared transaction, which has no backend.
    pub pid: Option<i32>,
    /// `pg_stat_activity.backend_type`: `client backend`, `autovacuum
    /// worker`, …; `prepared transaction` for one; or `unknown` for a
    /// backend this role may not see (another role's, without
    /// `pg_read_all_stats`), whose query reads `<insufficient privilege>`.
    pub backend_type: String,
    /// `pg_stat_activity.query`, as far as this role may see it.
    pub query: String,
    /// The lock mode it holds, as `pg_locks` spells it.
    pub mode: String,
}

impl Blocker {
    /// Whether a capture operation cancels this blocker: only an autovacuum
    /// worker, and not one preventing wraparound, which would only start
    /// again (and which Postgres's own deadlock check never cancels either).
    /// [`cancel_autovacuum`] checks the same again, in the statement that
    /// cancels it.
    pub fn cancellable(&self) -> bool {
        self.backend_type == "autovacuum worker"
            && self.query.starts_with("autovacuum: ")
            && !self.query.contains("(to prevent wraparound)")
    }
}

impl std::fmt::Display for Blocker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.pid {
            Some(pid) => write!(f, "pid {pid}")?,
            None => write!(f, "no pid")?,
        }
        let query: String = self.query.chars().take(BLOCKER_QUERY_CHARS).collect();
        write!(f, " ({}, {}): {query}", self.backend_type, self.mode)
    }
}

/// Who holds a lock on `table` that conflicts with `conflicts` (lock modes as
/// `pg_locks` spells them), other than this session.
pub async fn blockers(
    client: &impl GenericClient,
    table: &str,
    conflicts: &[&str],
) -> Result<Vec<Blocker>, tokio_postgres::Error> {
    Ok(client
        .query(
            "select l.pid, \
                    case when l.pid is null then 'prepared transaction' \
                         else coalesce(a.backend_type, 'unknown') end, \
                    coalesce(a.query, ''), l.mode \
             from pg_catalog.pg_locks l \
             left join pg_catalog.pg_stat_activity a on a.pid = l.pid \
             where l.locktype = 'relation' and l.granted \
               and l.database = (select oid from pg_catalog.pg_database \
                                 where datname = pg_catalog.current_database()) \
               and l.relation = pg_catalog.to_regclass($1) \
               and l.pid is distinct from pg_catalog.pg_backend_pid() \
               and l.mode = any($2) \
             order by l.pid",
            &[&crate::defs::ddl::regclass_arg(table), &conflicts],
        )
        .await?
        .into_iter()
        .map(|row| Blocker {
            pid: row.get(0),
            backend_type: row.get(1),
            query: row.get(2),
            mode: row.get(3),
        })
        .collect())
}

/// Cancels `pid` if, as this statement reads it, it is still an autovacuum
/// worker that isn't preventing wraparound and still holds a lock on `table`
/// that conflicts with `conflicts`. Returns `None`, having signalled
/// nothing, if it isn't, and otherwise `pg_cancel_backend`'s result.
///
/// Checking in the statement that cancels, rather than trusting the
/// [`blockers`] read a round trip earlier, keeps a pid the worker released
/// in between, and an application backend then given it, from being
/// cancelled. The wraparound check reads the worker's `query`, which
/// Postgres cuts to `track_activity_query_size` bytes: a text that may have
/// been cut (within a multibyte character of the limit) could have lost its
/// `(to prevent wraparound)`, so it isn't cancelled.
async fn cancel_autovacuum(
    client: &impl GenericClient,
    pid: i32,
    table: &str,
    conflicts: &[&str],
) -> Result<Option<bool>, tokio_postgres::Error> {
    Ok(client
        .query_opt(
            "select pg_catalog.pg_cancel_backend(a.pid) \
             from pg_catalog.pg_stat_activity a \
             where a.pid = $1 \
               and a.backend_type = 'autovacuum worker' \
               and a.query like 'autovacuum: %' \
               and a.query not like '%(to prevent wraparound)%' \
               and pg_catalog.octet_length(a.query) + 4 < ( \
                   select setting::int from pg_catalog.pg_settings \
                   where name = 'track_activity_query_size') \
               and exists ( \
                   select 1 from pg_catalog.pg_locks l \
                   where l.pid = a.pid and l.locktype = 'relation' and l.granted \
                     and l.relation = pg_catalog.to_regclass($2) \
                     and l.mode = any($3))",
            &[&pid, &crate::defs::ddl::regclass_arg(table), &conflicts],
        )
        .await?
        .map(|row| row.get(0)))
}

/// Reports, and where allowed cancels, what keeps a capture operation from
/// its table lock (see the module doc's "A blocking autovacuum").
struct Blockers {
    what: &'static str,
    last_checked: Option<Instant>,
    last_logged: Option<Instant>,
    /// Set once Postgres refused to cancel an autovacuum worker for this
    /// role: it won't allow it on a later attempt either.
    cancel_refused: bool,
}

impl Blockers {
    fn new(what: &'static str) -> Self {
        Self {
            what,
            last_checked: None,
            last_logged: None,
            cancel_refused: false,
        }
    }

    /// After a timed-out attempt: reads the blockers (at most once per
    /// [`BLOCKER_CHECK_INTERVAL`]), logs them (at most once per
    /// [`BLOCKER_LOG_INTERVAL`]), and cancels each cancellable one. A failure
    /// here is logged and otherwise ignored: the retry goes on either way.
    async fn inspect(&mut self, client: &impl GenericClient, table: &str, conflicts: &[&str]) {
        let now = Instant::now();
        if self
            .last_checked
            .is_some_and(|checked| now.duration_since(checked) < BLOCKER_CHECK_INTERVAL)
        {
            return;
        }
        self.last_checked = Some(now);
        let found = match blockers(client, table, conflicts).await {
            Ok(found) => found,
            Err(error) => {
                tracing::debug!(what = self.what, table, %error, "reading lock blockers failed");
                return;
            }
        };
        if self
            .last_logged
            .is_none_or(|logged| now.duration_since(logged) >= BLOCKER_LOG_INTERVAL)
        {
            self.last_logged = Some(now);
            let listed: Vec<String> = found.iter().map(Blocker::to_string).collect();
            tracing::info!(
                what = self.what,
                table,
                blockers = ?listed,
                "capture DDL waiting for a table lock these sessions hold"
            );
        }
        if self.cancel_refused {
            return;
        }
        for blocker in found.iter().filter(|b| b.cancellable()) {
            let Some(pid) = blocker.pid else { continue };
            match cancel_autovacuum(client, pid, table, conflicts).await {
                Ok(None) => {}
                Ok(Some(signalled)) => {
                    tracing::info!(
                        what = self.what,
                        table,
                        pid,
                        signalled,
                        "cancelled an autovacuum worker holding the table capture needs"
                    );
                }
                Err(error) => {
                    self.cancel_refused = true;
                    tracing::info!(
                        what = self.what,
                        table,
                        pid,
                        %error,
                        "can't cancel the autovacuum worker holding the table capture needs; \
                         waiting it out"
                    );
                    return;
                }
            }
        }
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
            backend_type: backend_type.to_string(),
            query: query.to_string(),
            mode: "ShareUpdateExclusiveLock".to_string(),
        }
    }

    #[test]
    fn only_an_ordinary_autovacuum_is_cancelled() {
        assert!(blocker("autovacuum worker", "autovacuum: VACUUM public.orders").cancellable());
        assert!(blocker("autovacuum worker", "autovacuum: ANALYZE public.orders").cancellable());
        assert!(
            !blocker(
                "autovacuum worker",
                "autovacuum: VACUUM ANALYZE public.orders (to prevent wraparound)"
            )
            .cancellable()
        );
        // A backend this role may not see is never cancelled.
        assert!(!blocker("unknown", "<insufficient privilege>").cancellable());
        assert!(
            !blocker(
                "autovacuum worker",
                "autovacuum: VACUUM public.orders (to prevent wraparound)"
            )
            .cancellable()
        );
        assert!(!blocker("client backend", "vacuum public.orders").cancellable());
        assert!(!blocker("prepared transaction", "").cancellable());
    }

    #[test]
    fn a_blocker_names_its_pid_and_query() {
        let long = format!("insert into orders {}", "x".repeat(500));
        let shown = blocker("client backend", &long).to_string();
        assert!(shown.starts_with("pid 42 (client backend, ShareUpdateExclusiveLock): insert"));
        assert!(shown.len() < 300, "{shown}");
        let prepared = Blocker {
            pid: None,
            ..blocker("prepared transaction", "")
        };
        assert_eq!(
            prepared.to_string(),
            "no pid (prepared transaction, ShareUpdateExclusiveLock): "
        );
    }
}
