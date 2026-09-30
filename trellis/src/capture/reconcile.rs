//! The staging worker's capture reconcile pass (#622 C5).
//!
//! Every reconcile pass of the maintenance loop brings each table's capture
//! triggers to what the catalog needs, one table at a time, before the
//! backfill discharge runs:
//!
//! - a table some definition reads ([`crate::defs::publication_tables`]) is
//!   installed, widened or narrowed to its [`super::columns::capture_spec`]
//!   ([`super::install::reconcile`]);
//! - a table this instance captures that nothing reads any more is
//!   uninstalled.
//!
//! Before it regenerates a table's functions, the pass pauses every
//! definition that reads a column the table no longer has (#622 C6,
//! [`crate::staging::schema_change::pause_readers_of_missing`]) and leaves
//! the table for the next pass.
//!
//! # Never waiting on `apply`'s path
//!
//! Defining a transform only registers it. Installing and widening take a
//! lock on the application's table, which waits out every holder (an
//! autovacuum included; nothing here cancels a backend, #622 plan Q1), so
//! they only run here, in the background. A pass spends at most its
//! `deadline` retrying locked tables. The first attempt on each table always
//! runs, so a table whose lock is held doesn't stop another's join: it only
//! makes that table [`Progress::Waiting`], and the next pass tries it again.
//! Its [`LockWait`] report is kept in memory ([`lock_wait`]), where
//! [`crate::Trellis::status`] reports it on each definition it keeps waiting.
//!
//! # Which definitions a discharge may dispatch
//!
//! A `waiting_to_backfill` definition starts applying ring rows as soon as a
//! discharge dispatches it, so it must not be dispatched while any table it
//! reads lacks a column it needs in its images. The pass works out, from the
//! same catalog snapshot it built the specs from, which waiting definitions
//! are [`PassOutcome::ready`]:
//!
//! 1. its source is captured and current after this pass, or is another
//!    definition's target (the target-mutation seam feeds it, not a trigger);
//! 2. so is the to-side of every relationship it reads a column through;
//! 3. and no such to-side (other than the source itself) still has a marker
//!    its install or widen parked with a capture gate
//!    (`pending_backfill.capture_gate_lsn`).
//!
//! Rule 3 closes C3's documented gap, "the gate only holds definitions
//! sourced from the gated table". A definition sourced from `U` that reads a
//! new column of `T` through a relationship waits on `U`'s marker, which no
//! gate on `T` holds. So it waits until `T`'s own gated marker has
//! discharged, which happens only once every change to `T` staged before the
//! widen has drained, deferred reverses included
//! ([`crate::staging::converge::table_changes_pending_through`]). That
//! marker also refreshes `T`'s settled projections when `T` is a to-side
//! (`intake::publication::park_widen_marker`), which repairs the projection
//! columns the old images wrote as `NULL`.
//!
//! The staging worker only parks registration markers for ready definitions
//! and the discharge only dispatches ready ones
//! (`intake::publication::run_pending_backfills_for`). A definition that
//! registered after the pass read the catalog is in neither list, so it
//! waits for the next pass.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Mutex, PoisonError};
use std::time::Instant;

use tokio_postgres::{Client, GenericClient};

use super::CaptureError;
use super::columns::{CaptureCatalog, capture_spec, load_catalog};
use super::install::{self, CaptureAction, LockWait, Progress};
use super::sql::{CaptureEvent, trigger_name};
use crate::defs::TransformStatus;

/// What one [`reconcile`] pass did and found.
#[derive(Debug, Default)]
pub struct PassOutcome {
    /// Every table whose capture is installed and current for the catalog
    /// this pass read.
    pub captured: BTreeSet<String>,
    /// The `waiting_to_backfill` definitions a discharge may dispatch (see
    /// the module doc), in id order.
    pub ready: Vec<i64>,
    /// Each table an install, widen or uninstall is still waiting on.
    pub waiting: Vec<LockWait>,
    /// Each table whose capture couldn't be brought current for another
    /// reason (no primary key, a missing column, a failed statement), with
    /// the error. The pass goes on with the other tables.
    pub failed: Vec<(String, CaptureError)>,
}

/// One pass: brings every table in `desired` (the tables some definition
/// reads, as [`crate::defs::publication_tables`] lists them) to its capture
/// spec, uninstalls this instance's capture from every other table, and
/// works out which waiting definitions are ready. `deadline` bounds the
/// retries on locked tables, shared by the whole pass.
///
/// An `Err` means the pass couldn't read the catalog; a table that fails on
/// its own is in [`PassOutcome::failed`].
pub async fn reconcile(
    client: &mut Client,
    schema: &str,
    desired: &[String],
    deadline: Instant,
) -> Result<PassOutcome, CaptureError> {
    let snapshot = read_snapshot(client, schema).await?;
    let installed = installed_tables(&*client, schema).await?;
    let database: String = client
        .query_one("select pg_catalog.current_database()::text", &[])
        .await?
        .get(0);
    let instance = instance_key(&database, schema);
    let instance = instance.as_str();
    let mut outcome = PassOutcome::default();

    for table in desired {
        // #622 C6: a definition that reads a column the table no longer has
        // pauses before the table's functions are regenerated, whether or
        // not a write has marked it yet. The next pass's catalog no longer
        // counts it.
        match crate::staging::schema_change::pause_readers_of_missing(
            client,
            schema,
            &snapshot.catalog,
            table,
        )
        .await
        {
            Ok(false) => {}
            Ok(true) => {
                forget_lock_wait(instance, table);
                continue;
            }
            Err(err) => {
                forget_lock_wait(instance, table);
                outcome.failed.push((table.clone(), err));
                continue;
            }
        }
        let spec = match capture_spec(&*client, &snapshot.catalog, table).await {
            Ok(spec) => spec,
            Err(err) => {
                // No longer waiting for a lock, whatever it did last pass.
                forget_lock_wait(instance, table);
                outcome.failed.push((table.clone(), err));
                continue;
            }
        };
        match install::reconcile(client, schema, &spec, Some(deadline)).await {
            Ok(Progress::Done(action)) => {
                if action != CaptureAction::Unchanged {
                    tracing::info!(table = %table, action = ?action, "capture reconciled");
                }
                forget_wait(instance, table);
                outcome.captured.insert(table.clone());
            }
            Ok(Progress::Waiting(wait)) => {
                let wait = remember_wait(instance, *wait);
                report(instance, table, wait.operation.as_str(), || {
                    tracing::info!(table = %table, "{wait}; retrying next pass");
                });
                outcome.waiting.push(wait);
            }
            Err(err) => {
                forget_lock_wait(instance, table);
                outcome.failed.push((table.clone(), err));
            }
        }
    }

    let desired_set: HashSet<&String> = desired.iter().collect();
    for table in installed.iter().filter(|t| !desired_set.contains(t)) {
        match install::uninstall(client, schema, table, Some(deadline)).await {
            Ok(Progress::Done(removed)) => {
                if removed {
                    tracing::info!(table = %table, "capture uninstalled: nothing reads the table");
                }
                forget_wait(instance, table);
            }
            Ok(Progress::Waiting(wait)) => {
                let wait = remember_wait(instance, *wait);
                report(instance, table, wait.operation.as_str(), || {
                    tracing::info!(table = %table, "{wait}; retrying next pass");
                });
                outcome.waiting.push(wait);
            }
            Err(err) => {
                forget_lock_wait(instance, table);
                outcome.failed.push((table.clone(), err));
            }
        }
    }

    // A table that is neither read nor installed any more can't be waited on.
    let known: HashSet<&String> = desired.iter().chain(installed.iter()).collect();
    forget_waits_except(instance, &known);

    for (table, err) in &outcome.failed {
        let text = err.to_string();
        report(instance, table, &text, || {
            tracing::warn!(table = %table, error = %err, "capture of a source table failed; retrying next pass");
        });
    }

    let gated = gated_marker_tables(&*client).await?;
    outcome.ready = ready_definitions(&snapshot, desired, &outcome.captured, &gated);
    Ok(outcome)
}

/// The catalog one pass works from, read in one snapshot so the capture
/// specs and the waiting definitions agree.
struct Snapshot {
    catalog: CaptureCatalog,
    /// Every `waiting_to_backfill` definition: id, qualified source, and the
    /// to-sides of the relationships it reads a column through.
    waiting: Vec<(i64, String, BTreeSet<String>)>,
    /// Every definition's target: the tables the target-mutation seam feeds.
    targets: HashSet<String>,
}

async fn read_snapshot(client: &mut Client, schema: &str) -> Result<Snapshot, CaptureError> {
    let txn = client
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;
    let catalog = load_catalog(&txn, schema).await?;
    let rows = txn
        .query(
            "select id, source_table, definition_text from transform_definitions \
             where status = $1 order by id",
            &[&TransformStatus::WaitingToBackfill.as_str()],
        )
        .await?;
    let targets = txn
        .query("select target_table from transform_definitions", &[])
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    txn.commit().await?;
    let mut waiting = Vec::with_capacity(rows.len());
    for row in rows {
        let source: String = row.get(1);
        let def = crate::defs::parse(row.get::<_, &str>(2))
            .map_err(|e| CaptureError::Catalog(e.into()))?;
        let to_sides = to_sides_read(&catalog, &source, &def);
        waiting.push((row.get(0), source, to_sides));
    }
    Ok(Snapshot {
        catalog,
        waiting,
        targets,
    })
}

/// The to-side of every relationship declared on `source` that `def` reads
/// a column through.
fn to_sides_read(
    catalog: &CaptureCatalog,
    source: &str,
    def: &crate::defs::ast::TransformDef,
) -> BTreeSet<String> {
    let names: HashSet<String> = crate::defs::eval::relationship_references(def)
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    catalog
        .relationships
        .iter()
        .filter(|rel| rel.qualified_from_table() == source && names.contains(&rel.def.name))
        .map(|rel| rel.qualified_to_table())
        .collect()
}

/// The module doc's three rules.
fn ready_definitions(
    snapshot: &Snapshot,
    desired: &[String],
    captured: &BTreeSet<String>,
    gated: &HashSet<String>,
) -> Vec<i64> {
    let desired: HashSet<&String> = desired.iter().collect();
    // A table another definition targets is fed by the seam: nothing to
    // capture. Any other table not in `desired` is read by a definition
    // registered after the pass read `desired` (which it does before the
    // snapshot), so nothing captures it yet.
    let current = |table: &String| {
        captured.contains(table) || (!desired.contains(table) && snapshot.targets.contains(table))
    };
    snapshot
        .waiting
        .iter()
        .filter(|(_, source, to_sides)| {
            current(source)
                && to_sides
                    .iter()
                    .all(|t| current(t) && (t == source || !gated.contains(t)))
        })
        .map(|(id, _, _)| *id)
        .collect()
}

/// Every table with a marker an install or widen parked, still pending.
async fn gated_marker_tables(client: &impl GenericClient) -> Result<HashSet<String>, CaptureError> {
    Ok(client
        .query(
            "select table_name from pending_backfill where capture_gate_lsn is not null",
            &[],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect())
}

/// Every table instance `schema` has any capture piece on: a function whose
/// comment names it, or one of the instance's triggers.
pub async fn installed_tables(
    client: &impl GenericClient,
    schema: &str,
) -> Result<BTreeSet<String>, CaptureError> {
    let triggers: Vec<String> = CaptureEvent::ALL
        .iter()
        .map(|event| trigger_name(schema, *event))
        .collect();
    Ok(client
        .query(
            "select c.j ->> 'table' from ( \
                 select d.description::jsonb as j \
                 from pg_catalog.pg_proc p \
                 join pg_catalog.pg_description d \
                   on d.objoid = p.oid \
                  and d.classoid = 'pg_catalog.pg_proc'::pg_catalog.regclass \
                  and d.objsubid = 0 \
                 where p.pronamespace = ( \
                     select oid from pg_catalog.pg_namespace where nspname = $1) \
                   and d.description like '{\"trellis_capture\":1,%') c \
             where c.j ->> 'table' is not null \
             union \
             select n.nspname::text || '.' || r.relname::text \
             from pg_catalog.pg_trigger t \
             join pg_catalog.pg_class r on r.oid = t.tgrelid \
             join pg_catalog.pg_namespace n on n.oid = r.relnamespace \
             where t.tgname = any($2) and not t.tgisinternal",
            &[&schema, &triggers],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect())
}

// ---------------------------------------------------------------------
// The latest lock wait per table, in memory
// ---------------------------------------------------------------------

/// The latest [`LockWait`] of each table whose capture operation is still
/// waiting, keyed by instance (database and schema) and table. In memory only (#622 plan
/// Q9: it describes other sessions, so nothing can re-derive it, and it
/// stays out of the schema): a staging worker in another process reports
/// its waits in its own log only.
static LOCK_WAITS: Mutex<Option<HashMap<(String, String), LockWait>>> = Mutex::new(None);

fn with_waits<T>(f: impl FnOnce(&mut HashMap<(String, String), LockWait>) -> T) -> T {
    let mut guard = LOCK_WAITS.lock().unwrap_or_else(PoisonError::into_inner);
    f(guard.get_or_insert_with(HashMap::new))
}

/// Records `wait`, keeping the first pass's `waiting_since` while the same
/// operation keeps waiting, and returns what it recorded.
fn remember_wait(instance: &str, mut wait: LockWait) -> LockWait {
    with_waits(|waits| {
        let key = (instance.to_string(), wait.table.clone());
        if let Some(previous) = waits.get(&key)
            && previous.operation == wait.operation
        {
            wait.waiting_since = wait.waiting_since.min(previous.waiting_since);
        }
        waits.insert(key, wait.clone());
        wait
    })
}

fn forget_wait(instance: &str, table: &str) {
    with_waits(|waits| waits.remove(&(instance.to_string(), table.to_string())));
    with_reports(|reports| reports.remove(&(instance.to_string(), table.to_string())));
}

/// How often [`report`] repeats a table's unchanged wait or failure at its
/// full level. A pass runs every `reconcile_interval` (5 s by default), and a
/// wait can last as long as an autovacuum.
const REPORT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// What [`report`] last logged per `(schema, table)`, and when.
type Reports = HashMap<(String, String), (String, Instant)>;

static REPORTS: Mutex<Option<Reports>> = Mutex::new(None);

fn with_reports<T>(f: impl FnOnce(&mut Reports) -> T) -> T {
    let mut guard = REPORTS.lock().unwrap_or_else(PoisonError::into_inner);
    f(guard.get_or_insert_with(HashMap::new))
}

/// Runs `log` when `table` newly waits or fails, when what it reports
/// (`what`) changed, or once [`REPORT_INTERVAL`] has passed since it last
/// ran; otherwise logs `what` at `debug`. Cleared when the table's capture
/// lands.
fn report(instance: &str, table: &str, what: &str, log: impl FnOnce()) {
    let key = (instance.to_string(), table.to_string());
    let due = with_reports(|reports| {
        let due = reports
            .get(&key)
            .is_none_or(|(last, at)| last != what || at.elapsed() >= REPORT_INTERVAL);
        if due {
            reports.insert(key, (what.to_string(), Instant::now()));
        }
        due
    });
    if due {
        log();
    } else {
        tracing::debug!(table = %table, "{what}; still retrying");
    }
}

/// Forgets `table`'s lock wait only, keeping what [`report`] last logged for
/// it: a table that failed for another reason is no longer waiting for a
/// lock, but its failure is still rate-limited.
fn forget_lock_wait(instance: &str, table: &str) {
    with_waits(|waits| waits.remove(&(instance.to_string(), table.to_string())));
}

/// Forgets every wait and report of the instance with schema `schema` in
/// database `database`: its staging worker in this process stopped, so
/// nothing here keeps them current any more.
pub fn forget_instance(database: &str, schema: &str) {
    let instance = instance_key(database, schema);
    with_waits(|waits| waits.retain(|(i, _), _| *i != instance));
    with_reports(|reports| reports.retain(|(i, _), _| *i != instance));
}

fn forget_waits_except(instance: &str, known: &HashSet<&String>) {
    with_waits(|waits| waits.retain(|(i, table), _| i != instance || known.contains(table)));
    with_reports(|reports| reports.retain(|(i, table), _| i != instance || known.contains(table)));
}

/// The latest lock wait of `table`'s capture in the instance with schema
/// `schema` in database `database`, if its install, widen or uninstall is
/// still waiting for the table lock as of the staging worker's last pass in
/// this process.
pub fn lock_wait(database: &str, schema: &str, table: &str) -> Option<LockWait> {
    let key = (instance_key(database, schema), table.to_string());
    with_waits(|waits| waits.get(&key).cloned())
}

/// The registry's key for one instance: its database and schema. Two
/// instances in one process can share a schema name in different databases.
fn instance_key(database: &str, schema: &str) -> String {
    format!("{database}\u{1f}{schema}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(waiting: &[(i64, &str, &[&str])]) -> Snapshot {
        Snapshot {
            catalog: CaptureCatalog::default(),
            waiting: waiting
                .iter()
                .map(|(id, source, to_sides)| {
                    (
                        *id,
                        source.to_string(),
                        to_sides.iter().map(|t| t.to_string()).collect(),
                    )
                })
                .collect(),
            targets: HashSet::new(),
        }
    }

    fn set(tables: &[&str]) -> BTreeSet<String> {
        tables.iter().map(|t| t.to_string()).collect()
    }

    fn strings(tables: &[&str]) -> Vec<String> {
        tables.iter().map(|t| t.to_string()).collect()
    }

    fn wait_on(table: &str) -> LockWait {
        LockWait {
            table: table.to_string(),
            operation: install::LockingOperation::Install,
            lock_mode: "ShareRowExclusiveLock".to_string(),
            waiting_since: std::time::SystemTime::now(),
            observed_at: std::time::SystemTime::now(),
            blockers: Vec::new(),
        }
    }

    #[test]
    fn a_stopped_instance_forgets_its_waits_and_no_other() {
        // Names no other test uses: the registry is process-global.
        let (stopped, other) = ("db_forget_a", "db_forget_b");
        remember_wait(&instance_key(stopped, "s"), wait_on("public.t"));
        remember_wait(&instance_key(other, "s"), wait_on("public.t"));
        forget_instance(stopped, "s");
        assert_eq!(lock_wait(stopped, "s", "public.t"), None);
        assert!(lock_wait(other, "s", "public.t").is_some());
        forget_instance(other, "s");
    }

    #[test]
    fn a_definition_is_ready_once_its_source_is_captured() {
        let snap = snapshot(&[(1, "public.u", &[]), (2, "public.v", &[])]);
        let desired = strings(&["public.u", "public.v"]);
        let ready = ready_definitions(&snap, &desired, &set(&["public.u"]), &HashSet::new());
        assert_eq!(ready, vec![1], "v's install is still waiting");
    }

    #[test]
    fn a_seam_fed_source_needs_no_capture() {
        let mut snap = snapshot(&[(1, "public.target", &[])]);
        snap.targets.insert("public.target".to_string());
        let ready = ready_definitions(&snap, &[], &BTreeSet::new(), &HashSet::new());
        assert_eq!(ready, vec![1]);
    }

    #[test]
    fn a_source_missing_from_desired_that_nothing_targets_is_not_seam_fed() {
        // Registered between the pass's read of `desired` and its snapshot.
        let snap = snapshot(&[(1, "public.u", &[]), (2, "public.v", &["public.w"])]);
        let desired = strings(&["public.u"]);
        let ready = ready_definitions(&snap, &desired, &set(&["public.u"]), &HashSet::new());
        assert_eq!(ready, vec![1], "neither v nor w is captured yet");
    }

    #[test]
    fn a_to_side_whose_widen_waits_holds_the_reader() {
        let snap = snapshot(&[(1, "public.u", &["public.t"])]);
        let desired = strings(&["public.u", "public.t"]);
        let ready = ready_definitions(&snap, &desired, &set(&["public.u"]), &HashSet::new());
        assert!(ready.is_empty());
    }

    #[test]
    fn a_to_side_with_a_gated_marker_holds_the_reader_but_the_source_s_own_gate_does_not() {
        let desired = strings(&["public.u", "public.t"]);
        let captured = set(&["public.u", "public.t"]);
        let gated: HashSet<String> = ["public.t".to_string()].into();
        let snap = snapshot(&[
            (1, "public.u", &["public.t"]),
            (2, "public.t", &["public.t"]),
        ]);
        assert_eq!(
            ready_definitions(&snap, &desired, &captured, &gated),
            vec![2],
            "a self-relationship is held by its own marker's gate, not by rule 3"
        );
        assert_eq!(
            ready_definitions(&snap, &desired, &captured, &HashSet::new()),
            vec![1, 2]
        );
    }
}
