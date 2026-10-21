//! The staging worker's capture reconcile pass (#622 C5).
//!
//! Every reconcile pass of the maintenance loop brings each table's capture
//! triggers to what the catalog needs, one table at a time, before the
//! backfill discharge runs:
//!
//! - a table some definition reads ([`crate::defs::tables_to_capture`]) is
//!   installed, widened or narrowed to its [`super::columns::capture_spec`]
//!   ([`super::install::reconcile`]);
//! - a table this instance captures that nothing reads any more is
//!   uninstalled.
//!
//! First, the pass pauses every definition that reads a table in a
//! partition or inheritance hierarchy, which it can't capture (#707,
//! [`crate::staging::schema_change::pause_readers_in_hierarchy`]), and leaves
//! that table's capture as it is. Define refuses such a table, but it can
//! join a hierarchy later, or be dropped and recreated as one.
//!
//! Once partitioned sources are served (`defs::catalog`'s
//! `PARTITIONED_SOURCES_CAPTURED`), a partitioned root is not "in a
//! hierarchy": the pass fingerprints its tree, pauses the readers it stopped
//! complying for, and records the tree and the corrections for its partition
//! maintenance once capture covers every node ([`super::partitions`], ADR-0017
//! P3). Then it sweeps the source for groups that route to no current leaf
//! and queues their correction ([`super::complement`], P7). While that gate
//! is closed, the check above pauses its readers.
//!
//! Before it regenerates a table's functions, the pass pauses every
//! definition that reads a column the table no longer has (#622 C6,
//! [`crate::staging::schema_change::pause_readers_of_missing`]), or reads a
//! table whose row-level security now applies to Trellis's role (#745), or
//! that a logical-replication subscription now replicates into (#751)
//! ([`crate::staging::schema_change::pause_readers_of_unsupported`]), and
//! leaves the table for the next pass. Those last two checks also run on
//! each table another definition targets, which the pass doesn't capture
//! (the target-mutation seam feeds it) but its readers still read. There the
//! pass also pauses the definition that writes the target when its
//! row-level security now applies to the worker's role, which would filter
//! the writes (#765).
//!
//! It also pauses every definition one of whose key columns on the table
//! (its source key, a `GROUP BY` key, a join column or to-side key of a
//! relationship it reads through) now has a type or collation define would
//! refuse, or changed type in a way that renders the keys already stored
//! differently, whose relationship's join columns no longer match, or that
//! created a column, typed from one the table widened, that can't hold
//! the type define would give it now (#760, #767, #824,
//! [`crate::staging::schema_change::pause_readers_of_retyped`]). A table
//! Trellis created whose every such column widened by changing only the
//! catalog (a longer `varchar`, say) is re-typed in place instead, with no
//! pause. That check runs on the seam-fed tables too. It compares a
//! definition's typed copies with the live schema only when something it
//! reads has changed since the last comparison found them all current: the
//! pass fingerprints those tables' columns once, before any check
//! ([`crate::staging::drift_memo`], #858).
//!
//! Last, it compares the table's read columns with the baseline the last
//! install, widen, narrow or pass recorded (#703, `capture::baseline`): a
//! column dropped and re-added, retyped other than by a pure widening, or
//! whose values a same-type `ALTER ... TYPE ... USING` rewrote. It rebuilds
//! the table's applying readers as `Trellis::request_backfill` would, or
//! pauses a definition the change may have left invalid.
//!
//! Before any of that, the pass resumes each definition an upstream's resume
//! paused, once that upstream is `live` (#828, #970,
//! [`crate::staging::quarantine::resume_caused_definitions`]), and finishes
//! every resume left waiting on it to re-type Trellis's copies (#767,
//! [`crate::staging::quarantine::finish_requested_resumes`]), so a re-type
//! the first step requests is done in the same pass.
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
//! Its [`LockWait`] is recorded in `capture_holdups`, where
//! [`crate::Trellis::status`], in any process, reports it on each definition
//! it keeps waiting. So is the error of a table whose capture fails for
//! another reason (no primary key, a failed statement, #687). Each pass
//! rewrites that table: a table it brings current, or no longer captures,
//! loses its row.
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
//! (`intake::markers::park_widen_marker`), which repairs the projection
//! columns the old images wrote as `NULL`.
//!
//! The staging worker only parks registration markers for ready definitions
//! and the discharge only dispatches ready ones
//! (`intake::markers::run_pending_backfills_for`). A definition that
//! registered after the pass read the catalog is in neither list, so it
//! waits for the next pass.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Mutex, PoisonError};
use std::time::Instant;

use tokio_postgres::types::Type;
use tokio_postgres::{Client, GenericClient};

use super::CaptureError;
use super::columns::{CaptureCatalog, capture_spec, load_catalog};
use super::install::{self, CaptureAction, LockWait, Progress};
use super::partitions;
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
    /// Each partitioned source whose tree the pass read in full because a
    /// fingerprint changed, none was stored, the process had not yet checked
    /// it, or it was non-compliant (ADR-0017, [`super::partitions`]).
    pub partition_deep_checks: Vec<String>,
    /// Each partitioned source's every-pass sweep for groups that route to
    /// no current leaf ([`super::complement`]).
    pub partition_sweeps: Vec<(String, super::complement::Sweep)>,
}

/// One pass: brings every table in `desired` (the tables some definition
/// reads, as [`crate::defs::tables_to_capture`] lists them) to its capture
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
    // #970: a definition paused by its upstream's re-type resumes once that
    // upstream is live; a re-type that requests is done by the next call.
    // #767: a resume left waiting on re-typed copies is finished first, so
    // this pass's snapshot sees the ones it completes as waiting.
    //
    // #1205: before any resume, and before any table is looked at: a logical
    // restore pauses every definition and discards the ring, so nothing below
    // acts on the original database's records.
    super::restore::detect(client).await?;
    crate::staging::quarantine::resume_caused_definitions(client, schema).await?;
    crate::staging::quarantine::finish_requested_resumes(client, schema).await?;
    let snapshot = read_snapshot(client, schema).await?;
    let installed = installed_tables(&*client, schema).await?;
    let database: String = client
        .query_typed_one("select pg_catalog.current_database()::text", &[])
        .await?
        .get(0);
    let instance = instance_key(&database, schema);
    let instance = instance.as_str();
    // #858: read before any check, so a change that commits during the pass
    // is checked again by the next one (`staging::drift_memo`).
    let drift =
        crate::staging::drift_memo::Fingerprints::read(&*client, schema, &snapshot.catalog).await?;
    let mut outcome = PassOutcome::default();

    for table in desired {
        // #999: a table some definition reads that is gone from its name
        // (dropped, renamed away, moved to another schema) pauses its readers;
        // a resume re-validates them against whatever is there then.
        match super::triggers::pause_readers_of_gone_table(client, &snapshot.catalog, table).await {
            Ok(false) => {}
            Ok(true) => continue,
            Err(err) => {
                outcome.failed.push((table.clone(), err));
                continue;
            }
        }
        // #707: a table that joined a partition or inheritance hierarchy
        // after define, or was recreated as one, can't be captured: its
        // readers pause, and its capture is left as it is.
        //
        // ADR-0017 P3: a partitioned root that is no partition and in no
        // inheritance hierarchy is not "in a hierarchy". The pass fingerprints
        // its tree instead, pauses the readers it stopped complying for, and
        // records what it found once capture covers every node.
        let mut partitioned = None;
        match partitions::begin(client, instance, &snapshot.catalog, table).await {
            Ok(partitions::Begin::Held { deep }) => {
                if deep {
                    outcome.partition_deep_checks.push(table.clone());
                }
                continue;
            }
            Ok(partitions::Begin::Proceed(proceed)) => {
                if proceed.deep {
                    outcome.partition_deep_checks.push(table.clone());
                }
                partitioned = Some(proceed);
            }
            Ok(partitions::Begin::NotPartitioned) => {
                match crate::staging::schema_change::pause_readers_in_hierarchy(
                    client,
                    &snapshot.catalog,
                    table,
                )
                .await
                {
                    Ok(false) => {}
                    Ok(true) => continue,
                    Err(err) => {
                        outcome.failed.push((table.clone(), err));
                        continue;
                    }
                }
            }
            Err(err) => {
                outcome.failed.push((table.clone(), err));
                continue;
            }
        }
        // #745: row-level security that applies to Trellis's role filters
        // every read of the table, and #751: a subscription's changes to it
        // are never captured, so its readers pause, whatever capture does.
        match crate::staging::schema_change::pause_readers_of_unsupported(
            client,
            schema,
            &snapshot.catalog,
            table,
        )
        .await
        {
            Ok(false) => {}
            Ok(true) => continue,
            Err(err) => {
                outcome.failed.push((table.clone(), err));
                continue;
            }
        }
        // #707: capture triggers or functions changed outside Trellis pause
        // the table's readers, and nothing is reinstalled until a resume.
        match super::triggers::check(client, schema, &snapshot.catalog, table).await {
            Ok(false) => {}
            Ok(true) => continue,
            Err(err) => {
                outcome.failed.push((table.clone(), err));
                continue;
            }
        }
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
                // Neither waiting nor failing: the next pass decides afresh.
                continue;
            }
            Err(err) => {
                outcome.failed.push((table.clone(), err));
                continue;
            }
        }
        // #1204: a type renamed or moved to another schema keeps its oid and
        // its values; its name is re-recorded where Trellis casts by it, and
        // the key-type check below then finds the name it recorded.
        if let Err(err) = super::baseline::follow_renamed_types(client, table).await {
            outcome.failed.push((table.clone(), err));
            continue;
        }
        // #760, #767: a key column whose type or collation define would
        // refuse now, a join pair that no longer matches, a type change that
        // renders the stored keys differently, or a widening past a typed
        // copy pauses the definitions it concerns. After the missing-column
        // check, which owns a key column that's gone.
        match crate::staging::schema_change::pause_readers_of_retyped(
            client,
            schema,
            instance,
            &snapshot.catalog,
            &drift,
            table,
        )
        .await
        {
            Ok(false) => {}
            Ok(true) => continue,
            Err(err) => {
                outcome.failed.push((table.clone(), err));
                continue;
            }
        }
        // #703 R1, R3b: a read column dropped and re-added, retyped, or
        // its values rewritten in place, since the last pass. Its readers
        // rebuild, or pause where their definition may no longer be valid.
        match super::baseline::check(client, schema, &snapshot.catalog, table).await {
            Ok(false) => {}
            Ok(true) => continue,
            Err(err) => {
                outcome.failed.push((table.clone(), err));
                continue;
            }
        }
        let spec = match capture_spec(&*client, &snapshot.catalog, table).await {
            Ok(spec) => spec,
            Err(err) => {
                outcome.failed.push((table.clone(), err));
                continue;
            }
        };
        match install::reconcile(client, schema, &spec, Some(deadline)).await {
            Ok(Progress::Done(action)) => {
                if action != CaptureAction::Unchanged {
                    crate::instance_log::info!(table = %table, action = ?action, "capture reconciled");
                }
                // Every node carries capture now, so a new leaf's rows are
                // corrected from here on: record the tree and queue them.
                if let Some(proceed) = partitioned {
                    match partitions::commit(client, instance, proceed).await {
                        Ok(queued) if queued > 0 => {
                            crate::instance_log::info!(table = %table, corrections = queued, "partition maintenance detected; corrections queued");
                        }
                        Ok(_) => {}
                        Err(err) => {
                            outcome.failed.push((table.clone(), err));
                            continue;
                        }
                    }
                    // ADR-0017 P7: groups that route to no current leaf, which
                    // a leaf created and dropped between two passes leaves.
                    match super::complement::sweep(client, schema, &snapshot.catalog, table).await {
                        Ok(sweep) => {
                            if sweep.queued {
                                crate::instance_log::info!(table = %table, "groups that route to no partition found; correction queued");
                            }
                            outcome.partition_sweeps.push((table.clone(), sweep));
                        }
                        Err(err) => {
                            outcome.failed.push((table.clone(), err));
                            continue;
                        }
                    }
                }
                forget_report(instance, table);
                outcome.captured.insert(table.clone());
            }
            Ok(Progress::Waiting(wait)) => {
                let wait = remember_wait(&*client, *wait).await?;
                report(instance, table, wait.operation.as_str(), || {
                    crate::instance_log::info!(table = %table, "{wait}; retrying next pass");
                });
                outcome.waiting.push(wait);
            }
            Err(err) => {
                outcome.failed.push((table.clone(), err));
            }
        }
    }

    let desired_set: HashSet<&String> = desired.iter().collect();
    // #745, #751: a table another definition targets isn't in `desired` (the
    // seam feeds it, not a trigger), but its readers read it as the
    // worker's role all the same (not the ring's owner: no capture function
    // reads it), and the seam doesn't see a subscription's writes to it
    // either. #765: and the definition that targets it writes it as the
    // worker's role, so row-level security that applies to that role pauses
    // the writer. This loop runs over every target, read or not. A seam-fed
    // table whose check pauses a definition, or fails, isn't current for
    // `ready_definitions` either.
    let mut seam_held: HashSet<String> = HashSet::new();
    let mut seam_fed: Vec<&String> = snapshot
        .targets
        .iter()
        .filter(|t| !desired_set.contains(t))
        .collect();
    seam_fed.sort();
    for &table in &seam_fed {
        match crate::staging::schema_change::pause_readers_of_unsupported(
            client,
            schema,
            &snapshot.catalog,
            table,
        )
        .await
        {
            Ok(false) => {}
            Ok(true) => {
                seam_held.insert(table.clone());
                continue;
            }
            Err(err) => {
                seam_held.insert(table.clone());
                outcome.failed.push((table.clone(), err));
                continue;
            }
        }
        // #760, #767: a chained definition's key columns and typed copies
        // are checked on its source target too, which a resume upstream can
        // re-type.
        match crate::staging::schema_change::pause_readers_of_retyped(
            client,
            schema,
            instance,
            &snapshot.catalog,
            &drift,
            table,
        )
        .await
        {
            Ok(false) => {}
            Ok(true) => {
                seam_held.insert(table.clone());
            }
            Err(err) => {
                seam_held.insert(table.clone());
                outcome.failed.push((table.clone(), err));
            }
        }
    }
    for table in installed.iter().filter(|t| !desired_set.contains(t)) {
        match install::uninstall(client, schema, table, Some(deadline)).await {
            Ok(Progress::Done(removed)) => {
                if removed {
                    crate::instance_log::info!(table = %table, "capture uninstalled: nothing reads the table");
                }
                forget_report(instance, table);
            }
            Ok(Progress::Waiting(wait)) => {
                let wait = remember_wait(&*client, *wait).await?;
                report(instance, table, wait.operation.as_str(), || {
                    crate::instance_log::info!(table = %table, "{wait}; retrying next pass");
                });
                outcome.waiting.push(wait);
            }
            Err(err) => {
                outcome.failed.push((table.clone(), err));
            }
        }
    }

    // A table that is neither read nor installed any more can't be waited on.
    let known: HashSet<&String> = desired.iter().chain(installed.iter()).collect();
    forget_reports_except(instance, &known);
    crate::staging::drift_memo::retain_tables(
        instance,
        &known
            .iter()
            .chain(seam_fed.iter())
            .map(|t| t.as_str())
            .collect(),
    );

    for (table, err) in &outcome.failed {
        // No longer waiting for a lock, whatever it did last pass.
        remember_failure(&*client, table, err).await?;
        let text = err.to_string();
        report(instance, table, &text, || {
            crate::instance_log::warn!(table = %table, error = %err, "capture of a source table failed; retrying next pass");
        });
    }

    // Every other table's capture is current, or none is wanted: nothing
    // holds it back any more.
    let held: Vec<&str> = outcome
        .waiting
        .iter()
        .map(|wait| wait.table.as_str())
        .chain(outcome.failed.iter().map(|(table, _)| table.as_str()))
        .collect();
    client
        .execute_typed(
            "delete from capture_holdups where table_name <> all($1)",
            &[(&held, Type::TEXT_ARRAY)],
        )
        .await?;

    // A source nothing reads any more has no recorded tree to keep.
    partitions::forget_except(&*client, desired).await?;
    // Nor a baseline, once nothing captures it.
    let kept: Vec<&str> = known.iter().map(|t| t.as_str()).collect();
    super::baseline::forget_except(&*client, &kept).await?;
    super::triggers::forget_except(&*client, &kept).await?;

    let gated = gated_marker_tables(&*client).await?;
    outcome.ready = ready_definitions(&snapshot, desired, &outcome.captured, &gated, &seam_held);
    Ok(outcome)
}

/// The catalog one pass works from, read in one snapshot so the capture
/// specs and the waiting definitions agree.
struct Snapshot {
    catalog: CaptureCatalog,
    /// Every `waiting_to_backfill` definition: id, qualified source, and the
    /// to-sides of the relationships it reads a column through (a `JOIN`'s
    /// joined table among them).
    waiting: Vec<(i64, String, BTreeSet<String>)>,
    /// Every definition's target: the tables the target-mutation seam feeds.
    targets: HashSet<String>,
}

/// The disruption half of a capture pass (#707, #999), alone and on the
/// caller's connection: for each of `desired`, pauses the readers of a table
/// that is gone from its name or whose capture triggers or functions were
/// changed outside Trellis, exactly as [`reconcile`] does, and installs and
/// repairs nothing. Returns the tables it found disrupted (including ones an
/// earlier pass already paused the readers of).
///
/// A test helper calls it after a fixture load so that the readers are
/// paused when it returns, instead of at the staging worker's next pass
/// (#1181). It is safe beside a running worker: a pause is recorded once per
/// definition.
pub async fn detect_disruptions(
    client: &mut Client,
    schema: &str,
    desired: &[String],
) -> Result<Vec<String>, CaptureError> {
    let snapshot = read_snapshot(client, schema).await?;
    let mut found = Vec::new();
    // #1205: a logical restore pauses every definition.
    if super::restore::detect(client).await? {
        found.extend(desired.iter().cloned());
        return Ok(found);
    }
    for table in desired {
        if super::triggers::pause_readers_of_gone_table(client, &snapshot.catalog, table).await?
            || super::triggers::check(client, schema, &snapshot.catalog, table).await?
        {
            found.push(table.clone());
        }
    }
    Ok(found)
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
            "select id, source_table, definition_text, grammar_version, join_table \
             from transform_definitions where status = $1 order by id",
            &[&TransformStatus::WaitingToBackfill.as_str()],
        )
        .await?;
    let targets = txn
        .query_typed("select target_table from transform_definitions", &[])
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    txn.commit().await?;
    let mut waiting = Vec::with_capacity(rows.len());
    for row in rows {
        let source: String = row.get(1);
        let def = crate::defs::grammar::load(row.get::<_, &str>(2), row.get(3))
            .map_err(|e| CaptureError::Catalog(e.into()))?;
        let mut to_sides = to_sides_read(&catalog, &source, &def);
        // #1107: a JOIN's joined table is read like its source.
        to_sides.extend(row.get::<_, Option<String>>(4));
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
    seam_held: &HashSet<String>,
) -> Vec<i64> {
    let desired: HashSet<&String> = desired.iter().collect();
    // A table another definition targets is fed by the seam: nothing to
    // capture, unless this pass held it back (`seam_held`, #745). Any other
    // table not in `desired` is read by a definition registered after the
    // pass read `desired` (which it does before the snapshot), so nothing
    // captures it yet.
    let current = |table: &String| {
        captured.contains(table)
            || (!desired.contains(table)
                && snapshot.targets.contains(table)
                && !seam_held.contains(table))
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
        .query_typed(
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
///
/// A partition of a captured root is not listed: its triggers belong to the
/// root's. Neither is a table (or one under a table) whose `DETACH
/// CONCURRENTLY` is still pending: a writer with an older snapshot can still
/// write through the parent into it, so its triggers come off only once the
/// detach has completed (ADR-0017). Both exclusions are each one subtree
/// walk per recorded table or pending detach, not an ancestor walk per
/// trigger, so a steady pass over a large tree stays cheap. The walks read
/// `pg_inherits` alone: `pg_partition_tree` locks every node, and a leaf under
/// `ACCESS EXCLUSIVE` would stall the whole pass, which no `lock_timeout`
/// bounds.
pub async fn installed_tables(
    client: &impl GenericClient,
    schema: &str,
) -> Result<BTreeSet<String>, CaptureError> {
    let triggers: Vec<String> = CaptureEvent::ALL
        .iter()
        .map(|event| trigger_name(schema, *event))
        .collect();
    Ok(client
        .query_typed(
            "with recursive recorded as ( \
                 select c.j ->> 'table' as name from ( \
                     select d.description::jsonb as j \
                     from pg_catalog.pg_proc p \
                     join pg_catalog.pg_description d \
                       on d.objoid = p.oid \
                      and d.classoid = 'pg_catalog.pg_proc'::pg_catalog.regclass \
                      and d.objsubid = 0 \
                     where p.pronamespace = ( \
                         select oid from pg_catalog.pg_namespace where nspname = $1) \
                       and d.description like '{\"trellis_capture\":1,%') c \
                 where c.j ->> 'table' is not null), \
             covered(relid) as ( \
                 select k.oid from (select distinct name from recorded) x \
                 join pg_catalog.pg_namespace kn \
                   on kn.nspname = pg_catalog.split_part(x.name, '.', 1)::name \
                 join pg_catalog.pg_class k \
                   on k.relnamespace = kn.oid \
                  and k.relname = pg_catalog.split_part(x.name, '.', 2)::name \
                 union \
                 select i.inhrelid from covered c \
                 join pg_catalog.pg_class p on p.oid = c.relid and p.relkind = 'p' \
                 join pg_catalog.pg_inherits i \
                   on i.inhparent = c.relid and not i.inhdetachpending), \
             pending(relid) as ( \
                 select i.inhrelid from pg_catalog.pg_inherits i \
                 where i.inhdetachpending \
                 union \
                 select i.inhrelid from pending d \
                 join pg_catalog.pg_inherits i on i.inhparent = d.relid) \
             select name from recorded \
             union \
             select n.nspname::text || '.' || r.relname::text \
             from pg_catalog.pg_class r \
             join pg_catalog.pg_namespace n on n.oid = r.relnamespace \
             where r.oid in ( \
                   select t.tgrelid from pg_catalog.pg_trigger t \
                   where t.tgname = any($2) and not t.tgisinternal) \
               and r.oid not in (select relid from pending) \
               and not (r.relispartition and r.oid in (select relid from covered))",
            &[(&schema, Type::TEXT), (&triggers, Type::TEXT_ARRAY)],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect())
}

// ---------------------------------------------------------------------
// What holds each table's capture back: `capture_holdups`
// ---------------------------------------------------------------------

/// Records `wait` in `capture_holdups`, replacing any failure, and keeping
/// the first pass's `waiting_since` while the same operation keeps waiting.
/// Returns what it recorded. The blockers are stored as the lines
/// [`crate::CaptureWait::blockers`] reports.
async fn remember_wait(
    client: &impl GenericClient,
    mut wait: LockWait,
) -> Result<LockWait, CaptureError> {
    let blockers: Vec<String> = wait
        .blockers
        .iter()
        .map(|blocker| blocker.describe(wait.observed_at))
        .collect();
    wait.waiting_since = client
        .query_one(
            "insert into capture_holdups as h \
                 (table_name, since, operation, lock_mode, observed_at, blockers) \
             values ($1, $2, $3, $4, $5, $6) \
             on conflict (table_name) do update set \
                 since = case when h.operation = excluded.operation \
                              then least(h.since, excluded.since) \
                              else excluded.since end, \
                 operation = excluded.operation, lock_mode = excluded.lock_mode, \
                 observed_at = excluded.observed_at, blockers = excluded.blockers, \
                 error = null, columns = null \
             returning since",
            &[
                &wait.table,
                &wait.waiting_since,
                &wait.operation.as_str(),
                &wait.lock_mode,
                &wait.observed_at,
                &blockers,
            ],
        )
        .await?
        .get(0);
    Ok(wait)
}

/// Records `table`'s failure in `capture_holdups`, replacing any wait, and
/// keeping the first pass's `since` while it keeps failing with the same
/// error.
async fn remember_failure(
    client: &impl GenericClient,
    table: &str,
    err: &CaptureError,
) -> Result<(), CaptureError> {
    let error = err.to_string();
    let columns = match err {
        CaptureError::MissingColumn { column, .. } => vec![column.clone()],
        _ => Vec::new(),
    };
    client
        .execute(
            "insert into capture_holdups as h (table_name, since, error, columns) \
             values ($1, pg_catalog.clock_timestamp(), $2, $3) \
             on conflict (table_name) do update set \
                 since = case when h.error = excluded.error then h.since \
                              else excluded.since end, \
                 error = excluded.error, columns = excluded.columns, \
                 operation = null, lock_mode = null, observed_at = null, blockers = null",
            &[&table, &error, &columns],
        )
        .await?;
    Ok(())
}

/// Forgets what [`report`] last logged for `table`: its capture landed.
fn forget_report(instance: &str, table: &str) {
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
        crate::instance_log::debug!(table = %table, "{what}; still retrying");
    }
}

/// Forgets every report of the instance with schema `schema` in database
/// `database`: its staging worker in this process stopped, so a worker that
/// takes over logs afresh, and tries each in-place re-type that failed once
/// more. Its `capture_holdups` rows stay: they are what its last pass found,
/// and the next worker's first pass rewrites them.
pub fn forget_instance(database: &str, schema: &str) {
    let instance = instance_key(database, schema);
    with_reports(|reports| reports.retain(|(i, _), _| *i != instance));
    crate::staging::schema_change::forget_failed_retypes(&instance);
    crate::staging::drift_memo::forget_instance(&instance);
}

fn forget_reports_except(instance: &str, known: &HashSet<&String>) {
    with_reports(|reports| reports.retain(|(i, table), _| i != instance || known.contains(table)));
}

/// The registry's key for one instance: its database and schema. Two
/// instances in one process can share a schema name in different databases.
pub(crate) fn instance_key(database: &str, schema: &str) -> String {
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

    #[test]
    fn a_definition_is_ready_once_its_source_is_captured() {
        let snap = snapshot(&[(1, "public.u", &[]), (2, "public.v", &[])]);
        let desired = strings(&["public.u", "public.v"]);
        let ready = ready_definitions(
            &snap,
            &desired,
            &set(&["public.u"]),
            &HashSet::new(),
            &HashSet::new(),
        );
        assert_eq!(ready, vec![1], "v's install is still waiting");
    }

    #[test]
    fn a_seam_fed_source_needs_no_capture() {
        let mut snap = snapshot(&[(1, "public.target", &[])]);
        snap.targets.insert("public.target".to_string());
        let ready = ready_definitions(
            &snap,
            &[],
            &BTreeSet::new(),
            &HashSet::new(),
            &HashSet::new(),
        );
        assert_eq!(ready, vec![1]);
    }

    #[test]
    fn a_seam_fed_table_the_pass_held_back_holds_its_readers() {
        // #745: row-level security applies to the target another definition
        // reads, as its source or a to-side.
        let mut snap = snapshot(&[
            (1, "public.target", &[]),
            (2, "public.u", &["public.target"]),
            (3, "public.u", &[]),
        ]);
        snap.targets.insert("public.target".to_string());
        let desired = strings(&["public.u"]);
        let held: HashSet<String> = ["public.target".to_string()].into();
        let ready = ready_definitions(&snap, &desired, &set(&["public.u"]), &HashSet::new(), &held);
        assert_eq!(ready, vec![3]);
    }

    #[test]
    fn a_source_missing_from_desired_that_nothing_targets_is_not_seam_fed() {
        // Registered between the pass's read of `desired` and its snapshot.
        let snap = snapshot(&[(1, "public.u", &[]), (2, "public.v", &["public.w"])]);
        let desired = strings(&["public.u"]);
        let ready = ready_definitions(
            &snap,
            &desired,
            &set(&["public.u"]),
            &HashSet::new(),
            &HashSet::new(),
        );
        assert_eq!(ready, vec![1], "neither v nor w is captured yet");
    }

    #[test]
    fn a_to_side_whose_widen_waits_holds_the_reader() {
        let snap = snapshot(&[(1, "public.u", &["public.t"])]);
        let desired = strings(&["public.u", "public.t"]);
        let ready = ready_definitions(
            &snap,
            &desired,
            &set(&["public.u"]),
            &HashSet::new(),
            &HashSet::new(),
        );
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
            ready_definitions(&snap, &desired, &captured, &gated, &HashSet::new()),
            vec![2],
            "a self-relationship is held by its own marker's gate, not by rule 3"
        );
        assert_eq!(
            ready_definitions(&snap, &desired, &captured, &HashSet::new(), &HashSet::new()),
            vec![1, 2]
        );
    }
}
