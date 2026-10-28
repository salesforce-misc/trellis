//! The scheduled build (ADR-0002 "A build is Re-derive over
//! chunks, and applies from its first chunk"): a 1-1 target's build chunk
//! ([`one_to_one::run_chunk`]) and its scheduling (F2). It builds the shapes
//! it serves (see "Shapes"), a fresh definition's and a resumed one's alike:
//! a 1-1 target by chunks, and an aggregate or a `JOIN` by its plan job's one
//! absolute write.
//!
//! # The scheduled build (F2, F3)
//!
//! - **Start** ([`start_ready_builds`]). The staging worker's reconcile pass
//!   starts each ready definition: once its source's capture gate is clear, one
//!   transaction moves it `waiting_to_backfill -> backfilling` with
//!   `transform_definitions.build = 'rederive'` and enqueues its plan job.
//!   From that commit it applies (`defs::model::APPLYING_SQL`, B1): Apply
//!   folds its source's changes into its target, so nothing needs
//!   a catch-up afterwards. The start also records `build_seg`, the segment active at
//!   its commit, and a page re-derives rather than applies the keys of any
//!   batch at or below it (see `start`). A resume leaves the rebuild to
//!   this start (`super::quarantine::resume_transform`).
//! - **Rebuild** (F3). A resumed 1-1 definition keeps its ledger, so its
//!   build is a Re-derive over its existing entries, with no truncate. Its
//!   start also enqueues a **sweep** job ([`run_sweep`]): once the plan job
//!   and every chunk are done, it walks the ledger and re-derives each live
//!   entry the chunks didn't (its `basis` is null or older than the start),
//!   which is a key deleted while the definition was frozen. A fresh build's
//!   ledger is empty at its start, so it gets no sweep.
//! - **Rebuild of an applying definition** (F7). `Trellis::request_backfill`
//!   and a capture re-install repair a table's `live` readers by rebuilding
//!   them ([`start_rebuilds`]): the call's own
//!   transaction moves each `live -> backfilling` and enqueues one plan job
//!   and one sweep job, two rows per definition, so the call stays fast
//!   whatever the table's size. The definition applies throughout, the plan
//!   re-derives every row the table has, and the sweep re-derives every
//!   entry the plan didn't, so a delete that never reached the target is
//!   repaired too. The status is `backfilling` when the call returns, which
//!   is what a caller polling for `live` waits on. A request while a build
//!   runs enqueues another plan and sweep; the definition is `live` when
//!   every one is done.
//!
//!   An aggregate (`super::group_basis`) has no entries to re-derive, so its
//!   rebuild is its sweep job alone, which recomputes
//!   every group in place in one transaction
//!   (`super::group_basis::rebuild`): its readers see the old values
//!   until that commits, then the new ones, never an empty or partial
//!   target. Over a partitioned source it is one such transaction per leaf
//!   of the recorded tree, then one for the groups that route to no leaf
//!   ([`run_group_rebuild`]), so a reader sees a leaf's groups change as its
//!   transaction commits. A fresh aggregate build never has a sweep, so a
//!   sweep row is always this rebuild; the fresh build is the plan job's
//!   (below). A `JOIN` (`super::join`) is rebuilt the same way, by the sweep
//!   job alone, whichever of its two tables the request names
//!   ([`rebuild_candidates`]): both structures and `T` in one transaction
//!   ([`super::join::build`]).
//! - **Plan** ([`run_plan`]). For an aggregate or a `JOIN` the plan job is
//!   the whole build ([`run_group_rebuild_held`]). Otherwise a drain worker walks the source's primary key
//!   and enqueues `rederive` chunks of `ClientOptions::build_chunk_rows`
//!   rows, [`PLAN_BATCH`] per transaction (Q13). The walk and every chunk
//!   compare the key under the collations the walk started under, which the
//!   rows record (`backfill_chunks.key_collations`), so re-collating
//!   the key mid-build doesn't reorder it under the ranges.
//! - **Work order** ([`work_once`], B6). A drain worker takes segments first,
//!   then a plan job, then one chunk, and a chunk only under the ring's backlog bound ([`chunk_allowed`]). A chunk's
//!   entry lock gives up after [`CHUNK_LOCK_TIMEOUT`], and the chunk is
//!   retried after a short backoff without a charge.
//! - **Live** ([`try_complete`], B7). The flip is strict: the plan job is
//!   done and every chunk is done, checked under the definition's row lock. The same transaction
//!   deletes the definition's done `backfill_chunks` rows, so the table holds
//!   one build's rows at a time, not every build's.
//!
//! A pause freezes the build where it is: claims skip a frozen definition. A
//! drop takes the chunk rows (`on delete cascade`) with the target.
//!
//! # Shapes
//!
//! The build serves a plain 1-1 target (chunks, [`one_to_one`]: the chunk
//! locks its keys' entries on the 1-1 ledger as a page does, rewrites the
//! target rows from one snapshot and stamps the entries), and an aggregate or
//! a `JOIN` (the plan job's one absolute write, [`run_group_rebuild_held`]).
//! A chunk that fails on its data splits until the key fails alone, and the
//! key is quarantined.
//!
//! # Seam-fed sources (F6)
//!
//! A source that is another definition's target (or a `JOIN`'s joined table
//! that is one) is never captured: its writes reach Apply through the
//! target-mutation seam
//! (`super::target_mutations`), whose writer reads the list of applying
//! definitions inside its own apply transaction. A writer that read it before
//! this build's start committed doesn't stage its rows for the definition, and
//! it can commit after a chunk's snapshot, so neither the chunk nor the ring
//! would carry its write. (A captured source has no such writer: a page's
//! definition list is read after its segment's seal fence, which is before
//! the start commit, so every change a page holds without the definition was
//! committed before the start and is in every chunk's snapshot.)
//!
//! So the build of a seam-fed source is fenced, once per build ([`run_plan`];
//! a group-basis aggregate's or a `JOIN`'s one-transaction build runs after
//! the fence, in the plan job). The plan
//! job's first act is to take a fence: the id of a transaction that began
//! after the start commit,
//! recorded on the plan row (`backfill_chunks.fence_xid`). It enqueues no
//! chunk until the oldest running transaction began after the fence
//! ([`fence_settled`]), so every writer that could have missed the build has
//! ended, and every chunk's snapshot sees its rows. While it waits it gives
//! its claim back and tries again ([`FENCE_RETRY`]), and
//! `Trellis::status` names the fence (`DefinitionStatus::build_wait`). Only a
//! seam-fed build waits, and only on transactions older than its fence: a
//! long transaction elsewhere delays seam-fed builds. A 1-1 build's writes to
//! an upstream go through the seam as any writer's do, so a rebuild of a 1-1
//! upstream needs no rebuild of its readers.
//!
//! **Plain 1-1 targets (F8a, [`one_to_one`]).** A 1-1 target takes the same
//! scheduled build:
//! the start, the plan job, the sweep on a rebuild and the strict flip. Its
//! chunk writes the target rows themselves, ordered against pages by the
//! keys' entries on the 1-1 slim ledger.
//!
//! **Field builds (F8b).** `ALTER TRANSFORM ... ADD`/`ALTER` and a column
//! resume rebuild only the fields they change, in the background
//! ([`start_field_build`]). The fields they change take in every field that
//! reads one of them by alias, directly or through others
//! (`defs::alias_readers::AliasReaders`), since its value moves with theirs; a column
//! pause holds such a reader out of Apply with the field it reads, so the
//! resume rebuilds both. Each chunk takes the readers in again from the
//! definition as it stands when the chunk is planned, so a reader an edit
//! added after the build was registered (of a field awaiting its capture,
//! which this build releases) is written too. The call's own transaction (`apply` only
//! registers) moves the definition `live -> backfilling` with `build =
//! 'rederive'`, or leaves it `backfilling` when a build is already running,
//! and enqueues a plan job whose `backfill_chunks.fields` names the fields.
//! The field applies from that commit, as a whole build's definition does
//! (B1): no `column_status` row holds it out of Apply, so every page after
//! the commit writes it from its change's image, and nothing needs a
//! catch-up. The plan job enqueues chunks with the same scope, and the same
//! strict flip moves the definition `live` once they are all done, so the
//! definition reads `backfilling` until every field it was building is.
//!
//! - **The chunk** of a plain 1-1 target ([`one_to_one::run_field_chunk`])
//!   takes the keys' entry lock and then rewrites just the fields of their
//!   existing target rows from one snapshot, leaving the entries alone. An
//!   aggregate's fields are never held out of Apply, so neither call builds
//!   one.
//! - **The capture gate.** A field that reads a source column the source's
//!   capture may not image yet can't apply before capture images it:
//!   a page would meet rows staged without the column and quarantine their
//!   keys. `ALTER` holds such a field out of Apply with a `column_status`
//!   row marked `awaiting_capture`, and the plan job starts by waiting
//!   ([`field_build_ready`]): until the installed capture images every
//!   column the definition reads and the source's capture gate is clear, it
//!   gives its claim back for a later try. Then one transaction releases the
//!   field, and the walk begins after it, so every chunk's snapshot is
//!   taken after the field applies.

use std::time::{Duration, Instant};

use tokio_postgres::types::Type;
use tokio_postgres::{GenericClient, IsolationLevel, Transaction};

use crate::defs::chunk_queue::{self, ChunkQueueError, ChunkWork, ClaimFence, ClaimedChunk};
use crate::defs::model::{Definition, TransformStatus};
use crate::defs::{catalog, ddl};
use crate::metrics::{self, BuildStatement};
use crate::pool::{Pool, quote_ident};

use super::apply::ApplyError;
use super::quarantine;

pub mod one_to_one;

/// How long a plan job waits before it looks at its fence again (F6).
const FENCE_RETRY: Duration = Duration::from_millis(500);

/// What one 1-1 sweep batch did ([`one_to_one::sweep_batch`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepOutcome {
    /// The ledger entries the batch read past, live or not.
    pub scanned: i64,
    /// The live entries among them the build hadn't re-derived, which the
    /// batch re-derived.
    pub rederived: usize,
    /// The last entry key it read past: the next batch's cursor. `None`
    /// when it read none.
    pub next: Option<String>,
    /// It read fewer than it was allowed: the sweep is done.
    pub finished: bool,
}

/// How long a chunk waits for its entry lock before giving up.
/// A drain page waits up to `locks::LOCK_TIMEOUT` for the same locks, so a
/// page never queues for long behind a chunk, and a chunk never holds up a
/// page that got there first. The caller retries the chunk later.
pub const CHUNK_LOCK_TIMEOUT: Duration = Duration::from_secs(1);

// ---------------------------------------------------------------------
// The scheduled build
// ---------------------------------------------------------------------

/// Source rows per chunk unless the client says otherwise.
pub const DEFAULT_CHUNK_ROWS: i64 = 10_000;

/// How many chunk boundaries the plan job commits per transaction,
/// so the first chunks run while the walk goes on.
pub const PLAN_BATCH: usize = 100;

/// The `application_name` a build chunk's transaction runs under, so
/// `pg_stat_activity` tells its statements from a drain page's, which share
/// their text (`one_to_one_ledger::lock_entries`).
pub const CHUNK_APPLICATION_NAME: &str = "trellis build chunk";

/// The `transform_definitions.build` word of a running Re-derive build.
pub(crate) const BUILD_REDERIVE: &str = "rederive";

/// Whether the partition correction engine
/// (`super::partition_correction`) can re-derive the keys of `definition`
/// when a node of its partitioned source is added: an aggregate on the group
/// basis, which its runner corrects a reader of the source with; a `JOIN` is
/// not.
pub(crate) fn corrected_on_partitioned_source(definition: &Definition) -> bool {
    super::group_basis::route(&definition.def, &definition.source_columns).is_some()
}

/// Whether `definition` is on the group basis (`super::group_basis`),
/// a `JOIN` (`super::join`) included. A plain aggregate's rebuild is its sweep job alone
/// ([`start_rebuilds`], [`run_group_rebuild`]), and its fresh build or resume
/// is its plan job's one absolute write of the same kind ([`run_plan`]),
/// with no chunk, as is a `JOIN`'s ([`super::join::build`]).
fn takes_group_basis(definition: &Definition) -> bool {
    super::group_basis::route(&definition.def, &definition.source_columns).is_some()
        || super::join::applies(&definition.def)
}

/// Starts a Re-derive build for every `waiting_to_backfill` definition in
/// `ready`. `ready` is what the staging worker's capture pass found
/// dispatchable (`capture::reconcile`). One whose source (or a `JOIN`'s
/// joined table) has a capture gate a pending change still holds
/// ([`crate::capture::gate`]) would meet rows imaged without a column it
/// reads, so it is left `waiting_to_backfill` for a later pass, as is one
/// whose start timed out waiting for the column-pause lock or failed
/// transiently. A start that failed for good, a failure that would
/// reproduce on every pass such as a dropped ledger table, pauses that
/// definition and what is downstream of its target with the error as its
/// `capture_failure` ([`super::halt::halt_failed_start`]), so it neither
/// fails this call nor stops the other definitions' starts. Only a lost
/// connection returns an error from a start.
///
/// A start is one transaction ([`start`]).
pub async fn start_ready_builds(
    client: &mut tokio_postgres::Client,
    pool: &Pool,
    ready: &[i64],
) -> Result<(), ApplyError> {
    for &id in ready {
        let Some(definition) = catalog::definition_by_id(pool, id).await? else {
            continue;
        };
        if definition.status != TransformStatus::WaitingToBackfill {
            continue;
        }
        // A JOIN's plan job builds both structures
        // (`super::join`), so both of its tables' gates hold it.
        // A 1-1 JOIN's build reads `b` too (ADR-0021).
        let join = match &definition.def.key_space {
            crate::defs::ast::KeySpace::Join(_) | crate::defs::ast::KeySpace::JoinOneToOne(_) => {
                catalog::join_table_of(&*client, id).await?
            }
            _ => None,
        };
        let mut gated = crate::capture::gate::holds(&*client, &definition.source_table).await?;
        if let Some(joined) = &join {
            gated = gated || crate::capture::gate::holds(&*client, joined).await?;
        }
        if gated {
            crate::instance_log::debug!(
                definition_id = id,
                table = %definition.source_table,
                "re-derive build held by its source's capture gate"
            );
            continue;
        }
        if awaits_capture(&*client, &definition).await? {
            crate::instance_log::debug!(
                definition_id = id,
                table = %definition.source_table,
                "re-derive build held until its source's capture images an edited field's column"
            );
            continue;
        }
        match start(client, pool.schema(), &definition).await {
            Ok(Some(dependents)) => {
                // The build has started; readers elsewhere that the release
                // left paused are resumed after it.
                resume_released_dependents(pool, &definition.def.target, dependents).await;
            }
            Ok(None) => {}
            // A start waits for the column-pause lock. One that
            // times out wrote nothing; it stays `waiting_to_backfill` for
            // the next pass, as one held by its capture gate does. The rest
            // of the pass goes on.
            Err(ApplyError::ColumnPauseLockTimeout(err)) => {
                crate::instance_log::warn!(
                    definition_id = id,
                    error = %err,
                    "re-derive build start waited out the column-pause lock; retrying next pass"
                );
            }
            // A lost connection fails the pass, which reconnects.
            Err(err) if client.is_closed() => return Err(err),
            // A transient failure (a deadlock, a cancelled statement) wrote
            // nothing either; the next pass starts it again.
            Err(err) if super::quarantine::is_transient_error(&err) => {
                crate::instance_log::warn!(
                    definition_id = id,
                    error = %err,
                    "re-derive build start failed transiently; retrying next pass"
                );
            }
            // Anything else reproduces on every pass (a dropped ledger
            // table), and returning it would stop the pass before it
            // started the other definitions' builds. Pause this definition, and what is
            // downstream of its target, with the error as its
            // `capture_failure`, as a build that keeps failing does
            // (`fail_chunk`); a resume rebuilds it, or refuses with the
            // repair.
            Err(err) => {
                match super::halt::halt_failed_start(
                    pool,
                    &definition.target_table,
                    &err.to_string(),
                )
                .await
                {
                    Ok(paused) => crate::instance_log::warn!(
                        definition_id = id,
                        paused = ?paused,
                        error = %err,
                        "re-derive build start failed; paused the definitions it reaches \
                         (resume them once the cause is fixed)"
                    ),
                    Err(halt_err) => crate::instance_log::warn!(
                        definition_id = id,
                        error = %err,
                        halt_error = %halt_err,
                        "re-derive build start failed, and pausing on it failed; retrying next pass"
                    ),
                }
            }
        }
    }
    // The gates that held a build and are clear now have served.
    crate::capture::gate::release_passed(&*client)
        .await
        .map_err(capture_error)?;
    Ok(())
}

/// A capture error a build step met, as the [`ApplyError`] it fails with.
fn capture_error(err: crate::capture::CaptureError) -> ApplyError {
    use crate::capture::CaptureError;
    match err {
        CaptureError::Db(err) => ApplyError::from(err),
        CaptureError::Catalog(err) => ApplyError::from(err),
        CaptureError::Intake(err) => ApplyError::from(err),
        other => ApplyError::from(crate::intake::IntakeError::InvalidTableName(
            other.to_string(),
        )),
    }
}

/// Whether `definition` has a field `ALTER TRANSFORM` holds out of Apply
/// until its source's capture images the field's column
/// (`column_status.awaiting_capture`, see the module doc's "Field builds")
/// and the installed capture doesn't image every column the definition
/// reads yet. A resumed definition can: its resume discards the field
/// build that would have released the field, and its rebuild's start
/// releases it instead ([`start`]), once this is false and the capture gate
/// is clear.
async fn awaits_capture(
    client: &impl GenericClient,
    definition: &Definition,
) -> Result<bool, ApplyError> {
    let awaiting: bool = client
        .query_one(
            "select exists (select 1 from column_status \
             where transform_table = $1 and awaiting_capture)",
            &[&definition.def.target],
        )
        .await?
        .get(0);
    if !awaiting {
        return Ok(false);
    }
    let read: std::collections::BTreeSet<String> =
        crate::defs::oracle::referenced_source_columns(&definition.def)
            .into_iter()
            .collect();
    Ok(!capture_images(client, &definition.source_table, &read).await?)
}

/// Releases `target`'s `awaiting_capture` pauses in `txn` (see the module
/// doc's "Field builds"), as only a build's start may: those the edit that
/// made them still owns are deleted, and one an operator pause, a fuse trip
/// or an upstream pause's cascade took over only stops
/// waiting, and stays paused for its own resume. With `fields`, only those.
///
/// A row it deletes is a pause that ends, so it un-cascades from it as a
/// resume does ([`quarantine::uncascade`]): the field may have
/// passed a pause on to its readers (a cascade through it, or one a sibling
/// path gave it), and the resume that took its own edge away left the row,
/// since it awaited its capture. The readers it releases in `target` come
/// back from its caller's build as the fields it covers (an edit's build
/// lists every sibling reading an edited field by alias, and a Re-derive
/// build rebuilds all). Those in other definitions are returned: they need
/// a field build of their own, which the caller starts once it has
/// committed, with [`resume_released_dependents`].
///
/// The delete runs again while it finds rows: one that read the first
/// pass's rows by alias had an edge into it then, and none now.
async fn release_awaiting_capture(
    txn: &impl GenericClient,
    target: &str,
    fields: Option<&[String]>,
) -> Result<Vec<(String, String)>, tokio_postgres::Error> {
    let fields: Option<Vec<String>> = fields.map(<[String]>::to_vec);
    let mut dependents = Vec::new();
    loop {
        let deleted: Vec<String> = txn
            .query(
                "delete from column_status s \
                 where s.transform_table = $1 and s.awaiting_capture \
                   and ($2::text[] is null or s.column_name = any($2)) \
                   and not s.local_fuse \
                   and not exists ( \
                       select 1 from column_pause_cascades c \
                       where c.downstream_transform = s.transform_table \
                         and c.downstream_column = s.column_name) \
                 returning s.column_name",
                &[&target, &fields],
            )
            .await?
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        if deleted.is_empty() {
            break;
        }
        dependents.extend(
            quarantine::uncascade(txn, target, deleted)
                .await?
                .dependents,
        );
    }
    txn.execute(
        "update column_status set awaiting_capture = false \
         where transform_table = $1 and awaiting_capture \
           and ($2::text[] is null or column_name = any($2))",
        &[&target, &fields],
    )
    .await?;
    Ok(dependents)
}

/// Resumes the readers in other definitions that [`release_awaiting_capture`]
/// left with no reason to stay paused, as a resume of the
/// column would have: each is released and rebuilt by a field build of its
/// definition. After the release's commit, which released the column-pause
/// lock the resume takes.
///
/// A failure is logged, not returned. The release has committed, so a
/// retry of the caller finds nothing to release and wouldn't resume them
/// either, and failing the caller would charge its own build (or cost the
/// capture pass its other definitions) for another definition's trouble. The
/// readers stay paused, with their edge gone, as after a resume that fails
/// partway, and their own `RESUME` recovers them.
pub(crate) async fn resume_released_dependents(
    pool: &Pool,
    target: &str,
    dependents: Vec<(String, String)>,
) {
    if dependents.is_empty() {
        return;
    }
    match quarantine::resume_pairs(pool, target, dependents.clone().into()).await {
        Ok(resumed) => crate::instance_log::info!(
            target,
            ?resumed,
            "readers of a field released from its pause resumed"
        ),
        Err(err) => crate::instance_log::warn!(
            target,
            ?dependents,
            error = %err,
            "readers of a field released from its pause stay paused; RESUME them"
        ),
    }
}

/// Starts `definition`'s Re-derive build in one transaction:
/// under its row lock, moves it `waiting_to_backfill -> backfilling` with
/// `build = 'rederive'` and enqueues its plan job. From that commit it
/// applies (`defs::model::APPLYING_SQL`): every page whose definition list
/// is read after it folds the source's changes into the ledger, and a
/// change committed before it is visible to every chunk's snapshot.
///
/// A definition whose ledger isn't empty (a resumed one, F3) also gets a
/// sweep job ([`run_sweep`]). Both rows carry the start's transaction id,
/// which the sweep's filter reads: every chunk's snapshot is taken after
/// the start commits, so its `xmax` is past that id.
///
/// **The start's segment.** Pages drain batches out of order, so a
/// page that ran before the start can drain a key's later change while an
/// older one waits in an earlier batch, which a page drains after the
/// start. The later change never reaches the definition, and the older
/// one's image is stale: applied, it would count a key that a chunk then
/// can't find (it was deleted) back in for good. So the start also stores
/// `build_seg`, the segment active at its commit, read under a share lock
/// on `segment_pointer` that keeps seal phase 1 (`super::seal`) from moving
/// it until the start commits. That segment's fence is taken after the
/// start commits, so every change committed before the start is in a batch
/// at or below `build_seg`, and a page that drains one re-derives its keys
/// for the definition rather than applying their changes
/// ([`ledger::page_may_predate_build`]). Every batch above it holds only
/// changes committed after the start, drained by pages that read the
/// definition list after it, so none is dropped.
///
/// Returns `None`, writing nothing, when the definition left
/// `waiting_to_backfill` meanwhile, and otherwise the readers in other
/// definitions its release of `awaiting_capture` pauses left paused
/// ([`release_awaiting_capture`]), for the caller to resume once this has
/// committed.
async fn start(
    client: &mut tokio_postgres::Client,
    schema: &str,
    definition: &Definition,
) -> Result<Option<Vec<(String, String)>>, ApplyError> {
    let txn = client.transaction().await?;
    // The column-pause lock, exclusive: this start releases the
    // definition's `awaiting_capture` pauses ([`release_awaiting_capture`]),
    // which a pause, a resume and a define read and write. Before the
    // definition row, where every taker of the lock orders it
    // (`crate::locks::lock_column_pauses`); this start has no fence. A wait
    // that times out fails the start, and the capture pass takes it again.
    crate::locks::lock_column_pauses(
        &txn,
        schema,
        crate::locks::ColumnPauseLock::Exclusive,
        crate::locks::ColumnPauseOp::Capture,
    )
    .await?;
    let status: Option<String> = txn
        .query_opt(
            "select status from transform_definitions where id = $1 for update",
            &[&definition.id],
        )
        .await?
        .map(|row| row.get(0));
    if status.as_deref() != Some(TransformStatus::WaitingToBackfill.as_str()) {
        txn.rollback().await?;
        return Ok(None);
    }
    // A 1-1 JOIN's build (ADR-0021, #1400) doesn't exist yet. Its plan job
    // would otherwise walk `a` as a plain 1-1 source and write the target
    // from `a` alone. The caller pauses the definition with this as its
    // `capture_failure`.
    if crate::defs::join::is_one_to_one(&definition.def) {
        txn.rollback().await?;
        return Err(ApplyError::GroupBasisUnsupported {
            target: definition.target_table.clone(),
            what: "a build of a 1-1 JOIN (its build path is not built yet)",
        });
    }
    let ledger = ddl::qualified_target_table_ident(&crate::defs::ledger::ledger_table_name(
        &definition.target_table,
    ));
    // A group-basis definition has no ledger, and nothing to sweep.
    let empty: bool = takes_group_basis(definition)
        || txn
            .query_one(&format!("select not exists (select 1 from {ledger})"), &[])
            .await?
            .get(0);
    txn.execute(
        "update transform_definitions set status = $2, build = $3 where id = $1",
        &[
            &definition.id,
            &TransformStatus::Backfilling.as_str(),
            &BUILD_REDERIVE,
        ],
    )
    .await?;
    // The caller found any field awaiting capture ready, so it
    // applies from this commit, and the build's chunks write it.
    let dependents = release_awaiting_capture(&txn, &definition.def.target, None).await?;
    txn.execute(
        "insert into backfill_chunks (definition_id, kind, fuse_rearmed_at, start_xid) \
         select id, $2, fuse_rearmed_at, pg_current_xact_id() \
         from transform_definitions where id = $1",
        &[&definition.id, &chunk_queue::KIND_PLAN],
    )
    .await?;
    if !empty {
        txn.execute(
            "insert into backfill_chunks (definition_id, kind, fuse_rearmed_at, start_xid) \
             select id, $2, fuse_rearmed_at, pg_current_xact_id() \
             from transform_definitions where id = $1",
            &[&definition.id, &chunk_queue::KIND_SWEEP],
        )
        .await?;
    }
    // The start's segment, last, so the share lock that keeps the
    // active segment from sealing until this commits is held only for the
    // commit. See this function's doc.
    txn.execute(
        "update transform_definitions set build_seg = p.active_seq \
         from (select active_seq from segment_pointer for share) p where id = $1",
        &[&definition.id],
    )
    .await?;
    txn.commit().await?;
    crate::instance_log::info!(
        definition_id = definition.id,
        target = %definition.target_table,
        from = %TransformStatus::WaitingToBackfill.as_str(),
        to = %TransformStatus::Backfilling.as_str(),
        rebuild = !empty,
        "transform status transition: re-derive build started"
    );
    Ok(Some(dependents))
}

/// The definitions reading one of `tables` as their source, or a `JOIN`
/// reading one as its joined table, that a rebuild could take:
/// applying ones (`live`, and `backfilling` under a
/// Re-derive build), in id order. A candidate only: [`rebuildable`] decides, under the row locks.
pub(crate) async fn rebuild_candidates(
    client: &impl GenericClient,
    tables: &[String],
) -> Result<Vec<i64>, tokio_postgres::Error> {
    Ok(client
        .query(
            "select id from transform_definitions \
             where (source_table = any($1) or join_table = any($1)) \
               and (status = $2 or (status = $3 and build = $4)) \
             order by id",
            &[
                &tables,
                &TransformStatus::Live.as_str(),
                &TransformStatus::Backfilling.as_str(),
                &BUILD_REDERIVE,
            ],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect())
}

/// The definitions among `ids` that a rebuild takes: the ones that
/// still apply (`live`, or `backfilling` under a Re-derive build). The caller
/// holds their rows `for update` (`intake::catch_up`), so what this reads
/// holds until it commits.
pub(crate) async fn rebuildable(
    client: &impl GenericClient,
    ids: &[i64],
) -> Result<Vec<i64>, tokio_postgres::Error> {
    Ok(client
        .query(
            "select id from transform_definitions \
             where id = any($1) and (status = $2 or (status = $3 and build = $4)) \
             order by id",
            &[
                &ids,
                &TransformStatus::Live.as_str(),
                &TransformStatus::Backfilling.as_str(),
                &BUILD_REDERIVE,
            ],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect())
}

/// Rebuilds the definitions `ids` ([`rebuildable`]) in `txn`, the caller's
/// transaction: moves a `live` one to `backfilling` with `build =
/// 'rederive'` (one already building stays as it is), and enqueues a plan
/// job and a sweep job for each, both whole-definition (`fields` null), as
/// [`start`] does for a resumed definition: a rebuild re-derives the rows
/// the table has and sweeps the entries it no longer backs. That is one row
/// of each per definition, so the call stays fast whatever the table's size.
/// A group-basis definition (one with a hidden group table,
/// `super::group_basis`, or a `JOIN`'s hidden tables) gets the sweep job
/// alone, which recomputes its groups in place ([`run_group_rebuild`]).
///
/// The definition applies throughout (B1), as it did before the call. The
/// caller holds each definition's row `for update`, and calls
/// [`stamp_rebuild_seg`] as the last statement before it commits.
///
/// From the commit the definition reads `backfilling`, so a caller that
/// waits for `live` waits for the rebuild.
pub(crate) async fn start_rebuilds(
    txn: &impl GenericClient,
    ids: &[i64],
) -> Result<(), tokio_postgres::Error> {
    if ids.is_empty() {
        return Ok(());
    }
    let moved: Vec<i64> = txn
        .query(
            "update transform_definitions set status = $2, build = $3 \
             where id = any($1) and status = $4 returning id",
            &[
                &ids,
                &TransformStatus::Backfilling.as_str(),
                &BUILD_REDERIVE,
                &TransformStatus::Live.as_str(),
            ],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    for kind in [chunk_queue::KIND_PLAN, chunk_queue::KIND_SWEEP] {
        txn.execute(
            &format!(
                "insert into backfill_chunks (definition_id, kind, fuse_rearmed_at, start_xid) \
                 select id, $2, fuse_rearmed_at, pg_current_xact_id() \
                 from transform_definitions d where id = any($1) \
                   and ($2 = '{sweep}' or not exists ( \
                       select 1 from pg_catalog.pg_class c \
                       where c.relname in ({hidden}, {join_hidden}) \
                         and c.relnamespace = (select relnamespace from pg_catalog.pg_class \
                                               where oid = 'transform_definitions'::regclass))) \
                 order by id",
                sweep = chunk_queue::KIND_SWEEP,
                hidden = super::group_basis::hidden_table_name_sql("d.id"),
                join_hidden = crate::defs::join::HiddenTable::TruncateLock.name_sql("d.id"),
            ),
            &[&ids, &kind],
        )
        .await?;
    }
    if !moved.is_empty() {
        crate::instance_log::info!(
            ids = ?moved,
            from = %TransformStatus::Live.as_str(),
            to = %TransformStatus::Backfilling.as_str(),
            "transform status transition: re-derive rebuild started"
        );
    }
    Ok(())
}

/// Records the segment active at the commit of the rebuild
/// [`start_rebuilds`] enqueued for `ids` as their `build_seg`, as
/// [`start`] does for a build it starts. Last, so the share lock that keeps
/// the active segment from sealing until the transaction commits is held
/// only for the commit.
pub(crate) async fn stamp_rebuild_seg(
    txn: &impl GenericClient,
    ids: &[i64],
) -> Result<(), tokio_postgres::Error> {
    if ids.is_empty() {
        return Ok(());
    }
    txn.execute(
        "update transform_definitions set build_seg = p.active_seq \
         from (select active_seq from segment_pointer for share) p where id = any($1)",
        &[&ids],
    )
    .await?;
    Ok(())
}

/// Whether a definition read as `status` with `build` can take a field
/// build ([`start_field_build`]): one that applies. That is a
/// `live` one, or a `backfilling` one under a Re-derive build, which the field
/// build joins. A frozen definition doesn't apply.
pub(crate) fn takes_field_build(status: TransformStatus, build: Option<&str>) -> bool {
    status == TransformStatus::Live
        || (status == TransformStatus::Backfilling && build == Some(BUILD_REDERIVE))
}

/// Starts a field build of `fields` on definition `id` in `txn`, the
/// caller's transaction (see the module doc's "Field builds"): moves a
/// `live` definition to `backfilling` with `build = 'rederive'` (one already
/// building, stays as it is) and enqueues a plan job
/// scoped to `fields`. The caller holds the definition's row `for update`
/// and has checked
/// [`takes_field_build`] against what it read under that lock (`status`),
/// and commits. From that commit the definition applies as before, the
/// fields included unless a `column_status` row holds one out, and it reads
/// `backfilling` until the build's last chunk is done ([`try_complete`]).
pub(crate) async fn start_field_build(
    txn: &impl GenericClient,
    id: i64,
    status: TransformStatus,
    fields: &[String],
) -> Result<(), tokio_postgres::Error> {
    if status == TransformStatus::Live {
        txn.execute(
            "update transform_definitions set status = $2, build = $3 where id = $1",
            &[&id, &TransformStatus::Backfilling.as_str(), &BUILD_REDERIVE],
        )
        .await?;
    }
    txn.execute(
        "insert into backfill_chunks (definition_id, kind, fuse_rearmed_at, start_xid, fields) \
         select id, $2, fuse_rearmed_at, pg_current_xact_id(), $3 \
         from transform_definitions where id = $1",
        &[&id, &chunk_queue::KIND_PLAN, &fields],
    )
    .await?;
    let to = if status == TransformStatus::Live {
        TransformStatus::Backfilling
    } else {
        status
    };
    crate::instance_log::info!(
        definition_id = id,
        from = %status.as_str(),
        to = %to.as_str(),
        ?fields,
        "transform status transition: field build registered"
    );
    Ok(())
}

/// What a field build's plan job found at its start ([`field_build_ready`]).
enum FieldStart {
    /// Every field applies: the walk may begin.
    Go,
    /// A field still waits for its source's capture to image a column it
    /// reads; the job gives its claim back for a later try.
    Wait,
    /// The job's claim no longer holds.
    Superseded,
}

/// How long a field build's plan job waits before it looks at its source's
/// capture again ([`field_build_ready`]).
const FIELD_CAPTURE_RETRY: Duration = Duration::from_millis(500);

/// A field build's start (see the module doc's "Field builds"): releases
/// the `awaiting_capture` pauses `ALTER TRANSFORM` put on `fields` once the
/// definition's source is ready for them, in one transaction fenced by the
/// plan job's claim. Ready means what a build start requires of a
/// registration (`start_ready_builds`): the installed capture images every
/// source column the definition reads ([`capture_images`]),
/// and no change at or below the source's capture gate is still pending
/// ([`crate::capture::gate::holds`]), so no row staged without a column is left for a
/// page to meet.
///
/// A pause the edit no longer owns (an operator pause or a fuse trip
/// upgraded it to `local_fuse`, or an upstream pause cascaded onto it)
/// only stops waiting: the field stays out of Apply and out of
/// the build's chunks until its own resume.
///
/// The release bumps the version fence ([`bump_version_fence`]) as its
/// transaction's first lock, so its wait for the pages in flight holds
/// nothing a page could be waiting on. A look under the claim
/// alone comes first, so a call that has nothing to release, or whose
/// capture isn't ready, doesn't bump the fence; the release then looks again
/// under its locks.
async fn field_build_ready(
    pool: &Pool,
    definition: &Definition,
    fields: &[String],
    fence: &ClaimFence<'_>,
) -> Result<FieldStart, ChunkQueueError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    if !fence.hold(&*txn).await? {
        txn.rollback().await?;
        return Ok(FieldStart::Superseded);
    }
    let release = capture_release(&txn, definition, fields, false).await?;
    txn.rollback().await?;
    match release {
        CaptureRelease::Nothing => return Ok(FieldStart::Go),
        CaptureRelease::NotReady => return Ok(FieldStart::Wait),
        CaptureRelease::Ready => {}
    }

    let txn = client.transaction().await?;
    // A page that read the paused fields before this commit must not apply
    // after it. The claim's idle timeout goes first: from the bump on, a
    // stalled worker holds up every page of the source, not just its chunk.
    fence.arm(&*txn).await?;
    bump_version_fence(&*txn, &definition.source_table).await?;
    // The column-pause lock, exclusive, after the fence wait and
    // before the claim and the rows: the release below writes pause state
    // (`release_awaiting_capture`). See `crate::locks::lock_column_pauses`.
    // A wait that times out fails this job's attempt, which retries.
    crate::locks::lock_column_pauses(
        &*txn,
        pool.schema(),
        crate::locks::ColumnPauseLock::Exclusive,
        crate::locks::ColumnPauseOp::Capture,
    )
    .await?;
    if !fence.hold(&*txn).await? {
        txn.rollback().await?;
        return Ok(FieldStart::Superseded);
    }
    match capture_release(&txn, definition, fields, true).await? {
        CaptureRelease::Nothing => {
            txn.rollback().await?;
            return Ok(FieldStart::Go);
        }
        CaptureRelease::NotReady => {
            txn.rollback().await?;
            return Ok(FieldStart::Wait);
        }
        CaptureRelease::Ready => {}
    }
    let dependents = release_awaiting_capture(&*txn, &definition.def.target, Some(fields)).await?;
    txn.commit().await?;
    resume_released_dependents(pool, &definition.def.target, dependents).await;
    crate::instance_log::info!(
        definition_id = definition.id,
        ?fields,
        "field build's capture is ready; its fields apply from here"
    );
    Ok(FieldStart::Go)
}

/// What [`capture_release`] found for a field build's `fields`.
enum CaptureRelease {
    /// None of them awaits its capture.
    Nothing,
    /// One does, and the source's capture isn't ready for it yet.
    NotReady,
    /// One does, and the capture is ready: release them.
    Ready,
}

/// Whether [`field_build_ready`] has `fields`' `awaiting_capture` pauses to
/// release, reading them `for update` when `lock`.
async fn capture_release(
    txn: &Transaction<'_>,
    definition: &Definition,
    fields: &[String],
    lock: bool,
) -> Result<CaptureRelease, ChunkQueueError> {
    let lock = if lock { " for update" } else { "" };
    let awaiting = txn
        .query(
            &format!(
                "select column_name from column_status \
                 where transform_table = $1 and column_name = any($2) and awaiting_capture{lock}"
            ),
            &[&definition.def.target, &fields],
        )
        .await?;
    if awaiting.is_empty() {
        return Ok(CaptureRelease::Nothing);
    }
    let read: std::collections::BTreeSet<String> =
        crate::defs::oracle::referenced_source_columns(&definition.def)
            .into_iter()
            .collect();
    let images = capture_images(txn, &definition.source_table, &read)
        .await
        .map_err(build_error)?;
    if !images
        || crate::capture::gate::holds(txn, &definition.source_table)
            .await
            .map_err(build_error)?
    {
        return Ok(CaptureRelease::NotReady);
    }
    Ok(CaptureRelease::Ready)
}

/// Bumps `source_table`'s version fence (`source_table_versions`) in `txn`,
/// as `ALTER TRANSFORM` does, for a commit that releases a 1-1 field's
/// `column_status` pause into a field build: a column resume, or the field
/// build's capture release. A column pause bumps it too, for the
/// opposite direction (`quarantine::bump_pause_fence`).
///
/// A page reads the paused columns in Phase 2 and leaves them out of its
/// writes. One that read them before the release must not apply after it:
/// it would write a key's other columns from its change and leave the
/// field as the page of an older change wrote it, computed after the
/// release and drained first, out of order. I2 passes both (the older
/// change's `lsn` is below the newer one's), the build's chunk for the key
/// may have run already, and no catch-up follows. The bump waits for every
/// page holding the fence `for share` (Phase 3 holds it to its commit), and
/// a page that reaches the fence after the commit misses it and computes
/// again, with the field.
///
/// Callers bump it as their transaction's first lock, as `ALTER TRANSFORM`
/// does. A page takes the fence before any other lock, so a
/// bump that holds nothing while it waits can't close a cycle; one that
/// holds the definition row can, through a page queued on a third
/// transaction that wants that row.
pub(crate) async fn bump_version_fence(
    txn: &impl GenericClient,
    source_table: &str,
) -> Result<(), tokio_postgres::Error> {
    txn.execute(
        "insert into source_table_versions (source_table, version) values ($1, 1) \
         on conflict (source_table) \
         do update set version = source_table_versions.version + 1",
        &[&source_table],
    )
    .await?;
    Ok(())
}

/// Whether every row staged for `table` from now on carries `columns`
/// ([`field_build_ready`]): `table` is another definition's target, fed by
/// the target-mutation seam rather than captured; or nothing is installed on
/// it, so no row lacking a column can exist and the install, from a catalog
/// read after the edit, images them all; or its installed functions are
/// current and image every one of `columns`.
async fn capture_images(
    client: &impl GenericClient,
    table: &str,
    columns: &std::collections::BTreeSet<String>,
) -> Result<bool, ApplyError> {
    use crate::capture::install::Installed;

    if catalog::is_definition_target(client, table).await? {
        return Ok(true);
    }
    let schema: String = client
        .query_one("select pg_catalog.current_schema()::text", &[])
        .await?
        .get(0);
    let installed = crate::capture::install::installed(client, &schema, table)
        .await
        .map_err(capture_error)?;
    Ok(match installed {
        Installed::Absent => true,
        Installed::Partial { .. } => false,
        Installed::Complete { spec, current } => {
            current && columns.iter().all(|c| spec.columns().contains(c))
        }
    })
}

/// Gives `chunk`'s claim back without a charge, to be claimed again after
/// `delay` (a field build's plan job waiting on capture). A claim already
/// reclaimed from `claimed_by` is left to its new holder.
pub(crate) async fn defer_claim(
    pool: &Pool,
    chunk: &ClaimedChunk,
    claimed_by: &str,
    delay: Duration,
) -> Result<(), ChunkQueueError> {
    let client = pool.get().await?;
    client
        .execute(
            "update backfill_chunks \
             set claimed_by = null, claimed_at = null, \
                 next_attempt_at = now() + make_interval(secs => $3) \
             where id = $1 and claimed_by = $2",
            &[&chunk.id, &claimed_by, &delay.as_secs_f64()],
        )
        .await?;
    Ok(())
}

/// How a drain worker runs the Re-derive build's work ([`work_once`]).
#[derive(Debug, Clone, Copy)]
pub struct WorkerOptions {
    /// Source rows per chunk the plan job enqueues
    /// (`ClientOptions::build_chunk_rows`).
    pub chunk_rows: i64,
    /// `ClientOptions::drain_batch_cap`: a chunk is claimed only while the
    /// sealed, undrained rows are under twice this.
    pub drain_batch_cap: usize,
    /// How often a claimed plan job or chunk refreshes its claim.
    pub heartbeat_interval: Duration,
    /// The fleet's reclaim TTL ([`ClaimFence`]'s idle bound).
    pub reclaim_ttl: Duration,
}

/// What one [`work_once`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// No Re-derive build is running, or none had work for this worker.
    Idle,
    /// The ring's backlog held the build's chunks back.
    Backpressure,
    /// Ran (some of) a plan job.
    Planned,
    /// Ran one chunk, or a sweep job's batches.
    Chunk,
    /// Moved a finished build's definition to `live`.
    Completed,
}

impl Step {
    /// Whether the step did work, so the caller loops straight back.
    pub fn progressed(self) -> bool {
        !matches!(self, Step::Idle | Step::Backpressure)
    }
}

/// One step of the Re-derive builds' work for a drain worker, which runs it
/// after its segments: a plan job, else one chunk, a sweep batch
/// run or a partition correction's turn (`super::partition_correction`) if the
/// ring's backlog allows ([`chunk_allowed`]). A building definition with
/// nothing left to do is moved to `live` ([`try_complete`]).
///
/// A definition that is frozen gets nothing: its work isn't claimed.
pub async fn work_once(
    pool: &Pool,
    claimed_by: &str,
    options: &WorkerOptions,
) -> Result<Step, ChunkQueueError> {
    let building = building(pool).await?;
    if building.is_empty() {
        return Ok(Step::Idle);
    }

    let claimed = {
        let client = pool.get().await?;
        chunk_queue::claim_chunks_of(&**client, claimed_by, 1, &[chunk_queue::KIND_PLAN]).await?
    };
    if let Some(chunk) = claimed.into_iter().next() {
        run_claimed(pool, &chunk, claimed_by, options).await;
        return Ok(Step::Planned);
    }

    let allowed = {
        let client = pool.get().await?;
        chunk_allowed(&**client, options.drain_batch_cap).await?
    };
    if allowed {
        let claimed = {
            let client = pool.get().await?;
            chunk_queue::claim_chunks_of(
                &**client,
                claimed_by,
                1,
                &[
                    chunk_queue::KIND_REDERIVE,
                    chunk_queue::KIND_SWEEP,
                    chunk_queue::KIND_CORRECTION,
                ],
            )
            .await?
        };
        if let Some(chunk) = claimed.into_iter().next() {
            run_claimed(pool, &chunk, claimed_by, options).await;
            return Ok(Step::Chunk);
        }
    }

    // Nothing to claim: a build whose last chunk committed may be
    // done. Each of those checks after its own commit; this catches a worker
    // that died between the two.
    let mut completed = false;
    for id in &building {
        completed |= try_complete(pool, *id).await?;
    }
    Ok(if completed {
        Step::Completed
    } else if allowed {
        Step::Idle
    } else {
        Step::Backpressure
    })
}

/// Every definition a Re-derive build is running for that isn't frozen, in
/// id order: each `backfilling` one under a Re-derive build.
async fn building(pool: &Pool) -> Result<Vec<i64>, ChunkQueueError> {
    let client = pool.get().await?;
    let params = building_params();
    Ok(client
        .query(
            &format!(
                "select d.id from transform_definitions d where {} order by d.id",
                building_predicate(),
            ),
            &[&params.0, &params.1],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect())
}

/// The `where` of [`building`], over `transform_definitions d`, with `$1` and
/// `$2` bound by [`building_params`].
fn building_predicate() -> &'static str {
    "d.build = $1 and d.status = $2"
}

fn building_params() -> (&'static str, &'static str) {
    (BUILD_REDERIVE, TransformStatus::Backfilling.as_str())
}

/// What [`work_probe`] found.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WorkProbe {
    /// Whether a build step could find anything to do (see [`work_probe`]).
    pub may_exist: bool,
    /// How long until the soonest unclaimed, undone chunk stops backing off
    /// (`next_attempt_at`). That edge is a clock, not a row change, so nothing
    /// announces it; an idle worker schedules its own wake for it. `None`
    /// when no such chunk exists, and when `may_exist` (the worker isn't
    /// idle, so it isn't asked).
    pub next_due: Option<Duration>,
}

/// Whether a drain worker's build step could find anything to do, in one
/// prepared statement (one transaction): a chunk of any kind that is undone,
/// unclaimed and not backing off, of a definition in a dispatchable status
/// ([`chunk_queue::claim_chunks_of`]'s candidates, minus its kind and sweep
/// filters), or a definition a
/// Re-derive build is running for ([`building`] is not empty). It is a
/// superset of both probes the step makes on every idle wake (`claim_chunks_of`,
/// then `building`), so a `false` means each of them would have found
/// nothing, and a `true` only means they are worth running (#273).
/// It also reports when the soonest backing-off chunk comes due, which no row
/// change announces.
pub async fn work_probe(pool: &Pool) -> Result<WorkProbe, ChunkQueueError> {
    let client = pool.get().await?;
    // Both answers stop early: `exists` at the first row, and the soonest
    // retry is only looked for when nothing is claimable (an uncorrelated
    // subquery in a `case` branch not taken never runs), so a build with
    // many chunks pending doesn't scan them all on every pass.
    let sql = format!(
        "select p.may_exist, case when not p.may_exist then ( \
                select extract(epoch from min(bc.next_attempt_at) - now())::float8 \
                from backfill_chunks bc \
                where not bc.done and bc.claimed_by is null and bc.next_attempt_at > now() \
                  and {gate}) end \
         from (select exists ( \
                   select 1 from backfill_chunks bc \
                   where not bc.done and bc.claimed_by is null \
                     and (bc.next_attempt_at is null or bc.next_attempt_at <= now()) \
                     and {gate} \
               ) or exists (select 1 from transform_definitions d where {building}) as may_exist) p",
        gate = chunk_queue::dispatchable_gate_sql("$3"),
        building = building_predicate(),
    );
    let statement = client.prepare_cached(&sql).await?;
    let params = building_params();
    let dispatchable = TransformStatus::dispatchable();
    let row = client
        .query_one(&statement, &[&params.0, &params.1, &dispatchable])
        .await?;
    let next_due: Option<f64> = row.get(1);
    Ok(WorkProbe {
        may_exist: row.get(0),
        next_due: next_due.map(|secs| Duration::from_secs_f64(secs.max(0.0))),
    })
}

/// Whether a drain worker may claim a build chunk now: the
/// sealed, undrained rows are under twice `drain_batch_cap`, and the ring
/// has a free slot for the next seal (the seal's own refusal predicate,
/// `append::ring_slot_is_free`). A chunk claimed past either would compete
/// with the drains the backlog is waiting on, or hold off the seal.
pub async fn chunk_allowed(
    client: &impl GenericClient,
    drain_batch_cap: usize,
) -> Result<bool, ChunkQueueError> {
    let bound = i64::try_from(drain_batch_cap.max(1))
        .unwrap_or(i64::MAX / 2)
        .saturating_mul(2);
    Ok(client
        .query_typed_one(
            "select coalesce((select sum(row_count)::bigint from segments \
                     where state in ('sealed', 'draining') \
                       and drained_mask <> ((1::bigint << bucket_count) - 1)), 0) < $1 \
                 and not exists (select 1 from segments \
                     where ring_slot = ((select ring_slot from segment_pointer) + 1) % $2)",
            &[
                (&bound, Type::INT8),
                (&i32::from(super::append::RING_SIZE), Type::INT4),
            ],
        )
        .await?
        .get(0))
}

/// Runs a claimed plan job or chunk ([`chunk_queue::claim_chunks_of`]), and
/// gives it up through [`chunk_queue::fail_chunk`] if it fails, which
/// records, logs and backs it off. [`work_once`] claims and runs one;
/// a test can claim one by hand and run it here.
pub async fn run_claimed(
    pool: &Pool,
    chunk: &ClaimedChunk,
    claimed_by: &str,
    options: &WorkerOptions,
) {
    let ran = match &chunk.work {
        ChunkWork::Plan { cursor } => {
            run_plan(pool, chunk, claimed_by, cursor.clone(), options).await
        }
        ChunkWork::Rederive { lo, hi } => {
            run_rederive(pool, chunk, claimed_by, lo.as_deref(), hi, options).await
        }
        ChunkWork::Sweep { cursor } => {
            run_sweep(pool, chunk, claimed_by, cursor.clone(), options).await
        }
        ChunkWork::Correction { correction_id } => {
            super::partition_correction::run_chunk(
                pool,
                chunk,
                *correction_id,
                claimed_by,
                options.heartbeat_interval,
                options.reclaim_ttl,
                &super::partition_correction::Options {
                    max_pages: Some(super::partition_correction::CLAIM_PAGES),
                },
                &mut super::partition_correction::Outcome::default(),
            )
            .await
        }
    };
    let Err(err) = ran else {
        return;
    };
    if crate::locks::is_lock_not_available(&err) {
        metrics::increment_build_chunk_lock_timeouts();
    }
    if let Err(fail_err) = chunk_queue::fail_chunk(pool, chunk, claimed_by, &err).await {
        crate::instance_log::warn!(
            definition_id = chunk.definition_id,
            chunk_id = chunk.id,
            error = %err,
            record_error = %fail_err,
            "re-derive build step failed, and recording the failure failed too; the \
             stale-claim sweep frees it for a retry"
        );
    }
}

fn build_error(err: impl Into<ApplyError>) -> ChunkQueueError {
    ChunkQueueError::Build(Box::new(err.into()))
}

/// Runs one claimed Re-derive chunk: [`one_to_one::run_chunk`] and the chunk's
/// done mark in one `read committed` transaction (ADR-0002 I1: the chunk's
/// snapshot must be its read statement's own), fenced by its claim
/// ([`ClaimFence`]). The done mark commits with the entries it
/// stands for, so a chunk that dies before its commit runs again whole, and
/// one that committed is never run again. Then [`try_complete`].
async fn run_rederive(
    pool: &Pool,
    chunk: &ClaimedChunk,
    claimed_by: &str,
    lo: Option<&str>,
    hi: &str,
    options: &WorkerOptions,
) -> Result<(), ChunkQueueError> {
    let setup_started = Instant::now();
    // Before anything the plan is made from ([`plan_epoch`]).
    let epoch = plan_epoch(&**pool.get().await?, chunk.definition_id).await?;
    let definition = catalog::definition_by_id(pool, chunk.definition_id)
        .await?
        .ok_or(ChunkQueueError::DefinitionNotFound {
            definition_id: chunk.definition_id,
        })?;
    // The range is read under the key collations its plan job walked under,
    // whatever the key's collation is now.
    let key_collations = chunk.key_collations.as_deref();
    let plan = match &chunk.fields {
        None => {
            let mut plan = one_to_one_plan(pool, &definition).await?;
            plan.pin_key_collations(key_collations);
            ChunkPlan::Whole(plan)
        }
        Some(fields) => FieldPlan::for_chunk(pool, &definition, fields, key_collations).await?,
    };
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        &**pool.get().await?,
        super::interleave::PausePoint::BeforeChunkTransaction,
        &definition.target_table,
    )
    .await?;
    let _heartbeat = chunk_queue::ChunkHeartbeat::spawn(
        pool.clone(),
        chunk.id,
        claimed_by.to_string(),
        options.heartbeat_interval,
    );
    let fence = ClaimFence::new(chunk.id, claimed_by, options.reclaim_ttl);
    let mut client = pool.get().await?;
    let started = Instant::now();
    let txn = client
        .build_transaction()
        .isolation_level(IsolationLevel::ReadCommitted)
        .start()
        .await?;
    txn.batch_execute(&format!(
        "set local application_name = '{CHUNK_APPLICATION_NAME}'"
    ))
    .await?;
    if !hold_plan_epoch(&*txn, &fence, chunk.definition_id, epoch).await? {
        txn.rollback().await?;
        crate::instance_log::debug!(
            definition_id = chunk.definition_id,
            chunk_id = chunk.id,
            "build chunk planned before an edit to its definition; planning it again"
        );
        return defer_claim(pool, chunk, claimed_by, Duration::ZERO).await;
    }
    if !fence.hold(&*txn).await? {
        txn.rollback().await?;
        return chunk_queue::discard_if_superseded(pool, chunk, claimed_by).await;
    }
    metrics::record_build_statement(BuildStatement::ChunkSetup, setup_started.elapsed());
    let keys = match &plan {
        ChunkPlan::Whole(plan) => {
            let outcome = one_to_one::run_chunk(&txn, plan, lo, hi)
                .await
                .map_err(build_error)?;
            outcome.keys
        }
        ChunkPlan::Field(FieldPlan::OneToOne(plan, fields)) => {
            let outcome = one_to_one::run_field_chunk(&txn, plan, fields, lo, hi)
                .await
                .map_err(build_error)?;
            outcome.keys
        }
    };
    let commit_started = Instant::now();
    txn.execute(
        "update backfill_chunks set done = true, claimed_by = null, claimed_at = null \
         where id = $1",
        &[&chunk.id],
    )
    .await?;
    txn.commit().await?;
    metrics::record_build_statement(BuildStatement::ChunkCommit, commit_started.elapsed());
    metrics::record_build_chunk(started.elapsed(), keys as u64);
    try_complete(pool, chunk.definition_id).await?;
    Ok(())
}

/// What one claimed `rederive` row runs.
enum ChunkPlan {
    /// A whole build's chunk.
    Whole(one_to_one::OneToOnePlan),
    /// A field build's chunk.
    Field(FieldPlan),
}

/// The version fence of definition `id`'s source (`source_table_versions`),
/// which a chunk or a sweep batch of a 1-1 target reads before it plans,
/// and takes `for share` as its transaction's first lock
/// ([`hold_plan_epoch`]). A chunk plans before its transaction
/// (its fields and its paused columns),
/// so an edit can commit in between: `ALTER TRANSFORM`, a column pause or
/// resume, or a field build's capture release, each of which bumps the
/// fence. If the chunk then went on, the edit's own field build could
/// already have written keys the chunk had yet to lock, and the chunk would
/// put its older plan's values over them, or stamp an entry's `basis` past
/// a change whose page then can't write the new field; a chunk planned
/// before a pause would write the paused column after it. One
/// that finds the fence moved rolls back and plans again. One that finds it
/// unmoved holds it to its commit, so a bump that comes later waits for the
/// chunk: the edit's build reaches its keys only after it commits, and a
/// pause returns only once its write has landed. A define on the same
/// source bumps the fence too, which costs such a chunk a retry.
async fn plan_epoch(
    client: &impl GenericClient,
    id: i64,
) -> Result<Option<i64>, tokio_postgres::Error> {
    Ok(client
        .query_opt(
            "select v.version from transform_definitions d \
             join source_table_versions v on v.source_table = d.source_table \
             where d.id = $1",
            &[&id],
        )
        .await?
        .map(|row| row.get(0)))
}

/// Takes the version fence of definition `id`'s source `for share` in a
/// chunk's or sweep batch's `txn`, and returns whether it still reads
/// `epoch`, the [`plan_epoch`] the plan was made under. It is the
/// transaction's first lock, before the claim's ([`ClaimFence::hold`]) and
/// the keys' entries, as a page takes the fence before any other lock: a
/// bump waits for the transactions holding the fence, and holds nothing
/// while it does, so a writer that holds no other lock when it
/// queues on a bump can't close a cycle through it. A resume, say, bumps
/// the fence and then locks every chunk a worker holds.
///
/// The claim's idle timeout goes first: from here on, a stalled worker
/// holds up every bump of the source's fence, not just its chunk.
async fn hold_plan_epoch(
    txn: &impl GenericClient,
    claim: &ClaimFence<'_>,
    id: i64,
    epoch: Option<i64>,
) -> Result<bool, tokio_postgres::Error> {
    claim.arm(txn).await?;
    let held: Option<i64> = txn
        .query_opt(
            "select v.version from transform_definitions d \
             join source_table_versions v on v.source_table = d.source_table \
             where d.id = $1 for share of v",
            &[&id],
        )
        .await?
        .map(|row| row.get(0));
    Ok(held == epoch)
}

/// A field build's chunk (see the module doc's "Field builds").
enum FieldPlan {
    /// A plain 1-1 target's: [`one_to_one::run_field_chunk`] over the
    /// fields.
    OneToOne(one_to_one::OneToOnePlan, Vec<String>),
}

impl FieldPlan {
    /// The plan of a chunk of `definition`'s field build over `fields`.
    /// Only a 1-1 target holds a field out of Apply, so only a 1-1 target
    /// has a field build; anything else is
    /// [`BackfillError::Unsupported`](crate::defs::backfill::BackfillError::Unsupported).
    async fn for_chunk(
        pool: &Pool,
        definition: &Definition,
        fields: &[String],
        key_collations: Option<&[Option<String>]>,
    ) -> Result<ChunkPlan, ChunkQueueError> {
        if let Some(mut plan) = one_to_one::OneToOnePlan::for_definition(pool, definition)
            .await
            .map_err(build_error)?
        {
            plan.pin_key_collations(key_collations);
            // With every field that reads one of `fields` by alias in the
            // definition as it is now: a later edit can add a
            // reader of a field this build releases (one awaiting its
            // capture), which Apply holds out with that field and which no
            // other build writes once it is released. A chunk planned
            // before such an edit is planned again ([`plan_epoch`]).
            let mut fields: std::collections::HashSet<String> = fields.iter().cloned().collect();
            crate::defs::alias_readers::AliasReaders::of(&definition.def).close(&mut fields);
            let fields: Vec<String> = definition
                .def
                .fields
                .iter()
                .filter(|f| fields.contains(&f.name))
                .map(|f| f.name.clone())
                .collect();
            return Ok(ChunkPlan::Field(FieldPlan::OneToOne(plan, fields)));
        }
        // Every 1-1 definition define accepts renders
        // (`one_to_one::buildable`), so this is any other key space.
        Err(build_error(
            crate::defs::backfill::BackfillError::Unsupported(
                "only a 1-1 target has a field build".to_string(),
            ),
        ))
    }
}

/// `definition`'s 1-1 build plan, or [`BackfillError::Unsupported`] when its
/// shape no longer takes a Re-derive build.
///
/// [`BackfillError::Unsupported`]: crate::defs::backfill::BackfillError::Unsupported
async fn one_to_one_plan(
    pool: &Pool,
    definition: &Definition,
) -> Result<one_to_one::OneToOnePlan, ChunkQueueError> {
    one_to_one::OneToOnePlan::for_definition(pool, definition)
        .await
        .map_err(build_error)?
        .ok_or_else(|| {
            build_error(crate::defs::backfill::BackfillError::Unsupported(
                "the definition's shape no longer takes a re-derive build".to_string(),
            ))
        })
}

/// The cursor [`stage_null_keys`] reads.
const NULL_KEYS_CURSOR: &str = "trellis_null_keys_cursor";

/// Rows per page of [`stage_null_keys`]' cursor.
const NULL_KEYS_PAGE: i64 = 10_000;

/// Stages an image-less `Recompute` for every key of `source_table` with a
/// `NULL` part, for a 1-1 target's build (F6). Only a source keyed by a
/// nullable unique index has such keys: another aggregate's or `JOIN`'s
/// target, whose `GROUP BY` columns may be `NULL`. A 1-1 target over such a
/// source is keyed `unique nulls not distinct` so it holds their rows.
/// No chunk reaches them. A chunk's range is a row comparison, which is
/// `NULL` (so the row is out) wherever a `NULL` part decides it, and a
/// plan's boundaries are keys without one.
///
/// A page re-derives a staged key as it does a chunk's: under the key's
/// entry lock, from the source's live row, so it needs no more than the
/// chunk does. The keys are staged in the transaction that ends the plan,
/// after the fence, so every writer that could have missed the build has
/// ended. It does what a chunk can't for these keys: staged rows drain after
/// the build has flipped `live` (their `origin_lsn` is unknown, so they hold
/// every convergence token until they drain).
///
/// Streamed through a cursor, a page at a time, so a source with many such
/// keys holds one page in memory.
async fn stage_null_keys(
    txn: &Transaction<'_>,
    source_table: &str,
    pk: &[ddl::PrimaryKeyColumn],
) -> Result<(), ApplyError> {
    use crate::staging::append::{self, StagedChange};

    let nullable: Vec<String> = pk
        .iter()
        .filter(|c| c.nullable)
        .map(|c| format!("{} is null", quote_ident(&c.name)))
        .collect();
    if nullable.is_empty() {
        return Ok(());
    }
    txn.batch_execute(&format!(
        "declare {NULL_KEYS_CURSOR} cursor for \
         select {} from {} where {}",
        ddl::pk_key_sql_expr(pk, None),
        ddl::qualified_source_table(source_table),
        nullable.join(" or "),
    ))
    .await?;
    loop {
        let rows = txn
            .query(
                &format!("fetch forward {NULL_KEYS_PAGE} from {NULL_KEYS_CURSOR}"),
                &[],
            )
            .await?;
        if rows.is_empty() {
            break;
        }
        let page: Vec<StagedChange> = rows
            .iter()
            .map(|row| StagedChange::Recompute {
                src_table: source_table.to_string(),
                key: row.get(0),
                hop_gen: 0,
                src_changed: None,
                prior_image: None,
                origin_lsn: None,
            })
            .collect();
        append::append(txn, &page).await?;
    }
    txn.batch_execute(&format!("close {NULL_KEYS_CURSOR}"))
        .await?;
    Ok(())
}

/// Where a seam-fed build's fence stands ([`fence_settled`]).
enum FenceState {
    /// Every transaction open when the fence was taken has ended.
    Settled,
    /// One hasn't: the fence's transaction id.
    Pending(i64),
    /// The plan job's claim was reclaimed meanwhile.
    Superseded,
}

/// Takes the fence of the claimed plan job `chunk` if it has none, and
/// reports whether the oldest running transaction began after it (F6, see
/// the module doc's "Seam-fed sources").
///
/// The fence is a statement's own transaction id, assigned here in a
/// transaction of its own, begun after the start commit: every seam writer
/// that checked for readers before the start committed already has an id
/// (the check takes one first, `super::target_mutations`), ids are
/// assigned in order, and so each is below the fence. The snapshot's `xmin` is the oldest transaction id
/// still running, so it passes the fence exactly when they have all ended.
/// A fence already on the row is kept: an earlier attempt took it after the
/// start commit too, and a later one would only wait longer.
async fn fence_settled(
    pool: &Pool,
    chunk: &ClaimedChunk,
    claimed_by: &str,
) -> Result<FenceState, ChunkQueueError> {
    let client = pool.get().await?;
    let Some(row) = client
        .query_opt(
            "update backfill_chunks \
             set fence_xid = coalesce(fence_xid, pg_current_xact_id()) \
             where id = $1 and claimed_by = $2 \
             returning fence_xid::text::bigint",
            &[&chunk.id, &claimed_by],
        )
        .await?
    else {
        return Ok(FenceState::Superseded);
    };
    let fence: i64 = row.get(0);
    let settled: bool = client
        .query_one(
            "select pg_snapshot_xmin(pg_current_snapshot()) > \
                    (select fence_xid from backfill_chunks where id = $1)",
            &[&chunk.id],
        )
        .await?
        .get(0);
    Ok(if settled {
        FenceState::Settled
    } else {
        FenceState::Pending(fence)
    })
}

/// Runs a claimed plan job: walks the source's primary key
/// from the job's cursor, [`PLAN_BATCH`] boundaries at a time, each batch
/// committed as `rederive` chunks together with the cursor's advance, so
/// chunks run while the walk goes on and a job that dies resumes where its
/// last batch committed. The batch that reaches the last row marks the job
/// done. Each commit is fenced by the claim ([`ClaimFence`]).
async fn run_plan(
    pool: &Pool,
    chunk: &ClaimedChunk,
    claimed_by: &str,
    mut cursor: Option<String>,
    options: &WorkerOptions,
) -> Result<(), ChunkQueueError> {
    let definition = catalog::definition_by_id(pool, chunk.definition_id)
        .await?
        .ok_or(ChunkQueueError::DefinitionNotFound {
            definition_id: chunk.definition_id,
        })?;
    // The walk compares under the collations the job's first batch walked
    // under, and every chunk records them: an `alter column …
    // collate …` between two batches, or between a batch and a chunk, would
    // otherwise reorder the key under the boundaries already enqueued.
    let mut pk = ddl::source_primary_key(pool, &definition.source_table)
        .await
        .map_err(build_error)?;
    ddl::pin_key_collations(&mut pk, chunk.key_collations.as_deref());
    let key_collations = ddl::key_collations(&pk);
    let _heartbeat = chunk_queue::ChunkHeartbeat::spawn(
        pool.clone(),
        chunk.id,
        claimed_by.to_string(),
        options.heartbeat_interval,
    );
    let fence = ClaimFence::new(chunk.id, claimed_by, options.reclaim_ttl);
    // A field build starts once its fields apply, and its walk,
    // so every chunk, comes after that commit.
    if let Some(fields) = &chunk.fields
        && cursor.is_none()
    {
        match field_build_ready(pool, &definition, fields, &fence).await? {
            FieldStart::Go => {}
            FieldStart::Wait => {
                return defer_claim(pool, chunk, claimed_by, FIELD_CAPTURE_RETRY).await;
            }
            FieldStart::Superseded => {
                return chunk_queue::discard_if_superseded(pool, chunk, claimed_by).await;
            }
        }
    }
    // A seam-fed source's build waits for its fence before the walk, so
    // before any chunk (F6). A `JOIN` waits for it when either side is
    // another definition's target, before its one transaction: a writer of
    // that target that resolved its feed before the `JOIN` applied staged
    // nothing for it, and the build's `GROUP BY` must see its row.
    if chunk.fields.is_none()
        && cursor.is_none()
        && reads_a_target(&**pool.get().await?, &definition).await?
    {
        match fence_settled(pool, chunk, claimed_by).await? {
            FenceState::Settled => {}
            FenceState::Pending(fence) => {
                crate::instance_log::debug!(
                    definition_id = chunk.definition_id,
                    fence_xid = fence,
                    "re-derive build waits for its fence before planning a chunk"
                );
                return defer_claim(pool, chunk, claimed_by, FENCE_RETRY).await;
            }
            FenceState::Superseded => {
                return chunk_queue::discard_if_superseded(pool, chunk, claimed_by).await;
            }
        }
    }
    // A `JOIN`'s fresh build or resume is its whole build (ADR-0019):
    // both structures in one transaction, never a walk of the primary key.
    // It runs after the seam fence above, as a plain aggregate's does.
    if super::join::applies(&definition.def) {
        if chunk.fields.is_some() {
            return Err(build_error(ApplyError::GroupBasisUnsupported {
                target: definition.target_table.clone(),
                what: "a field build of a JOIN (ALTER TRANSFORM takes only a 1-1 target)",
            }));
        }
        // Its commit parks its target's readers, once, in the
        // transaction that completes the build (the last step, if it has
        // several).
        return run_group_rebuild_held(
            pool,
            chunk,
            claimed_by,
            &definition,
            &fence,
            cursor.as_deref(),
            ReadersOnCommit::Park,
        )
        .await;
    }
    if crate::defs::join::is_one_to_one(&definition.def) {
        return Err(build_error(ApplyError::GroupBasisUnsupported {
            target: definition.target_table.clone(),
            what: "a build of a 1-1 JOIN (its build path is not built yet)",
        }));
    }
    // A plain group-basis aggregate's fresh build or resume is its whole
    // build: one absolute write of every group, as a rebuild of a live
    // definition is (ADR-0002), run after the seam fence above so its
    // `GROUP BY` reads a racing writer's row. Over a partitioned source it is
    // one transaction per leaf of the recorded tree, then one for the groups
    // that route to no leaf, the job's `lo` holding the last leaf done. It
    // never walks the primary key: a `rederive` chunk has no meaning without
    // a ledger. Its commit parks its target's readers.
    let mut client = pool.get().await?;
    let aggregate = chunk.fields.is_none() && takes_group_basis(&definition);
    let recorded = aggregate
        && (!crate::defs::partition_tree::is_partitioned_root(&**client, &definition.source_table)
            .await?
            || crate::capture::partitions::recorded(&**client, &definition.source_table)
                .await
                .map_err(|e| ChunkQueueError::Correction(Box::new(e.into())))?
                .is_some());
    if recorded {
        drop(client);
        return run_group_rebuild_held(
            pool,
            chunk,
            claimed_by,
            &definition,
            &fence,
            cursor.as_deref(),
            ReadersOnCommit::Park,
        )
        .await;
    }
    // A plain aggregate over a partitioned root whose tree the capture pass
    // hasn't recorded yet takes an empty one, and refuses one with rows: its
    // per-leaf build needs the record.
    if cursor.is_none() && chunk.fields.is_none() && takes_group_basis(&definition) {
        let has_rows: bool = client
            .query_one(
                &format!(
                    "select exists (select 1 from {})",
                    ddl::qualified_source_table(&definition.source_table)
                ),
                &[],
            )
            .await?
            .get(0);
        if has_rows {
            return Err(build_error(ApplyError::GroupBasisUnsupported {
                target: definition.target_table.clone(),
                what: "a fresh build or resume over a partitioned source that has rows \
                       before its capture records the tree",
            }));
        }
    }
    loop {
        let started = Instant::now();
        let ranges = crate::defs::backfill::next_pk_ranges(
            &**client,
            &definition.source_table,
            &pk,
            cursor.as_deref(),
            options.chunk_rows,
            PLAN_BATCH,
        )
        .await
        .map_err(build_error)?;
        let finished = ranges.len() < PLAN_BATCH;
        let next = ranges.last().map(|(_, hi)| hi.clone()).or(cursor.clone());
        let txn = client.transaction().await?;
        // The definition row first, then the plan row, the order every other
        // chunk-side writer takes them in. The insert below checks its
        // foreign key against the definition row (`for key share`), which a
        // resume or a drop holds `for update` while it waits on the plan row
        // this batch locks: taken after the claim, that check closes a cycle.
        // Here a resume or drop in flight makes this batch wait holding
        // nothing, and one that committed first shows in the claim's stale
        // check, so the batch stops as superseded. The claim's idle timeout
        // goes first, so it bounds a stall that holds the definition row
        // too.
        fence.arm(&*txn).await?;
        txn.execute(
            "select 1 from transform_definitions where id = $1 for key share",
            &[&chunk.definition_id],
        )
        .await?;
        if !fence.hold(&*txn).await? {
            txn.rollback().await?;
            return chunk_queue::discard_if_superseded(pool, chunk, claimed_by).await;
        }
        // Test-only pause point. See `super::interleave`.
        #[cfg(any(test, feature = "test-util"))]
        super::interleave::pause_at(
            &*txn,
            super::interleave::PausePoint::AfterPlanHold,
            &definition.target_table,
        )
        .await?;
        let (los, his): (Vec<Option<&str>>, Vec<&str>) = ranges
            .iter()
            .map(|(lo, hi)| (lo.as_deref(), hi.as_str()))
            .unzip();
        txn.execute(
            "insert into backfill_chunks \
                 (definition_id, kind, lo, hi, fuse_rearmed_at, fields, key_collations) \
             select bc.definition_id, $4, r.lo, r.hi, bc.fuse_rearmed_at, bc.fields, $5 \
             from unnest($2::text[], $3::text[]) with ordinality as r(lo, hi, n) \
             cross join backfill_chunks bc \
             where bc.id = $1 \
             order by r.n",
            &[
                &chunk.id,
                &los,
                &his,
                &chunk_queue::KIND_REDERIVE,
                &key_collations,
            ],
        )
        .await?;
        if finished {
            // The keys no range reaches (F6), in the transaction that ends
            // the plan, so a plan that is done has staged them. A 1-1
            // target's field build stages them too: their rows hold the
            // resumed fields as well.
            let one_to_one = matches!(
                definition.def.key_space,
                crate::defs::ast::KeySpace::OneToOne
            );
            if one_to_one {
                stage_null_keys(&txn, &definition.source_table, &pk)
                    .await
                    .map_err(build_error)?;
            }
            txn.execute(
                "update backfill_chunks set lo = $2, done = true, claimed_by = null, \
                     claimed_at = null, key_collations = $3 \
                 where id = $1",
                &[&chunk.id, &next, &key_collations],
            )
            .await?;
        } else {
            txn.execute(
                "update backfill_chunks set lo = $2, key_collations = $3 where id = $1",
                &[&chunk.id, &next, &key_collations],
            )
            .await?;
        }
        txn.commit().await?;
        metrics::record_build_statement(BuildStatement::Plan, started.elapsed());
        if finished {
            crate::instance_log::debug!(
                definition_id = chunk.definition_id,
                "re-derive build planned its last chunk"
            );
            break;
        }
        cursor = next;
    }
    try_complete(pool, chunk.definition_id).await?;
    Ok(())
}

/// Runs a group-basis definition's claimed sweep job, which is its whole
/// rebuild (see the module doc's "Rebuild of an applying
/// definition"): [`super::group_basis::rebuild_definition`] (a `JOIN`'s,
/// [`super::join::build`]) and the job's
/// done mark in one `read committed` transaction, fenced by the claim
/// ([`ClaimFence`]), so a job that dies before its commit runs again whole
/// and one that committed never does. The rebuild is idempotent: run again,
/// it writes each group from a newer snapshot. Then [`try_complete`].
///
/// On an unpartitioned source that one transaction is the whole rebuild. On
/// a partitioned source the job is one such transaction per **leaf** of the
/// source's recorded tree, in oid order, then one for the **remainder**, the
/// groups that route to no recorded leaf ([`run_group_structure`]). Each
/// group's key holds every partition-key column (ADR-0017), so each routes to
/// exactly one structure and the transactions write disjoint sets of groups,
/// each a whole absolute write: no statement or lock covers more than one
/// leaf's groups. The job's cursor (`backfill_chunks.lo`) holds the oid of
/// the last leaf committed, so a job that dies resumes after it; the
/// remainder's transaction marks the job done. A page between two
/// transactions sees the first leaf's new bases and the second's old ones,
/// which is exact per group.
async fn run_group_rebuild(
    pool: &Pool,
    chunk: &ClaimedChunk,
    claimed_by: &str,
    definition: &Definition,
    cursor: Option<&str>,
    options: &WorkerOptions,
) -> Result<(), ChunkQueueError> {
    let _heartbeat = chunk_queue::ChunkHeartbeat::spawn(
        pool.clone(),
        chunk.id,
        claimed_by.to_string(),
        options.heartbeat_interval,
    );
    let fence = ClaimFence::new(chunk.id, claimed_by, options.reclaim_ttl);
    run_group_rebuild_held(
        pool,
        chunk,
        claimed_by,
        definition,
        &fence,
        cursor,
        ReadersOnCommit::Keep,
    )
    .await
}

/// What the commit of a group-basis build does for the readers of its target
/// ([`run_group_structure`]).
#[derive(Clone, Copy, PartialEq, Eq)]
enum ReadersOnCommit {
    /// Nothing: a rebuild in place of a `live` definition starts from hidden
    /// state that matches its target, so the seam rows it stages carry exact
    /// images.
    Keep,
    /// The readers are rebuilt
    /// (`intake::catch_up::park_target_catchup_if_read`): the plan job
    /// of a fresh build or a resume. A resume starts from partials that are
    /// empty while its target holds the rows of the earlier life, so the
    /// seam rows a page or the build stages meanwhile can carry prior images
    /// that match no row the readers hold. A fresh build has no reader, and
    /// the rebuild finds none.
    Park,
}

/// [`run_group_rebuild`] for a caller that already heartbeats the claim and
/// holds its [`ClaimFence`]: the plan job of a fresh build ([`run_plan`]).
async fn run_group_rebuild_held(
    pool: &Pool,
    chunk: &ClaimedChunk,
    claimed_by: &str,
    definition: &Definition,
    fence: &ClaimFence<'_>,
    cursor: Option<&str>,
    readers: ReadersOnCommit,
) -> Result<(), ChunkQueueError> {
    let mut client = pool.get().await?;
    if super::join::applies(&definition.def) {
        let partitioned = join_partitioned_sides(&**client, definition).await?;
        if !partitioned.is_empty() {
            drop(client);
            return run_join_steps(
                pool,
                chunk,
                claimed_by,
                definition,
                fence,
                cursor,
                readers,
                &partitioned,
            )
            .await;
        }
        let Some((written, deleted)) = run_group_structure(
            pool,
            &mut client,
            chunk,
            fence,
            definition,
            &Structure::Whole,
            readers,
        )
        .await?
        else {
            return chunk_queue::discard_if_superseded(pool, chunk, claimed_by).await;
        };
        crate::instance_log::debug!(
            definition_id = chunk.definition_id,
            written,
            deleted,
            "JOIN build replaced both structures and every group"
        );
        try_complete(pool, chunk.definition_id).await?;
        return Ok(());
    }
    if !crate::defs::partition_tree::is_partitioned_root(&**client, &definition.source_table)
        .await?
    {
        let Some((written, deleted)) = run_group_structure(
            pool,
            &mut client,
            chunk,
            fence,
            definition,
            &Structure::Whole,
            readers,
        )
        .await?
        else {
            return chunk_queue::discard_if_superseded(pool, chunk, claimed_by).await;
        };
        crate::instance_log::debug!(
            definition_id = chunk.definition_id,
            written,
            deleted,
            "group-basis rebuild replaced every group"
        );
        try_complete(pool, chunk.definition_id).await?;
        return Ok(());
    }
    let unsupported = |what: &'static str| {
        build_error(ApplyError::GroupBasisUnsupported {
            target: definition.target_table.clone(),
            what,
        })
    };
    let recorded = crate::capture::partitions::recorded(&**client, &definition.source_table)
        .await
        .map_err(|e| ChunkQueueError::Correction(Box::new(e.into())))?;
    let Some(recorded) = recorded else {
        return Err(unsupported(
            "a rebuild over a partitioned source whose tree is not recorded",
        ));
    };
    let Ok(leaves) = super::partition_correction::rebuild_leaves(&recorded) else {
        return Err(unsupported(
            "a rebuild over a partitioned source with a leaf that has no usable constraint text",
        ));
    };
    let Some(shape) = super::group_basis::route(&definition.def, &definition.source_columns) else {
        return Err(unsupported(
            "a rebuild of a definition the group basis does not take",
        ));
    };
    let names: Vec<&str> = shape.group_columns().iter().map(String::as_str).collect();
    let after: Option<u32> = cursor.and_then(|c| c.parse().ok());
    let mut remaining: Vec<&super::partition_correction::RebuildLeaf> = leaves
        .iter()
        .filter(|leaf| after.is_none_or(|done| leaf.oid > done))
        .collect();
    remaining.reverse();
    let (mut written, mut deleted) = (0, 0);
    // The leaves left, then the remainder, which marks the job done.
    loop {
        let leaf = remaining.pop();
        let structure = Structure::Partitioned {
            names: &names,
            recorded: &recorded,
            leaves: &leaves,
            leaf,
            last: leaf.is_none(),
        };
        let Some((w, d)) = run_group_structure(
            pool,
            &mut client,
            chunk,
            fence,
            definition,
            &structure,
            readers,
        )
        .await?
        else {
            return chunk_queue::discard_if_superseded(pool, chunk, claimed_by).await;
        };
        written += w;
        deleted += d;
        if leaf.is_none() {
            break;
        }
        #[cfg(any(test, feature = "test-util"))]
        super::interleave::pause_between_transactions(
            &**client,
            super::interleave::PausePoint::BetweenRebuildStructures,
            &definition.target_table,
        )
        .await?;
    }
    crate::instance_log::debug!(
        definition_id = chunk.definition_id,
        written,
        deleted,
        leaves = leaves.len(),
        "group-basis rebuild replaced every group, one leaf at a time"
    );
    try_complete(pool, chunk.definition_id).await?;
    Ok(())
}

/// Whether `definition` reads another definition's target: its source, or a
/// `JOIN`'s joined table. Such a table is fed by the target-mutation seam,
/// so a fresh build or a resume waits on its fence ([`fence_settled`]).
async fn reads_a_target(
    client: &impl GenericClient,
    definition: &Definition,
) -> Result<bool, ChunkQueueError> {
    if catalog::is_definition_target(client, &definition.source_table).await? {
        return Ok(true);
    }
    if crate::defs::join::spec(&definition.def).is_some()
        && let Some(joined) = catalog::join_table_of(client, definition.id).await?
    {
        return Ok(catalog::is_definition_target(client, &joined).await?);
    }
    Ok(false)
}

/// The sides of a `JOIN` that are a partitioned root with rows, `FROM` side
/// first: the sides whose build is per leaf. A partitioned side with
/// no rows builds as an unpartitioned one: there is nothing to bound.
async fn join_partitioned_sides(
    client: &impl GenericClient,
    definition: &Definition,
) -> Result<Vec<(crate::defs::join::Side, String)>, ChunkQueueError> {
    use crate::defs::join::Side;
    let mut sides = vec![(Side::From, definition.source_table.clone())];
    sides.extend(
        catalog::join_table_of(client, definition.id)
            .await?
            .map(|t| (Side::Joined, t)),
    );
    let mut partitioned = Vec::new();
    for (side, table) in sides {
        if builds_per_leaf(client, &table).await? {
            partitioned.push((side, table));
        }
    }
    Ok(partitioned)
}

/// Whether a `JOIN` side `table` builds per leaf: a partitioned root with
/// rows ([`join_partitioned_sides`]; `status()` reads the side under build
/// by the same rule).
pub(crate) async fn builds_per_leaf(
    client: &impl GenericClient,
    table: &str,
) -> Result<bool, tokio_postgres::Error> {
    if !crate::defs::partition_tree::is_partitioned_root(client, table).await? {
        return Ok(false);
    }
    Ok(client
        .query_one(
            &format!(
                "select exists (select 1 from {})",
                ddl::qualified_source_table(table)
            ),
            &[],
        )
        .await?
        .get(0))
}

/// One step of a `JOIN` build with a partitioned side, before its final `T`
/// step, as its cursor (`backfill_chunks.lo`) names it ([`run_join_steps`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinStepId {
    /// A whole unpartitioned side.
    Whole(crate::defs::join::Side),
    /// The leaf with this oid of a partitioned side's recorded tree.
    Leaf(crate::defs::join::Side, u32),
    /// The cells of a partitioned side that route to none of its leaves.
    Remainder(crate::defs::join::Side),
}

impl JoinStepId {
    /// The cursor the step's transaction writes: `side:whole`, `side:oid` or
    /// `side:remainder`.
    fn cursor(self) -> String {
        match self {
            JoinStepId::Whole(side) => format!("{}:whole", side_name(side)),
            JoinStepId::Leaf(side, oid) => format!("{}:{oid}", side_name(side)),
            JoinStepId::Remainder(side) => format!("{}:remainder", side_name(side)),
        }
    }
}

/// The steps of a `JOIN` build before its final one, in the order
/// [`run_join_steps`] runs them, for the sides that build per leaf
/// (`partitioned`, each with its leaves' oids in oid order):
///
/// 1. every unpartitioned side whole, `FROM` first;
/// 2. each partitioned side, `FROM` first: its leaves, then its remainder.
pub(crate) fn join_steps(partitioned: &[(crate::defs::join::Side, Vec<u32>)]) -> Vec<JoinStepId> {
    use crate::defs::join::Side;
    let mut steps = Vec::new();
    for side in [Side::From, Side::Joined] {
        if !partitioned.iter().any(|(s, _)| *s == side) {
            steps.push(JoinStepId::Whole(side));
        }
    }
    for side in [Side::From, Side::Joined] {
        let Some((_, leaves)) = partitioned.iter().find(|(s, _)| *s == side) else {
            continue;
        };
        steps.extend(leaves.iter().map(|oid| JoinStepId::Leaf(side, *oid)));
        steps.push(JoinStepId::Remainder(side));
    }
    steps
}

/// The steps of `steps` ([`join_steps`]) a job whose cursor is `cursor` has
/// still to run, the final `T` step aside: those after the step the cursor
/// names. A cursor that names none of them (the tree or a side's
/// partitioning changed since) starts the build over: every step is
/// idempotent.
pub(crate) fn join_steps_after<'s>(
    steps: &'s [JoinStepId],
    cursor: Option<&str>,
) -> &'s [JoinStepId] {
    match cursor.and_then(|c| steps.iter().position(|s| s.cursor() == c)) {
        Some(done) => &steps[done + 1..],
        None => steps,
    }
}

/// One side's recorded tree, for a `JOIN` build's per-leaf steps.
struct SideTree {
    side: crate::defs::join::Side,
    /// The side's cell columns (its `P` or `M` key), which a leaf's
    /// constraint text routes.
    names: Vec<String>,
    recorded: crate::capture::partitions::Recorded,
    leaves: Vec<super::partition_correction::RebuildLeaf>,
}

/// A `JOIN` build with a partitioned side that has rows (ADR-0019): a
/// sequence of bounded transactions, the job's cursor (`backfill_chunks.lo`,
/// `side:oid`) written in each, so a job that dies resumes after its last
/// committed step.
///
/// 1. Every unpartitioned side, whole, in one transaction each (`FROM`
///    first).
/// 2. Each partitioned side, `FROM` first: its leaves in oid order, then its
///    remainder, the cells that route to no recorded leaf.
/// 3. The final `T` step, which marks the job done and parks the readers'
///    rebuilds the readers ([`ReadersOnCommit::Park`]), once, in its
///    transaction.
///
/// An unrecorded tree, or a leaf without usable constraint text, fails the
/// job: there is no statement over the whole root to fall back to.
#[allow(clippy::too_many_arguments)]
async fn run_join_steps(
    pool: &Pool,
    chunk: &ClaimedChunk,
    claimed_by: &str,
    definition: &Definition,
    fence: &ClaimFence<'_>,
    cursor: Option<&str>,
    readers: ReadersOnCommit,
    partitioned: &[(crate::defs::join::Side, String)],
) -> Result<(), ChunkQueueError> {
    use crate::defs::join::Side;
    let unsupported = |what: &'static str| {
        build_error(ApplyError::GroupBasisUnsupported {
            target: definition.target_table.clone(),
            what,
        })
    };
    let mut client = pool.get().await?;
    let mut trees: Vec<SideTree> = Vec::new();
    for (side, table) in partitioned {
        let recorded = crate::capture::partitions::recorded(&**client, table)
            .await
            .map_err(|e| ChunkQueueError::Correction(Box::new(e.into())))?;
        let Some(recorded) = recorded else {
            return Err(unsupported(
                "a JOIN build over a partitioned side whose tree is not recorded",
            ));
        };
        let Ok(leaves) = super::partition_correction::rebuild_leaves(&recorded) else {
            return Err(unsupported(
                "a JOIN build over a partitioned side with a leaf that has no usable \
                 constraint text",
            ));
        };
        trees.push(SideTree {
            side: *side,
            names: crate::defs::join::side_key_columns(&definition.def, *side),
            recorded,
            leaves,
        });
    }
    let names: Vec<Vec<&str>> = trees
        .iter()
        .map(|t| t.names.iter().map(String::as_str).collect())
        .collect();
    let order: Vec<(Side, Vec<u32>)> = trees
        .iter()
        .map(|t| (t.side, t.leaves.iter().map(|l| l.oid).collect()))
        .collect();
    let ids = join_steps(&order);
    let tree_of = |side: Side| {
        let at = trees
            .iter()
            .position(|t| t.side == side)
            .expect("a leaf or remainder step is of a side with a tree");
        (&trees[at], names[at].as_slice())
    };
    let steps: Vec<(JoinStep<'_>, JoinStepId)> = join_steps_after(&ids, cursor)
        .iter()
        .map(|id| {
            let step = match *id {
                JoinStepId::Whole(side) => JoinStep::Whole { side },
                JoinStepId::Leaf(side, oid) => {
                    let (tree, names) = tree_of(side);
                    JoinStep::Leaf {
                        side,
                        names,
                        recorded: &tree.recorded,
                        leaf: tree
                            .leaves
                            .iter()
                            .find(|l| l.oid == oid)
                            .expect("a leaf step names a leaf of its tree"),
                    }
                }
                JoinStepId::Remainder(side) => {
                    let (tree, names) = tree_of(side);
                    JoinStep::Remainder {
                        side,
                        names,
                        recorded: &tree.recorded,
                        leaves: &tree.leaves,
                    }
                }
            };
            (step, *id)
        })
        .collect();
    let (mut written, mut deleted) = (0, 0);
    for (step, id) in &steps {
        let structure = Structure::Join {
            piece: JoinPiece::Step(step),
            cursor: Some(id.cursor()),
        };
        let Some((w, d)) = run_group_structure(
            pool,
            &mut client,
            chunk,
            fence,
            definition,
            &structure,
            readers,
        )
        .await?
        else {
            return chunk_queue::discard_if_superseded(pool, chunk, claimed_by).await;
        };
        written += w;
        deleted += d;
        #[cfg(any(test, feature = "test-util"))]
        super::interleave::pause_between_transactions(
            &**client,
            super::interleave::PausePoint::BetweenRebuildStructures,
            &definition.target_table,
        )
        .await?;
    }
    let structure = Structure::Join {
        piece: JoinPiece::Final,
        cursor: None,
    };
    let Some((w, d)) = run_group_structure(
        pool,
        &mut client,
        chunk,
        fence,
        definition,
        &structure,
        readers,
    )
    .await?
    else {
        return chunk_queue::discard_if_superseded(pool, chunk, claimed_by).await;
    };
    crate::instance_log::debug!(
        definition_id = chunk.definition_id,
        written = written + w,
        deleted = deleted + d,
        "JOIN build replaced both structures, one side and leaf at a time"
    );
    try_complete(pool, chunk.definition_id).await?;
    Ok(())
}

/// One step of [`run_join_steps`] before its final one.
enum JoinStep<'a> {
    /// A whole unpartitioned side.
    Whole { side: crate::defs::join::Side },
    /// One leaf of a partitioned side's recorded tree.
    Leaf {
        side: crate::defs::join::Side,
        names: &'a [&'a str],
        recorded: &'a crate::capture::partitions::Recorded,
        leaf: &'a super::partition_correction::RebuildLeaf,
    },
    /// The cells of a partitioned side that route to none of its leaves.
    Remainder {
        side: crate::defs::join::Side,
        names: &'a [&'a str],
        recorded: &'a crate::capture::partitions::Recorded,
        leaves: &'a [super::partition_correction::RebuildLeaf],
    },
}

/// What one transaction of a `JOIN`'s per-leaf build covers
/// ([`Structure::Join`]).
enum JoinPiece<'a> {
    Step(&'a JoinStep<'a>),
    /// The final `T` step.
    Final,
}

fn side_name(side: crate::defs::join::Side) -> &'static str {
    match side {
        crate::defs::join::Side::From => "from",
        crate::defs::join::Side::Joined => "joined",
    }
}

/// What one transaction of a group-basis rebuild covers.
enum Structure<'a> {
    /// The whole of an unpartitioned source: the job's only transaction.
    Whole,
    /// One leaf of a partitioned source's recorded tree, or, with no `leaf`,
    /// the remainder. `last` marks the job done.
    Partitioned {
        names: &'a [&'a str],
        recorded: &'a crate::capture::partitions::Recorded,
        leaves: &'a [super::partition_correction::RebuildLeaf],
        leaf: Option<&'a super::partition_correction::RebuildLeaf>,
        last: bool,
    },
    /// One transaction of a `JOIN`'s per-side, per-leaf build
    /// ([`run_join_steps`]): `cursor` is what the job's `lo` holds after a
    /// step, none for the final one, which marks the job done.
    Join {
        piece: JoinPiece<'a>,
        cursor: Option<String>,
    },
}

/// The scope of `leaf` in `txn`: the leaf locked `ACCESS SHARE` and the tree
/// checked against the record, both held to the commit. A leaf that is gone,
/// or a tree that changed, is read through the root, one probe per group,
/// and the change's own correction repairs what it moved (ADR-0017).
async fn locked_leaf_scope(
    txn: &tokio_postgres::Transaction<'_>,
    names: &[&str],
    recorded: &crate::capture::partitions::Recorded,
    leaf: &super::partition_correction::RebuildLeaf,
) -> Result<super::group_basis::RebuildScope, ChunkQueueError> {
    let locked = super::partition_correction::lock_recorded_node(txn, leaf.oid, recorded).await?;
    let (node, read_node) = match locked {
        Some(locked) => (Some(locked.name), locked.as_recorded),
        None => (None, false),
    };
    Ok(super::partition_correction::leaf_scope(
        names, recorded, leaf, node, read_node,
    ))
}

/// One transaction of a group-basis rebuild ([`run_group_rebuild`]): the
/// structure's absolute write ([`super::group_basis::rebuild_definition`]),
/// and in the same transaction the job's progress (the leaf's oid as its
/// cursor, or its done mark when `structure` is the last), fenced by the
/// claim. Returns the visible rows written and deleted, or `None` when the
/// claim no longer holds.
async fn run_group_structure(
    pool: &Pool,
    client: &mut tokio_postgres::Client,
    chunk: &ClaimedChunk,
    fence: &ClaimFence<'_>,
    definition: &Definition,
    structure: &Structure<'_>,
    readers: ReadersOnCommit,
) -> Result<Option<(usize, usize)>, ChunkQueueError> {
    let started = Instant::now();
    let txn = client
        .build_transaction()
        .isolation_level(IsolationLevel::ReadCommitted)
        .start()
        .await?;
    txn.batch_execute(&format!(
        "set local application_name = '{CHUNK_APPLICATION_NAME}'"
    ))
    .await?;
    // The definition row before the job row, the order a resume takes them
    // in. A resume holds the definition row `for update`, waits on the job
    // row, and only then drops and recreates the hidden table, which takes
    // `ACCESS EXCLUSIVE` on a table this write holds rows of
    // (`complete_resume`): without this lock the two would wait on each
    // other. A resume or drop in flight makes this transaction wait holding
    // nothing, and one that committed first shows in the claim's stale
    // check. The readers' rows the commit's rebuild locks `for update`
    // (`ReadersOnCommit::Park`) come last, in id order.
    fence.arm(&txn).await?;
    txn.execute(
        "select 1 from transform_definitions where id = $1 for key share",
        &[&chunk.definition_id],
    )
    .await?;
    if !fence.hold(&txn).await? {
        txn.rollback().await?;
        return Ok(None);
    }
    let (scope, cursor) = match structure {
        Structure::Whole => (None, None),
        Structure::Partitioned {
            names,
            recorded,
            leaf: Some(leaf),
            ..
        } => (
            Some(locked_leaf_scope(&txn, names, recorded, leaf).await?),
            Some(leaf.oid.to_string()),
        ),
        Structure::Partitioned {
            names,
            recorded,
            leaves,
            leaf: None,
            ..
        } => (
            Some(super::partition_correction::remainder_scope(
                names, recorded, leaves,
            )),
            None,
        ),
        Structure::Join { piece, cursor } => {
            let scope = match piece {
                JoinPiece::Step(JoinStep::Leaf {
                    names,
                    recorded,
                    leaf,
                    ..
                }) => Some(locked_leaf_scope(&txn, names, recorded, leaf).await?),
                JoinPiece::Step(JoinStep::Remainder {
                    names,
                    recorded,
                    leaves,
                    ..
                }) => Some(super::partition_correction::remainder_scope(
                    names, recorded, leaves,
                )),
                JoinPiece::Step(JoinStep::Whole { .. }) | JoinPiece::Final => None,
            };
            (scope, cursor.clone())
        }
    };
    let (written, deleted) = if super::join::applies(&definition.def) {
        let work = match structure {
            Structure::Join {
                piece: JoinPiece::Step(step),
                ..
            } => {
                let (JoinStep::Whole { side }
                | JoinStep::Leaf { side, .. }
                | JoinStep::Remainder { side, .. }) = step;
                super::join::BuildWork::Side {
                    side: *side,
                    scope: scope.as_ref(),
                }
            }
            Structure::Join {
                piece: JoinPiece::Final,
                ..
            } => super::join::BuildWork::Final,
            _ => super::join::BuildWork::Single,
        };
        super::join::build(&txn, pool.schema(), definition, &work)
            .await
            .map_err(build_error)?
    } else {
        super::group_basis::rebuild_definition(&txn, pool.schema(), definition, scope.as_ref())
            .await
            .map_err(build_error)?
            .unwrap_or((0, 0))
    };
    let advances = match structure {
        Structure::Partitioned { last: false, .. } => cursor.is_some(),
        Structure::Join { cursor, .. } => cursor.is_some(),
        _ => false,
    };
    if let (true, Some(cursor)) = (advances, &cursor) {
        txn.execute(
            "update backfill_chunks set lo = $2 where id = $1",
            &[&chunk.id, cursor],
        )
        .await?;
    } else {
        txn.execute(
            "update backfill_chunks set done = true, claimed_by = null, claimed_at = null \
             where id = $1",
            &[&chunk.id],
        )
        .await?;
    }
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        &txn,
        super::interleave::PausePoint::BeforeRebuildCommit,
        &definition.target_table,
    )
    .await?;
    // The transaction that completes the whole build rebuilds its readers,
    // once, not per leaf; the segment stamp is the last
    // statement before the commit.
    let finished = !advances;
    let rebuilt = if finished && readers == ReadersOnCommit::Park {
        crate::intake::catch_up::park_target_catchup_if_read(&txn, chunk.definition_id)
            .await
            .map_err(build_error)?
    } else {
        Vec::new()
    };
    stamp_rebuild_seg(&txn, &rebuilt).await?;
    txn.commit().await?;
    metrics::record_build_chunk(started.elapsed(), (written + deleted) as u64);
    Ok(Some((written, deleted)))
}

/// Runs a claimed sweep job (see the module doc): [`sweep_batch`]es
/// of [`WorkerOptions::chunk_rows`] entries from the job's cursor, each in
/// its own `read committed` transaction with the cursor's advance, fenced by
/// the claim ([`ClaimFence`]), so a job that dies resumes after its last
/// committed batch. The batch that reads the ledger's end marks the job done.
/// Then [`try_complete`].
async fn run_sweep(
    pool: &Pool,
    chunk: &ClaimedChunk,
    claimed_by: &str,
    mut cursor: Option<String>,
    options: &WorkerOptions,
) -> Result<(), ChunkQueueError> {
    // Before anything the plan is made from ([`plan_epoch`]).
    let epoch = plan_epoch(&**pool.get().await?, chunk.definition_id).await?;
    let definition = catalog::definition_by_id(pool, chunk.definition_id)
        .await?
        .ok_or(ChunkQueueError::DefinitionNotFound {
            definition_id: chunk.definition_id,
        })?;
    if takes_group_basis(&definition) {
        return run_group_rebuild(
            pool,
            chunk,
            claimed_by,
            &definition,
            cursor.as_deref(),
            options,
        )
        .await;
    }
    let plan = one_to_one_plan(pool, &definition).await?;
    let _heartbeat = chunk_queue::ChunkHeartbeat::spawn(
        pool.clone(),
        chunk.id,
        claimed_by.to_string(),
        options.heartbeat_interval,
    );
    let fence = ClaimFence::new(chunk.id, claimed_by, options.reclaim_ttl);
    let mut client = pool.get().await?;
    let start_xid: String = client
        .query_one(
            "select start_xid::text from backfill_chunks where id = $1",
            &[&chunk.id],
        )
        .await?
        .get(0);
    loop {
        let started = Instant::now();
        let txn = client
            .build_transaction()
            .isolation_level(IsolationLevel::ReadCommitted)
            .start()
            .await?;
        txn.batch_execute(&format!(
            "set local application_name = '{CHUNK_APPLICATION_NAME}'"
        ))
        .await?;
        // The batches done so far keep their cursor; the rest plan again.
        if !hold_plan_epoch(&*txn, &fence, chunk.definition_id, epoch).await? {
            txn.rollback().await?;
            crate::instance_log::debug!(
                definition_id = chunk.definition_id,
                "re-derive sweep planned before an edit to its definition; planning it again"
            );
            return defer_claim(pool, chunk, claimed_by, Duration::ZERO).await;
        }
        if !fence.hold(&*txn).await? {
            txn.rollback().await?;
            return chunk_queue::discard_if_superseded(pool, chunk, claimed_by).await;
        }
        let scan = options.chunk_rows.max(1);
        let outcome = one_to_one::sweep_batch(&txn, &plan, &start_xid, cursor.as_deref(), scan)
            .await
            .map_err(build_error)?;
        let next = outcome.next.clone().or(cursor.clone());
        txn.execute(
            "update backfill_chunks set lo = $2, done = $3, \
                 claimed_by = case when $3 then null else claimed_by end, \
                 claimed_at = case when $3 then null else claimed_at end \
             where id = $1",
            &[&chunk.id, &next, &outcome.finished],
        )
        .await?;
        txn.commit().await?;
        metrics::record_build_chunk(started.elapsed(), outcome.rederived as u64);
        if outcome.finished {
            crate::instance_log::debug!(
                definition_id = chunk.definition_id,
                "re-derive build swept the last of its ledger"
            );
            break;
        }
        cursor = next;
    }
    try_complete(pool, chunk.definition_id).await?;
    Ok(())
}

/// Moves definition `id` `backfilling -> live` once its Re-derive build is
/// done (the strict flip): the plan job is done and no other chunk is
/// left undone (one held across a resume doesn't count, as for the old
/// build). Returns whether it flipped.
///
/// Every transaction that can make that true (a chunk's, the plan job's
/// last) calls this after it commits, so the last of them sees
/// all the others committed. It checks without a lock first, and only then
/// takes the definition row `for update` and checks again, so only a
/// possibly-last caller takes the lock. The definition applied from its
/// start. The flip deletes the definition's done
/// rows.
pub async fn try_complete(pool: &Pool, id: i64) -> Result<bool, ChunkQueueError> {
    let mut client = pool.get().await?;
    if !build_done(&**client, id).await? {
        return Ok(false);
    }
    let txn = client.transaction().await?;
    txn.execute(
        "select 1 from transform_definitions where id = $1 for update",
        &[&id],
    )
    .await?;
    if !build_done(&*txn, id).await? {
        txn.rollback().await?;
        return Ok(false);
    }
    // The build's rows are spent: every claim and check filters on `not
    // done`, so nothing reads a done row, and the completion checks above
    // scan all of the definition's rows to find the undone ones. Deleting
    // them here keeps the table, and what those checks scan, to one build's
    // rows however many builds the definition has had. It takes the
    // done rows of a build a resume superseded too. An undone row left is a
    // [`chunk_queue::STALE`] one a worker still holds, which is discarded
    // when the worker gives it up.
    txn.execute(
        "delete from backfill_chunks where definition_id = $1 and done",
        &[&id],
    )
    .await?;
    txn.execute(
        "update transform_definitions set status = $2, build = null where id = $1",
        &[&id, &TransformStatus::Live.as_str()],
    )
    .await?;
    // The rebuild repaired what its readers may have held keys for
    // (a copy holding values of two widths while it ran, say); the staging
    // worker retries them once this commits.
    super::quarantine::request_upstream_releases(&*txn, id).await?;
    txn.commit().await?;
    crate::instance_log::info!(
        definition_id = id,
        from = %TransformStatus::Backfilling.as_str(),
        to = %TransformStatus::Live.as_str(),
        "transform status transition: re-derive build finished"
    );
    Ok(true)
}

/// [`try_complete`]'s test: `id` is under a Re-derive build and no current
/// chunk of it is undone. A dropped target reads as not done.
async fn build_done(client: &impl GenericClient, id: i64) -> Result<bool, ChunkQueueError> {
    Ok(client
        .query_typed_opt(
            &format!(
                "select not exists ( \
                     select 1 from backfill_chunks bc \
                     where bc.definition_id = d.id and not bc.done \
                       and not ({}) \
                 ) \
                 from transform_definitions d \
                 where d.id = $1 and d.status = $2 and d.build = $3",
                chunk_queue::STALE,
            ),
            &[
                (&id, Type::INT8),
                (&TransformStatus::Backfilling.as_str(), Type::TEXT),
                (&BUILD_REDERIVE, Type::TEXT),
            ],
        )
        .await?
        .is_some_and(|row| row.get::<_, bool>(0)))
}

/// Runs every Re-derive build that is running to its end, in this task: a
/// synchronous stand-in for the drain workers' build work
/// ([`work_once`]) for a test with none running, which needs its builds
/// `live` before it goes on. It plans and runs chunks and sweeps
/// with no backpressure (the test's ring may hold sealed segments it means
/// to drain later), until no definition is under a Re-derive build. A
/// frozen one is left as it is. Panics on any failure, or when the builds
/// make no progress for a while (a chunk that keeps failing backs off): it
/// is test harness.
///
/// A field build whose plan job waits on its source's capture
/// ([`field_build_ready`]) can't go on until the staging worker's capture
/// pass, which this doesn't run: once that is all that is left, it returns
/// with the build still running.
#[cfg(any(test, feature = "internals"))]
pub async fn settle_builds(pool: &Pool) {
    const CLAIMED_BY: &str = "settle_rederive_builds";
    const STALL: Duration = Duration::from_secs(10);
    let options = WorkerOptions {
        chunk_rows: DEFAULT_CHUNK_ROWS,
        drain_batch_cap: usize::MAX,
        heartbeat_interval: Duration::from_secs(5),
        reclaim_ttl: Duration::from_secs(60),
    };
    let mut last_progress = Instant::now();
    // A field build's plan job that gave its claim back to wait on capture
    // is tried again now, once: the capture pass that may have readied it
    // ran before this call, not on the job's backoff.
    {
        let client = pool.get().await.expect("acquire a connection");
        client
            .execute(
                "update backfill_chunks set next_attempt_at = null \
                 where kind = $1 and fields is not null and lo is null and not done \
                   and claimed_by is null and last_error is null",
                &[&chunk_queue::KIND_PLAN],
            )
            .await
            .expect("retry the field builds waiting on capture");
    }
    loop {
        let building = building(pool).await.expect("list the running builds");
        if building.is_empty() {
            return;
        }
        let mut progressed = false;
        let claimed = {
            let client = pool.get().await.expect("acquire a connection");
            chunk_queue::claim_chunks_of(
                &**client,
                CLAIMED_BY,
                1,
                &[
                    chunk_queue::KIND_PLAN,
                    chunk_queue::KIND_REDERIVE,
                    chunk_queue::KIND_SWEEP,
                    chunk_queue::KIND_CORRECTION,
                ],
            )
            .await
            .expect("claim build work")
        };
        for chunk in &claimed {
            run_claimed(pool, chunk, CLAIMED_BY, &options).await;
            progressed |= !deferred(pool, chunk).await;
        }
        for id in &building {
            progressed |= try_complete(pool, *id).await.expect("complete a build");
        }
        if progressed {
            last_progress = Instant::now();
        } else if only_capture_waits(pool).await {
            return;
        } else {
            assert!(
                last_progress.elapsed() < STALL,
                "the running re-derive builds {building:?} made no progress for {STALL:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

/// Whether `chunk`, just run, gave its claim back to wait on capture or on
/// its fence ([`defer_claim`]), which [`settle_builds`] doesn't count as
/// progress.
#[cfg(any(test, feature = "internals"))]
async fn deferred(pool: &Pool, chunk: &ClaimedChunk) -> bool {
    let client = pool.get().await.expect("acquire a connection");
    client
        .query_one(
            "select exists (select 1 from backfill_chunks \
             where id = $1 and not done and claimed_by is null and lo is null \
               and (fields is not null or fence_xid is not null or kind = 'correction') \
               and next_attempt_at > now())",
            &[&chunk.id],
        )
        .await
        .expect("read a build row")
        .get(0)
}

/// Whether every unfinished row of a running build is a field build's plan
/// job, not yet started, whose fields still await their source's capture
/// ([`settle_builds`]'s stop).
#[cfg(any(test, feature = "internals"))]
async fn only_capture_waits(pool: &Pool) -> bool {
    let client = pool.get().await.expect("acquire a connection");
    client
        .query_one(
            &format!(
                "select not exists ( \
                     select 1 from backfill_chunks bc \
                     join transform_definitions d on d.id = bc.definition_id \
                     where not bc.done and not ({}) \
                       and not (bc.kind = $1 and bc.fields is not null and bc.lo is null \
                                and exists (select 1 from column_status cs \
                                            where cs.transform_table = \
                                                  split_part(d.target_table, '.', 2) \
                                              and cs.column_name = any(bc.fields) \
                                              and cs.awaiting_capture))) \
                 and exists (select 1 from backfill_chunks bc where not bc.done)",
                chunk_queue::STALE
            ),
            &[&chunk_queue::KIND_PLAN],
        )
        .await
        .expect("read the running builds' rows")
        .get(0)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    /// A `JOIN` build's steps: unpartitioned sides whole (`FROM` first), then
    /// each partitioned side's leaves in the given order and its remainder;
    /// a cursor resumes after the step it names, and one it doesn't know
    /// starts over.
    #[test]
    fn join_steps_order_and_resume_after_the_cursor() {
        use crate::defs::join::Side::{From, Joined};
        let steps = join_steps(&[(From, vec![11, 12])]);
        assert_eq!(
            steps,
            [
                JoinStepId::Whole(Joined),
                JoinStepId::Leaf(From, 11),
                JoinStepId::Leaf(From, 12),
                JoinStepId::Remainder(From),
            ]
        );
        let cursors: Vec<String> = steps.iter().map(|s| s.cursor()).collect();
        assert_eq!(
            cursors,
            ["joined:whole", "from:11", "from:12", "from:remainder"]
        );
        assert_eq!(join_steps_after(&steps, None), &steps[..]);
        assert_eq!(join_steps_after(&steps, Some("from:11")), &steps[2..]);
        assert_eq!(
            join_steps_after(&steps, Some("from:remainder")),
            &[] as &[JoinStepId]
        );
        assert_eq!(
            join_steps_after(&steps, Some("from:99")),
            &steps[..],
            "a leaf the tree no longer has starts over"
        );
        assert_eq!(
            join_steps(&[(Joined, vec![5]), (From, vec![7])]),
            [
                JoinStepId::Leaf(From, 7),
                JoinStepId::Remainder(From),
                JoinStepId::Leaf(Joined, 5),
                JoinStepId::Remainder(Joined),
            ],
            "both sides partitioned: FROM first"
        );
        assert_eq!(
            join_steps(&[(Joined, vec![])]),
            [JoinStepId::Whole(From), JoinStepId::Remainder(Joined)]
        );
    }

    /// A 1-1 target's Re-derive chunk: a plain 1-1 `x + x` over a source of
    /// several chunks that overflows on one row. The failing chunk splits until the key fails
    /// alone, the key is quarantined, and the build goes `live` without it.
    #[tokio::test]
    async fn a_one_to_one_chunk_that_fails_on_its_data_quarantines_the_key() {
        use crate::defs::ast::ValueType;
        use crate::integer::IntWidth;

        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let config = crate::config::Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
        let pool = Pool::new(&config).expect("build a same-crate pool");
        let trellis = crate::app::Trellis::connect(config, crate::app::TrellisOptions::default())
            .await
            .expect("connect");
        let (mut raw, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        raw.batch_execute(&format!(
            "set search_path to {}, public; \
             create table public.nums (id bigint primary key, x integer); \
             insert into public.nums select i, i from generate_series(1, 300) i; \
             update public.nums set x = 2147483647 where id = 150",
            crate::config::DEFAULT_SCHEMA
        ))
        .await
        .expect("seed the source");
        let columns = HashMap::from([
            ("id".to_string(), ValueType::Integer(IntWidth::Int8)),
            ("x".to_string(), ValueType::Integer(IntWidth::Int4)),
        ]);
        crate::defs::install_definition(
            &pool,
            "TRANSFORM doubles FROM nums SELECT x + x AS doubled",
            &columns,
            "public",
        )
        .await
        .expect("install_definition");
        crate::client::reconcile_pass(&mut raw, &pool, crate::config::DEFAULT_SCHEMA)
            .await
            .expect("start the build");
        let status = || async {
            trellis
                .status("doubles")
                .await
                .expect("status")
                .expect("the definition exists")
        };
        assert_eq!(status().await.status, TransformStatus::Backfilling);

        let options = WorkerOptions {
            chunk_rows: 100,
            drain_batch_cap: 100_000,
            heartbeat_interval: Duration::from_secs(1),
            reclaim_ttl: Duration::from_secs(30),
        };
        let mut failure = None;
        for _ in 0..200 {
            let step = work_once(&pool, "worker", &options)
                .await
                .expect("a build step");
            if failure.is_none() {
                failure = status().await.backfill_failure;
            }
            if !step.progressed() {
                break;
            }
        }
        let failure = failure.expect("the failing chunk was on the status while it narrowed");
        assert!(
            failure.last_error.contains("out of range"),
            "{}",
            failure.last_error
        );
        let status = status().await;
        assert_eq!(status.status, TransformStatus::Live);
        assert_eq!(status.backfill_failure, None);
        let target = raw
            .query_one(
                "select count(*), count(*) filter (where id = 150), \
                        count(*) filter (where doubled = 2 * id) \
                 from public.doubles",
                &[],
            )
            .await
            .expect("read the target");
        assert_eq!(target.get::<_, i64>(0), 299);
        assert_eq!(target.get::<_, i64>(1), 0, "the failing key is left out");
        assert_eq!(target.get::<_, i64>(2), 299);
        let quarantined = trellis
            .sample_quarantined("doubles", None, 10)
            .await
            .expect("sample the quarantined keys");
        assert_eq!(quarantined.len(), 1);
        assert_eq!(quarantined[0].key, "150");
    }
}
