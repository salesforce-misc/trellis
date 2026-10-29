//! The other half of a definition's lifecycle (ADR-0014):
//! **pause** and **drop**.
//!
//! ```text
//! [*] --define--> Backfilling --> Live --pause--> Paused --drop--> [*]
//!                                   ^                 |
//!                                   +---- resume -----+   (a fresh backfill,
//!                                                          not a catch-up)
//! ```
//!
//! Three things this module deliberately does *not* do, each straight out of
//! the ADR:
//!
//! 1. **It does not invent a freezing mechanism.** Pausing writes
//!    [`TransformStatus::Paused`] into the same `transform_definitions.status`
//!    column the poison fuse already writes
//!    [`TransformStatus::Quarantined`] into, and the claim-time fold stops
//!    dispatching to the target for exactly the reason it already stops for a
//!    quarantined one: [`super::catalog::dependents_of`]'s applying-status
//!    predicate ([`TransformStatus::is_applying`]), the single gate every
//!    target resolution goes through. The one place that gate did not
//!    previously reach is the durable
//!    backfill-chunk queue, which is why
//!    [`super::chunk_queue::claim_chunks`] now consults the same column —
//!    same gate, extended to the one dispatch path that was outside it, not a
//!    second gate.
//!
//! 2. **It does not delete shared state.** A drop removes the target's data —
//!    always, there is no keep-the-rows option — and otherwise only what the
//!    *target* owns: its `transform_definitions` row (whose `on delete
//!    cascade` takes `backfill_chunks` with it), the edges leading *into* its schema node,
//!    and its per-column quarantine bookkeeping (`column_status`,
//!    `column_deaths`, `column_failures`, `column_pause_cascades`). Its
//!    whole-key quarantine (`poison`, `poison_held`, `key_deaths` and its
//!    `transform_fuse_gate` row) is per transform and goes with
//!    the definition row by `on delete cascade` too. Everything keyed to the
//!    *source* table and co-owned with sibling definitions — ring segments,
//!    claims, and the siblings' own quarantine — is untouched, because a
//!    sibling reading the same source is still using it and a drain worker
//!    may be mid-batch over it right now. A chunk a worker holds at drop time
//!    is released on its own heartbeat/TTL, never force-cleared.
//!
//! 3. **It does not cascade.** A definition chaining off the target being
//!    dropped refuses the drop and is named in the error
//!    ([`CatalogError::DependentsBlockDrop`]); the operator retires
//!    dependents first, in reverse dependency order. *Any* registered
//!    dependent blocks, whatever its status — see `drop_transform`'s doc
//!    comment for why a frozen or mid-backfill one is no safer to strand than
//!    a live one.
//!
//! Both verbs are **idempotent and non-transactional with any host
//! migration** (ADR-0014, "Pause and drop are idempotent"): they run on
//! Trellis's own pooled connections, so a framework migration that is
//! replayed or rolled back has to be safe in both directions. Pausing an
//! already-frozen definition succeeds as a no-op and dropping an absent one
//! succeeds as a no-op, so the two questions a replayed migration cannot
//! answer for itself — "did the pause land?", "is it already gone?" — both
//! resolve to success.
//!
//! **A drop never touches a source table.** It only removes catalog rows.
//! The staging worker's reconcile pass uninstalls capture from the catalog,
//! its only source of truth for what to capture (ADR-0002), so
//! a process that drops a transform needs no privileges on the source
//! tables.

use tokio_postgres::Transaction;

use super::catalog::CatalogError;
use super::model::{EdgeKind, TransformStatus};
use crate::pool::{Pool, quote_ident};

/// What [`pause_transform`] actually did — returned so a caller (and a test)
/// can tell a real transition from the idempotent no-op, even though both are
/// successes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PauseOutcome {
    /// The definition was live (or catching up, backfilling, or waiting to
    /// backfill) and is now [`TransformStatus::Paused`].
    Paused,
    /// The definition was already frozen — by an earlier pause, or by the
    /// poison fuse — and was left exactly as it was. ADR-0014's idempotency
    /// clause: this is a success, not a conflict.
    AlreadyFrozen(TransformStatus),
    /// The definition was already frozen, but waiting to be resumed: on its
    /// upstream's resume (`capture_failures.caused_by`), or on a
    /// resume request (`resume_requests`: an operator's `RESUME` waiting for
    /// an upstream to be live or for its copies to be re-typed). The
    /// pause withdrew that resume, so it stays paused like any operator
    /// pause.
    Withdrawn(TransformStatus),
}

/// What [`drop_transform`] actually did — same "distinguish the no-op from
/// the real thing without failing either" contract as [`PauseOutcome`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DropOutcome {
    /// The definition existed, was paused, and is gone.
    Dropped,
    /// No definition by that name was registered. ADR-0014's idempotency
    /// clause: dropping an absent definition is a success.
    Absent,
}

/// Freezes `target` — ADR-0014's deliberate, operator-driven half of the
/// pause state whose other half is the poison fuse's
/// [`crate::staging::quarantine::trip_transform_fuse_if_crossed`].
///
/// `target` is the transform's name, qualified or bare, as every other
/// operator-facing entry point in this crate takes it
/// ([`crate::staging::quarantine::resume_transform`],
/// [`crate::Trellis::status`]); see [`super::catalog::target_match_sql`].
///
/// Idempotent: a definition already [`TransformStatus::Paused`] *or*
/// [`TransformStatus::Quarantined`] keeps its status and is reported as
/// [`PauseOutcome::AlreadyFrozen`], unless it is waiting to be resumed: then
/// the pause withdraws that resume ([`withdraw_resume`]) and reports
/// [`PauseOutcome::Withdrawn`]. A quarantined definition is deliberately
/// **not** overwritten to `paused`: it is already frozen (which is all a
/// pause asks for), and flattening the poison trigger into an operator one
/// would erase the reason from [`crate::Trellis::quarantined`]'s report while
/// changing nothing about the definition's actual behaviour.
///
/// A definition still `backfilling` or `waiting_to_backfill` pauses fine —
/// the pause is a freeze on *dispatch*, and both of those states dispatch
/// (the chunk queue, and the build start respectively). Not
/// restricting the pause to `live` is what lets an operator stop a runaway
/// initial build, which is exactly the situation a pause exists for.
///
/// A target with no `transform_definitions` row at all is
/// [`CatalogError::TransformNotFound`], not a no-op: you cannot freeze
/// something that was never defined, and unlike a drop there is no
/// replayed-migration reading under which "it isn't there" is the outcome the
/// caller wanted.
#[tracing::instrument(name = "lifecycle.pause_transform", skip(pool), fields(transform = %target))]
pub(crate) async fn pause_transform(
    pool: &Pool,
    target: &str,
) -> Result<PauseOutcome, CatalogError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;

    let Some(locked) = locked_definition(&txn, target).await? else {
        return Err(CatalogError::TransformNotFound {
            transform: target.to_string(),
        });
    };
    let (id, status) = (locked.id, locked.status);

    if status.is_frozen() {
        if withdraw_resume(&txn, id).await? {
            txn.commit().await?;
            crate::instance_log::info!(
                transform = %target,
                status = %status.as_str(),
                "pause withdrew the transform's pending resume; it stays paused"
            );
            return Ok(PauseOutcome::Withdrawn(status));
        }
        // Commit rather than roll back: nothing was written, and committing
        // releases the `for update` lock the same way the mutating arm does.
        txn.commit().await?;
        crate::instance_log::info!(
            transform = %target,
            status = %status.as_str(),
            "pause is a no-op; transform was already frozen"
        );
        return Ok(PauseOutcome::AlreadyFrozen(status));
    }

    txn.execute(
        "update transform_definitions set status = $1 where id = $2",
        &[&TransformStatus::Paused.as_str(), &id],
    )
    .await?;
    txn.commit().await?;

    crate::instance_log::info!(
        transform = %target,
        from = %status.as_str(),
        to = %TransformStatus::Paused.as_str(),
        "transform paused"
    );
    Ok(PauseOutcome::Paused)
}

/// Withdraws frozen definition `id`'s pending resume, in the pause's
/// transaction, under its row lock: the resume request
/// (`resume_requests`) and the upstream cause (`capture_failures.caused_by`)
/// that the staging worker's capture pass would otherwise complete
/// (`staging::quarantine::finish_requested_resumes`,
/// `resume_caused_definitions`). Both resumes take the definition's row lock
/// and re-read what they act on under it, so whichever commits first wins.
///
/// Its `capture_failure` stops saying it resumes: a cause's text keeps the
/// reason the upstream's resume gave and ends with how to resume it; a
/// request's (`resuming: ...`) replaced the reason it was paused for when
/// the operator resumed it, and is replaced in turn. Returns whether there
/// was anything to withdraw.
async fn withdraw_resume(txn: &Transaction<'_>, id: i64) -> Result<bool, CatalogError> {
    let requested = txn
        .execute(
            "delete from resume_requests where transform_id = $1",
            &[&id],
        )
        .await?
        == 1;
    let record = txn
        .query_opt(
            "select error, caused_by is not null from capture_failures where transform_id = $1",
            &[&id],
        )
        .await?;
    let caused = record.as_ref().is_some_and(|row| row.get::<_, bool>(1));
    if !requested && !caused {
        return Ok(false);
    }
    let error = match record {
        Some(row) if caused => {
            let error: String = row.get(0);
            let reason = error
                .split_once(crate::staging::schema_change::CAUSED_RESUMES_ON_ITS_OWN)
                .map_or(error.as_str(), |(reason, _)| reason);
            format!("{reason}{WITHDRAWN_CAUSE}")
        }
        _ => WITHDRAWN_REQUEST.to_string(),
    };
    txn.execute(
        "update capture_failures set error = $2, caused_by = null, detected_at = now() \
         where transform_id = $1",
        &[&id, &error],
    )
    .await?;
    Ok(true)
}

/// What [`withdraw_resume`] appends to the reason an upstream's resume gave
/// for pausing a definition, in place of the sentence saying it resumes on
/// its own.
const WITHDRAWN_CAUSE: &str = " It was paused by the operator, so it stays paused: resume it to \
     bring its columns to the new types and rebuild it, or drop the definition and define it again";

/// The `capture_failure` of a definition whose resume request a pause
/// withdrew ([`withdraw_resume`]).
const WITHDRAWN_REQUEST: &str = "paused by the operator, which withdrew its resume before the \
     staging worker finished it. Resume it to rebuild it";

/// Pauses definition `id` because a capture function found `columns` of
/// `source_table` renamed or dropped, or the capture pass found
/// its primary key redefined, and records why in `capture_failures`:
/// `error` is the sentence `Trellis::status` reports. The record is what `Trellis::status` reports as
/// `capture_failure`, and what makes capture stop imaging for the
/// definition (`capture::columns`). Runs in the caller's transaction, which
/// the drain commits before it computes anything that could apply to it.
///
/// A definition already frozen keeps its status (a quarantined one stays
/// quarantined) but still gets the record, since its resume is the same
/// rebuild. An existing record is kept, so the first detection stands.
/// Returns whether this call paused it.
///
/// `caused_by` is the definition whose resume re-typed the columns of its
/// target that this pause is about, when that resume is what pauses it
/// (#828, `staging::schema_change::pause_readers_of_retyped`), and is
/// recorded with the pause.
pub(crate) async fn pause_for_capture_failure(
    txn: &Transaction<'_>,
    id: i64,
    source_table: &str,
    columns: &[String],
    error: &str,
    caused_by: Option<i64>,
) -> Result<bool, CatalogError> {
    pause_with_record(txn, id, source_table, columns, error, "capture", caused_by).await
}

/// Pauses definition `id` because Trellis's capture of a table it reads was
/// changed outside Trellis, or the table is gone. The record's
/// `kind` is `disruption`, which is what a test helper selects to resume
/// exactly these. Returns whether this call paused it.
///
/// A definition already frozen (an operator's `PAUSE`, say) gets the record
/// with kind `capture` instead, so it stays the operator's pause, which a
/// helper that resumes disruptions never resumes. It still gets a record:
/// the record is what makes its resume forget the table's recorded capture
/// (`staging::quarantine`), so the next pass reinstalls capture instead of
/// finding the disruption again and pausing it once more. Its row is locked
/// before its status is read, so a `PAUSE` committing meanwhile is seen and
/// never recorded as a disruption.
pub(crate) async fn pause_for_disruption(
    txn: &Transaction<'_>,
    id: i64,
    source_table: &str,
    error: &str,
) -> Result<bool, CatalogError> {
    let Some(row) = txn
        .query_opt(
            "select status = any($2) from transform_definitions where id = $1 for update",
            &[&id, &TransformStatus::dispatchable()],
        )
        .await?
    else {
        return Ok(false);
    };
    let kind = if row.get::<_, bool>(0) {
        "disruption"
    } else {
        "capture"
    };
    pause_with_record(txn, id, source_table, &[], error, kind, None).await
}

/// Pauses definition `id` because the database was restored from a logical
/// dump (`capture::restore`): its ordering state belongs to the
/// original database, so the record is marked `restored` and its resume resets
/// that state before the rebuild. Kind `disruption` for a definition that was
/// running (a test helper resumes those), `capture` for one already frozen,
/// which stays the operator's pause but resumes with the reset. An existing
/// record keeps its reason, which the restore's is put before. Returns
/// whether this call paused it.
pub(crate) async fn pause_for_restore(
    txn: &Transaction<'_>,
    id: i64,
    source_table: &str,
    error: &str,
) -> Result<bool, CatalogError> {
    let Some(row) = txn
        .query_opt(
            "select status = any($2) from transform_definitions where id = $1 for update",
            &[&id, &TransformStatus::dispatchable()],
        )
        .await?
    else {
        return Ok(false);
    };
    let kind = if row.get::<_, bool>(0) {
        "disruption"
    } else {
        "capture"
    };
    txn.execute(
        "insert into capture_failures (transform_id, source_table, columns, error, kind, restored) \
         values ($1, $2, '{}', $3, $4, true) \
         on conflict (transform_id) do update \
         set restored = true, caused_by = null, \
             error = case when capture_failures.restored then capture_failures.error \
                          else excluded.error || ' It was already paused: ' \
                               || capture_failures.error end",
        &[&id, &source_table, &error, &kind],
    )
    .await?;
    Ok(txn
        .execute(
            "update transform_definitions set status = $2 where id = $1 and status = any($3)",
            &[
                &id,
                &TransformStatus::Paused.as_str(),
                &TransformStatus::dispatchable(),
            ],
        )
        .await?
        == 1)
}

/// Pauses definition `id` because the enum values it reads changed identity
/// (ADR-0020): an enum type it reads was relabelled (`ALTER TYPE ... RENAME
/// VALUE`), or a column it reads was re-typed from one enum to
/// another. The label text Trellis staged and keyed by names no
/// value of the column's type. The record's
/// `relabel_lsn` is `lsn`, the WAL insert position the capture pass read
/// after finding it, raised by a later such change; its resume waits until no
/// change at or below it is pending on the tables the definition reads, and
/// rebuilds from the source (`staging::quarantine`). Kind `capture`. A
/// definition already frozen keeps its status but gets the record, so its
/// resume is the relabel's. An existing record's reason is kept, the
/// relabel's put before it the first time. Returns whether this call paused
/// it.
pub(crate) async fn pause_for_relabel(
    txn: &Transaction<'_>,
    id: i64,
    source_table: &str,
    error: &str,
    lsn: crate::PgLsn,
) -> Result<bool, CatalogError> {
    txn.execute(
        "insert into capture_failures \
             (transform_id, source_table, columns, error, kind, relabel_lsn) \
         select id, $2, '{}', $3, 'capture', $4 from transform_definitions where id = $1 \
         on conflict (transform_id) do update \
         set relabel_lsn = greatest(capture_failures.relabel_lsn, excluded.relabel_lsn), \
             error = case when capture_failures.relabel_lsn is not null \
                          then capture_failures.error \
                          else excluded.error || ' It was already paused: ' \
                               || capture_failures.error end",
        &[&id, &source_table, &error, &lsn],
    )
    .await?;
    Ok(txn
        .execute(
            "update transform_definitions set status = $2 where id = $1 and status = any($3)",
            &[
                &id,
                &TransformStatus::Paused.as_str(),
                &TransformStatus::dispatchable(),
            ],
        )
        .await?
        == 1)
}

/// Pauses definition `id` because it was written under a grammar version
/// outside this binary's window (ADR-0018), recording `error` as its
/// `capture_failure`. Returns whether this call paused it.
pub(crate) async fn pause_for_grammar_version(
    txn: &Transaction<'_>,
    id: i64,
    source_table: &str,
    error: &str,
) -> Result<bool, CatalogError> {
    pause_with_record(txn, id, source_table, &[], error, "capture", None).await
}

/// [`pause_for_capture_failure`] for a drain's halt
/// (`staging::halt`): pauses definition `id` because a page failed with a
/// halting error on `source_table` that it reads, directly or downstream of
/// a definition that does. The record's
/// `kind` is `halt`. Returns whether this call paused it.
pub(crate) async fn pause_for_halt(
    txn: &Transaction<'_>,
    id: i64,
    source_table: &str,
    error: &str,
) -> Result<bool, CatalogError> {
    pause_with_record(txn, id, source_table, &[], error, "halt", None).await
}

/// Records why `id` pauses in `capture_failures`, under `kind` and with
/// its upstream cause, unless a record already stands, and pauses it unless
/// it's frozen. Returns whether it paused it.
async fn pause_with_record(
    txn: &Transaction<'_>,
    id: i64,
    source_table: &str,
    columns: &[String],
    error: &str,
    kind: &str,
    caused_by: Option<i64>,
) -> Result<bool, CatalogError> {
    txn.execute(
        "insert into capture_failures (transform_id, source_table, columns, error, kind, caused_by) \
         select id, $2, $3, $4, $5, $6 from transform_definitions where id = $1 \
         on conflict (transform_id) do nothing",
        &[&id, &source_table, &columns, &error, &kind, &caused_by],
    )
    .await?;
    let paused = txn
        .execute(
            "update transform_definitions set status = $2 \
             where id = $1 and status = any($3)",
            &[
                &id,
                &TransformStatus::Paused.as_str(),
                &TransformStatus::dispatchable(),
            ],
        )
        .await?
        == 1;
    Ok(paused)
}

/// Removes `target`'s definition — ADR-0014's terminal reap.
///
/// **Only ever acts on a frozen definition.** There is no direct live-to-gone
/// edge in the lifecycle: quiescing first is a precondition, so the removal
/// never has to reason about a fold still dispatching to the target. A
/// definition that is not [`TransformStatus::Paused`] or
/// [`TransformStatus::Quarantined`] is refused with
/// [`CatalogError::TransformNotPaused`]; pause it first.
///
/// **Always removes the associated data.** The target table is dropped, full
/// stop — there is no option to retire the definition while keeping its rows.
/// Keeping derived rows after removing the definition that explains them has
/// no use worth naming, and the paused state already serves the caller who
/// wants the data to stick around unmaintained: leave it paused rather than
/// dropping it. Because the target table is Trellis-owned, dropping it is
/// Trellis's to do; the source table is the user's and is untouched.
///
/// **Refuses rather than cascades** when a definition still chains off this
/// target ([`CatalogError::DependentsBlockDrop`], naming them). *Any*
/// registered dependent blocks, not only a live one: the
/// refusal's question is "is this definition still registered and could it
/// still need the target?", and every status other than gone answers yes.
/// A `backfilling`/`waiting_to_backfill` dependent is building *from* the
/// target right now, so dropping it out from under the build fails it
/// mid-flight or leaves it holding garbage; a `paused` or `quarantined` one
/// is worse, because ADR-0014's resume rebuilds by a fresh backfill from
/// source — take that source away and the dependent can never be resumed at
/// all. The operator retires a chain from the leaves inward, which is exactly
/// reverse dependency order.
///
/// **Everything — the status precondition, the dependency refusal, and every
/// write — happens in one transaction, under the target's own `for update`
/// row lock**. The checks used to run on separate pooled
/// connections before the transaction opened, leaving a window in which a
/// concurrent `resume` could unfreeze the row, or a concurrent `define`
/// register a fresh dependent, after the check and before the commit. A
/// refusal is still write-free: it returns before any statement mutates
/// anything, and the transaction is rolled back on the way out.
///
/// Idempotent: an unregistered `target` is [`DropOutcome::Absent`], a
/// success.
#[tracing::instrument(name = "lifecycle.drop_transform", skip(pool), fields(transform = %target))]
pub(crate) async fn drop_transform(pool: &Pool, target: &str) -> Result<DropOutcome, CatalogError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;

    // The version fence of every table the definition reads, bumped as this
    // transaction's first lock. A frozen definition is out of
    // every page planned from now on, but a page that planned it before it
    // froze (a pause by the operator, or a halt or quarantine from a peer
    // drain) may still be in flight, and would fail on the tables dropped
    // below. The bump waits for every page holding the fence `for share`
    // (Phase 3 holds it to its commit), and a page that reaches the fence
    // after this commit misses it and plans again, without the definition.
    // A `JOIN` reads two tables, and a page of either may have planned it.
    // The names are read without a lock and bumped in table-name
    // order, the order every taker of two fences takes them in.
    let mut fences: Vec<String> = txn
        .query(
            &format!(
                "select distinct f from transform_definitions, \
                     unnest(array[source_table, join_table]) f \
                 where f is not null and {}",
                super::catalog::target_match_sql("target_table", 1),
            ),
            &[&target],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    fences.sort_unstable();
    for source in &fences {
        crate::staging::build::bump_version_fence(&*txn, source).await?;
    }

    // The column-pause lock, exclusive: this deletes the
    // target's pause rows and the cascade edges into and out of them, which
    // a column resume deletes too, in walk order, and a define reads. After
    // the fences, so it precedes the definition row below, as every taker of
    // the lock orders it (`crate::locks::lock_column_pauses`).
    crate::locks::lock_column_pauses(
        &*txn,
        pool.schema(),
        crate::locks::ColumnPauseLock::Exclusive,
        crate::locks::ColumnPauseOp::Drop,
    )
    .await?;

    // Read under `for update` and check *here*, not on a pooled connection
    // before this transaction opened: pause/drop are explicitly not
    // transactional with a host migration, so a concurrent resume could
    // otherwise unfreeze the row between a pre-check and this commit. Losing
    // that race must not silently drop a live definition.
    let Some(locked) = locked_definition(&txn, target).await? else {
        crate::instance_log::info!(transform = %target, "drop is a no-op; no such definition");
        return Ok(DropOutcome::Absent);
    };
    let (id, status, qualified) = (locked.id, locked.status, locked.target_table);
    // Everything below is keyed by the bare name, whichever spelling the
    // caller used.
    let target = qualified
        .split_once('.')
        .map_or(qualified.as_str(), |(_, t)| t);

    if !status.is_frozen() {
        return Err(CatalogError::TransformNotPaused {
            transform: target.to_string(),
            status,
        });
    }

    // Before any write, and inside the same transaction as the writes, so a
    // `define` that registers a new dependent cannot interleave between the
    // refusal check and the commit.
    let (blockers, column_detail) = dependency_blockers(&txn, target, &qualified, id).await?;
    if !blockers.is_empty() {
        return Err(CatalogError::DependentsBlockDrop {
            subject: target.to_string(),
            dependents: blockers,
            column_detail,
        });
    }

    // ADR-0014, "Quarantine state follows its owner": the target-keyed
    // per-column quarantine bookkeeping goes with the data it describes —
    // its forensic value is about rows that are about to stop existing (or
    // stop being maintained). These four tables are bare-transform-keyed
    // (they are not FK'd, so this has to be an explicit delete rather than a
    // cascade).
    //
    // The whole-key quarantine (`poison`, `poison_held`, `key_deaths`,
    // `transform_fuse_gate`) is keyed by the definition's id, and
    // goes with its row below by `on delete cascade`. A sibling on the same
    // source keeps its own.
    for table in ["column_status", "column_deaths", "column_failures"] {
        txn.execute(
            &format!("delete from {table} where transform_table = $1"),
            &[&target],
        )
        .await?;
    }
    txn.execute(
        "delete from column_pause_cascades \
         where downstream_transform = $1 or upstream_transform = $1",
        &[&target],
    )
    .await?;

    // `backfill_chunks` rides this out on its `on delete cascade`
    // — per-definition work, keyed to the
    // definition, removed with it. A chunk another worker holds right now is
    // deleted here too, but that is safe by construction: `finish_chunk`
    // locks its `transform_definitions` row and raises
    // `ChunkQueueError::DefinitionNotFound` when it is gone, and the worker's
    // write targets a table this same transaction is about to drop.
    //
    // Deleted *before* the node cleanup below: the node reap's
    // own `not exists (... transform_definitions ...)` guard has to run once
    // this row is actually gone, or it always finds itself still there and
    // never reaps the node — every drop would leave an orphaned
    // `schema_nodes` row behind. Purely an ordering fix; both deletes commit
    // together in this same transaction either way.
    txn.execute("delete from transform_definitions where id = $1", &[&id])
        .await?;

    // Edges *into* the target node are the ones this definition owns: its
    // `source` edge from its own source table, plus a `JOIN`'s `join` edge
    // from its joined table. Edges *out of* the target node
    // belong to whatever chains off it and are left alone.
    if let Some(node_id) = node_id_for(&txn, &qualified).await? {
        txn.execute(
            "delete from schema_edges where to_node_id = $1 and kind in ('source', 'join')",
            &[&node_id],
        )
        .await?;
        // The node itself only goes once nothing at all still names it — it
        // may well still be some other definition's source. This has to run
        // after the `transform_definitions` delete above, or the `not
        // exists` below always sees the row being dropped and never reaps
        // the node.
        txn.execute(
            "delete from schema_nodes n \
             where n.id = $1 \
               and not exists ( \
                   select 1 from schema_edges e \
                   where e.from_node_id = n.id or e.to_node_id = n.id \
               ) \
               and not exists ( \
                   select 1 from transform_definitions t \
                   where t.target_table = n.table_name or t.source_table = n.table_name \
               )",
            &[&node_id],
        )
        .await?;
    }

    // The data goes with the definition, unconditionally (ADR-0014, "Drop
    // always removes the associated data"). The target table is Trellis-owned,
    // so this is Trellis's to drop; a caller who wants the derived rows to
    // survive unmaintained leaves the definition paused instead of dropping it.
    txn.batch_execute(&format!(
        "drop table if exists {}; drop table if exists {}",
        quote_qualified(&qualified),
        quote_qualified(&super::ledger::ledger_table_name(&qualified)),
    ))
    .await?;
    // The hidden tables Trellis keeps in its catalog
    // schema for a definition are named by its id, and go with it: a JOIN's
    // and a group-basis aggregate's.
    txn.batch_execute(&super::join::drop_hidden_tables_sql(pool.schema(), id))
        .await?;
    txn.batch_execute(&crate::staging::group_basis::drop_hidden_table_sql(
        pool.schema(),
        id,
    ))
    .await?;
    // A 1-1 JOIN's `R`, `M`, `J` and lock row. Dropping every kind's tables
    // is harmless: each statement is `if exists`.
    txn.batch_execute(&super::join_one_to_one::drop_hidden_tables_sql(
        pool.schema(),
        id,
    ))
    .await?;
    // The target's ledger went with it
    // just above. Its truncate floor is keyed by the target's
    // name, so without this a later target of the same name would inherit
    // it.
    // A 1-1 JOIN's hidden ledgers `R` and `M` went with the tables above;
    // their floors are keyed by their quoted idents.
    let mut floors = super::join_one_to_one::HiddenTable::floor_keys(pool.schema(), id);
    floors.push(qualified.to_string());
    txn.execute(
        "delete from ledger_truncate_floor where target_table = any($1)",
        &[&floors],
    )
    .await?;

    // This definition may have been the last to read its tables.
    super::catalog::forget_unread_capture_records(&*txn).await?;

    txn.commit().await?;
    crate::instance_log::info!(
        transform = %target,
        target_table = %qualified,
        "transform dropped"
    );
    Ok(DropOutcome::Dropped)
}

/// Every still-registered definition that chains off the bare-named `target`
/// — ADR-0014's refusal set, gathered on `txn` so [`drop_transform`] can
/// run it inside the very transaction that does the removal.
///
/// `qualified_target` is the target's `"schema.table"` identity (the key
/// `schema_nodes` is on);`definition_id` is the target's own
/// `transform_definitions.id`, excluded from the result so a definition whose
/// source node and target node happen to be the same row cannot block its own
/// drop — previously impossible to hit only because the check was live-only
/// and a drop's subject is always frozen.
///
/// Returns blocker names, bare transform names, sorted and deduplicated.
async fn dependency_blockers(
    txn: &Transaction<'_>,
    target: &str,
    qualified_target: &str,
    definition_id: i64,
) -> Result<(Vec<String>, Vec<String>), CatalogError> {
    let mut blockers = source_edge_dependents(txn, qualified_target, definition_id).await?;
    blockers.sort();
    blockers.dedup();

    // For naming precision only (every blocker found here is
    // already in `blockers` above, by the DAG-property argument
    // `catalog::column_dependents`'s own doc comment makes) — for each of
    // this target's own declared columns, ask the same column-granularity
    // dependency-edge infrastructure `ALTER TRANSFORM ... DROP <field>`
    // uses for its own, load-bearing check, so a `DROP
    // TRANSFORM` refusal can name the exact column a dependent reads instead
    // of only "something reads this table."
    let column_detail = match txn
        .query_opt(
            "select definition_text, grammar_version from transform_definitions where id = $1",
            &[&definition_id],
        )
        .await?
    {
        Some(row) => {
            let text: String = row.get(0);
            match super::grammar::load(&text, row.get(1)) {
                Ok(def) => {
                    let mut detail = Vec::new();
                    for field in &def.fields {
                        let deps = super::catalog::column_dependents_any_keyspace(
                            txn,
                            target,
                            &field.name,
                        )
                        .await?;
                        detail.extend(
                            deps.iter()
                                .map(|(t, f)| format!("{t}.{f} reads {target}.{}", field.name)),
                        );
                    }
                    detail
                }
                // Same "best-effort, never fail the drop over this precision
                // feature" tolerance `column_dependents` itself already
                // applies to every *other* definition's text.
                Err(_) => Vec::new(),
            }
        }
        None => Vec::new(),
    };

    Ok((blockers, column_detail))
}

/// The bare target names of every definition reached by a `source` or
/// `join` edge out of `qualified_target`'s schema node — i.e. everything
/// defined `FROM <target>` or `JOIN <target>`.
///
/// The same graph walk [`super::catalog::dependents_of`] does, deliberately
/// *without* its applying-status filter: that filter is
/// right for the claim-time fold, which must not write a CDC delta into a
/// target whose baseline isn't settled, and wrong for a drop, which is asking
/// the different question of whether anything still *needs* the target. It is
/// a separate query rather than a parameter on `dependents_of` because the
/// two want different answers and only this one wants names alone — no
/// parse, no `source_columns` fan-out.
async fn source_edge_dependents(
    txn: &Transaction<'_>,
    qualified_target: &str,
    definition_id: i64,
) -> Result<Vec<String>, CatalogError> {
    let rows = txn
        .query(
            "select split_part(t.target_table, '.', 2) \
             from schema_nodes from_node \
             join schema_edges se on se.from_node_id = from_node.id and se.kind = any($2) \
             join schema_nodes to_node on to_node.id = se.to_node_id \
             join transform_definitions t on t.target_table = to_node.table_name \
             where from_node.table_name = $1 and t.id <> $3 \
             order by t.id",
            &[
                &qualified_target,
                &[EdgeKind::Source.as_str(), EdgeKind::Join.as_str()].as_slice(),
                &definition_id,
            ],
        )
        .await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

/// The row-locked identity of the bare-named `target`: its id, its status,
/// and its persisted qualified `"schema.table"`.
///
/// `for update` + `split_part(target_table, '.', 2)` is the same shape
/// [`crate::staging::quarantine::resume_transform`] uses, so a pause, a
/// resume and a drop racing each other on one definition serialize on its row
/// rather than interleaving. The qualified name comes back from *this* read
/// rather than an earlier unlocked one so every fact a drop acts on is read
/// under the lock it holds.
struct LockedDefinition {
    id: i64,
    status: TransformStatus,
    target_table: String,
}

async fn locked_definition(
    txn: &Transaction<'_>,
    target: &str,
) -> Result<Option<LockedDefinition>, CatalogError> {
    let row = txn
        .query_opt(
            &format!(
                "select id, status, target_table from transform_definitions \
                 where {} for update",
                super::catalog::target_match_sql("target_table", 1)
            ),
            &[&target],
        )
        .await?;
    Ok(row.map(|row| {
        let status_text: String = row.get(1);
        let status = TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
            panic!("transform_definitions.status held unrecognized value '{status_text}'")
        });
        LockedDefinition {
            id: row.get(0),
            status,
            target_table: row.get(2),
        }
    }))
}

async fn node_id_for(txn: &Transaction<'_>, qualified: &str) -> Result<Option<i64>, CatalogError> {
    let row = txn
        .query_opt(
            "select id from schema_nodes where table_name = $1",
            &[&qualified],
        )
        .await?;
    Ok(row.map(|row| row.get(0)))
}

/// A persisted `"schema.table"` rendered DDL-ready, component-independently
/// quoted — the same shape [`super::ddl::qualified_target_table`] produces,
/// but from the already-qualified string this module reads back rather than
/// from a `TransformDef` plus a configured schema. Reading it back is the
/// point: a definition persists the schema it *was* created under,
/// and dropping its table must address that one, not whatever
/// `Config::target_schema` happens to say today.
///
/// `transform_definitions.target_table` is fully qualified
/// and neither component can itself contain a `.` (the grammar rejects a
/// dotted identifier component), so the first `.` is the separator. A value
/// somehow missing one is quoted as a bare name and left to the connection's
/// `search_path`, rather than panicking over a row this crate's own writers
/// cannot produce.
fn quote_qualified(qualified: &str) -> String {
    match qualified.split_once('.') {
        Some((schema, table)) => format!("{}.{}", quote_ident(schema), quote_ident(table)),
        None => quote_ident(qualified),
    }
}
