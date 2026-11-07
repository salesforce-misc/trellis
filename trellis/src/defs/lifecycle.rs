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
//!    [`StoredStatus::Paused`] into the same `transform_definitions.status`
//!    column the poison fuse already writes
//!    [`StoredStatus::Quarantined`] into, and the claim-time fold stops
//!    dispatching to the target for exactly the reason it already stops for a
//!    quarantined one: [`super::catalog::dependents_of`]'s applying-status
//!    predicate ([`StoredStatus::is_applying`]), the single gate every
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
use super::model::{EdgeKind, StoredStatus};
use crate::pool::{Pool, quote_ident};
use crate::staging::pause_message::{self, Reason, Recheck};

/// What [`pause_transform`] actually did — returned so a caller (and a test)
/// can tell a real transition from the idempotent no-op, even though both are
/// successes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PauseOutcome {
    /// The definition was live (or catching up, backfilling, or waiting to
    /// backfill) and is now [`StoredStatus::Paused`].
    Paused,
    /// The definition was already frozen — by an earlier pause, or by the
    /// poison fuse — and was left exactly as it was. ADR-0014's idempotency
    /// clause: this is a success, not a conflict.
    AlreadyFrozen(StoredStatus),
    /// The definition was already frozen, but waiting to be resumed: on its
    /// upstream's resume (`capture_failures.caused_by`), or on a
    /// resume request (`resume_requests`: an operator's `RESUME` waiting for
    /// an upstream to be live or for its copies to be re-typed). The
    /// pause withdrew that resume, so it stays paused like any operator
    /// pause.
    Withdrawn(StoredStatus),
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
/// Idempotent: a definition already [`StoredStatus::Paused`] *or*
/// [`StoredStatus::Quarantined`] keeps its status and is reported as
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
        &[&StoredStatus::Paused.as_str(), &id],
    )
    .await?;
    txn.commit().await?;

    crate::instance_log::info!(
        transform = %target,
        from = %status.as_str(),
        to = %StoredStatus::Paused.as_str(),
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
    // Under the record's row lock, which every writer of its reasons takes
    // before it reads them (`staging::pause_message`): the reasons rewritten
    // below are the ones a refresh that committed meanwhile left.
    let record = txn
        .query_opt(
            "select source_table, caused_by is not null from capture_failures \
             where transform_id = $1 for update",
            &[&id],
        )
        .await?;
    let caused = record.as_ref().is_some_and(|row| row.get::<_, bool>(1));
    if !requested && !caused {
        return Ok(false);
    }
    if caused {
        pause_message::rewrite_reasons(txn, id, |reasons| {
            for reason in reasons.iter_mut() {
                if let Some((own, _)) = reason
                    .text
                    .split_once(crate::staging::schema_change::CAUSED_RESUMES_ON_ITS_OWN)
                {
                    reason.text = format!("{own}{WITHDRAWN_CAUSE}");
                }
            }
        })
        .await?;
    } else if let Some(row) = &record {
        let source_table: String = row.get(0);
        pause_message::replace_reasons(
            txn,
            id,
            &[Reason::standing(&source_table, &[], WITHDRAWN_REQUEST)],
        )
        .await?;
    }
    txn.execute(
        "update capture_failures set caused_by = null, detected_at = now() \
         where transform_id = $1",
        &[&id],
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
/// rebuild. An existing record keeps its kind and state, and its reasons
/// after this one ([`crate::staging::pause_message`]).
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
    let reason = Reason::standing(source_table, columns, error);
    pause_with_record(txn, id, reason, "capture", caused_by).await
}

/// [`pause_for_capture_failure`] for a schema change whose cause the
/// capture pass can read again while the definition stays paused
/// (`recheck`): columns renamed or dropped, or a primary key redefined.
/// The pass drops the reason once its cause is gone, and rewrites it when
/// it changes ([`crate::staging::pause_message`]).
pub(crate) async fn pause_for_schema_change(
    txn: &Transaction<'_>,
    id: i64,
    source_table: &str,
    columns: &[String],
    error: &str,
    recheck: Recheck,
) -> Result<bool, CatalogError> {
    let reason = Reason {
        recheck,
        ..Reason::standing(source_table, columns, error)
    };
    pause_with_record(txn, id, reason, "capture", None).await
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
            &[&id, &StoredStatus::dispatchable()],
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
    pause_with_record(
        txn,
        id,
        Reason::standing(source_table, &[], error),
        kind,
        None,
    )
    .await
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
            &[&id, &StoredStatus::dispatchable()],
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
    let already = txn
        .query_opt(
            "select restored from capture_failures where transform_id = $1",
            &[&id],
        )
        .await?
        .is_some_and(|row| row.get::<_, bool>(0));
    txn.execute(
        "insert into capture_failures (transform_id, source_table, columns, error, kind, restored) \
         values ($1, $2, '{}', $3, $4, true) \
         on conflict (transform_id) do update set restored = true, caused_by = null",
        &[&id, &source_table, &pause_message::NO_REASON_YET, &kind],
    )
    .await?;
    if !already {
        pause_message::add_reason(txn, id, Reason::standing(source_table, &[], error)).await?;
    }
    Ok(txn
        .execute(
            "update transform_definitions set status = $2 where id = $1 and status = any($3)",
            &[
                &id,
                &StoredStatus::Paused.as_str(),
                &StoredStatus::dispatchable(),
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
    let Some(row) = txn
        .query_opt(
            "select f.relabel_lsn is not null from transform_definitions d \
             left join capture_failures f on f.transform_id = d.id \
             where d.id = $1",
            &[&id],
        )
        .await?
    else {
        return Ok(false);
    };
    let relabelled = row.get::<_, Option<bool>>(0) == Some(true);
    txn.execute(
        "insert into capture_failures \
             (transform_id, source_table, columns, error, kind, relabel_lsn) \
         values ($1, $2, '{}', $3, 'capture', $4) \
         on conflict (transform_id) do update \
         set relabel_lsn = greatest(capture_failures.relabel_lsn, excluded.relabel_lsn)",
        &[&id, &source_table, &pause_message::NO_REASON_YET, &lsn],
    )
    .await?;
    if !relabelled {
        pause_message::add_reason(txn, id, Reason::standing(source_table, &[], error)).await?;
    }
    Ok(txn
        .execute(
            "update transform_definitions set status = $2 where id = $1 and status = any($3)",
            &[
                &id,
                &StoredStatus::Paused.as_str(),
                &StoredStatus::dispatchable(),
            ],
        )
        .await?
        == 1)
}

/// Pauses definition `id`, which an earlier release built over its source
/// key's `INCLUDE` columns (#1722, [`super::recorded_keys::backfill_in`]),
/// recording `error` as its `capture_failure`, kind `capture`: only an
/// operator's `RESUME`, which re-keys it, resumes it. A definition already
/// paused keeps the reason it has. Returns whether this call paused it.
pub(crate) async fn pause_for_included_key(
    txn: &Transaction<'_>,
    id: i64,
    source_table: &str,
    error: &str,
) -> Result<bool, CatalogError> {
    let reason = Reason::standing(source_table, &[], error);
    pause_with_record(txn, id, reason, "capture", None).await
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
    let reason = Reason::standing(source_table, &[], error);
    pause_with_record(txn, id, reason, "capture", None).await
}

/// Pauses definition `id`, which this binary can't run (ADR-0024 rules 3
/// and 6), recording `reason` (an [`super::floor::Unrunnable::reason`]) as its
/// `capture_failure`, kind `capture`: only an operator's `RESUME` under a
/// release that runs it resumes it. A definition already paused keeps its
/// reasons after this one ([`super::floor::put_unrunnable_reason`]), and loses any
/// resume that would complete without the operator: a pending resume request
/// and an upstream cause. Returns whether this changed anything, so a later
/// attach that finds it already recorded changes nothing.
pub(crate) async fn pause_for_unrunnable(
    txn: &Transaction<'_>,
    id: i64,
    source_table: &str,
    reason: &str,
) -> Result<bool, CatalogError> {
    let Some(row) = txn
        .query_opt(
            "select cf.caused_by is not null \
             from transform_definitions d \
             left join capture_failures cf on cf.transform_id = d.id \
             where d.id = $1 for update of d",
            &[&id],
        )
        .await?
    else {
        return Ok(false);
    };
    let mut changed = row.get::<_, Option<bool>>(0) == Some(true);
    changed |= txn
        .execute(
            "delete from resume_requests where transform_id = $1",
            &[&id],
        )
        .await?
        == 1;
    insert_record(txn, id, source_table, "capture", None).await?;
    let recorded = pause_message::rewrite_reasons(txn, id, |reasons| {
        super::floor::put_unrunnable_reason(reasons, source_table, reason);
    })
    .await?;
    if recorded {
        txn.execute(
            "update capture_failures set kind = 'capture', caused_by = null, detected_at = now() \
             where transform_id = $1",
            &[&id],
        )
        .await?;
    } else if changed {
        txn.execute(
            "update capture_failures set caused_by = null where transform_id = $1",
            &[&id],
        )
        .await?;
    }
    changed |= recorded;
    // A status a newer release wrote is paused over too (ADR-0024 rule 6):
    // nothing in this release can read it, and the record's reason says
    // which it was.
    let known: Vec<&str> = StoredStatus::ALL.iter().map(|s| s.as_str()).collect();
    changed |= txn
        .execute(
            "update transform_definitions set status = $2 \
             where id = $1 and (status = any($3) or status <> all($4))",
            &[
                &id,
                &StoredStatus::Paused.as_str(),
                &StoredStatus::dispatchable(),
                &known,
            ],
        )
        .await?
        == 1;
    Ok(changed)
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
    pause_with_record(
        txn,
        id,
        Reason::standing(source_table, &[], error),
        "halt",
        None,
    )
    .await
}

/// Records why `id` pauses in `capture_failures`, under `kind` and with
/// its upstream cause, and pauses it unless it's frozen. A record that
/// already stands keeps its kind and its state (its cause, a restore's, a
/// relabel's), and gets this reason before its own
/// ([`crate::staging::pause_message::add_reason`]). Returns whether it
/// paused it.
async fn pause_with_record(
    txn: &Transaction<'_>,
    id: i64,
    reason: Reason,
    kind: &str,
    caused_by: Option<i64>,
) -> Result<bool, CatalogError> {
    insert_record(txn, id, &reason.source_table, kind, caused_by).await?;
    pause_message::add_reason(txn, id, reason).await?;
    let paused = txn
        .execute(
            "update transform_definitions set status = $2 \
             where id = $1 and status = any($3)",
            &[
                &id,
                &StoredStatus::Paused.as_str(),
                &StoredStatus::dispatchable(),
            ],
        )
        .await?
        == 1;
    Ok(paused)
}

/// Inserts definition `id`'s `capture_failures` record, if it exists and
/// has none, with no reason yet, under `kind` and with its upstream cause;
/// an existing record is locked and left as it is.
async fn insert_record(
    txn: &Transaction<'_>,
    id: i64,
    source_table: &str,
    kind: &str,
    caused_by: Option<i64>,
) -> Result<(), CatalogError> {
    txn.execute(
        "insert into capture_failures (transform_id, source_table, columns, error, kind, caused_by) \
         select id, $2, '{}', $3, $4, $5 from transform_definitions where id = $1 \
         on conflict (transform_id) do update set transform_id = excluded.transform_id",
        &[
            &id,
            &source_table,
            &pause_message::NO_REASON_YET,
            &kind,
            &caused_by,
        ],
    )
    .await?;
    Ok(())
}

/// Removes `target`'s definition — ADR-0014's terminal reap.
///
/// **Only ever acts on a frozen definition.** There is no direct live-to-gone
/// edge in the lifecycle: quiescing first is a precondition, so the removal
/// never has to reason about a fold still dispatching to the target. A
/// definition that is not [`StoredStatus::Paused`] or
/// [`StoredStatus::Quarantined`] is refused with
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

    // ADR-0024 rule 5: only a release that meets the definition's floor
    // knows everything it owns, so a drop above the running release would be
    // partial. Read from the row locked above, so it is the floor this drop
    // acts on, and checked before the status: the remedy is the same whether
    // it is paused or not, and nothing below (the dependency refusal's
    // column detail parses the definition) runs on text this release may not
    // read.
    if let Some(running) = pool.floors().above_running(&locked.floor) {
        return Err(CatalogError::AboveRunningRelease {
            transform: target.to_string(),
            floor: locked.floor,
            running: running.to_string(),
        });
    }

    if !status.is_frozen() {
        return Err(CatalogError::TransformNotPaused {
            transform: target.to_string(),
            status: status.into(),
        });
    }

    // Before any write, and inside the same transaction as the writes, so a
    // `define` that registers a new dependent cannot interleave between the
    // refusal check and the commit.
    let (blockers, column_detail) =
        dependency_blockers(&txn, pool.floors(), target, &qualified, id).await?;
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
    // A table's recorded key goes with its last definition: a define over it
    // later records it again.
    txn.execute(
        "delete from definition_keys k where not exists ( \
             select 1 from transform_definitions d \
             where k.table_name in (d.source_table, d.target_table, d.join_table))",
        &[],
    )
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
    floors: &super::floor::Floors,
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
            "select definition_text, grammar_version, floor_version \
             from transform_definitions where id = $1",
            &[&definition_id],
        )
        .await?
    {
        Some(row) => {
            match super::grammar::load_stored(floors, row.get(0), row.get(1), row.get(2)) {
                Ok(Some(def)) => {
                    let mut detail = Vec::new();
                    for field in &def.fields {
                        let deps = super::catalog::column_dependents_any_keyspace(
                            txn,
                            floors,
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
                // applies to every *other* definition's text, and a
                // definition this release can't run names no column.
                Ok(None) | Err(_) => Vec::new(),
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
    status: StoredStatus,
    target_table: String,
    /// Its stored floor (ADR-0024), as `floor_version` holds it.
    floor: String,
}

async fn locked_definition(
    txn: &Transaction<'_>,
    target: &str,
) -> Result<Option<LockedDefinition>, CatalogError> {
    let row = txn
        .query_opt(
            &format!(
                "select id, status, target_table, floor_version from transform_definitions \
                 where {} for update",
                super::catalog::target_match_sql("target_table", 1)
            ),
            &[&target],
        )
        .await?;
    row.map(|row| {
        let status_text: String = row.get(1);
        let status = StoredStatus::from_row(&status_text)?;
        Ok(LockedDefinition {
            id: row.get(0),
            status,
            target_table: row.get(2),
            floor: row.get(3),
        })
    })
    .transpose()
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
