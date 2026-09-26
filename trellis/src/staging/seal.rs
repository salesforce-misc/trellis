//! Sealing: turning the append stream into immutable batches (issue #9,
//! stage 03). See docs/staging-and-claiming/03-sealing-and-the-fence.md —
//! this module implements the fence, the two-phase seal, the two guards
//! plus `Raced`, seal-on-demand, and age-gated crash recovery, in that
//! order below.
//!
//! Transaction-id family: the modern `pg_snapshot`/`xid8` family throughout
//! (`pg_current_xact_id()`, `pg_current_snapshot()`,
//! `pg_visible_in_snapshot`), matching `row_txid`'s and `fence_snapshot`'s
//! column types (see `V3__staging_ring.sql`) — not the legacy `txid_*`
//! family the design doc's SQL literally shows. tokio-postgres has no
//! `FromSql`/`ToSql` for `xid8`/`pg_snapshot`, so every value crosses the
//! wire as text (`::text` out, `::text::xid8`/`::text::pg_snapshot` back
//! in) — the same bridge `trellis::intake::publication`'s snapshot and
//! `fence_xid` handling uses.

use std::time::Duration;

#[cfg(any(test, feature = "internals"))]
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, GenericClient, Transaction};

use super::append::{self, RING_SIZE, ring_table_name};
use super::claim::{MIN_ROWS_TO_SPLIT, SEG_BUCKETS};
use super::error::StagingError;
use super::state::SegmentState;

/// The age gate on crash recovery (docs/staging-and-claiming/
/// 03-sealing-and-the-fence.md, "Crash recovery"): a `sealed` segment with
/// `seal_step1` set and no `fence_snapshot` is only recovered once it's been
/// stuck for at least this long, so recovery can never stomp a healthy
/// in-flight seal's sub-millisecond phase gap.
#[derive(Debug, Clone)]
pub struct SealConfig {
    pub age_gate: Duration,
}

impl Default for SealConfig {
    fn default() -> Self {
        Self {
            age_gate: Duration::from_secs(10),
        }
    }
}

/// What one successful seal did: which batch it closed and which segment is
/// now active in its place.
#[derive(Debug, Clone, Copy)]
pub struct SealOutcome {
    pub sealed_seg_seq: i64,
    pub sealed_ring_slot: i16,
    /// Which ring slot the newly-active segment took. Read only by
    /// `tests/sealing.rs`/`tests/retire.rs`, so it rides the `internals`
    /// feature (ADR-0012) rather than sitting in production builds unread.
    #[cfg(any(test, feature = "internals"))]
    pub next_ring_slot: i16,
}

/// Fetches the currently active segment's `(seg_seq, ring_slot)` from the
/// pointer — a plain read, no lock (see `trellis::staging::append`).
///
/// This reads the `segment_pointer` table, not the `ring_slot_mirror`
/// sequence ring writers read ([`append::active_ring_slot`]), on purpose:
/// the seal's own statements run at `READ COMMITTED`, and the table is the
/// registry's authority for `active_seq`. The two disagree between phase 1's
/// commit and phase 2's mirror set, which the fence argument in
/// [`seal_phase2`] depends on.
async fn active_pointer(client: &impl GenericClient) -> Result<(i64, i16), StagingError> {
    let row = client
        .query_one("select active_seq, ring_slot from segment_pointer", &[])
        .await?;
    Ok((row.get(0), row.get(1)))
}

/// The seal gate (docs/staging-and-claiming/03-sealing-and-the-fence.md):
/// `xmin(now) < xmax(predecessor's fence)` means a writer that targeted the
/// predecessor's slot hasn't settled yet. Three cases, not two: no
/// predecessor row at all (genesis, or already retired) does **not** hold
/// the gate — there's no `S_k` to wait on. A predecessor row that *exists*
/// but has no captured fence yet (mid-seal, or crashed between its own
/// phase 1 and phase 2) **holds** the gate — you can't prove settlement
/// against a snapshot you can't read, and letting the successor seal here
/// would let an in-flight writer land in neither batch. Recovery (age-gated,
/// not this check) is what eventually clears that wedge by publishing `S_k`.
async fn seal_gate_blocked(
    txn: &Transaction<'_>,
    predecessor_seg_seq: i64,
) -> Result<bool, StagingError> {
    let Some(row) = txn
        .query_opt(
            "select fence_snapshot::text from segments where seg_seq = $1",
            &[&predecessor_seg_seq],
        )
        .await?
    else {
        return Ok(false);
    };

    let fence: Option<String> = row.get(0);
    let Some(fence) = fence else {
        return Ok(true);
    };

    let blocked: bool = txn
        .query_one(
            "select pg_snapshot_xmin(pg_current_snapshot()) < pg_snapshot_xmax($1::text::pg_snapshot)",
            &[&fence],
        )
        .await?
        .get(0);
    Ok(blocked)
}

/// Phase 1 of the two-phase seal: one transaction. Checks both guards,
/// stamps `seal_step1`, fills the predecessor's `seal_step2`, allocates the
/// next active segment, and flips the pointer last. Does **not** capture the
/// fence — that's [`seal_phase2`], deliberately a separate transaction (the
/// `xmax` trap: capturing `S_k` before this commits can miss the highest
/// live xid at commit time).
///
/// Returns [`StagingError::RingFull`] or [`StagingError::SealGateBlocked`]
/// from the guards, or [`StagingError::Raced`] if another worker's flip won
/// first — all three are for the caller to retry/back off on, not to
/// surface as failures.
pub async fn seal_phase1(client: &mut Client) -> Result<SealOutcome, StagingError> {
    let txn = client.transaction().await?;

    let (active_seq, ring_slot) = active_pointer(&txn).await?;
    let next_ring_slot = (ring_slot + 1) % RING_SIZE;
    let next_seg_seq = active_seq + 1;

    if !append::ring_slot_is_free(&txn, next_ring_slot).await? {
        return Err(StagingError::RingFull {
            ring_slot: next_ring_slot,
        });
    }

    let predecessor_seg_seq = active_seq - 1;
    if predecessor_seg_seq > 0 && seal_gate_blocked(&txn, predecessor_seg_seq).await? {
        return Err(StagingError::SealGateBlocked);
    }

    // The bucket count (issue #14, docs/.../04-claiming-and-the-fold.md,
    // "Partitioning a batch across workers") is decided here, once, from
    // this row count and configuration alone — never from the live-worker
    // registry, which would make an immutable, safety-critical partition
    // swing with whichever workers happened to be registered at this
    // instant. Counting the about-to-be-sealed slot in the same transaction
    // as the flip is what pins `bucket_count` from the moment the batch is
    // sealed: no other writer can still be appending into it by the time
    // this transaction commits (the active pointer already moved), so the
    // count taken here is the batch's true, final row count.
    let table = ring_table_name(ring_slot)?;
    let row_count: i64 = txn
        .query_one(&format!("select count(*) from {table}"), &[])
        .await?
        .get(0);
    // The truncate barrier (issue #60, "The ordering hazard"): a batch
    // containing any `op = 'truncate'` row must seal single-bucket, so the
    // whole-keyspace clear and any same-batch post-truncate writes run as
    // one atomic Phase-3 transaction — never split across workers, which
    // could apply a post-truncate insert on one worker before another
    // worker's clear runs on the same target. Checked in the same
    // transaction as the row count above and the flip below, for the same
    // reason `bucket_count` itself is: nothing can still be appending into
    // this slot once this transaction commits, so this is the batch's true,
    // final answer, not a snapshot that could go stale.
    let has_truncate: bool = txn
        .query_one(
            &format!("select exists (select 1 from {table} where op = 'truncate')"),
            &[],
        )
        .await?
        .get(0);
    let bucket_count: i16 = if has_truncate {
        1
    } else if row_count >= MIN_ROWS_TO_SPLIT {
        SEG_BUCKETS as i16
    } else {
        1
    };

    debug_assert!(SegmentState::Active.can_transition_to(SegmentState::Sealed));
    let sealed = txn
        .query_opt(
            "update segments \
               set state = 'sealed', sealed_at = now(), seal_step1 = pg_current_xact_id(), \
                   bucket_count = $2, has_truncate = $3 \
             where seg_seq = $1 and state = 'active' \
             returning seg_seq",
            &[&active_seq, &bucket_count, &has_truncate],
        )
        .await?;
    if sealed.is_none() {
        // Another worker's flip won the race on this row; back off rather
        // than blindly inserting an already-taken seg_seq.
        return Err(StagingError::Raced);
    }

    // Fill the predecessor's seal_step2 with this flip's xid. Best-effort:
    // there is no predecessor for the very first segment.
    txn.execute(
        "update segments \
           set seal_step2 = (select seal_step1 from segments where seg_seq = $1) \
         where seg_seq = $2",
        &[&active_seq, &predecessor_seg_seq],
    )
    .await?;

    txn.execute(
        "insert into segments (seg_seq, ring_slot, state) values ($1, $2, 'active')",
        &[&next_seg_seq, &next_ring_slot],
    )
    .await?;

    // Flip the pointer last.
    txn.execute(
        "update segment_pointer set active_seq = $1, ring_slot = $2",
        &[&next_seg_seq, &next_ring_slot],
    )
    .await?;

    txn.commit().await?;

    Ok(SealOutcome {
        sealed_seg_seq: active_seq,
        sealed_ring_slot: ring_slot,
        #[cfg(any(test, feature = "internals"))]
        next_ring_slot,
    })
}

/// Phase 2 of the two-phase seal, and also crash recovery's reconstruction
/// step (docs/staging-and-claiming/03-sealing-and-the-fence.md — recovery
/// "reconstructs `S_k` at the flip boundary from `seal_step1`" by simply
/// running this now, long after the flip committed): the ring-slot mirror
/// set, then the `xmax`-trap fix (`SELECT pg_current_xact_id()`, in
/// **autocommit**, immediately before the snapshot), then capturing and
/// publishing `fence_snapshot`.
///
/// **The mirror set comes first** (issue #595). Ring writers resolve their
/// slot from `ring_slot_mirror` ([`append::active_ring_slot`]), not from
/// `segment_pointer`, so they keep targeting the sealed slot until this
/// statement moves the mirror to the slot phase 1 activated. The fence
/// argument, restated for the mirror: a writer takes its xid before it reads
/// the mirror, so every writer that read the old slot did so before this
/// set, hence before the bump below assigned its xid, hence with an xid
/// below `xmax(S_k)`. It is either visible in `S_k` (batch *k* claims it) or
/// in `S_k`'s in-progress list (the seal gate blocks `S_{k+1}` until it
/// settles, and batch *k+1*'s predecessor half claims it). No writer holding
/// an old-slot read has an xid at or above `xmax(S_k)`.
///
/// The same placement covers crash recovery: a sealer that died between
/// phase 1's commit and this set leaves the mirror on the sealed slot, and
/// writers keep landing there until recovery runs this function — every one
/// of them still before the recovery's own bump.
///
/// The set is guarded to the pointer still naming this seal's successor
/// (`active_seq = seg_seq + 1`), with the pointer row locked for the length
/// of the statement. A normal phase 2 always matches: the successor can't
/// seal until this fence is published. The guard is for a phase 2 that
/// stalls past the recovery age gate: recovery publishes `S_k`, the
/// successor seals and moves the mirror on, and the stalled call must not
/// then drag the mirror back to a slot that is already sealed. The row lock
/// orders the check against the successor's flip; a successor flip that
/// commits first makes the re-checked row fail the guard, so nothing is set.
/// A raced call that does match (a recoverer and a normal completion) sets
/// the same value twice, which is harmless.
///
/// Takes `&Client`, never a `Transaction`, so the xmax fix can't
/// accidentally run inside an open transaction — there, the `SELECT`
/// wouldn't commit and `latestCompletedXid` wouldn't move, silently
/// reintroducing the trap. The mirror set must also commit before the bump,
/// which only autocommit guarantees.
///
/// The write is scoped `state = 'sealed' and fence_snapshot is null`, so a
/// raced call (a concurrent recoverer, or a normal completion that beat it)
/// matches zero rows — a benign no-op, not an error.
///
/// `wake_channel` is `pg_notify`'d the instant this call is the one that
/// actually publishes the fence (issue #271) — this is the one transition
/// that makes the segment claimable, so it is the edge a drain worker
/// parked on `wake_channel` actually wants to hear about, not the "rows
/// landed in the active segment" edge `intake::advance_watermark_and_notify`
/// and `apply::drain_once`'s downstream-propagation notify already cover.
///
/// The `update` and the `pg_notify` are one statement (a `with` clause
/// feeding the updated row, if any, into `pg_notify`), not two — matching
/// `advance_watermark_and_notify`'s own "notify must not precede the fact it
/// announces" discipline, just achieved differently. That function opens an
/// explicit `Transaction` and issues the mutation and the `pg_notify` as two
/// statements inside it, relying on the caller's own commit to make both
/// atomic. This function can't do that: its first two statements (the
/// `xmax` fix and the snapshot capture, above) must each be *their own*
/// committed, autocommit statement — running them inside a transaction would
/// silently reintroduce the trap this function's own doc comment describes.
/// So instead of widening that transaction, this folds the mutation and the
/// notify into a single statement, which Postgres itself wraps in one
/// implicit transaction: `pg_notify` only evaluates for a row the `update`
/// actually touched, and a NOTIFY queued during a statement/transaction is
/// only delivered to another backend once that statement's implicit
/// transaction commits — so a listener can never observe this notify before
/// the fence it announces is durably visible, and a raced no-op call (the
/// `with` clause returns zero rows) never notifies at all.
pub async fn seal_phase2(
    client: &Client,
    seg_seq: i64,
    wake_channel: &str,
) -> Result<(), StagingError> {
    client
        .execute(
            "with successor as materialized ( \
                 select ring_slot from segment_pointer \
                 where active_seq = $1::bigint + 1 \
                 for update \
             ) \
             select setval('ring_slot_mirror', ring_slot, true) from successor",
            &[&seg_seq],
        )
        .await?;
    client.query_one("select pg_current_xact_id()", &[]).await?;
    let fence: String = client
        .query_one("select pg_current_snapshot()::text", &[])
        .await?
        .get(0);
    client
        .execute(
            "with published as ( \
                 update segments set fence_snapshot = $1::text::pg_snapshot \
                 where seg_seq = $2 and state = 'sealed' and fence_snapshot is null \
                 returning seg_seq \
             ) \
             select pg_notify($3, '') from published",
            &[&fence, &seg_seq, &wake_channel],
        )
        .await?;
    Ok(())
}

/// Whether `predecessor_seg_seq`'s own physical ring table still holds a row
/// that was never visible in its own already-published fence — a genuine
/// **phase-gap straggler** (docs/staging-and-claiming/03-sealing-and-the-fence.md,
/// "The scoping bug worth knowing about"/"The `xmax` trap"): a writer that
/// resolved the pointer as `predecessor_seg_seq` but committed into its slot
/// only *after* that segment's own fence (`S_k`) was captured. By
/// construction such a row can never become visible in `S_k` — that fence is
/// immutable once published (`seal_phase2`'s doc comment) — so the *only*
/// thing that can ever fold it in is the immediate successor's own fenced
/// read, via [`fenced_window`]'s predecessor-half union clause. That read
/// only ever runs once the successor itself gets sealed. Returns `false`
/// (nothing to catch) for a genesis segment, an already-retired predecessor,
/// or one still mid-crash-window (no fence published yet — a separate,
/// already-tracked stall `recover_stuck_seals` owns) — none of those are
/// this function's job to resolve.
async fn predecessor_has_unfenced_row(
    client: &impl GenericClient,
    predecessor_seg_seq: i64,
) -> Result<bool, StagingError> {
    if predecessor_seg_seq <= 0 {
        return Ok(false);
    }
    let Some(row) = client
        .query_opt(
            "select ring_slot, fence_snapshot::text from segments where seg_seq = $1",
            &[&predecessor_seg_seq],
        )
        .await?
    else {
        return Ok(false);
    };
    let ring_slot: i16 = row.get(0);
    let fence: Option<String> = row.get(1);
    let Some(fence) = fence else {
        return Ok(false);
    };
    let table = ring_table_name(ring_slot)?;
    let exists: bool = client
        .query_one(
            &format!(
                "select exists (select 1 from {table} \
                 where not pg_visible_in_snapshot(row_txid, $1::text::pg_snapshot))"
            ),
            &[&fence],
        )
        .await?
        .get(0);
    Ok(exists)
}

/// Seal-on-demand (docs/staging-and-claiming/03-sealing-and-the-fence.md,
/// "Who seals, and when"): seals the active segment if it's non-empty — the
/// busy-loop guard — **or** if it's empty but its immediate predecessor is
/// still stranding an unfenced phase-gap straggler ([`predecessor_has_unfenced_row`]):
/// without this second case, a predecessor's straggler that lands right as
/// the ring goes quiet (no further real traffic to seal the successor on its
/// own account) is stranded forever — the predecessor already reports
/// `state = 'drained'`, which `converge::converged_through`'s condition 3 no
/// longer treats as pending, so nothing ever revisits it, and the straggler
/// is silently never applied to any target (issue found via the generative
/// suite's client-restart/scale-out lifecycle properties: both simply
/// perturb timing enough to make this always-latent race common, though the
/// race itself has nothing to do with a client joining or leaving — see
/// `generative/tests/client_lifecycle.rs`'s doc comments for the full
/// writeup). This still seals **at most one** segment per call, and the
/// second case is self-limiting: it only ever fires while the *current*
/// active segment's immediate predecessor genuinely has an unfenced row, so
/// once that row is folded in by this seal's own successor read, the next
/// active segment's own (different, now-fully-fenced) predecessor no longer
/// qualifies and this stops recursing — it does not degrade into resealing
/// empty segments forever on a truly idle ring.
///
/// A `RingFull` guard is answered with exactly one retirement pass
/// ([`super::retire::retire_drained_segments`], stage 06) plus one retry, per
/// the design doc — this is also the liveness unblock for a saturated ring:
/// without it, a ring full of drained-but-not-yet-retired slots wedges every
/// subsequent seal forever (issue #58). A `SealGateBlocked` guard is treated
/// as this call simply having nothing to do yet (`Ok(None)`), never an
/// error — the design doc calls both guards "backpressure, never overwrite,"
/// and a refused seal here is always a correct, retryable outcome, not a
/// reason to tear down and reconnect the maintenance loop's connection.
///
/// `wake_channel` is threaded straight through to [`seal_phase2`] (issue
/// #271): a seal that actually completes here is the one transition that
/// makes a segment claimable, so it is exactly the edge a drain worker
/// parked on `wake_channel` wants to wake to. A refused seal (`Ok(None)`,
/// either guard, or a genuinely empty ring) never reaches `seal_phase2` at
/// all, so it never notifies — there is nothing new for a listener to wake
/// to.
pub async fn seal_if_active_nonempty(
    client: &mut Client,
    wake_channel: &str,
) -> Result<Option<SealOutcome>, StagingError> {
    let (active_seq, ring_slot) = active_pointer(client).await?;
    let table = ring_table_name(ring_slot)?;
    let nonempty: bool = client
        .query_one(&format!("select exists (select 1 from {table})"), &[])
        .await?
        .get(0);
    if !nonempty && !predecessor_has_unfenced_row(client, active_seq - 1).await? {
        return Ok(None);
    }

    let attempt = match seal_phase1(client).await {
        Ok(outcome) => Ok(outcome),
        Err(StagingError::RingFull { .. }) => {
            super::retire::retire_drained_segments(client).await?;
            seal_phase1(client).await
        }
        Err(other) => Err(other),
    };
    let outcome = match attempt {
        Ok(outcome) => outcome,
        Err(StagingError::SealGateBlocked) => return Ok(None),
        Err(other) => return Err(other),
    };
    seal_phase2(client, outcome.sealed_seg_seq, wake_channel).await?;
    analyze_sealed_slot_best_effort(client, outcome.sealed_ring_slot).await;
    Ok(Some(outcome))
}

/// Best-effort, single-column `ANALYZE (origin_lsn)` on a slot that just
/// stopped growing (docs/staging-and-claiming/07-convergence-and-await.md,
/// "Making the honest question cheap"): ring tables carry
/// `autovacuum_enabled = off` (`V3__staging_ring.sql`), which also disables
/// autoANALYZE, so condition 3 of the convergence predicate
/// (`trellis::staging::converge::converged_through`) would otherwise pick a
/// sequential scan at every table size, forever, with no threshold it ever
/// crosses. Single-column so nothing else in the table's plan shape moves.
///
/// Best-effort: a failed `ANALYZE` costs plan quality on this one slot, not
/// correctness, so it must never fail the seal that's already committed by
/// the time this runs. Errors are swallowed after `seal_if_active_nonempty`
/// has already returned its outcome — logging is left to the caller's own
/// tracing setup rather than this crate taking a logging dependency.
async fn analyze_sealed_slot_best_effort(client: &Client, ring_slot: i16) {
    let Ok(table) = ring_table_name(ring_slot) else {
        return;
    };
    let _ = client
        .batch_execute(&format!("analyze {table} (origin_lsn)"))
        .await;
}

/// Age-gated crash recovery: finds every segment stuck `state = 'sealed'`
/// with `seal_step1` set, `fence_snapshot` still `NULL`, and sealed longer
/// ago than `age_gate`, and reconstructs its fence via [`seal_phase2`].
/// Returns the recovered `seg_seq`s.
///
/// The age gate is what keeps this from racing every normal seal — without
/// it, recovery would be a second, unsynchronized writer to the
/// soon-to-be-published fence.
///
/// `wake_channel` is threaded straight through to [`seal_phase2`] (issue
/// #271), same as [`seal_if_active_nonempty`]: a recovered segment becomes
/// claimable at exactly the moment its fence is (re)published here, which is
/// the same edge a normal seal's completion wakes a listener to — recovery
/// finishing what a crashed seal started is not a different transition.
pub async fn recover_stuck_seals(
    client: &Client,
    config: &SealConfig,
    wake_channel: &str,
) -> Result<Vec<i64>, StagingError> {
    let age_gate_secs = config.age_gate.as_secs_f64();
    let stuck: Vec<(i64, i16)> = client
        .query(
            "select seg_seq, ring_slot from segments \
             where state = 'sealed' and seal_step1 is not null and fence_snapshot is null \
               and sealed_at < now() - (interval '1 second' * $1)",
            &[&age_gate_secs],
        )
        .await?
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();

    for &(seg_seq, ring_slot) in &stuck {
        seal_phase2(client, seg_seq, wake_channel).await?;
        // Mirror the normal seal path: a crash-recovered slot has just
        // stopped growing too, so give condition 3 of the convergence
        // predicate its `origin_lsn` statistics rather than leaving it a
        // permanent sequential scan (`autovacuum_enabled = off` ⇒ no
        // autoANALYZE). Best-effort — plan quality only, never fails recovery.
        analyze_sealed_slot_best_effort(client, ring_slot).await;
    }
    Ok(stuck.into_iter().map(|(seg_seq, _)| seg_seq).collect())
}

/// The both-slots fence window, factored into the **one** place this
/// predicate lives (docs/staging-and-claiming/04-claiming-and-the-fold.md,
/// "Practical notes on the fold"): batch `seg_seq` = rows of its own slot
/// visible in its fence, `UNION ALL` rows of the predecessor's slot visible
/// in this fence and **not** visible in the predecessor's own fence. The
/// `NOT visible` clause is scoped to the predecessor half only — see
/// [03](03-sealing-and-the-fence.md)'s "scoping bug" section for why applying
/// it uniformly silently loses phase-gap writers.
///
/// `columns` is the caller's projection — `fenced_rows` below projects a
/// physical row identity, [`super::fold::fold`] projects full columns for
/// aggregation. Both consume this one function rather than each embedding
/// the fence predicate, so there is exactly one definition to get right.
///
/// Returns the window as raw SQL text plus its fence parameters in
/// placeholder order (`$1` = this segment's fence, `$2` = the predecessor's,
/// if one applies) — the caller embeds the text (as a CTE or subquery) and
/// binds the returned params first, continuing its own placeholders from
/// `params.len() + 1`.
///
/// A `sealed` segment with no `fence_snapshot` is the crash window; this
/// fails loud (`UnfencedSealedSegment`) rather than guessing admit-all or
/// admit-none.
pub(crate) async fn fenced_window(
    client: &impl GenericClient,
    seg_seq: i64,
    columns: &str,
) -> Result<(String, Vec<String>), StagingError> {
    let row = client
        .query_one(
            "select ring_slot, fence_snapshot::text from segments where seg_seq = $1",
            &[&seg_seq],
        )
        .await?;
    let ring_slot: i16 = row.get(0);
    let fence: Option<String> = row.get(1);
    let fence = fence.ok_or(StagingError::UnfencedSealedSegment { seg_seq })?;
    let table = ring_table_name(ring_slot)?;

    let mut sql = format!(
        "select {columns} from {table} \
         where pg_visible_in_snapshot(row_txid, $1::text::pg_snapshot)"
    );
    let mut params = vec![fence];

    // Batch 0 has no predecessor row at all — S_{-1} is the empty snapshot,
    // so there is no predecessor half to add.
    if let Some(predecessor) = client
        .query_opt(
            "select ring_slot, fence_snapshot::text from segments where seg_seq = $1",
            &[&(seg_seq - 1)],
        )
        .await?
    {
        let predecessor_ring_slot: i16 = predecessor.get(0);
        let predecessor_fence: Option<String> = predecessor.get(1);
        if let Some(predecessor_fence) = predecessor_fence {
            let predecessor_table = ring_table_name(predecessor_ring_slot)?;
            sql.push_str(&format!(
                " union all select {columns} from {predecessor_table} \
                 where pg_visible_in_snapshot(row_txid, $1::text::pg_snapshot) \
                   and not pg_visible_in_snapshot(row_txid, $2::text::pg_snapshot)"
            ));
            params.push(predecessor_fence);
        }
        // A predecessor that exists but has no fence yet is itself mid-crash-
        // window; that's `UnfencedSealedSegment` on *its own* fenced window
        // call, not something this batch's read can paper over.
    }

    Ok((sql, params))
}

/// The both-slots fence read: [`fenced_window`] projected down to a physical
/// row identity (`tableoid:ctid`) per matching row — stable enough to check
/// "exactly once" across batches in tests, without requiring test data to
/// use globally-unique keys.
#[cfg(any(test, feature = "internals"))]
pub async fn fenced_rows(
    client: &impl GenericClient,
    seg_seq: i64,
) -> Result<Vec<String>, StagingError> {
    let (sql, params) =
        fenced_window(client, seg_seq, "tableoid::text || ':' || ctid::text").await?;
    let param_refs: Vec<&(dyn ToSql + Sync)> =
        params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
    let rows = client
        .query(&sql, &param_refs)
        .await?
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    Ok(rows)
}
