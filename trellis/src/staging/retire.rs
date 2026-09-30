//! Retirement: freeing a `drained` segment's ring slot for reuse (issue
//! #13/#58, stage 06's cleanup half — quarantine, stage 06's other half, is
//! issue #16). See
//! docs/staging-and-claiming/06-cleanup-and-reclaim.md, "Retiring a batch".
//!
//! **The guarantee: no cleanup step can retire un-applied work.** Every
//! removal is gated on a proof that nobody can still need it — the four
//! conditions in [`RETIRE_CANDIDATES_SQL`] below, lifted straight from the
//! design doc.
//!
//! This is the piece the old `seal::cleanup_stub` never was: that stub only
//! ever deleted a `sealed` row whose slot was already empty (what stage 03's
//! own one-retry-on-`RingFull` needed at the time it was written, before
//! claiming/apply existed to ever produce a `drained` row). A `drained`
//! segment's row was never removed by anything, so once three widely-spaced
//! writes each seal into their own segment, the fourth seal's `RingFull`
//! retry finds nothing to clean up and fails again — forever, since nothing
//! ever revisits it. This module is the fix: the real retirement pass,
//! wired into [`crate::client`]'s maintenance loop and (still) run as
//! `seal_if_active_nonempty`'s one `RingFull` retry.
//!
//! [`collect_tombstones`] is the ledgers' half of the same cleanup (#623
//! D7): a tombstone is only needed while an older change to its key can
//! still arrive, and the drained prefix says when none can.

use tokio_postgres::Client;

use super::append::ring_table_name;
use super::error::StagingError;
use crate::pool::quote_ident;

/// The four eligibility conditions, as one query, ordered oldest-`seg_seq`
/// first so a single pass retires in the order the design doc's "single-pass
/// leak" note assumes (skip *s*, retire *s+1* is still safe — it just leaves
/// *s* for the next pass — but retiring oldest-first means that's the
/// exception, not the rule).
///
/// 1. `seal_step2 is not null` — the successor has sealed, closing this
///    batch's own fence boundary.
/// 2. `pg_snapshot_xmin(pg_current_snapshot()) > seal_step2` — every
///    transaction that could still be writing into this batch's slot has
///    finished. `seal_step2` is a plain `xid8` (the successor's flip
///    transaction), not a snapshot — compared directly against the current
///    snapshot's `xmin`, matching [`super::seal`]'s own `xid8`/`pg_snapshot`
///    family choice (see that module's doc comment) rather than the design
///    doc's legacy `txid_snapshot_xmin` spelling.
/// 3. The successor (`seg_seq + 1`) is also `drained` — written as *no
///    successor row with a state other than `drained`*, so an **absent**
///    successor (already retired) qualifies too.
/// 4. No live row with `seg_seq` less than the successor's is anything but
///    `drained` — a lagging batch anywhere behind the boundary makes every
///    candidate ineligible, not just its own slot.
const RETIRE_CANDIDATES_SQL: &str = "\
    select seg_seq, ring_slot from segments s \
    where s.state = 'drained' \
      and s.seal_step2 is not null \
      and pg_snapshot_xmin(pg_current_snapshot()) > s.seal_step2 \
      and not exists ( \
          select 1 from segments successor \
          where successor.seg_seq = s.seg_seq + 1 and successor.state <> 'drained' \
      ) \
      and not exists ( \
          select 1 from segments older \
          where older.seg_seq < s.seg_seq + 1 and older.state <> 'drained' \
      ) \
    order by s.seg_seq asc";

/// Retires every eligible `drained` segment: locks its ring slot, deletes
/// the registry row, then truncates the slot — in that exact order, each in
/// its own transaction so one candidate's outcome (retired, skipped, or
/// raced) never blocks another's.
///
/// Returns the `seg_seq`s actually retired this pass. A candidate that a
/// concurrent reclaimer already removed, or a seal already re-seeded under a
/// new `seg_seq`, is silently skipped — see the per-candidate doc comment
/// below on why the delete must be the thing that decides.
pub async fn retire_drained_segments(client: &mut Client) -> Result<Vec<i64>, StagingError> {
    let candidates: Vec<(i64, i16)> = client
        .query(RETIRE_CANDIDATES_SQL, &[])
        .await?
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();

    let mut retired = Vec::with_capacity(candidates.len());
    for (seg_seq, ring_slot) in candidates {
        if retire_one(client, seg_seq, ring_slot).await? {
            retired.push(seg_seq);
        }
    }
    Ok(retired)
}

/// One candidate's retire attempt, exactly as
/// docs/staging-and-claiming/06-cleanup-and-reclaim.md spells it:
///
/// ```sql
/// LOCK TABLE seg_<n> IN ACCESS EXCLUSIVE MODE NOWAIT;
/// DELETE FROM segments WHERE seg_seq = :s AND ring_slot = :n AND state = 'drained';
/// TRUNCATE seg_<n>;
/// ```
///
/// **`NOWAIT`.** A held lock (an in-flight claim reader, or another
/// reclaimer) means skip and retry next tick — a blocking retirement pass
/// would turn a slow drain into a stalled fleet. Lock contention is the one
/// expected failure mode here, so it's distinguished by SQLSTATE
/// (`lock_not_available`) from a genuine error, which propagates.
///
/// **`DELETE` before `TRUNCATE`.** The registry delete *is* the reclaim
/// claim, keyed on `(seg_seq, ring_slot, state = 'drained')`. `seg_seq` is
/// monotonic and never reused, so if another reclaimer already removed the
/// row, or a seal re-seeded this slot under a *new* `seg_seq`, the delete
/// matches zero rows and this rolls back **without truncating** — the other
/// order (truncate first) could destroy a slot a seal had already re-seeded.
///
/// Returns whether this call actually retired the segment.
async fn retire_one(
    client: &mut Client,
    seg_seq: i64,
    ring_slot: i16,
) -> Result<bool, StagingError> {
    let table = ring_table_name(ring_slot)?;
    let txn = client.transaction().await?;

    if let Err(err) = txn
        .batch_execute(&format!(
            "lock table {table} in access exclusive mode nowait"
        ))
        .await
    {
        if is_lock_not_available(&err) {
            return Ok(false);
        }
        return Err(err.into());
    }

    let deleted = txn
        .execute(
            "delete from segments where seg_seq = $1 and ring_slot = $2 and state = 'drained'",
            &[&seg_seq, &ring_slot],
        )
        .await?;
    if deleted == 0 {
        // Already retired (or re-seeded under a new seg_seq) by someone
        // else between the candidate scan and this transaction. Dropping
        // `txn` here rolls back — no truncate.
        return Ok(false);
    }

    txn.batch_execute(&format!("truncate {table}")).await?;
    txn.commit().await?;
    Ok(true)
}

/// The highest `seg_seq` at or below which every segment is drained (the
/// *contiguous drained prefix*), and the highest drained `seg_seq`. Segments
/// drain out of order, so the two differ while an older one lags. A retired
/// segment was drained, so the prefix is the segment below the oldest live
/// one that isn't, or the newest segment when none lags (in practice the
/// active one always does).
const DRAINED_PREFIX_SQL: &str = "\
    select coalesce(min(seg_seq) filter (where state <> 'drained') - 1, max(seg_seq)), \
           max(seg_seq) filter (where state = 'drained') \
    from segments";

/// Every ledger table there is, schema-qualified and quoted: a
/// definition's `<target>__ledger`, where it exists. Only aggregate targets
/// have one today; a 1-1 target's (#623 D6) is found the same way, with the
/// same ordering-state columns, so [`collect_tombstones`] collects it as is.
const LEDGER_TABLES_SQL: &str = "\
    select format('%I.%I', n.nspname, c.relname) \
    from transform_definitions d \
    join pg_catalog.pg_namespace n on n.nspname = split_part(d.target_table, '.', 1) \
    join pg_catalog.pg_class c on c.relnamespace = n.oid \
     and c.relname = split_part(d.target_table, '.', 2) || $1 \
    order by 1";

/// The most tombstones one GC transaction deletes.
const TOMBSTONE_BATCH: i64 = 5_000;

/// The most GC transactions one pass runs per ledger, so a backlog is worked
/// off over several maintenance ticks rather than holding one up.
const TOMBSTONE_BATCHES_PER_PASS: usize = 4;

/// Deletes the tombstones no change can still need (#623 D7): in every
/// ledger, each `__tombstone` entry whose `__applied_seg` is at or below the
/// contiguous drained prefix `P` ([`DRAINED_PREFIX_SQL`]). Returns how many
/// went.
///
/// **Why it's sound.** A tombstone exists so that an older change to its key,
/// arriving after the delete, is skipped (ADR-0002 I2 and I4); once one is
/// collected, such a change would apply to a fresh entry and bring the row
/// back. None can arrive. Capture is by trigger, so a same-key predecessor
/// of the delete committed before the delete's trigger ran: every snapshot
/// that sees the delete sees it too. A batch is the rows its fence sees and
/// its predecessor's fence doesn't (`seal::fenced_window`), so the
/// predecessor is in the delete's batch or an earlier one, and `applied_seg`
/// is at least the delete's batch (a page writes the highest `seg_seq` it
/// drains, never lowering it). At or below `P` every batch is drained: the
/// predecessor has already been applied, or skipped. The highest drained
/// segment is not enough, since an older one may still lag (the
/// `early_tombstone_gc` plant).
///
/// A tombstone written by a Re-derive has a `basis` that the collection
/// forgets. A change the basis saw but a later batch carries then applies,
/// instead of being skipped. That is only a transient: the read saw no
/// row, so the basis also saw a later delete of the key, which is in that
/// batch or a later one, and applies after it.
///
/// **Against a page.** Each batch is a short transaction of its own. It
/// takes the ledger's `ROW EXCLUSIVE` lock `NOWAIT`, so a build or drop of
/// the ledger (or of its definition) makes this pass skip it rather than
/// wait. It skips the entries a page has locked, so it never waits on one,
/// and each delete re-checks its predicate on the row it locks: an entry a
/// page has just revived is no longer a tombstone. A page's entry lock
/// retakes itself if a delete lands between its placeholder insert and
/// its lock (`ledger::apply_ledger_target`).
pub async fn collect_tombstones(client: &mut Client) -> Result<u64, StagingError> {
    let row = client.query_one(DRAINED_PREFIX_SQL, &[]).await?;
    let prefix: Option<i64> = row.get(0);
    #[cfg_attr(not(any(test, feature = "test-util")), allow(unused_variables))]
    let highest_drained: Option<i64> = row.get(1);
    let Some(through) = prefix else {
        return Ok(0);
    };
    // Planted bug (#557): collect through the highest drained segment, not
    // the contiguous drained prefix. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let through = match highest_drained {
        Some(highest)
            if crate::plant::fires(crate::plant::Plant::EarlyTombstoneGc, highest > through) =>
        {
            highest
        }
        _ => through,
    };

    let ledgers: Vec<String> = client
        .query(LEDGER_TABLES_SQL, &[&crate::defs::ledger::LEDGER_SUFFIX])
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    let delete = |ledger: &str| {
        let tombstone = quote_ident(crate::defs::ledger::TOMBSTONE_COLUMN);
        let seg = quote_ident(crate::defs::ledger::APPLIED_SEG_COLUMN);
        format!(
            "delete from {ledger} where ctid = any(array( \
                 select ctid from {ledger} where {tombstone} and {seg} <= $1 \
                 limit $2 for update skip locked))"
        )
    };
    let mut collected = 0;
    for ledger in &ledgers {
        let sql = delete(ledger);
        for _ in 0..TOMBSTONE_BATCHES_PER_PASS {
            let txn = client.transaction().await?;
            if let Err(err) = txn
                .batch_execute(&format!("lock table {ledger} in row exclusive mode nowait"))
                .await
            {
                if is_lock_not_available(&err) || is_undefined_table(&err) {
                    break;
                }
                return Err(err.into());
            }
            let deleted = txn.execute(&sql, &[&through, &TOMBSTONE_BATCH]).await?;
            txn.commit().await?;
            collected += deleted;
            if deleted < TOMBSTONE_BATCH as u64 {
                break;
            }
        }
    }
    Ok(collected)
}

fn is_undefined_table(err: &tokio_postgres::Error) -> bool {
    err.code() == Some(&tokio_postgres::error::SqlState::UNDEFINED_TABLE)
}

fn is_lock_not_available(err: &tokio_postgres::Error) -> bool {
    matches!(
        err.code(),
        Some(code) if *code == tokio_postgres::error::SqlState::LOCK_NOT_AVAILABLE
    )
}
