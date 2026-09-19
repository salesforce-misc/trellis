//! Convergence and the read-your-writes predicate (issue #12, stage 07). See
//! docs/staging-and-claiming/07-convergence-and-await.md — this module
//! implements the four-condition predicate, the cheap gate, the (never
//! polled) observability count, and the caller-side await poll, in that
//! order below.
//!
//! **Condition 4, the quarantine/poison band** (doc section "the one
//! exclusion is parked quarantine work"): issue #16's `poison_held` table is
//! what this term reads — not `poison` (the marker; a poisoned key with
//! nothing parked yet has nothing pending) and not `key_deaths` (a pure
//! counter, no origin position of its own). A row parked in `poison_held`
//! keeps its original `origin_lsn`/`lsn` from the batch that excluded it
//! (see `quarantine::park_batch_contribution`), so it gates the predicate
//! exactly like an un-drained ring row would, until [`quarantine::release_key`]
//! replays it back onto the active batch and it drains for real. This is
//! what makes convergence never report "converged" across a parked key.
//!
//! **Deferred to #14/#15:** the doc's "a round did no work" classification
//! table (pause-lease-active, refused-seal backpressure, peer-holds-batch)
//! describes a *worker-side drain-to-convergence helper* that depends on
//! claiming/sweeping (#14/#15) and cleanup (#13) machinery this crate
//! doesn't have yet. This module builds only the caller-side poll
//! ([`await_converged`]) the doc's "Watermark tokens" section describes —
//! not that helper.
//!
//! **The trap this module exists to avoid:** the design doc's per-batch
//! summary band (`min_origin_lsn`/`max_lsn`, aggregated inside the seal's
//! phase-1 flip transaction) is the obvious optimization for condition 3 —
//! one indexed comparison per batch instead of a scan — and it is wrong: a
//! band aggregated inside the flip describes what that transaction could
//! see, not the slot, so a straddler or a phase-gap writer
//! (docs/staging-and-claiming/03-sealing-and-the-fence.md) can each land in
//! the slot outside it and produce a false `converged`. Nothing in
//! `segments` (see `V3__staging_ring.sql`) even carries those two columns
//! today — they are vestigial in the doc, not a real surface to restore a
//! reader for. Condition 3 below asks the slot directly instead.

use std::time::{Duration, Instant};

use tokio_postgres::GenericClient;
use tokio_postgres::types::PgLsn;

use super::append::{RING_SIZE, ring_table_name};
use super::error::StagingError;

/// Builds one fragment per ring slot (`seg_0..seg_3`) via `f(ring_slot,
/// table_name)`, then joins them with `sep`. Every predicate below that must
/// reason about "every ring table" shares this rather than each hand-rolling
/// its own unroll of `RING_SIZE`.
///
/// `pub(crate)` (issue #132, epic #127): `apply`'s guard (c) in-flight check
/// needs the exact same "reason about every ring table" pattern, scoped to a
/// specific `src_table`/join-key/`lsn` predicate instead of this module's
/// own origin-lsn-threshold one — see `apply::from_side_change_in_flight`.
pub(crate) fn per_ring_table(sep: &str, f: impl Fn(i16, &str) -> String) -> String {
    (0..RING_SIZE)
        .map(|slot| {
            let table = ring_table_name(slot).expect("0..RING_SIZE is always a valid ring slot");
            f(slot, table)
        })
        .collect::<Vec<_>>()
        .join(sep)
}

/// `converged_through(token)`: true iff every un-reflected effect of a
/// commit at or below `token` is already reflected. **One SQL statement**,
/// so all conditions are evaluated against one committed snapshot — see the
/// doc's "The predicate" section for why splitting this into separate
/// queries reintroduces the race it's built to close.
///
/// - **Condition 1** — `confirmed_lsn >= token` from `replication_progress`.
///   A **missing** row (or no rows at all) reads as NOT converged: the
///   `coalesce(..., false)` around the comparison is what makes absence
///   fail closed rather than vacuously succeed. `min(confirmed_lsn)` rather
///   than a single row so a caller running against more than one slot's
///   progress row can't have one lagging slot hidden behind another.
/// - **Condition 2** — the active batch's ring table holds no row with
///   `origin_lsn <= token`. The active `ring_slot` is resolved from
///   `segment_pointer` in this same statement (so a concurrent seal can't
///   slip between the resolve and the scan), and each candidate slot's term
///   is written as `(select min(origin_lsn) from seg_N) <= token` — a
///   `Limit 1` plan over the `origin_lsn` index regardless of statistics —
///   never `exists(...)`, because the active slot grows continuously and no
///   `ANALYZE` cadence keeps a growing table's histogram current (doc:
///   "Making the honest question cheap").
/// - **Condition 3** — no *non-active* slot holds a still-pending row with
///   `origin_lsn <= token`, under one definition of "pending": the row's
///   owning `segments` row has `state <> 'drained'`. This is `exists(...)`
///   (short-circuits on the first hit), because sealed/draining slots *do*
///   get a single-column `ANALYZE (origin_lsn)` on seal (see
///   `super::seal::seal_phase2`), so the planner has real statistics to seek
///   with. Deliberately **not** narrowed to "buckets not drained" or
///   "band-only" — a part-drained batch's whole slot counts as pending, the
///   safe over-report the doc's "Never narrow condition 3" section demands.
/// - **Condition 4** — the quarantine/poison band: no `poison_held` row for
///   this `origin_lsn is null or origin_lsn <= $1`. Same NULL-gates-old
///   reasoning as condition 2/3 below, since a parked recompute-shaped row
///   carries no `origin_lsn` either.
///
/// `origin_lsn = '0/0'` means "unknown, conservatively old" and is `<=` any
/// token, so a zero-origin pending row always gates conditions 2 and 3 —
/// that falls out of plain `<=` with no special-casing. A SQL `NULL`
/// `origin_lsn` (every row the three non-CDC producers stage — see
/// `append.rs`'s `StagedChange::Recompute`, which never sets the field at
/// all) means the same "unknown", so it's handled explicitly alongside the
/// `<=` comparison (`origin_lsn is null or origin_lsn <= token`) rather than
/// folded into it: `NULL <= token` is itself `NULL`, which three-valued SQL
/// logic would otherwise let quietly vanish from a `WHERE` filter or an
/// aggregate instead of gating the row.
pub async fn converged_through(
    client: &impl GenericClient,
    token: PgLsn,
) -> Result<bool, StagingError> {
    // `origin_lsn` is nullable — the three non-CDC producers' `Recompute`
    // shape (`append.rs`) never sets it, so a purely-recompute row always
    // reads NULL here, not `'0/0'`. Both are "unknown", and the doc pins
    // "unknown" to "conservatively old" — a NULL must gate exactly like the
    // explicit sentinel would. Two things go wrong if that's left implicit:
    // `min(origin_lsn)` silently ignores NULL rows rather than forcing the
    // minimum down to "always gates", and a bare `origin_lsn <= $1` in a
    // `WHERE` clause silently *excludes* NULL rows instead of matching them
    // — either one is a false `converged` waiting to happen the day a batch
    // turns out to be recompute-only. Below, `... or origin_lsn is null`
    // covers that case explicitly, in a form the `origin_lsn` btree index
    // still serves directly (a plain existence probe on the same index,
    // rather than a rewrite of the indexed expression a `coalesce(...)`
    // wrapper would otherwise force) — so no plan quality is traded away to
    // get the NULL case right. The outer `coalesce(..., false)` on
    // condition 2 is the separate, unrelated fix for the *empty*-table case:
    // `min()` over zero rows is NULL too, and that must mean "nothing to
    // gate on", not "unknown, so block everything".
    let condition2 = per_ring_table(" or ", |slot, table| {
        format!(
            "((select ring_slot from segment_pointer) = {slot} \
              and coalesce( \
                  (select min(origin_lsn) from {table}) <= $1 \
                  or exists (select 1 from {table} where origin_lsn is null), \
                  false \
              ))"
        )
    });

    let condition4 = "select 1 from poison_held \
                       where origin_lsn is null or origin_lsn <= $1";

    // Invariant this leans on: a physical `seg_N` only holds rows while it has
    // a `segments` row (the `exists (... segments ...)` term). A slot's rows
    // are appended only while it is the *active* slot, which always carries a
    // registry row, and retirement (`retire::retire_drained_segments`) always
    // deletes the `segments` row and truncates the table atomically, in one
    // transaction — so a populated slot with no registry row is unreachable.
    // If that ever changed, such orphaned rows would silently *not* gate (the
    // `exists` is false) — a false `converged`.
    //
    // **A `'drained'` owner does not, on its own, clear a row.** A phase-gap
    // straggler (docs/staging-and-claiming/03-sealing-and-the-fence.md, "The
    // scoping bug worth knowing about") can physically land in `r`'s slot
    // *after* its owning segment's own fence (`s.fence_snapshot`) was
    // captured — by construction it can never become visible in that
    // immutable fence, so `s` can reach `'drained'` (every row *it* actually
    // scanned got folded and applied) while this one straggler row sits
    // there, folded by nobody. Only the immediate successor's own fenced
    // read (`fenced_window`'s predecessor-half union clause) can ever claim
    // it, and that only runs once the successor itself seals
    // (`seal::seal_if_active_nonempty`'s straggler-catching case). Until
    // then this row is still genuinely pending, so a `'drained'` owner only
    // clears `r` when `r` itself was actually visible in that owner's own
    // fence — never unconditionally. Without this, a straggler landing right
    // as the ring goes quiet reports `converged` while its target never gets
    // written — the exact silent-loss shape this predicate exists to
    // prevent.
    let condition3 = per_ring_table(" union all ", |slot, table| {
        format!(
            "select 1 from {table} r \
             where (select ring_slot from segment_pointer) <> {slot} \
               and (r.origin_lsn is null or r.origin_lsn <= $1) \
               and exists ( \
                   select 1 from segments s \
                   where s.ring_slot = {slot} \
                     and (s.state <> 'drained' \
                          or (s.fence_snapshot is not null \
                              and not pg_visible_in_snapshot( \
                                  r.row_txid, s.fence_snapshot))) \
               )"
        )
    });

    let sql = format!(
        "select \
             coalesce((select min(confirmed_lsn) from replication_progress) >= $1, false) \
             and not ({condition2}) \
             and not exists ({condition3}) \
             and not exists ({condition4})"
    );

    let row = client.query_one(&sql, &[&token]).await?;
    Ok(row.get(0))
}

/// "Is anything pending at all?" — the cheap gate the doc's "Ask for the
/// sign, not the number" table describes: `exists(...)` over every slot
/// (active tail included) whose owning segment isn't `drained`, so it
/// short-circuits on the first pending row rather than scanning everything.
/// No token: this asks whether *anything* is pending anywhere, not whether
/// a particular position has cleared.
///
/// Unlike [`converged_through`]'s condition 2/3 split, this doesn't need to
/// special-case the active slot's statistics: it takes no `origin_lsn`
/// bound, so there's no inequality selectivity for the planner to guess at
/// either way — a plain existence check is cheap on any slot regardless of
/// `ANALYZE` cadence.
#[cfg(any(test, feature = "test-util"))]
pub async fn has_pending(client: &impl GenericClient) -> Result<bool, StagingError> {
    let arms = per_ring_table(" union all ", |slot, table| {
        format!(
            "select 1 from {table} \
             where exists ( \
                 select 1 from segments s \
                 where s.ring_slot = {slot} and s.state <> 'drained' \
             )"
        )
    });
    // Issue #16: a parked `poison_held` row is pending too — it hasn't
    // drained, it's excluded from the active batch until release replays it.
    let sql = format!("select exists ({arms} union all select 1 from poison_held)");
    let row = client.query_one(&sql, &[]).await?;
    Ok(row.get(0))
}

/// "How much is pending?" — observability only. **Never poll this**: it is
/// a full `count(*)` across every ring slot, linear in the number of pending
/// rows with no index to serve it (there is no inequality here to seek
/// against — every row counts). Every polled consumer wants [`has_pending`]
/// or [`converged_through`]'s sign, not this number.
#[cfg(any(test, feature = "internals"))]
pub async fn pending_count(client: &impl GenericClient) -> Result<i64, StagingError> {
    let arms = per_ring_table(" union all ", |slot, table| {
        format!(
            "select count(*) as c from {table} \
             where exists ( \
                 select 1 from segments s \
                 where s.ring_slot = {slot} and s.state <> 'drained' \
             )"
        )
    });
    // Issue #16: every parked `poison_held` row counts as pending too.
    let sql = format!(
        "select coalesce(sum(c), 0)::bigint from \
         ({arms} union all select count(*) as c from poison_held) counts"
    );
    let row = client.query_one(&sql, &[]).await?;
    Ok(row.get(0))
}

/// Takes a watermark token: `pg_current_wal_lsn()` on the caller's own
/// connection. The caller must run this *after* its mutation has committed
/// — taken any earlier, it would bound the write from below instead of
/// above, and [`await_converged`] could return before the write is actually
/// reflected (docs/staging-and-claiming/07-convergence-and-await.md,
/// "Watermark tokens").
pub async fn watermark_token(client: &impl GenericClient) -> Result<PgLsn, StagingError> {
    let row = client.query_one("select pg_current_wal_lsn()", &[]).await?;
    Ok(row.get(0))
}

/// Polls [`converged_through`] until it reports `true` or `timeout` is
/// exhausted. Backoff starts at 5ms and doubles to a 250ms ceiling
/// (monotonic — never resets within one call), which keeps the poll cheap
/// for a token that clears quickly without hammering the database on a
/// token that takes a while.
///
/// The engine's own workers do the actual draining; this only waits for
/// them. On timeout, returns [`StagingError::ConvergenceTimeout`] — a named
/// variant, not a generic "values not stabilizing" message (see the error
/// variant's own doc comment for why that framing is actively misleading
/// here).
pub async fn await_converged(
    client: &impl GenericClient,
    token: PgLsn,
    timeout: Duration,
) -> Result<(), StagingError> {
    const INITIAL_BACKOFF: Duration = Duration::from_millis(5);
    const MAX_BACKOFF: Duration = Duration::from_millis(250);

    let started = Instant::now();
    let mut backoff = INITIAL_BACKOFF;
    loop {
        if converged_through(client, token).await? {
            return Ok(());
        }
        let waited = started.elapsed();
        if waited >= timeout {
            return Err(StagingError::ConvergenceTimeout { token, waited });
        }
        tokio::time::sleep(backoff.min(timeout - waited)).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

// --- Boundary marker: the loose observability view lives on the other side ---
//
// A "list every pending row" helper (docs/staging-and-claiming/
// 07-convergence-and-await.md, "An observability view, distinct from the
// fenced read") belongs *below this line*, not above it: it is a read view,
// not a fenced one, and may include a straggler the fold will attribute to a
// later batch. That's fine for a self-check auditor's exemption map or an
// overdue-work battery — over-exempting and over-reporting are both
// conservative there — but it must never be wired into [`converged_through`]
// or any other fenced/strict read above. No such helper exists yet:
// [`has_pending`]/[`pending_count`] cover this module's own needs, so this
// marker is left for whichever stage adds one.
