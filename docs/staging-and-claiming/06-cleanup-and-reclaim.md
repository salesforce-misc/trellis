# Stage 6 — Cleanup: retiring a batch, and quarantining a killer change

← [Apply and exactly-once deltas](05-apply-and-exactly-once-deltas.md) · next → [Convergence and await](07-convergence-and-await.md)

**What this stage owns:** bounding storage, returning ring slots to service, and
keeping one undrainable change from wedging the system forever.

**The guarantee:** *no cleanup step can retire un-applied work.* Every removal is
gated on a proof that nobody can still need it.

## Retiring a batch

A `drained` batch is eligible for `TRUNCATE` when **all four** hold:

1. **`seal_step2 IS NOT NULL`** — its successor sealed, so its own fence boundary
   is closed;
2. **`txid_snapshot_xmin(current) > seal_step2`** — every transaction that could
   still write into it has finished;
3. **its successor is also `drained`** — nobody needs it as the *predecessor*
   half of a both-slots read ([03](03-sealing-and-the-fence.md));
4. **no batch older than that successor is anything but `drained`** — no lagging
   worker behind the boundary.

Then:

```sql
LOCK TABLE seg_<n> IN ACCESS EXCLUSIVE MODE NOWAIT;   -- NOWAIT: skip, never block
DELETE FROM segments WHERE seg_seq = :s AND ring_slot = :n AND state = 'drained';
TRUNCATE seg_<n>;
```

Each line fixes a real bug:

- **`NOWAIT`.** If the lock is held, the pass **skips** and retries next tick — a
  blocking cleanup pass turns a slow drain into a stalled fleet.
- **`DELETE` before `TRUNCATE`.** The registry delete *is* the reclaim claim,
  keyed on `(seg_seq, ring_slot, state = 'drained')`. `seg_seq` is monotonic and
  never reused, so if another reclaimer already removed the row — or a seal
  re-seeded the slot under a *new* `seg_seq` — the delete matches zero rows and we
  roll back without truncating. The reverse order can destroy a slot a seal
  already re-seeded.
- **Removing the registry row frees the slot**, not the truncate — which is why
  [02](02-the-staging-ring.md)'s `RingFull` check reads the registry, not the
  table. `TRUNCATE` retires the storage in one shot; per-row deletes would
  reintroduce the vacuum problem [02](02-the-staging-ring.md) exists to avoid.

Condition 3 is phrased as *"no successor that is NOT drained"*, so an **absent**
successor qualifies: a registry row is removed only by a completed reclaim, which
requires `drained`, so absent means it retired and its straddlers are applied.
Demanding a present-and-drained successor would strand a batch forever whenever
its successor retired first — a lock skip can retire *s+1* and leave *s*
ineligible, leaking a ring slot into a permanent `RingFull` wedge.

**Condition 4 is why an undrainable batch is an instance-wide stop**, not a
per-table one: one batch stuck below the boundary makes *every* candidate
ineligible, so the ring fills and seals fail. That is the intended semantic for a
genuine schema error ([05](05-apply-and-exactly-once-deltas.md)) — and the reason
quarantine exists for everything that is *not* one.

## Who runs the sweeps

The maintenance pass runs on the **idle tick** of every drain loop and of the
worker pool's sweep (default 5 s). It:

1. recovers any crashed mid-seal batch's fence ([03](03-sealing-and-the-fence.md));
2. reclaims claims older than the reclaim window (default 30 s);
3. retires eligible drained batches.

It is also the **liveness unblock for a saturated ring**: a worker that gets
`RingFull` from a seal runs the pass and retries once — else a ring full of
drained-but-not-yet-retired slots wedges the drain forever.

Recovery from a dead claimant is bounded by *reaching an idle tick*, so under
never-idle load its batch waits until the queue drains. This stays sound: every
pending predicate counts non-`drained` batches, so a deferred reclaim adds
latency, never false convergence.

## Quarantine: when a change deterministically kills its worker

A change that reliably crashes the apply must not wedge its batch forever — but
silently skipping it makes a caller waiting on that change wait forever, or worse,
be told it converged.

**1. Isolate before blaming.** On a non-transient, non-halting apply failure, each
folded record is applied *alone* inside a `BEGIN … ROLLBACK` probe. Blame lands on
the specific key(s) that fail on their own, so an innocent batch-mate is neither
charged nor evicted. If no single key reproduces it, the error is surfaced, not
blamed.

**2. Count deaths per key, off the immutable rows.** Batch rows are immutable and
carry no counter, so the counter lives in its own table keyed by `(table, key)`. A
**clean** drain clears the counters for the keys it applied, so a transient death
does not accumulate toward a false eviction.

**3. Evict past a threshold, and hold the work.** At `deaths >= N` (default 5; `0`
disables), the key's folded record is copied to a marker table, its contribution
parked, and the batch **re-folded without it** and retried. Survivors drain, the
batch reaches `drained`, the ring keeps moving.

**4. The parked work — not the marker — is the source of truth.** The marker is
*per key*, but the fold excludes poisoned keys **globally**, so a healthy *later*
change to a poisoned key in a different batch would vanish when that batch
retired. Therefore **every batch that excludes a poisoned key parks its own folded
contribution before it marks drained, in the same transaction**, keyed
`(table, key, batch)`.

```sql
-- inside the Phase-3 transaction, before the drained mark
INSERT INTO poison_held (src_table, key, seg_seq, op, lsn, old_image, new_image, origin_lsn, ...)
SELECT ... -- this batch's fold, restricted to already-poisoned keys
ON CONFLICT (src_table, key, seg_seq) DO NOTHING;
```

It is deliberately **not** bucket-scoped: it parks the whole batch's contribution,
complete and idempotent on the extended key, so a co-worker on another bucket
parking the same rows is a no-op, not a conflict.

**Release is operator-driven, one transaction:** replay every held row for the key
— in batch then position order — into the active batch, then delete the held rows,
the marker, and the death counter. The ordered replay telescopes the per-key delta
chain back together ([05](05-apply-and-exactly-once-deltas.md), property 3). Each
replayed row keeps its **original** origin position, so its band stays blocked
until the release actually drains — the key is never in neither place, which would
make the read-your-writes predicate lie ([07](07-convergence-and-await.md)).

**What must never be quarantined.** Two error classes are deterministic and
key-attributable yet caused by the *declared schema*, not the data — a tripped hop
bound (a real value cycle) and a relationship endpoint that is not a source
column. Quarantining either converts a loud, actionable error into a key that
blocks reads forever. They propagate, the instance stops, and that is correct.

## The one sanctioned exception to immutability

A staged row naming a **dropped** table can never be applied: the apply raises a
definition-changed error before writing, so the batch never drains — leaking a
ring slot *and* blocking reads for its band forever.

The escape hatch is a targeted purge — delete that table's rows from every ring
table and the quarantine track — invoked only when a full schema reload still does
not know the table. It is the **only** sanctioned write to a sealed batch's rows,
and worth marking as such in code so nothing else grows into the exception.

## Failure matrix

| Crash point | Left behind | Recovery |
|---|---|---|
| worker during Phase 2 | batch stays `draining`; **that worker's** bucket claims go stale | the sweep deletes the stale claims and, if none remain, returns the batch to `sealed`. A co-worker's claims are untouched — a dead worker costs the fleet its own share, not the batch |
| worker errors (not a crash) at any phase | nothing applied — a fold error precedes every write, an apply error rolls back | the function that *took* the claim releases it on the spot, re-claimable in milliseconds, no TTL wait |
| worker mid-Phase-3 before commit | whole transaction rolls back | as above — exactly once, because apply and mark share one commit |
| worker after Phase-3 commit | its buckets' bits are set; batch is `drained` only once the mask fills | the cleanup pass retires it once every bucket lands and the four conditions hold |
| worker dies holding a claim already reclaimed | a second worker may claim the reclaimed buckets while the first still computes | the first's completion deletes zero claim rows → errors and rolls back; only the current claimant's apply commits |
| process restart with `draining` batches | every claim is stale by definition | the first idle sweep reclaims them |
| a staged row naming a dropped table | the batch can never drain | the targeted purge above |
| reclaimer racing a reclaimer | — | the `ACCESS EXCLUSIVE NOWAIT` lock serializes, and the registry delete is the claim |
