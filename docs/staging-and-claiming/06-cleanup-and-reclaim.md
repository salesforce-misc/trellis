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

**Condition 4 is why an undrainable batch would be an instance-wide stop**, not a
per-table one: one batch stuck below the boundary makes *every* candidate
ineligible, so the ring fills and seals fail. That is why no failure class must
leave a batch undrainable: a genuine schema error pauses the definitions it
reaches so the batch drains without them
([05](05-apply-and-exactly-once-deltas.md#failure-classification)), and
quarantine exists for everything that is *not* one. The one fallback that
re-claims instead is a halt that pauses nothing (see 05).

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

**1. Isolate before blaming.** On a non-transient, non-halting apply failure,
the page's `COMMIT` included (a
halting one pauses its closure instead — see *What must never be quarantined*
below), the batch is bisected with `BEGIN … ROLLBACK` probes: each half is
applied on its own, and only a half that fails is split and probed again, down to
single records. A probe runs `SET CONSTRAINTS ALL IMMEDIATE` before its
`ROLLBACK`, so a deferred constraint on a target fails in the probe as it does
at `COMMIT`. Blame lands only on the specific key(s) that fail on their own, so
an innocent batch-mate is neither charged nor evicted. If no single key reproduces it, the error is
surfaced, not blamed. Bisection finds a failing key in about `2·log2(n)` probes
rather than `n`, and a per-call probe cap bounds the rest.

Blame also names the **transform**: a key that fails alone is probed again
with the transforms reading its table directly left out but one, to find the
one(s) whose apply it fails in. One that still fails with every direct reader left
out fails in the work done for the transforms reading the table through a
relationship: each relationship to the table is probed alone, and the readers of
each one that fails alone are charged, or every relationship's readers when none
does. One that fails with no direct reader alone fails only when several apply it
together: with two, both are charged; with more, each is left out in turn, and
every one whose absence lets it apply is charged. When no one is (two separate
failing pairs), nobody is, and the page is a drain holdup
([known gap 24](../known-correctness-gaps.md#24-a-change-only-two-separate-pairs-of-definitions-fail-on-together)).
A failure in the source itself (an image that won't decode) fails for every reader,
and is charged once per transform that hits it.

**2. Count deaths per key, off the immutable rows.** Batch rows are immutable and
carry no counter, so the counter lives in its own table keyed by `(transform,
table, key)`. A **clean** drain clears the counters for the keys it applied, for
every transform that applied them, so a transient death does not accumulate
toward a false eviction.

**3. Evict past a threshold, and hold the work.** At `deaths >= N` (default 5; `0`
disables), the key is marked poisoned **for that transform**, its contribution
parked for it, and the batch recomputed **without it in that transform's apply**
and retried. Every other transform reading the key keeps applying it. Survivors
drain, the batch reaches `drained`, the ring keeps moving. A transform whose
poisoned keys reach the same threshold is quarantined; the count is its own, never
a sibling's.

**4. The parked work — not the marker — is the source of truth.** The marker is
*per transform and key*, and the transform's apply leaves the poisoned key out of
**every** later batch, so a later change to it in a different batch would vanish
for that transform when that batch retired. Therefore **every batch that leaves a
poisoned key out of a transform parks its own folded contribution for that
transform before it marks drained, in the same transaction**, keyed
`(transform, table, key, batch)`.

```sql
-- inside the Phase-3 transaction, before the drained mark
INSERT INTO poison_held (transform_id, src_table, key, seg_seq, op, lsn, old_image, new_image, origin_lsn, ...)
SELECT ... -- this batch's fold, restricted to keys poisoned for the transform
ON CONFLICT (transform_id, src_table, key, seg_seq) DO NOTHING;
```

A batch parks for a transform only while the transform still holds the key: a
release or resume that deleted the key's rows after the batch was computed has
the key re-derived from its live row, and a row parked after it would be held
for a key nothing holds, blocking every later read.

The fold itself runs once per key per batch. A relationship's reverse work
for a to-side key skips it only once every transform reading through the
relationship that isn't frozen holds the key, or none is left that isn't frozen
(a define or resume of its first reader refreshes its projection). A change
every reader holds is dropped from the batch before its images are decoded.

It is deliberately **not** bucket-scoped: it parks the whole batch's contribution,
complete and idempotent on the extended key, so a co-worker on another bucket
parking the same rows is a no-op, not a conflict.

**Release is operator-driven, one transaction** (`Trellis::release_key`): stage one
`Recompute` of the key into the active batch, then delete the transform's held rows,
its marker, and its death counter for the key. Its first lock is a bump of the key's
table's version fence, which waits for every page holding the fence: a page that parks
a change for the key commits first and the release takes its row too, one that
computed before the release misses its fence and computes again, and one that read
the key as held but the fence after the release parks nothing, since a page parks
only while the key's `poison` row is there. The
held rows are not replayed: a replayed row would carry the releaser's `row_txid`,
not its source commit's, so a replay could regress a ledger entry a later
Re-derive already moved past. The `Recompute` is a Re-derive of the key from its
current row on every target that reads it
([05](05-apply-and-exactly-once-deltas.md#the-ledger)): idempotent for a transform
that applied the key all along, and parked again for one that still holds it. It
keeps the held rows' earliest origin position, so the key's band stays blocked
until the release actually drains — the key is never in neither place, which
would make the read-your-writes predicate lie ([07](07-convergence-and-await.md)).
A resume of the transform releases every key it holds: it deletes its held rows,
marker and death counters, and its rebuild re-derives the keys from the source.

**A held key blocks its own transform's band, not a sibling's.** The
cluster-wide await counts every transform's held rows, since it waits on
everything. A wait scoped to one transform (the self-check auditor's) counts only
that transform's, because a sibling applies the key as usual. A new transform's
capture gate (`table_changes_pending_through`) counts none: a held row is never
replayed with its images, and the new transform holds no key.

**What must never be quarantined.** The halting errors are deterministic yet
caused by the *declared schema*, not the data — a source key the drain can't use
(`NoPrimaryKey`, `UnsupportedPrimaryKeyType`), a tripped hop bound (a real value
cycle), an aggregate target off the ledger (`AggregateOffLedger`), and Postgres
refusing the drain's role a read or write (`42501`, row-level security or a
missing privilege, #766). Every key
reproduces them, so quarantining would blame one key for nobody's fault and turn
a loud, actionable error into a key that blocks reads forever. Instead the drain
pauses the closure the failure reaches — left `paused` with a `capture_failures`
row of `kind` `halt` — and drains the batch without it, so the ring keeps
retiring and every other definition keeps converging
([05](05-apply-and-exactly-once-deltas.md#failure-classification), #663).

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
