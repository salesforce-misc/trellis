# Stage 4 — Claiming a batch, and the claim-time fold

← [Sealing and the fence](03-sealing-and-the-fence.md) · next → [Apply and exactly-once deltas](05-apply-and-exactly-once-deltas.md)

**What this stage owns:** handing a sealed batch to workers — possibly several at
once — and collapsing a batch's many raw changes into one record per key.

**The guarantee:** *the claim is an optimization, not a correctness mechanism.*
Any interleaving of claim, reclaim, double-execution and crash converges to the
same derived state. Correctness comes from the fence
([03](03-sealing-and-the-fence.md)) plus the atomic apply
([05](05-apply-and-exactly-once-deltas.md)) — never from claim exclusivity.

## The claim is a cursor, not a take

Nothing is removed, moved, or marked when a batch is claimed. A claim is a
*positional, re-issuable cursor*: if the claimant dies, another worker re-claims
and re-drains from scratch, and the result is identical because the batch is
immutable and the recompute deterministic. That is what lets the reclaim window be
short and the claim mechanism cheap.

Contrast `UPDATE … SET claimed_by = me` on each row: the claim becomes a write,
the rows become hot-update targets, and losing a claim means reconciling partial
state.

## Partitioning a batch across workers

A batch is a unit of *batching*, not necessarily one unit of *work*. A large
batch is split into buckets at seal time, and workers claim buckets.

**How many buckets** is decided **once, by the seal, from configuration and batch
size only**: at least `MIN_ROWS_TO_SPLIT` (256) rows seals into `SEG_BUCKETS`
(default 8); anything smaller gets 1. It deliberately **does not** consult the
live-worker registry — workers register lazily and the count swings run to run, so
an immutable, safety-critical partition must not be sampled from a registry that
moves under it.

**How large a share** one claim takes *is* where adapting to the live fleet
belongs — being wrong there costs a pass of under-parallelism, not a double apply.
The share is `ceil(free_buckets / live_workers)`. Both extremes are wrong: *every*
free bucket is the single-worker bulk path again; *one* bucket makes a lone worker
pay `bucket_count` claims, folds and applies for one batch.

**Exclusivity is the primary key**, not a lock:

```sql
INSERT INTO seg_claims (seg_seq, bucket, claimed_by, claimed_at)
SELECT ... FROM free_buckets WHERE rn <= ceil(nfree / live_workers)
ON CONFLICT (seg_seq, bucket) DO NOTHING
RETURNING seg_seq, bucket;
```

Two workers computing overlapping shares both insert; the loser is returned fewer
rows. No bucket is ever held twice.

> **Measured cost (issue #277):** "the loser is returned fewer rows" happens
> only *after* the winner commits. `ON CONFLICT` against an uncommitted insert
> waits on the inserter's transaction, and the claim shares one with the fold
> (`drain_many`'s Phase 1). Workers that compute the same free share from the
> same snapshot all go for the same lowest bucket, so every loser waits out the
> winner's whole fold, then gets nothing and moves on. Under saturating load
> with 8 drain workers, ~40% of busy engine backend time went to this wait, and
> at 16 workers it was ~60%. A prototype that committed each claim before
> folding removed the wait entirely. Throughput didn't move at 400 or 4,000
> groups beyond run-to-run noise, because per-row drain work binds there
> first. With 400 groups, the workers freed from the claim queue went on to
> queue for the aggregate's target-row pre-lock. Fixing this won't raise
> throughput until that per-row cost comes down.

**The claim is one statement.** The claim rows and the batch's `sealed → draining`
flip are the same `WITH … INSERT … UPDATE …` statement, so they commit together.
Splitting them opens a crash window: a process dying in the gap leaves claim rows
on a still-`sealed` batch, the completion guard `state = 'draining'` then matches
nothing, and that worker can never complete a bucket it legitimately holds until
the 30 s reclaim TTL.

> **Postgres gotcha:** a data-modifying CTE that nothing references can be planned
> away. The flip CTE must be referenced through a `count(*)` guard in the outer
> query; an unreferenced `FOR UPDATE` pre-lock CTE gets pruned and locks nothing.

**A row's bucket is a total function of the row and its batch.** Every row is in
exactly one of its batch's buckets: none in two (a double apply), none in zero
(silently lost work). That is why the routing key is *stored*, not recomputed, and
`bucket_count` fixed at the seal — changing either mid-drain moves a row out of a
completed bucket into an incomplete one, and it applies twice.

There is **one bucket definition, in SQL**, with one caller (the fold's filter);
which buckets a worker holds is read from the claims table, never recomputed. Any
host-language `bucket_of_route` should be a **test oracle** against that SQL, so
"the two agree" is a property the tests check, not a risk the engine runs.

### What key-routing buys and what it does not

The routing key is `(src_table, key)` — deliberately not the aggregate group key.
Two obstacles are real:

- **Three of the four producers have no image to derive a group key from.** They
  append bare recompute triggers, so a group key would need a per-row catalog
  lookup and live source read on the hot append path — the exact cost the generated
  column exists to avoid.
- **A per-batch bucket is not stable across batches.** `bucket_count` is per-batch
  and buckets re-assign at every claim, so group G in batch *k* and *k+1* need not
  meet the same worker, which co-location would require.

So key-routing buys **disjoint bulk parallelism** and keeps every row of a key in
one bucket, but it does **not** co-locate an aggregate group: two workers on two
buckets of one batch still contend on a hot group row. A known limitation, not an
accident.

Nor does it **order a key's writes across batches.** A key lands in one bucket
*per batch*, but batches drain out of order and in parallel, so two batches'
records for the same key can reach Phase 3 in either order. Deltas commute, so
that's harmless for them. Absolute (1-1) writes don't, and Phase 3's per-key
ordering lock and basis check exist for that
([05](05-apply-and-exactly-once-deltas.md#absolute-writes-do-not-commute-the-basis-check)).

**The honest cost:** the fold filter `route % bucket_count = ANY(mine)` rides the
sequential scan the fold already does, so **every holder scans the whole batch for
its 1/b**. The fold is `O(b × S)` for a batch of S rows, of which `(b−1) × S` is
redundant. There is deliberately no index on a ring table — with
`autovacuum_enabled = off` there are no statistics, so the planner would ignore
one anyway ([02](02-the-staging-ring.md)).

## The claim-time fold

The merge the write-time upsert once did now happens at **read** time: group the
claimed batch's fenced window by `(table, key)` into one record each.

Four rules are load-bearing — the same four the write-time merge established:

| Field | Rule | Why |
|---|---|---|
| `new_image` | **last** — post-image of the highest-`lsn` change (`change_id` breaks ties) | the delta's add side `+f(new)` must track the latest image |
| `old_image` | **first** — pre-image of the lowest-`lsn` change | the state the key was last materialized from; the delta's removal side `−f(old)` |
| `src_changed` | **OR** | any source contribution makes the record a source change, so downstream propagation fires; dropping the OR reintroduces a mutual-derivation livelock |
| `origin_lsn` | **LEAST** | the oldest-origin marker read-your-writes soundness depends on ([07](07-convergence-and-await.md)) |

Plus `lsn` GREATEST (over *every* row, image-less included, so the watermark
still covers them) and a hop-generation counter that resets to 0 on any source
change.

`change_id` (issue #31) is store-assigned from a shared `nextval` sequence,
monotonic across the ring. It records the one order commit `lsn` can't: every row
of a transaction shares its commit `lsn`, so ordering an INSERT before a later
UPDATE of the same key needs an intra-commit order. `lsn` stays primary;
`change_id` only breaks ties within a single commit.

### The two kinds of missing image

The fold's sharpest subtlety: some producers emit changes, others bare triggers,
so both arg-extremes must distinguish **"there is genuinely no image here"** from
**"this producer does not carry images"** — two facts needing different answers:

- A key **born inside the batch** (insert-then-update) folds to `old_image =
  NULL`, because its lowest-`lsn` change is the insert, which has no pre-image (a
  delete symmetrically has no post-image). That NULL is a *fact about the change*
  and it suppresses a spurious `−f(old)`. It is more correct than the old
  write-time `COALESCE(old_image, EXCLUDED.old_image)` merge, and it must survive
  the fold. **Do not reintroduce a blanket `COALESCE` here.**
- An **image-less row** — both images NULL — is not a change at all. Aggregates
  like `array_agg` don't skip NULLs, so before the discriminator such a row won
  whichever ordering its `lsn` topped and handed the drain a NULL image, which the
  delta path reads as "no side to apply". The failure ran both ways: a re-derive
  restaged at `pg_current_wal_lsn()` killed the `+f(new)` and **under**-counted, up
  to deleting a live group; reverse propagation and backfill restaged below and
  killed the `−f(old)`, **over**-counting with a phantom member.

The discriminator is therefore *"does this row carry any image at all"*, **not**
*"is this image column null"*:

```sql
-- scope both arg-extremes to image-bearing rows only
WHERE old_image IS NOT NULL OR new_image IS NOT NULL
```

An insert qualifies via its `new_image`, a delete via its `old_image`, a
primary-key move-out via its `old_image` — so each still contributes its honest
NULL on the other side, while a bare trigger is excluded from both. A key whose
rows are all image-less folds to both images NULL, which is correct: recompute
from live source and take a zero delta.

The corollary binds *producers*, not just the fold: within one window, for one
`(src_table, key)`, an image-bearing row always beats an image-less one — **even
when the image-less one is newer**. A producer that can stage both shapes for the
same key must therefore pick one. See issue #180's downstream propagation of an
extinct aggregate group: it stages a real image-bearing delete carrying the
group's captured pre-delete image, but drops back to an image-less `Recompute`
for any key the same batch also *wrote*, precisely so the delete cannot
annihilate the write and leave a live group subtracted downstream. Issue #196
threads the same capture through a deleted 1-1 target row's own downstream
propagation, reusing this same guard rather than a second copy of it.

One field is deliberately **not** filtered: `op`, because the only load-bearing op
is the `truncate` sentinel, and that sentinel is itself image-less. Filtering `op`
would fold it to NULL and lose the truncate.

### Practical notes on the fold

- It reads **both slots** — the sealed slot and its predecessor — under the two
  visibility terms from [03](03-sealing-and-the-fence.md). The bucket filter sits
  *inside* that fenced window, so a key never folds across a boundary its worker
  does not own.
- Raise the session sort memory before the fold (Trellis uses 64 MB) so the
  ordered aggregates sort in memory rather than spilling on a large batch.
- Carry `first_seen = min(appended_at)` per key — the latency origin. It must be
  per-row, **not** the batch's creation timestamp: an active batch is created empty
  and its age is unbounded during idle, so creation-time latency over-reports wildly
  — a change drained in milliseconds appears to take seconds.

## Keeping a claim alive

*Progress* (never correctness) depends on a live worker keeping its claim, and
that takes two mechanisms, because a worker owns one connection that is busy for
the whole of each step.

- **In-line**, on the worker's own connection: refresh `claimed_at` before each
  source table read. Enough for the steady-state trickle, where a whole drain is
  milliseconds.
- **Out-of-band**: a process-wide daemon on its own connection refreshes every
  registered claim in one statement every 5 s, against a 30 s reclaim window.

The out-of-band half makes the cadence a function of **wall time**, not how many
source tables the batch touches. Without it, the bulk shape — one large transaction
touching one source table — runs a drain that outlives the reclaim window on its
single in-line heartbeat: the sweeper reclaims, the worker re-claims, and the two
trade the batch forever.

Two details make it cheap and safe: the daemon opens **no connection** until a
claim has been registered for a full interval (a fast drain costs nothing) and
exits after a minute idle; and its refresh takes rows `FOR UPDATE SKIP LOCKED`, so
it yields rather than waits on a row an in-flight apply holds.

> **Invariant:** heartbeat cadence ≪ reclaim window ≪ the cost of a re-drain, and
> the cadence must be bounded by wall time, not by the shape of the batch. A change
> that adds an uninterruptible step to the drain without checking the keepalive
> still covers it reintroduces the reclaim livelock.

## Two ways a claim comes back

Both clear the claimant and bump the claim epoch. Because the rows were never
consumed, either way the re-claim is a clean re-drain from scratch.

**Released**, immediately, by the worker itself, on **any** error. Nothing was
applied — a fold error precedes every write and an apply error rolls back — so the
claim covers work that did not happen. This is load-bearing for *latency*: without
it the only route back is the TTL, parking a routine, retryable failure for 30
seconds. And it isn't exotic: **every definition change trips the version fence**
on a worker whose loaded schema predates it — exactly what "change a formula, then
wait for it" does.

Because release makes a fence miss instantly re-claimable, it removes the TTL's
accidental role as a **rate limiter** — so the loop must back off on *consecutive*
fence misses itself (zero wait for the first, so read-your-writes stays fast, then
doubling from 10 ms to a 1 s ceiling, reset by any clean drain). A definition the
reload never resolves is then re-claimed at a bounded rate, not hot-looping.

Every release is scoped `claimed_by = me`, so a claim already reclaimed or taken
over is untouched. A caller that took the claim *itself* keeps it and retries in
place.

**Reclaimed**, on the TTL, by anyone. The backstop for a worker that *died* and
could not release. The sweep takes its rows `FOR UPDATE SKIP LOCKED` for the
mirror-image reason the keepalive does: a registry row locked by its own
claimant's apply transaction is not a dead claimant, and the sweep must never block
behind one.

## A gate that was considered and not adopted: the pause lease

Suspending *claiming* fleet-wide — a heartbeated **lease** with an `expires_at`
gating the claim at the top of a drain call — was scaffolded as one way to give an
auditor a quiescent read. It is **not** how the shipped auditor works.

The self-check auditor is `Trellis::self_check`
([ADR-0013](../decisions/0013-self-check-production-recompute-audit.md)), and it gets
its quiescence a different way: it awaits convergence through a watermark, reads the
target and its recompute inside one `REPEATABLE READ` transaction, and reports a
divergence only if it *survives a fresh await*. Its strict mode assumes writes to the
audited tables are stopped by the caller, not that Trellis pauses its own claim path.
No caught-up-read guarantee depends on suspending claiming fleet-wide, so the pause
lease has no consumer and the scaffolding is being removed (see #191).

## What is load-bearing here

- **Claim exclusivity is `INSERT … ON CONFLICT DO NOTHING`** on a `(batch, bucket)`
  primary key — a unique constraint, not a lock manager. `FOR UPDATE SKIP LOCKED`
  only serves the sweep.
- **The fold rules are the core of this stage.** The four-rule table and the
  image-bearing discriminator are where the silent bugs live.
- **No write-time merge.** It makes staged rows mutable, which forces the
  survivor-rewrite machinery back into existence ([05](05-apply-and-exactly-once-deltas.md)).
- **The routing key does not co-locate the contended thing, and says so.**
