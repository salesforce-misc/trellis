# Stage 5 — Applying, and why a delta lands exactly once

← [Claiming and the fold](04-claiming-and-the-fold.md) · next → [Cleanup and reclaim](06-cleanup-and-reclaim.md)

**What this stage owns:** turning folded records into derived writes, and doing
it in a way that survives crashes, duplicate execution, concurrent workers, and
concurrent definition changes.

**The guarantee:** *a non-idempotent effect — an aggregate delta — is applied
exactly once. Never twice, never zero times.* And it is guaranteed
**structurally**, with no per-key applied-marker to keep in sync.

## The three phases

```mermaid
sequenceDiagram
    participant W as Worker
    participant DB as Postgres

    rect rgb(31,111,235,0.10)
    Note over W,DB: Phase 1 — claim (one statement, committed alone)
    W->>DB: claim a bucket share (1 statement), COMMIT
    end

    rect rgb(31,111,235,0.05)
    Note over W,DB: fold (a read txn; one page of at most drain_batch_cap records)
    W->>DB: fold the fenced window → one record per (table, key)
    end

    rect rgb(160,160,160,0.12)
    Note over W,DB: Phase 2 — compute (NO transaction, NO locks held)
    W->>DB: read source rows, prefetch related rows
    W->>W: evaluate derivations in dependency order
    W->>DB: heartbeat the claim, per source table
    end

    rect rgb(46,160,67,0.12)
    Note over W,DB: Phase 3 — apply ∪ mark (ONE transaction)
    W->>DB: BEGIN
    W->>DB: 1. version fence (FOR SHARE on each computed source)
    W->>DB: 2. derived writes, gone-key deletes, truncate handling<br/>(1-1: per-key ordering lock + basis check)
    W->>DB: 3. aggregate + join maintenance (delta arithmetic)
    W->>DB: 4. downstream staging — into the ACTIVE batch
    W->>DB: 5. last page: mark this claim's buckets drained<br/>earlier page: check the claim, advance the cursor
    W->>DB: 6. pg_notify
    W->>DB: COMMIT
    end
```

Phases 2 and 3 run once per **page**. A share that fits
`ClientOptions::drain_batch_cap` (100,000 folded records by default) is one page,
folded whole; a larger one is walked in pages of at most the cap, each its own
fold, compute and Phase 3 transaction (see
[04](04-claiming-and-the-fold.md#paging-a-share-larger-than-the-cap)). The claim
commits on its own before any fold, so a peer's claim never waits on this
worker's fold (#328).

**Phase 2 holds no locks and no transaction.** Deliberate: compute can be
arbitrarily expensive without blocking intake, another worker, or — by holding a
snapshot open — the seal gate and cleanup pass. The price is that the world can
move under you, which the version fence and the immutable batch exist to handle.

## The exactly-once argument

Three properties give it, and **all three are structural** — there is no
bookkeeping to keep in sync, no "applied" flag, no per-key watermark.

### 1. The claimed batch is immutable

A worker folds a *sealed* batch. No concurrent change can land in it: a source
change arriving mid-compute appends to the **active** batch and is a different
batch. There is no "a row changed under me" case to reconcile.

This includes the worker's own downstream staging: Step 4 appends into the
**active** batch, never onto the one being drained. A design where a worker can
stage into its own claimed batch loses the property immediately.

### 2. Apply ∪ mark-drained are one transaction

The derived writes, the aggregate maintenance, the downstream appends, and the
batch's `draining → drained` mark all commit together. This is why the target
store and the staging store are the same Postgres database: without a shared
transaction there is no exactly-once for non-idempotent effects.

- A crash **before** the commit rolls back the delta *and* the mark. The claim
  expires, the batch returns to `sealed`, and it re-drains from the last
  committed page.
- A crash **after** commits both.

Under a bucket claim the unit is the claimed *bucket set*: its apply, the deletion
of its claim rows, and the OR of its bits into the batch's coverage mask are the
same commit. The batch becomes `drained` exactly when the mask fills.

The completion statement does two jobs at once:

```sql
-- (1) the claim check and the mask are one act
DELETE FROM seg_claims
 WHERE seg_seq = :s AND claimed_by = :me AND bucket = ANY(:page_buckets)
RETURNING bucket;
-- any page bucket MISSING means "my claim was lost mid-drain" → raise → the whole
-- Phase-3 transaction rolls back → the current claimant re-drains it exactly once

-- (2) OR my buckets in; complete iff that fills every bucket
UPDATE segments
   SET drained_mask = drained_mask | :mask,
       state = CASE WHEN (drained_mask | :mask) = ((1::int8 << bucket_count) - 1)
                    THEN 'drained' ELSE state END
 WHERE seg_seq = :s AND state = 'draining';
```

#### A page is its own transaction

A share larger than the cap drains in pages, and each page commits its own
apply. What makes that exactly-once is a durable per-bucket cursor,
`drain_cursor(seg_seq, bucket)`: the last page key a committed page covered. A
page that is not its buckets' last ends its Phase 3 transaction with a claim
check and a cursor advance instead of the completion statement:

```sql
-- (1) the claim check, and a heartbeat
UPDATE seg_claims SET claimed_at = now()
 WHERE seg_seq = :s AND claimed_by = :me RETURNING bucket;
-- must return EVERY bucket :me holds, or raise ClaimLost → the page rolls back

-- (2) the cursor moves in the same commit as the page's apply
INSERT INTO drain_cursor (seg_seq, bucket, after_route, after_src_table, after_key)
SELECT :s, b, :route, :src_table, :key FROM unnest(:page_buckets) AS b
ON CONFLICT (seg_seq, bucket) DO UPDATE SET ...;
```

- A committed cursor means "applied through here", exactly once. Only the last
  page runs the completion statement, so `drained_mask` gains a bucket's bit only
  when the whole share has applied.
- The update's row lock is what makes the reclaim sweep's `SKIP LOCKED` pass over
  an in-flight page. A reclaim that committed first deleted the row, so the update
  misses it; one still in flight holds the lock, so the update waits and then
  misses it. Either way a stale claimant's page never commits.
- The cursor lives in its own table, not on `seg_claims`: reclaim deletes the
  claim row, and a cursor that died with it would make the next claimant re-apply
  every committed page. A bucket with a cursor and no claim is free under the
  unchanged claim statement, and its next claimant resumes after the cursor.
- A key never splits inside a segment (pages are key ranges), so the fold's rules
  below hold per page unchanged.

### 3. The per-key fold telescopes

Within one batch, N changes to a key fold to one record whose old side is the
*earliest* pre-image and whose new side is the *latest* post-image. Across
batches, each batch's net delta chains onto the last: **batch *k*'s old side is
exactly the state batch *k−1*'s new side left.**

Because the deltas are invertible they also **commute**, so an out-of-order drain
converges to the same total.

## Absolute writes do not commute: the basis check

Batches drain out of `seg_seq` order, and key-routing does not order a key's
writes *across* batches ([04](04-claiming-and-the-fold.md)). The argument above
covers that for deltas only. Every write path has to be checked against this
table:

| Write kind | Commutes? | Idempotent? | What makes an out-of-order drain safe |
|---|---|---|---|
| Aggregate deltas (`+new − old`) | yes | **no** | exactly-once (the three properties above), plus the recompute horizon below whenever a group was also re-derived |
| Aggregate forced recomputes and extinction deletes (a live `GROUP BY` read) | **no** | yes | the recompute horizon, below |
| 1-1 field writes and deletes, from an image or a live recompute read | **no** | yes | the per-key ordering lock and basis check, below |
| Relationship projection writes | no | yes | the per-row `prev_lsn` ordering guard |
| Truncate | no | yes | the drain barrier (full serialization) |

A 1-1 write is an absolute value: "set this key's row to `f(row)`". Two such writes
for one key in two batches don't commute. If the batch computed from the older
source state reaches Phase 3 last, its value overwrites the newer one, and nothing
is left staged to correct it. That is the same **permanent staleness** the version
fence exists for, caused by ordinary data changes instead of a definition change
(issue #344). It needs no crash or retry to happen. It only needs one worker to
stall between Phase 2 and Phase 3, e.g. over a large catch-up batch.

So every 1-1 write and delete carries its **basis**: the source row it was
computed from (the staged image, or Phase 2's live read), or "no source row" for a
delete. Phase 3 applies it only if that basis is still the source's current state:

1. **The ordering lock.** Before writing any 1-1 target, Phase 3 takes a
   transaction-scoped advisory lock on every `(target, stripe)` its keys fall in,
   in ascending order. A stripe is the key's claim bucket (`route % 8`), so the
   workers draining one batch's buckets in parallel never contend. Only batches from
   different segments that share a bucket serialize. It can't be the target row's
   `FOR UPDATE` alone: a stale insert racing a delete has no row to lock.
2. **The basis check**, under that lock, in one statement: re-read each key's
   source row and compare. A write's basis holds if the current row contains
   every column of it with the same text (containment, since an image omits an
   unchanged TOASTed column). A delete's basis holds if the row is gone.
3. **A change whose basis no longer holds** is re-evaluated against the current
   row, when its definition reads no relationship: written if the row exists,
   deleted if not. Otherwise (or if evaluation fails) it is re-staged as a bare
   recompute, which a later batch reads live.

Why that converges: every Phase 3 for a key runs one at a time, and each one
writes only a value computed from the source's state *at that moment*. A source
change that commits after the check has a batch of its own still to come, and
that batch waits on the lock and then sees the change. So the last Phase 3 for a
key always writes that key's final state.

Re-evaluating instead of skipping matters for a hot key. If the source changes
faster than a batch drains, every batch's basis is stale by the time it applies.
Skipping would leave the target frozen until the key went quiet.

### Aggregate groups: the recompute horizon

This section covers aggregate targets that are not on the ledger yet: any
target with an `AVG`, `MIN`/`MAX` or composed field, a float argument, or a
relationship. A plain `SUM`/`COUNT` target is on the ledger instead (see
[Aggregate groups: the ledger](#aggregate-groups-the-ledger), #623 D3), with
no horizon, pre-lock or probe.

An aggregate group can also receive an absolute write. An image-less change
(a catch-up enumeration, a chained hop's `Recompute`, a relationship fallback, a
truncate) has no prior state to diff, so Phase 3 re-derives the whole group from
a live `GROUP BY` read of the source. So does a change whose fold included a
`Recompute`, even though it carries the CDC images it folded with (issue #392,
see [04](04-claiming-and-the-fold.md#the-two-kinds-of-missing-image)): both
groups its images name are re-derived rather than given the delta. The delta path's existence probe is a live
read as well: when it finds the group empty, it deletes the group's row.

Either read can see a source commit whose own CDC delta has not been applied yet.
That delta may be in a later batch, in an earlier batch that drains later, in
another bucket of the same batch, or on the other side of a grain migration. When
it lands, it counts the commit a second time (issue #321). Ordering batches
cannot prevent this, because batches do not drain in order.

So the live read records its basis as a WAL position, and a delta checks it:

1. **The horizon.** The forced path's recompute statement also writes
   `pg_current_wal_insert_lsn()` into the group row's hidden
   `__trellis_recompute_lsn`. The function is evaluated while the statement runs,
   after its snapshot is taken. A commit visible to that snapshot wrote its
   commit record before it became visible, so its `end_lsn` (the `lsn` intake
   stamps on its ring rows) is at or below the stamped value.
2. **The extinct horizon.** A deleted row can't hold a horizon, so each batch
   whose live reads find a group empty raises its target's single row in
   `aggregate_extinct_horizon` to the insert position after those reads. That
   includes a group with no row to delete: its delta for this batch is dropped
   just the same, so the read may have absorbed a delete still in flight. A row
   that the delta path later creates starts with that value as its own horizon,
   since the empty group it grew from was the result of a live read too.
3. **The check.** The fold keeps each key's *earliest* image-bearing `lsn`
   (`min_image_lsn`). A folded delta telescopes every commit from that one to its
   latest. Under the pre-lock, each delta group compares that value against its
   row's horizon, or against the target's extinct horizon when it has no row. At
   or below, the delta may already be counted, so the group moves to the forced
   path and is re-derived. Above, the delta applies as usual. The check runs per
   group, so the two sides of a grain migration are judged against their own
   groups.
4. **A key born and died in the batch.** Its insert and delete fold to no image
   and a zero delta, but the fold keeps the images that name its groups
   (`vanished_images`, issue #486). Each such group gets the same check against
   its row's horizon, and is re-derived when it fails. Otherwise it gets the
   delta path's existence probe but no write: an earlier probe may have kept
   the row only because the key was live then, with its insert still in
   flight, and nothing else will find the group empty. A group with no row is
   left alone, because a live read that counted the key would have written
   one, and whatever removed it since was a live read that found the group
   without the key.

This is the same "re-evaluate, never skip" choice as the 1-1 basis check. An LSN
at or below the horizon only *may* have been read, so skipping the delta would be
unsound. Re-deriving is correct either way. The cost is that a group keeps being
re-derived while intake lags behind the apply and changes keep arriving for it.
In steady state that is a catch-up effect. A target reaches its readers through
one feed only, the seam (a relationship-endpoint target included, since issue
#375), so a chained aggregate sees no CDC for it at all. The horizon still
matters for one window: the upgrade that took endpoint targets out of the
publication, where CDC for an endpoint written before the drop still arrives
after the seam's `Recompute` for the same write.

The same rule covers a definition's inline enumeration at `DEFINE` time
(issue #322), which has no intake to wait on. The #312 watermark wait in
[01](01-intake-and-lsn-confirmation.md) is now an optimization that makes these
re-derivations rarer, not a correctness requirement. ADR-0016 retired that
inline enumeration (#418): every ring capture runs in the backfill discharge,
behind the #312 wait
([data-flow](../data-flow.md#capturing-a-tables-existing-rows)).

### The ledger

ADR-0002 replaces the horizon with a per-key ledger. Apply maintains it for
plain aggregate targets since #623 D3 (next section). Every other target's
ledger is only written by its build, and goes stale after the target's first
change until its own part of #623 moves it.

Every target has one, `<target>__ledger` in the target's schema, created in the
registration transaction and dropped with the target (`defs::ledger`). It holds
one entry per source key (`__from_key`, in the ring's `key` encoding) with the
ordering state apply will judge a change against:

- `__applied_lsn` and `__applied_seg`: the `lsn` and ring segment of the last
  change applied to the entry. A Re-derive leaves `__applied_lsn` alone
  (#623 Q1). `__applied_seg` is the tombstone GC watermark (#623 Q7).
- `__basis`: the `pg_current_snapshot()` of the read that last wrote the entry,
  taken in the same statement as that read (I1). A change whose transaction is
  visible in it is already counted.
- `__tombstone`: whether the key's last applied change deleted it.

An aggregate target's entries also hold:

- the row's `GROUP BY` values, typed as the target's;
- `__member`;
- one column per distinct aggregate argument (`__arg0`, …), holding the
  argument's value, with the argument's collation when it is text;
- `__join_key` for a relationship-fed target (written from D5).

Every group row is then a pure function of its live members' entries: `SUM(x)`
is `sum(__argN)` over them, its hidden count `count(__argN)`, `COUNT(*)` the
entry count, and so on. The group row carries that count as `__trellis_members`.
A 1-1 target's ledger holds only the key and the ordering state, because the
target row holds the values.

The one-pass aggregate build writes the ledger. It empties it, reads the source
into it in one statement whose snapshot becomes every entry's basis, and then
writes the group rows as a `GROUP BY` over it. The 1-1 build writes no entries.

### Aggregate groups: the ledger

An aggregate target whose every field is `SUM(x)`, `COUNT(*)` or `COUNT(x)`
over an exact numeric source column, grouped by plain source columns, is on the
ledger (`staging::ledger`, #623 D3). Each page applies its records for such a
target in one transaction, in four steps:

1. **Lock (I1, I5).** Insert a non-member placeholder entry for every key the
   page has no entry for, then lock every entry `for update`, sorted by key, in
   one statement.
2. **Re-derive read.** A record staged as a `recompute`, or folded with one, is a
   Re-derive. So is a record with no change to apply. One statement reads those
   keys' current source rows *and* `pg_current_snapshot()`, after the lock. A key
   with no row becomes a tombstone.
3. **Entries, then groups, in one statement.** It first updates each entry:
   - A Re-derive writes the entry from its read. It sets `__basis` to the
     read's snapshot and leaves `__applied_lsn` alone (#623 Q1). Every change
     the read saw is visible in that snapshot. Every change it missed is still
     pending with its own ring row, and its trigger `lsn` may be below any
     position the read could record.
   - An Apply writes the entry from the record's new image, or makes a tombstone
     for a delete. It sets `__applied_lsn` to the change's `lsn`. It changes the
     entry only if all three hold (I2):
     - the change's source transaction (`row_txid`) is not visible in the
       entry's `__basis`;
     - its `lsn` is above `__applied_lsn`;
     - its `lsn` is above the target's truncate floor.

   The statement then sums each updated entry's move from its old state to its
   new one into per-group increments: the member count, and per argument its
   sum and its non-null count. It upserts them in group order, incrementing every
   column (I3). A `SUM` goes `NULL` when its non-null count reaches 0.
4. **Empty groups go.** Groups whose `__trellis_members` reached 0 are deleted.

There is no live `GROUP BY`, probe or horizon, and a page takes its locks in one
order: entries, then groups, each in one sorted statement. A Re-derive of an
unchanged key moves nothing, so a go-live re-read after the build writes no
group rows.

Each written or deleted group reaches the seam with its prior image. PG 17 has
no `OLD` in `RETURNING`, so the image is rebuilt from the upsert's result minus
the increments. A group the upsert created has no prior image.

A fold record carries the identity of the change that won its post-image
(`last_change`: its `lsn` and `row_txid`, see
[04](04-claiming-and-the-fold.md)). That change is the one an Apply judges.

**Truncate.** A source `TRUNCATE` empties the ledger, deletes every group row,
and raises the target's truncate floor (`ledger_truncate_floor`) to the
truncate's `lsn` (#623 Q6). `TRUNCATE` takes `ACCESS EXCLUSIVE`, so every
earlier writer's trigger ran below that `lsn` and every later writer's above it.

**Release and the orphan sweep.** Releasing a quarantined key
(`staging::release_key`) stages one `Recompute` of it and discards the parked
rows. A replayed row would carry the releaser's `row_txid`, not the source
transaction's. A catch-up discharge's orphan sweep finds live entries the
source no longer backs, and stages a `Recompute` of each instead of deleting
their groups directly.

## What this replaced

The old, mutable-worklist design needed two extra mechanisms, both now gone:

- An **`lsn` compare-and-delete**: clear a claimed key only if its `lsn` is
  unchanged since the claim, so concurrent re-stages survive.
- A **survivor rewrite**: when a re-stage *did* land on a claimed row mid-compute,
  advance that survivor's `old_image` to the image just applied, or the re-drain
  double-subtracts.

Both existed *only* because the worklist was mutable; Property 1 removes the race.
**A patch that reintroduces a mutable claimed batch must reintroduce them both** —
that is the tell for whether a proposed change is actually equivalent.

## The delta model

For a row with key `pk`, maintaining a measure `f` over group `g` in one Phase-3
transaction:

| Op | Effect |
|---|---|
| INSERT | `g(new) += f(new)` |
| DELETE | `g(old) -= f(old_image)` |
| UPDATE, grain unchanged | `g -= f(old_image)` and `g += f(new)` — one group, net delta |
| UPDATE, grain changed | **grain migration**: `g(old) -= f(old_image)` *and* `g(new) += f(new)` — two groups |

The drain needs exactly two facts per folded record: the **old-side image** (to
subtract; present iff `old_image IS NOT NULL`) and the **new-side image** (to
add; present iff the key is still live).

Folding the new side from the **staged post-image at the claimed position** — not
from a live source read — is what keeps the delta scan-free *and* closes a
read-ahead window: a live read at apply time can see a *later* state than the
batch is accounting for, and then the next batch subtracts an old image that was
never added.

Composite measures fold their hidden partials, never themselves: `avg` maintains
`{m}__sum` and `{m}__count` and recomputes the visible ratio from them.

**Not every measure is delta-able**; the gate is explicit: only exact, invertible
folds qualify. `count(*)`, `count(col)`, and `sum`/`avg` over
int/numeric are in. `min`/`max` are not invertible (removing the current maximum
tells you nothing about the next one) and take a probe-assisted recompute path
instead. Floats need care: naïve deltas drift unboundedly because IEEE-754
addition is non-associative, so the accumulator is kept in exact decimal and only
rendered to float — and `Inf`/`NaN` are tracked as counts, not delta-invertible at
all (`Inf − Inf = NaN`).

**The north star for the exact types is byte-identical convergence to a
from-scratch `GROUP BY` oracle after every op and every drain interleaving** — far
stronger than "eventually approximately right", and what makes the path auditable.

## The version fence: the one failure idempotency cannot fix

Idempotent recompute self-heals almost everything. It does not heal **permanent
staleness**: a stale in-flight write landing *after* a definition change's
re-derivation completes, with nothing left staged to correct it.

The design:

- **The definition applied is always the current one.** A staged change is pure
  identity ("recompute me"); logic is looked up fresh at compute time. Version the
  *catalog*, not the pending changes.
- **Per-source-table versions.** One monotonic version per source table. A
  definition change is one transaction scoped to the edited table: bump its
  version, write the new definitions, stage its affected keys, commit, notify.
- **The fence:** Phase 3 asserts every table it *evaluated* is still at the version
  it loaded — `FOR SHARE` on that table's meta row, which serializes against the
  definition change's `FOR UPDATE`. Mismatch → roll back, reload, recompute. No
  worker can commit values computed under a superseded definition, and an edit to
  one table never fences batches working on other tables.
- **Backstop:** the edit's re-derivation stages its rows at a higher position, so
  they land in a **later** batch than any in-flight drain's and cannot be swallowed
  by a batch already claimed.

The fence covers definition changes only. The same staleness caused by two
batches for one key draining out of order is closed separately, by the per-key
ordering lock and basis check ([above](#absolute-writes-do-not-commute-the-basis-check)).

**The fence runs first in Phase 3**, so a superseded batch rolls back before
touching a derived row. The fence set must include tables that are *evaluated* but
absent from the one-to-one write plan — an aggregate-only source, an equi-join
parent — or a grain change mid-batch slips through.

## Lock ordering, because parallel workers will overlap

Two workers whose batches touch overlapping aggregate groups will contend on the
same group rows. That is fine; deadlocking on them is not. Every statement that
writes group rows takes its locks in a **single consistent order — ascending
group key** — via an ordered pre-lock ahead of the write:

```sql
WITH locked AS (
  SELECT group_key FROM agg_target WHERE group_key = ANY(:groups)
  ORDER BY group_key FOR UPDATE
)
-- ... the actual merge, guarded so `locked` is genuinely referenced
```

A consistent total lock order has no cycle, so overlapping workers serialize on a
shared hot group instead of deadlocking. That plus a bounded, idempotent retry on
residual serialization failures (`40001`/`40P01`) is the whole deadlock story.

> **The same Postgres gotcha as the claim statement:** an unreferenced `FOR
> UPDATE` pre-lock CTE gets pruned and locks nothing. Force it with a `count(*)`
> guard in the outer query — this is an easy bug to ship and a silent one.

### No lock wait holds a snapshot open (ADR-0002 I7)

A consistent order stops deadlocks; it doesn't bound a wait. A drain page
queued behind a lock holds its transaction, and with it its snapshot, its
transaction id and every lock it took before, for as long as the holder
does. Vacuum can't pass it, and the sealer's gate, which waits out every
transaction running when the last fence was taken, refuses every seal
meanwhile. In #617 a drain batch's ledger insert waited 1 h 50 min behind
chunk transactions: the open transaction pinned the slot's `restart_lsn`,
`pg_wal` reached 190 GB, and the sealer was refused for the whole wait
([#617 final](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5859141160)).

**The rule.** Every lock wait in a Trellis transaction is bounded by
`lock_timeout`, and on `55P03` the transaction rolls back and the work is
retried with backoff from outside any transaction.

- **The bound is on the session.** Every connection Trellis opens, pooled or
  dedicated, caps its `lock_timeout` at `locks::LOCK_TIMEOUT` when it
  connects (a shorter setting it was given is kept). That bounds the explicit
  lock statements (the pre-locks above, `FOR SHARE` on the version fence,
  advisory stripe locks, DDL) and the implicit waits nobody writes down: an
  `INSERT ... ON CONFLICT` waiting on another transaction's uncommitted key
  (#617's wait), an `UPDATE` of a row another transaction holds. A setting
  per transaction would cover only the transactions someone remembered.
- **The cap is two minutes, for now.** The invariant is that the wait is
  bounded, not that it is short: two minutes is 55 times shorter than #617's
  wait. It is sized for the aggregate group pre-lock above, which queues
  drain pages that touch the same groups one behind another. In `bench
  fold-in-ratio` at ratio 10 (40k groups, every page touching most of them)
  the longest page transaction, its wait included, was 89 s, when eight
  ~28k-record pages queued together, and 100k-record pages ran 47 s. A 5 s
  cap fired 75 times there, and each retry lost its place in the queue. The
  value is interim: #623 removes the group pre-lock, and with it the reason
  a Trellis transaction waits this long on another.
- **A drain keeps its claim across the retry.** A page whose transaction
  times out is classified transient; the drain backs off (50 ms, doubling to
  1 s) and retries the page, recomputing it, while the heartbeat keeps its
  claim fresh. A lock timeout doesn't count against the five attempts other
  transient failures get: the drain retries it for up to three timeouts, then
  surfaces it, and the worker releases the claim like any drain failure.
- **Everywhere else** the caller is already a retry loop: the claim, the
  sealer and the chunk paths fail the step and their loop tries again next
  tick. A catalog call from the application (alter, pause, drop, resume)
  returns the error.

### DDL on a user table never blocks a writer (ADR-0002 I6)

A DDL statement waiting for a lock on a user table sits in that table's lock
queue, and every later lock request that conflicts with it queues behind it.
For a lock that conflicts with `ROW EXCLUSIVE` (`CREATE TRIGGER`'s `SHARE ROW
EXCLUSIVE`, #622) that is every application writer, for as long as whatever
the DDL waits on stays open. In #565 E7 a bare `CREATE TRIGGER` stalled
every writer for 25 s behind one open transaction; with a 50 ms
`lock_timeout` retried every 200 ms the worst writer wait was 52 ms
([E7](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119)).

**The rule.** DDL on a user table runs in a retry loop (`locks::DdlRetry`),
each attempt its own transaction under a short per-attempt `lock_timeout`, one
attempt per `locks::USER_TABLE_DDL_RETRY_INTERVAL` (200 ms), until it lands.
The per-attempt timeout depends on the lock the DDL takes:

- **A lock writers queue behind** (`CREATE`/`DROP TRIGGER`'s `SHARE ROW
  EXCLUSIVE`, from #622): `locks::USER_TABLE_DDL_LOCK_TIMEOUT`, 50 ms, E7's
  shape.
- **`SHARE UPDATE EXCLUSIVE`** (`ALTER PUBLICATION ... ADD/DROP TABLE` in
  `reconcile_publication`, today's only user-table DDL): writers don't
  conflict with it, so they never queue behind it, and I6 doesn't bound it.
  Only I7's rule applies: it mustn't wait with a snapshot open for a long
  time behind whatever holds the table (a manual `VACUUM`, `CREATE INDEX
  CONCURRENTLY`, an `ALTER TABLE`, an autovacuum). Its per-attempt timeout
  is `locks::share_update_exclusive_ddl_timeout`: twice the session's
  `deadlock_timeout`, at least 2 s, at most `LOCK_TIMEOUT`. It must outwait
  `deadlock_timeout` because that is when a waiter runs the deadlock check,
  and the check is what cancels an autovacuum that blocks it. With a 50 ms
  timeout the check never runs. On Postgres 17, with a throttled autovacuum
  on the table, every 50 ms `ALTER PUBLICATION` attempt timed out, so the
  join would have waited out the whole vacuum, hours on a large table. A
  2 s attempt had the vacuum cancelled and landed in 1.0 s. An
  anti-wraparound autovacuum is never cancelled; the loop waits that one out.

The maintenance loop, the only sealer, makes one attempt per reconcile pass
and leaves the change to the next pass; startup retries until it lands. The
retire path's `TRUNCATE` is the older precedent: it takes its lock `NOWAIT`
and skips the slot until the next tick.

**Open for #622.** `CREATE`/`DROP TRIGGER` conflict with `SHARE UPDATE
EXCLUSIVE` too, so an autovacuum blocks them in the same way. Their 50 ms
timeout can't outwait `deadlock_timeout` without queueing writers for that
long, so a trigger join on a table under a long autovacuum waits the vacuum
out.

## Downstream propagation, and why it terminates

Step 4 stages the keys whose derived values depend on what just changed —
including, for a one-to-many aggregate, the parent groups of a changed child.
This is where the old-image requirement from
[01](01-intake-and-lsn-confirmation.md) is cashed in: a child **delete** or a
**re-parent** must refresh both the group the child joined (from the live row) and
the group it left (from the staged old image, which is the only place that
information still exists).

Two termination mechanisms:

- **Filtered staging.** Dependents are staged only for tables that actually have a
  derivation reading the changed table, so an acyclic dependency graph drains to
  empty. Cycles are rejected at definition time.
- **A schema-derived hop bound.** Each staged dependent carries a hop generation
  one past its deepest trigger (reset to 0 by any fresh source change — hence the
  `src_changed` OR rule in the fold). A wave climbing beyond the graph's
  cross-table depth plus slack cannot happen on the declared schema, so it raises a
  named error identifying the bound, the generation and the cycling tables.

An absolute round ceiling survives as defence-in-depth, catching runaways the hop
bound cannot (e.g. an unbounded stream of fresh intake). It is no longer the
contract, so its message stays hedged: exceeding it is *not* necessarily a cycle.

## Suppressing no-op writes

A source may feed several named derived tables, and a change usually affects only
one of them. The write carries a value-diff guard:

```sql
INSERT INTO target (...) VALUES (...)
ON CONFLICT (pk) DO UPDATE SET ...
 WHERE (target.a, target.b) IS DISTINCT FROM (EXCLUDED.a, EXCLUDED.b)
```

so a target a change did not affect sees no tuple churn. It is the relief valve
for hot tables, and what makes per-source (not per-target) version fencing free: a
sibling target's idempotent recompute writes nothing.

The set of keys **physically written** is then the "this recompute changed
something" signal that filters downstream propagation. Deleted keys count as
changed.

## Failure classification

Treating every Phase-3 failure uniformly turns a transient blip into a quarantined
key, or a schema error into a silently parked one. The classification:

| Class | Examples | Treatment |
|---|---|---|
| **Transient** | serialization/deadlock (`40001`/`40P01`), lock-not-available, statement timeout, dropped connection | retry with backoff; **charge nothing to any key** — a transient failure is not attributable. A lock timeout retries for up to three `lock_timeout`s with the claim held rather than five attempts ([I7](#no-lock-wait-holds-a-snapshot-open-adr-0002-i7)) |
| **Version fence miss** | a definition changed mid-drain | reload the schema and retry; back off on *consecutive* misses only |
| **Halting schema diagnosis** | a tripped hop bound (a real cross-table value cycle); a relationship endpoint that is not a source column | **propagate loudly**; never quarantine. Quarantining would convert a loud, actionable error into a key that blocks reads forever |
| **Ordering artefact** | a delta guard tripped while a lower-numbered batch is still outstanding | self-heals; charge only once every predecessor has drained |
| **Claim lost** | a page's claim check or completion finds a held bucket's claim gone | surface; never isolate. The page rolled back, and whoever holds the buckets now resumes from the last committed cursor |
| **Everything else** | a genuinely poisonous change | isolate and charge — see [06](06-cleanup-and-reclaim.md) |

The halting class deserves emphasis: **failing that way stops the whole instance,
deliberately.** The offending batch can never drain, cleanup requires every older
batch drained, so the ring fills and seals start failing. That is correct for a
genuine schema cycle — nothing may be silently skipped — but it is instance-wide
rather than scoped to the named tables, and the error message should say so. It
also needs a metric (counter plus last reason), because "stopped" and "slow" look
identical from outside otherwise.

## Invariants

1. **The claim is an optimization; the fence plus the atomic apply ∪ mark are the
   correctness mechanism.** A patch that makes correctness depend on claim
   exclusivity is wrong.
2. **The claimed batch is immutable.** Every producer writes to the *active*
   batch, including a worker doing downstream propagation.
3. **Apply and its claim ending are one commit**, per claimed bucket set and
   page: the cursor advance on an earlier page, the drained mark on the last.
   Splitting them reintroduces double-counting on the non-idempotent delta path.
4. **A row's bucket is a total function of the row and its batch** — no row in
   two buckets, none in zero.
5. **The fold's four rules are individually load-bearing.**
6. **Recompute reads committed current state and is deterministic.** A written
   value is never *wrong* — it is a deterministic function of the state that was
   read — only possibly *superseded*, and a later batch guarantees the superseding
   recompute runs. Parallelism costs some redundant recomputes, never a wrong
   final value. "Later" means later to *commit*, not later in `seg_seq`: for an
   absolute write, invariant 7 is what makes that true.
7. **A non-commutative write commits only if its basis is still current**, checked
   under a lock that serializes every Phase 3 for that key. A new write path must
   either commute (deltas), check its basis, or be serialized some other way
   (the `prev_lsn` guard, the truncate barrier). An aggregate group's absolute
   writes record their basis as a recompute horizon, and a delta that would
   commute with other deltas still has to check it
   ([the recompute horizon](#aggregate-groups-the-recompute-horizon)). See the
   classification table under
   [the basis check](#absolute-writes-do-not-commute-the-basis-check).
8. **No Trellis transaction waits for a lock longer than `lock_timeout`**, and a
   timed-out transaction is retried from outside any transaction, never
   waited out inside one. Every connection caps the setting when it connects
   ([I7](#no-lock-wait-holds-a-snapshot-open-adr-0002-i7)).
9. **DDL on a user table runs in a retry loop of short transactions**, one
   attempt per 200 ms. DDL writers queue behind runs under a 50 ms
   `lock_timeout`, so no application writer queues behind it for longer than
   that. `SHARE UPDATE EXCLUSIVE` DDL, which writers don't queue behind, waits
   past `deadlock_timeout` so a blocking autovacuum is cancelled
   ([I6](#ddl-on-a-user-table-never-blocks-a-writer-adr-0002-i6)).
