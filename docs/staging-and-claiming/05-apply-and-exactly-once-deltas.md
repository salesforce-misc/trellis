# Stage 5 — Applying, and why a delta lands exactly once

← [Claiming and the fold](04-claiming-and-the-fold.md) · next → [Cleanup and reclaim](06-cleanup-and-reclaim.md)

**What this stage owns:** turning folded records into derived writes, and doing
it in a way that survives crashes, duplicate execution, concurrent workers, and
concurrent definition changes.

**The guarantee:** *a non-idempotent effect — an aggregate delta — is applied
exactly once. Never twice, never zero times.* It rests on one transaction and
one per-key record: a batch's writes commit with its drained mark, and a group
moves by exactly the changes its basis hasn't counted, in the same statement.

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
    W->>DB: 2. derived writes, gone-key deletes, truncate handling<br/>(1-1: sorted entry lock + I2 test)
    W->>DB: 3. aggregate groups (sorted group lock, basis test, partial increments)
    W->>DB: 4. downstream staging — into the ACTIVE batch
    W->>DB: 5. last page: mark this claim's buckets drained<br/>earlier page: check the claim, advance the cursor
    W->>DB: 6. pg_notify (the instance's wake channel)
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
arbitrarily expensive without blocking a writer, another worker, or — by holding a
snapshot open — the seal gate and cleanup pass. The price is that the world can
move under you, which the version fence and the immutable batch exist to handle.

## The exactly-once argument

Three properties give it. The first two are structural; the third is the
group's basis, which every write to a group moves in the same statement.

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

### 3. A group moves by the changes its basis hasn't counted

A group-basis aggregate keeps no per-key record. Each group has a **basis**
(the snapshot of the last absolute write to it: a build's, a rebuild's or a
recompute's) and a **stamp**. A page takes the ring's rows for each key
unfolded, each with its old and new image, and for every change adds the
difference between the contribution of its new image and of its old one to
the group's partials. It adds a change only if its transaction is not visible
in the group's basis, so the write that took the basis (which read the source
in its own snapshot) and the page never both count it, and only above the
target's truncate floor. A page holds a group's lock from its lock statement
to its commit, so the basis it tests is one no other page is about to change.

So a group's partials are the contributions of the source rows its basis saw
plus the changes that came after, each counted once, whichever batch or page
it came from ([below](#aggregate-groups-the-group-basis)). A batch draining
after a newer one for the same key adds nothing twice.

## Absolute writes do not commute: the 1-1 ledger

Batches drain out of `seg_seq` order, and key-routing does not order a key's
writes *across* batches ([04](04-claiming-and-the-fold.md)). The argument above
covers that for deltas only. Every write path has to be checked against this
table:

| Write kind | Commutes? | Idempotent? | What makes an out-of-order drain safe |
|---|---|---|---|
| Aggregate group increments (`+new − old`, per change) | yes | **no** | exactly-once (the three properties above), and the group's basis test |
| Aggregate absolute writes (a build's or a recompute's `GROUP BY`) | **no** | yes | the group lock and the basis, below |
| 1-1 field writes and deletes, from an image or a Re-derive read | **no** | yes | the 1-1 ledger's entry lock and I2 test, below |
| Truncate | no | yes | the drain barrier (full serialization) |

I2 is one test on a 1-1 ledger: an Apply changes its entry only if its
source transaction is not visible in the entry's `__basis`, its `lsn` is above
`__applied_lsn`, and its `lsn` is above the target's truncate floor. An
aggregate's group applies the same test against the group's basis.

A 1-1 write is an absolute value: "set this key's row to `f(row)`". Two such writes
for one key in two batches don't commute. If the batch computed from the older
source state reaches Phase 3 last, its value overwrites the newer one, and nothing
is left staged to correct it. That is the same **permanent staleness** the version
fence exists for, caused by ordinary data changes instead of a definition change
(issues #344, #392). It needs no crash or retry to happen. It only needs one
worker to stall between Phase 2 and Phase 3, e.g. over a large catch-up batch.

So a 1-1 target is on its ledger (`staging::one_to_one_ledger`, #623 D6),
whose entry for a key holds only the ordering state ([below](#the-ledger)); the
target row holds the values. A change is an **Apply** when it carries the
image of the commit it came from (its `lsn` and source `txid`), and a
**Re-derive** when it was staged as a `recompute` or folded with one. Every
record of a page whose oldest batch is at or below the target's `build_seg`,
the segment its Re-derive build started in, is a Re-derive too (#733): such a
batch may hold a change committed before the start whose later change drained
before it, unapplied, so its image is stale. Phase 2
evaluates an Apply's image; Phase 3 settles each 1-1 target's records in one
pass:

1. **The entry lock (I5).** Insert an entry for every key with none, then
   lock every other entry `for update`, sorted by key. A new key's Apply is
   settled by its insert, since with no entry I2 is only the truncate floor;
   a Re-derive's entry is a placeholder until step 3. Every Phase 3 writer of
   the key's target row holds the entry, so every Phase 3 for a key runs one
   at a time. It can't be the target row's `FOR UPDATE`: a stale insert
   racing a delete has no row to lock. A key whose tombstone the GC collects
   between the two statements fails the page transiently and it is retried
   (#712).

   The one writer of a 1-1 target row that doesn't hold the entry is the
   orphan sweep (`intake::resume_orphans`), which a marker's discharge runs
   for a resume or a one-pass build's go-live catch-up. It only deletes. It
   judges a row unbacked on the snapshot of its own read, locks the rows it
   judged in key order, and then deletes each one only if the source still
   has no row of its key. A page writes a 1-1 row under the row's lock, from
   a source read it made before the write, so once the sweep holds the rows
   its delete's snapshot sees every source row such a page read. It keeps a
   row a page re-derived from a source row inserted after the sweep's read
   (#883): that row's entry has a basis that holds the insert, so the
   insert's own Apply is refused, and deleting the row would lose it. The
   sweep runs only for definitions the Re-derive build doesn't serve (a
   resumed definition it does serve gets the build's own sweep instead),
   until milestone F (#625) moves them.
2. **The Re-derive read (I1).** The Re-derived keys' source rows and
   `pg_current_snapshot()`, in one statement, after the lock. A key with no
   row is a delete.
3. **The entries (I2).** A Re-derive sets the entry's `__basis` to the read's
   snapshot and leaves `__applied_lsn` alone (#623 Q1). An Apply applies only
   if the I2 test above passes; it
   then sets `__applied_lsn`. Either raises `__applied_seg` and marks a
   tombstone when the key has no row after it. The statement returns the keys
   it changed. A placeholder step 1 inserted that neither step wrote (an
   Apply at or below the truncate floor, or a Re-derive skipped as failing
   to evaluate)
   is deleted in the same transaction (#774): it holds no ordering state, so
   I2 treats it as no entry, and the tombstone GC collects only tombstones.
4. **The target rows** of exactly those keys are upserted or deleted.

Why that converges: an Apply the test refuses is already reflected by a
Re-derive whose snapshot saw its commit, or older than the change last
applied, or from before a source truncate. A change a Re-derive's snapshot
did not see committed after the change last applied (one key's writes are
ordered by its row lock), so its `lsn` is above `__applied_lsn`, and its
transaction is not in `__basis`: when it drains it waits on the entry lock
and then applies. So the last Phase 3 for a key always leaves its final state.

Nothing has to be re-read for a hot key: an Apply is judged by its own
position, so a key whose source changes faster than a batch drains still
gets each batch's newest image.

`ALTER TRANSFORM`'s field rebuild and a column resume are field builds
(`staging::build`, #625 F8b): background chunks that lock a range of keys'
entries as a page does and rewrite just the changed columns of their target
rows from one snapshot, leaving the entries as they are.

### The ledger

ADR-0002 gives every 1-1 target a per-key ledger. Apply maintains it
(`staging::one_to_one_ledger`,
[above](#absolute-writes-do-not-commute-the-1-1-ledger)).

Every 1-1 target has one, `<target>__ledger` in the target's schema, created in the
registration transaction and dropped with the target (`defs::ledger`). It holds
one entry per source key (`__from_key`, in the ring's `key` encoding) with the
ordering state apply will judge a change against:

- `__applied_lsn` and `__applied_seg`: the `lsn` and ring segment of the last
  change applied to the entry. A Re-derive leaves `__applied_lsn` alone
  (#623 Q1). `__applied_seg` is the tombstone GC watermark (#623 Q7). A
  Re-derive's read is live, so it stamps at least the newest segment its
  snapshot sees, not only the page's own: a tombstone must outlast every
  change its `__basis` saw (#742).
- `__basis`: the `pg_current_snapshot()` of the read that last wrote the entry,
  taken in the same statement as that read (I1). A change whose transaction is
  visible in it is already counted.
- `__tombstone`: whether the key's last applied change deleted it.

A 1-1 target's ledger holds only the key and the ordering state, because the
target row holds the values. It is keyed by `__from_key` and indexes its
tombstones by `__applied_seg` (the tombstone GC's index, below).

A 1-1 target is built one of two ways. The Re-derive build (`staging::build`,
#625) serves a 1-1 target on a captured source or on another definition's
target: it applies from its start and re-derives the source in chunks, each
inserting the entries its keys don't have yet and locking the others as a page
does, and stamping them as a Re-derive does (below). Every other 1-1 target
keeps the one-pass build.

### Aggregate groups: the group basis

An aggregate (`staging::group_basis`, ADR-0002 "Aggregates: a basis per
group") keeps each group's partials (its row count `__n`, and per distinct
argument its non-null count and, for a `SUM` or `AVG`, its sum) in a hidden
table in the catalog schema, `agg_<id>`, beside the group's basis and stamp.
The visible target holds only the declared columns, computed from the
partials whenever the visible row is written. The grammar admits `COUNT(*)`,
`COUNT(x)`, and `SUM`/`AVG` over `smallint`, `integer`, `bigint` and
`numeric(p,s)`, because only those can be kept exact from each change alone.
An argument may be an expression (`SUM(v + 1)`), evaluated per change over
the image as the source's row type.

A page, per aggregate target:

1. **Stage.** One statement splits each ring row into sides (its old image
   negated, and its new image), evaluates the group and each argument on
   each, and sums them per group, transaction and statement into a session
   temporary table. It reads no basis, so this work, most of a page's,
   happens before any group is locked.
2. **Lock (I1, I5).** One statement inserts every group the staged sides name,
   sorted, and locks the ones that exist.
3. **Apply.** One later statement, so under `READ COMMITTED` its snapshot
   sees every committed basis, keeps a staged sum only when its transaction
   isn't visible in the group's basis and its `lsn` is above the target's
   truncate floor, adds what is kept to the hidden row, and writes the
   visible row when a visible value changed: an upsert while `__n > 0`, a
   delete when it falls to 0.

Each changed visible row reaches the seam with its prior image, computed from
the partials before the page's change. A `Recompute` (a release of a key, or
a marker's re-read) names groups instead of carrying images: the page
rewrites each of them from a `GROUP BY` of the source in the statement's
snapshot, which becomes the group's basis.

**Builds.** A build or rebuild is one absolute write per structure: it locks
the groups of the enumeration (the target's, the source's, and those the
ring's old images name) in key order, then one statement runs the source's
`GROUP BY` from its snapshot and replaces each group's partials, basis, stamp
and visible row. Pages keep applying, against the new bases once it commits.
It is the plan job's whole build
([data flow](../data-flow.md#an-aggregates-or-a-joins-build)).

**Zero groups.** A group whose partials are all zero has no visible row, but
its hidden row can still matter: a change its basis saw but a page hasn't
drained yet must still be refused. The maintenance loop's GC collects a zero
group only once the contiguous drained prefix reaches its stamp
(`staging::retire::collect_tombstones`), without waiting on a page.

**Truncate.** A source `TRUNCATE` empties a 1-1 target's ledger, or an
aggregate's hidden table, clears the target's rows, and raises the target's
truncate floor (`ledger_truncate_floor`) to the truncate's `lsn` (#623 Q6). `TRUNCATE` takes
`ACCESS EXCLUSIVE`, so every earlier writer's trigger ran below that `lsn` and
every later writer's above it; a write later in the truncating transaction
itself is above it too, since the truncate's own ring row moves the WAL insert
position on. The floor is the second line of defence: the drain barrier
already orders the truncate's batch after every earlier one and before every
later one (no segment above an undrained truncate's is claimable,
`next_claimable_segments`; see [truncate-propagation-spec.md](truncate-propagation-spec.md)),
and the fold voids its own batch's rows from before it. Segments are fenced
in order (the seal gate seals none past an unfenced predecessor), so no
segment past a truncate's is claimable before the truncate's `has_truncate`
is decided.

**Release and the orphan sweep.** Releasing a quarantined key
(`staging::release_key`) stages one `Recompute` of it and discards the parked
rows. A replayed row would carry the releaser's `row_txid`, not the source
transaction's. On an aggregate the release also recomputes, in
its own transaction, every group the held row's `agg_groups` names (the
groups of every parked change's images) and the key's current row's, locked
in one sorted statement and rewritten from a `GROUP BY` of the source with
that statement's snapshot as each group's basis; a page with a `Recompute`
recomputes the groups it names the same way, under the page's group locks. A
1-1 target's catch-up discharge runs an orphan sweep that deletes the rows
the source no longer backs.

**Tombstone GC (I4).** Maintenance deletes each tombstone whose
`__applied_seg` is at or below the **contiguous drained prefix**, the highest
`seg_seq` at or below which every segment is drained, not the highest drained
segment (`staging::retire::collect_tombstones`). An earlier change to the same
key committed before the delete's trigger ran, so it is in the delete's batch
or an earlier one, and at or below the prefix it has been applied or refused.
A Re-derive stamps at least the newest segment its snapshot sees (#742), so a
tombstone it wrote outlives every change its `__basis` would refuse. A later
change to a collected key finds no entry and gets a fresh one, where I2
reduces to the truncate floor.

The GC skips the ledger of a definition under a build: one that is
`backfilling`, or whose Re-derive build a pause or quarantine froze (#723).
A build chunk inserts a new key's entry from a snapshot taken before the
insert, so a tombstone written after that snapshot must still be there when
the insert looks for the key. A GC batch checks the definition and holds its
row `for key share` until it commits, and a build's start takes that row
`for update`, so a batch either commits before the start or sees the build
and skips. The tombstones of deletes applied during a build therefore wait
for it to finish, which bounds them by the write rate times the build's
length.

Only a tombstone carries `__applied_seg` (#775). A write that leaves an entry
a tombstone raises it to `greatest(old, its segment)`; one that leaves the
entry live leaves it alone. The ledger indexes its tombstones by it
(`(__applied_seg) where __tombstone`), so a GC batch seeks the collectable
ones under a pinned index-scan plan (#738), however many tombstones wait
above the drained prefix, and an Apply or Re-derive that leaves an entry live
changes no indexed column and can be HOT. A live entry's stale
stamp never has to protect anything: a delete's Apply applies only a change
its entry's `basis` doesn't see, so every change the `basis` does see
completed before the delete and is in the delete's batch or an earlier one,
which the Apply's stamp covers, and a Re-derive that deletes the key stamps
its own read's segment (`defs::ledger::tombstone_seg_sql`).

## The delta model

For a source key, maintaining a measure `f` over group `g` in one Phase-3
transaction, with the change's old image `o` and new image `n` from the ring
row:

| Op | Effect |
|---|---|
| INSERT | `g(n) += f(n)` |
| DELETE | `g(o) -= f(o)` |
| UPDATE, grain unchanged | `g += f(n) − f(o)` — one group, net delta |
| UPDATE, grain changed | **grain migration**: `g(o) -= f(o)` *and* `g(n) += f(n)` — two groups |

Each change carries both images, and a group's basis, not a per-key record,
decides whether the page adds it. A page never folds a key's rows into one:
the first old and last new image are wrong when a group's basis falls between
two of a key's changes, so each row is its own side.

Composite measures fold their hidden partials, never themselves: `avg` keeps
a running sum and count and writes the ratio when it writes the visible row.

**Not every measure is delta-able**; the gate is explicit: only exact,
invertible folds are maintained by increments. `count(*)`, `count(col)`, and
`sum`/`avg` over `smallint`, `integer`, `bigint` and `numeric(p,s)` are in.
Everything else is refused at define, named by its family
([transforms](../transforms.md#supported-sources-and-targets)): `min`/`max` are
not invertible (removing the current maximum tells you nothing about the next
one), and a float `sum`/`avg` would drift because IEEE-754 addition is
non-associative (and `Inf − Inf = NaN`).

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
- **Column pauses and releases are definition changes too.** A 1-1 plan leaves a
  paused column out of its writes, so a column pause, a column resume and a field
  build's capture release each bump the fence of the definition's source, as their
  transaction's first lock. In both directions a writer that planned under the old
  set either commits before the change does (the bump waits for it) or misses the
  fence and computes again. So a column a resume releases is never left as a stale
  page wrote it, and a column a pause holds out takes no write after `PAUSE`
  returns. A 1-1 build chunk plans its columns before its transaction, so it takes
  the fence `FOR SHARE` first thing in that transaction and plans again if it has
  moved.

The fence covers definition changes only. The same staleness caused by two
batches for one key draining out of order is closed separately, by the 1-1
ledger ([above](#absolute-writes-do-not-commute-the-1-1-ledger)).

**The fence runs first in Phase 3**, so a superseded batch rolls back before
touching a derived row. It takes its tables' fences in table-name order, the
order every transaction that bumps more than one (a `JOIN` definition's define,
drop, resume or freeze bumps both its tables') takes them in, so a page and a
bump never each hold a fence the other wants. The fence set must include
tables that are *evaluated* but absent from the one-to-one write plan — an
aggregate-only source, an equi-join parent — or a grain change mid-batch slips
through.

## Lock ordering, because parallel workers will overlap

Two workers whose batches touch overlapping keys or groups will contend on the
same rows. That is fine; deadlocking on them is not. A page takes its locks in
one consistent order: its 1-1 targets, then its aggregate targets, each in
target order, and per target **its entries, then its rows, each in one sorted
statement**. The entry lock is ordered by key
([the 1-1 ledger](#absolute-writes-do-not-commute-the-1-1-ledger),
[the group basis](#aggregate-groups-the-group-basis)); a 1-1 target's rows are
then pre-locked in key order, and an aggregate's group upsert writes its
groups in group order.

A consistent total lock order has no cycle, so overlapping workers serialize on a
shared hot group instead of deadlocking. That plus a bounded, idempotent retry on
residual serialization failures (`40001`/`40P01`) is the whole deadlock story.

### No lock wait holds a snapshot open (ADR-0002 I7)

A consistent order stops deadlocks; it doesn't bound a wait. A drain page
queued behind a lock holds its transaction, and with it its snapshot, its
transaction id and every lock it took before, for as long as the holder
does. Vacuum can't pass it, and the sealer's gate, which waits out every
transaction running when the last fence was taken, refuses every seal
meanwhile. In #617 a drain batch's ledger insert waited 1 h 50 min behind
chunk transactions, and the sealer was refused for the whole wait
([#617 final](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5859141160)).

**The rule.** Every lock wait in a Trellis transaction is bounded by
`lock_timeout`, and on `55P03` the transaction rolls back and the work is
retried with backoff from outside any transaction.

- **The bound is on the session.** Every connection Trellis opens, pooled or
  dedicated, caps its `lock_timeout` at `locks::LOCK_TIMEOUT` when it
  connects (a shorter setting it was given is kept). That bounds the explicit
  lock statements (the 1-1 ledger's entry locks above, `FOR SHARE` on the version fence,
  advisory stripe locks, DDL) and the implicit waits nobody writes down: an
  `INSERT ... ON CONFLICT` waiting on another transaction's uncommitted key
  (#617's wait), an `UPDATE` of a row another transaction holds. A setting
  per transaction would cover only the transactions someone remembered.
- **The cap is 30 seconds.** The invariant is that the wait is bounded, not
  that it is short. The cap was two minutes while the aggregate group
  pre-lock queued drain pages that touch the same groups one behind
  another: in `bench fold-in-ratio` at ratio 10 (40k groups, every page
  touching most of them) the longest page transaction, its wait included,
  was 89 s, and a 5 s cap fired 75 times. #623 D5 removed the pre-lock, and
  D9 re-measured the same run on disk: the longest page is now 6.6 s, and no
  page waits out a 30 s or a 10 s cap. The cap is the smallest of the two
  whose longest page stays under a third of it, so a slow checkpoint can't
  turn ordinary pages into retries. Going lower waits for #629's
  measurements.
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

**The rule.** DDL on a user table (`CREATE`/`DROP TRIGGER`, #622) runs in a
retry loop (`locks::DdlRetry`), each attempt its own transaction under
`locks::USER_TABLE_DDL_LOCK_TIMEOUT` (50 ms), E7's shape, one attempt per
`locks::USER_TABLE_DDL_RETRY_INTERVAL` (200 ms). The staging worker's
reconcile pass leaves a table whose lock stays held to the next pass
([01](01-capture-by-triggers.md#installing-widening-narrowing-and-uninstalling)).
The retire path's `TRUNCATE` is the older precedent: it takes its lock
`NOWAIT` and skips the slot until the next tick.

An autovacuum holding the table blocks the DDL the same way. Postgres cancels
one only when a waiter runs its deadlock check, after `deadlock_timeout`,
which a 50 ms attempt never reaches, so a join on a table under a long
autovacuum waits the vacuum out (#622 plan Q1).

## Downstream propagation, and why it terminates

Step 4 stages the keys whose derived values depend on what just changed: every
key a step above physically wrote in a target another definition reads. An
aggregate needs no old image for this: the change's own images name the groups
a key was in.
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
bound cannot (e.g. an unbounded stream of fresh source changes). It is not the contract, so its message stays hedged: exceeding it is *not* necessarily a cycle.

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
| **Halting schema diagnosis** | a source key the drain can't use (`NoPrimaryKey`, `UnsupportedPrimaryKeyType`); a tripped hop bound (`HopBoundExceeded`, a cross-table value cycle or a run-away wave); an aggregate the group basis can't apply (`GroupBasisUnsupported`); Postgres refusing the drain's role a read or write (`42501`: row-level security, which every Trellis session turns into an error with `row_security = off`, or a missing privilege) | **pause the closure it reaches, loudly**; never quarantine. Quarantining would blame one key for a failure every key reproduces, and turn a loud, actionable error into a key that blocks reads forever |
| **Claim lost** | a page's claim check or completion finds a held bucket's claim gone | surface; never isolate. The page rolled back, and whoever holds the buckets now resumes from the last committed cursor |
| **Everything else** | a genuinely poisonous change | isolate and charge — see [06](06-cleanup-and-reclaim.md) |

The halting class deserves emphasis: **failing that way pauses the definitions
the failure reaches, not the instance** (#663). Every key reproduces it, so the
page can never drain as it stands; instead the drain pauses a closure and retries
the page without it:

- for a key error, every definition that reads the table (as its source or as
  a `JOIN`'s joined table);
- for a hop bound outside a cycle, the readers of the table the wave ran away
  through — not the definition that wrote it; inside a cycle, every member;
- for an aggregate the group basis can't apply, the definition that writes that
  target;
- for a refused read or write (#766), found from the catalog as the drain's
  own role, since Postgres's message names no key and only a bare table: the
  readers of each table they read that the role can't (`SELECT`, or policies
  that apply to it), and the writer of each target it can't write. The
  page's retries, and the isolation probes they run, then skip every table no
  unfrozen definition reads, as for a table whose key can't be used (#768);
- and, in every case, everything downstream of those, so no hop target goes
  quietly stale.

Each is left `paused` with a `capture_failures` row of `kind` `halt` naming the
table and the error, which `status` and `definitions()` surface. The page then
commits without the paused definitions' shares, its segments retire, and every
other definition keeps converging. Nothing is silently skipped: a paused
definition is visibly stale until `RESUME TRANSFORM` rebuilds it from its
sources. One halt is one episode — one error line, and one increment of the
halting-stop counter (count plus last reason), so "halted" and "slow" stay
distinguishable. Resuming while the cause persists halts it again as a new
episode, and a closure's members can be resumed in any order. A halt that pauses
nothing because a peer already paused the closure retries the page once. One
that still pauses nothing (a refused read or write the catalog can't pin on a
table, [observability — A halting failure](../observability.md#a-halting-failure))
surfaces the error and records the page as a drain holdup (#817, ADR-0003's
retry policy), which `status` reports as `drain_failure` on the definitions
reading its tables, and the worker re-claims the page at the poll interval
under a collapsed warning (#660).

A backfill marker's discharge that Postgres refuses the same way (`42501`),
while it plans a build or while a go-live catch-up re-reads the source, halts
through the same attribution (#813), read from the catalog as the discharge's
own role. It pauses what the refusal reaches with `kind` `halt` and retries
the marker at once without them. A frozen definition doesn't count as a reader
of a table, so once every reader of the refused table is paused, the retry
doesn't enumerate it, and the marker discharges.
A refusal the catalog can't pin pauses nothing, and the marker backs off as
for any failed discharge, with the error on `backfill_failure`.

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
   (the `prev_lsn` guard, the truncate barrier). A ledger entry or a group records its
   basis, and every change to it is checked against it, so a group's
   increments stay exactly-once. See the
   classification table under
   [the 1-1 ledger](#absolute-writes-do-not-commute-the-1-1-ledger).
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
