# Stage 5 — Applying, and why a delta lands exactly once

← [Claiming and the fold](04-claiming-and-the-fold.md) · next → [Cleanup and reclaim](06-cleanup-and-reclaim.md)

**What this stage owns:** turning folded records into derived writes, and doing
it in a way that survives crashes, duplicate execution, concurrent workers, and
concurrent definition changes.

**The guarantee:** *a non-idempotent effect — an aggregate delta — is applied
exactly once. Never twice, never zero times.* It rests on one transaction and
one per-key record: a batch's writes commit with its drained mark, and a group
moves by exactly what its keys' ledger entries move, in the same statement.

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
    W->>DB: 3. aggregate ledgers (sorted entry lock, I2 test, group increments),<br/>relationship projections
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
arbitrarily expensive without blocking a writer, another worker, or — by holding a
snapshot open — the seal gate and cleanup pass. The price is that the world can
move under you, which the version fence and the immutable batch exist to handle.

## The exactly-once argument

Three properties give it. The first two are structural; the third is the
ledger entry, which every write to a group moves in the same statement.

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

### 3. A group moves by its entries' moves

Within one batch, N changes to a key fold to one record carrying the *latest*
post-image. A page writes that record into the key's ledger entry and, in the
same statement, adds the entry's move (from the state it held to the state it
now holds) to the group's increments
([below](#aggregate-groups-the-ledger)). The entry is the key's last-applied
state, so what a page subtracts is **exactly what the entry's last write
added**, whichever batch that came from.

The moves telescope: a group row, plus any of its increments a Re-derive build
has queued and not yet merged ([below](#aggregate-groups-the-ledger)), is always
the sum of its live members' entries. With no build running, the group row alone
is.
A change the entry's I2 test refuses moves nothing, so a batch draining after a
newer one for the same key adds nothing twice.

## Absolute writes do not commute: the 1-1 ledger

Batches drain out of `seg_seq` order, and key-routing does not order a key's
writes *across* batches ([04](04-claiming-and-the-fold.md)). The argument above
covers that for deltas only. Every write path has to be checked against this
table:

| Write kind | Commutes? | Idempotent? | What makes an out-of-order drain safe |
|---|---|---|---|
| Aggregate group increments (`+new − old`, per entry) | yes | **no** | exactly-once (the three properties above), and the entry's I2 test |
| Aggregate entry writes, from an image or a Re-derive read | **no** | yes | the ledger's entry lock and I2 test, below |
| 1-1 field writes and deletes, from an image or a Re-derive read | **no** | yes | the 1-1 ledger's entry lock and I2 test, below |
| Relationship projection writes | no | yes | the per-row `prev_lsn` ordering guard |
| Truncate | no | yes | the drain barrier (full serialization) |

I2 is one test, shared by both ledgers: an Apply changes its entry only if its
source transaction is not visible in the entry's `__basis`, its `lsn` is above
`__applied_lsn`, and its `lsn` is above the target's truncate floor.

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
   at a time. The one exception is the orphan sweep
   (`intake::resume_orphans`), which a marker's discharge runs for a resume or a
   one-pass build's go-live catch-up: it deletes 1-1 target rows, and aggregate
   groups, the source no longer backs by key, without the entry. It judges
   them unbacked on the snapshot of its own read, and any later change drains
   after the sweep commits and re-derives the key, so it is ordered by that
   snapshot argument instead. It runs only for definitions the Re-derive build
   doesn't serve (a resumed definition it does serve gets the build's own
   sweep instead), until milestone F (#625) moves them. It can't be the target row's `FOR UPDATE`: a stale insert racing a
   delete has no row to lock. A key whose tombstone the GC collects between
   the two statements fails the page transiently and it is retried (#712).
2. **The Re-derive read (I1).** The Re-derived keys' source rows and
   `pg_current_snapshot()`, in one statement, after the lock. A key with no
   row is a delete.
3. **The entries (I2).** A Re-derive sets the entry's `__basis` to the read's
   snapshot and leaves `__applied_lsn` alone (#623 Q1). An Apply applies only
   if the I2 test above passes; it
   then sets `__applied_lsn`. Either raises `__applied_seg` and marks a
   tombstone when the key has no row after it. The statement returns the keys
   it changed. A placeholder step 1 inserted that neither step wrote (an
   Apply at or below the truncate floor, or a Re-derive the page re-staged
   or skipped as failing to evaluate)
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

A relationship-enriched 1-1 target evaluates a Re-derive against the
relationship projection Phase 2 read, as an Apply is. A Re-derived row that
now joins through a key Phase 2 did not resolve is re-staged as a bare
recompute instead, which a later page reads.

`ALTER TRANSFORM`'s field rebuild and a column resume are field builds
(`staging::build`, #625 F8b): background chunks that lock a range of keys'
entries as a page does and rewrite just the changed columns of their target
rows from one snapshot, leaving the entries as they are. A relationship-
enriched 1-1 target's field build re-derives each key's whole row instead,
settled the same way as a page's Re-derive, except that its chunk reads the
relationship projection in its transaction, after the entry lock and its
read of the rows, rather than before it as a page's Phase 2 does. A chunk
can sit between its plan and its lock for a long time, and a parent change
whose recompute drained in that window would otherwise have its value
written over by the chunk's older read (#832). A parent change that commits
after the chunk's read stages a recompute whose page waits on the entry
lock and writes after the chunk. The chunk reads the projection by its
primary key with sequential scans off, as the entry lock reads the ledger, so
a projection whose statistics lag its size isn't read in full while the
entries stay locked. A to-many relationship's to-side is read by its join
column as Phase 2 reads it, since that column may have no index.

### The ledger

ADR-0002 gives every target a per-key ledger. Apply maintains it for every
aggregate target (`staging::ledger`, next section) and every 1-1 target
(`staging::one_to_one_ledger`, [above](#absolute-writes-do-not-commute-the-1-1-ledger)).

Every target has one, `<target>__ledger` in the target's schema, created in the
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

An aggregate target's entries also hold:

- the row's `GROUP BY` values, typed as the target's;
- `__member`;
- one column per distinct aggregate argument (`__arg0`, …), holding the
  argument's value, with the argument's collation when it is text;

Every group row is then a pure function of its live members' entries: `SUM(x)`
is `sum(__argN)` over them, its hidden count `count(__argN)`, `COUNT(*)` the
entry count, and so on. The group row carries that count as `__trellis_members`.
A 1-1 target's ledger holds only the key and the ordering state, because the
target row holds the values.

Every ledger is keyed by `__from_key` and indexes its tombstones by
`__applied_seg` (the tombstone GC's index, below). An aggregate ledger also
has a `GROUP BY` index over its live members (`where __member and not
__tombstone`) unless the Re-derive build takes its target and none of the
target's fields is recomputed (#723). Only three readers find a group's
entries through that index: a recomputed field's rewrite, the one-pass
build's group writes, and the orphan sweep. The last two run only for
targets the Re-derive build doesn't take. Every other statement reads the
ledger by key, so a ledger without the index saves every entry write the
cost of maintaining it, which on a ledger far larger than `shared_buffers`
is most of a build's WAL, as full-page images of the index's leaves.

A target is built one of two ways. The Re-derive build (`staging::build`,
#625) serves an aggregate target with no relationship path and a 1-1 target
with none, on a captured source (not another definition's target): it
applies from its start and re-derives the source in chunks, each inserting
the entries its keys don't have yet and locking the others as a page does,
and stamping them as a Re-derive does (below).
Every other target keeps the one-pass build. For an aggregate it empties the
ledger, reads the source into it in one statement whose snapshot becomes
every entry's basis, and then writes the group rows as a `GROUP BY` over it.
A relationship-enriched 1-1 target's one-pass build writes no entries: a
key's first change after it inserts the key's entry, and the go-live
catch-up's Re-derives stamp every key's basis.

### Aggregate groups: the ledger

Every aggregate target is on the ledger (`staging::ledger`). Its fields are
of two kinds (#623 D3, D4):

- **Maintained** by increments: `SUM(x)` and `AVG(x)` over an exact numeric
  argument, `COUNT(*)` and `COUNT(x)`. `AVG(x)` keeps a hidden running sum and
  count and writes `sum / count::numeric`, which is Postgres's own `avg()`
  over an exact numeric argument.
- **Recomputed** from the group's live entries after the upsert: every other
  field (`MIN`/`MAX`, `BOOL_AND`/`BOOL_OR`, a float `SUM`/`AVG`, a composed
  field such as `SUM(a) + COUNT(b)`).

An argument may be an expression (`SUM(v + 1)`), evaluated per change over
the image as the source's row type. A target that reads a value, or groups by
one, through a to-one relationship is no different (#623 D5): its statements
left-join each relationship's to-side and read the parent live, after the
entry lock and in the same statement as the child's read. A parent change
re-derives every child it reaches; a child that has since moved off the
parent has a change of its own pending, whose write reads its new parent.
Each page applies its records for an aggregate target in one transaction, in
five steps:

1. **Lock (I1, I5).** Insert an entry for every key the page has no entry
   for, then lock every other entry `for update`, sorted by key, in one
   statement. As on a 1-1 target, a new key's Apply is written into the entry
   its insert creates, since with no entry I2 is only the truncate floor
   (#775); step 3 leaves that entry alone and counts it as a move into its
   group from no entry. A Re-derive's new key, an Apply at or below the
   floor, and every new key of a target that reads a relationship (whose
   parents must be read after the lock) get a non-member placeholder that
   step 3 writes. A key whose tombstone the GC collects between the two
   statements fails the page transiently and it is retried (#712).
2. **Re-derive read.** A record staged as a `recompute`, or folded with one, is a
   Re-derive. So is a record with no change to apply, and every record of a
   page whose oldest batch is at or below the target's `build_seg`, as on a
   1-1 target (#733). One
   statement reads those keys' current source rows *and*
   `pg_current_snapshot()`, after the lock. A key with no row becomes a
   tombstone.
3. **Entries, then groups, in one statement.** It first updates each entry:
   - A Re-derive writes the entry from its read. It sets `__basis` to the
     read's snapshot and leaves `__applied_lsn` alone (#623 Q1). Every change
     the read saw is visible in that snapshot. Every change it missed is still
     pending with its own ring row, and its trigger `lsn` may be below any
     position the read could record.
   - An Apply writes the entry from the record's new image, or makes a tombstone
     for a delete. It sets `__applied_lsn` to the change's `lsn`, and only if
     the I2 test passes (as on a 1-1 target, [above](#absolute-writes-do-not-commute-the-1-1-ledger)).
   - An Apply at or below the truncate floor that step 1 gave a placeholder
     deletes it instead (#774), as on a 1-1 target: nothing else would write
     it, and the tombstone GC collects only tombstones.

   The statement then sums each updated entry's move from its old state to its
   new one into per-group increments: the member count, and per argument its
   sum and its non-null count. It upserts them in group order, incrementing every
   column (I3). A `SUM` goes `NULL` when its non-null count and its sum both
   reach 0.
4. **Recomputed fields.** Each group the upsert wrote has its recomputed
   fields rewritten from its live member entries. The upsert holds every
   written group's row lock, so this statement sees every entry change any
   other page made to those groups (a build chunk takes no group lock, and
   the merger recomputes every group it writes).
5. **Empty groups go.** Groups whose every accumulator (`__trellis_members`,
   each count, each sum) reached 0 are deleted. With one writer of groups that
   is the same as the member count reaching 0. A Re-derive build adds a second
   writer (below), and then a group can reach 0 members while a sum is
   still owed to it.

A page takes its locks in one order: entries (an insert for new keys, then
one sorted `for update` of the rest), then groups, in one sorted upsert. A Re-derive of an unchanged key moves nothing, so a go-live re-read
after the build writes no group rows.

Each written or deleted group reaches the seam with its prior image. PG 17 has
no `OLD` in `RETURNING`, so the image is rebuilt from the upsert's result minus
the increments. A group the upsert created has no prior image.

A fold record carries the identity of the change that won its post-image
(`last_change`: its `lsn` and `row_txid`, see
[04](04-claiming-and-the-fold.md)). That change is the one an Apply judges.

**Build chunks and the merger (#625).** A Re-derive build re-derives the
source a primary-key range at a time. An aggregate target's chunk first
inserts the entries of the keys that have none, in one statement that reads
their rows with `pg_current_snapshot()` and the active segment, writes each
entry once from that read (`__basis` the snapshot, a deleted key a tombstone
stamped with the segment), and appends their groups' increments. A key whose
entry is there by the time its insert runs is left alone. The insert's
snapshot is taken before its uniqueness check, which is sound because no
Apply can have written a key since that snapshot without leaving its entry
for the check to find: the only thing that removes an entry an Apply wrote,
the tombstone GC, skips a ledger while its definition is under a build
(below). A page deletes only a placeholder it wrote no change to, which
holds no applied change, and a source truncate commits before the chunk's
first read, which holds the source's lock, so it empties no change the
snapshot doesn't see. The chunk
then takes the same entry lock as a page on the keys that already had an
entry, and one more statement reads their rows afresh, rewrites their
entries and appends their increments. Both steps run under a 1 s
`lock_timeout`, so a chunk gives way to a page. On a fresh build every key
is new, and the insert is the whole chunk. Every chunk stamps `__applied_seg`
with the segment its read saw, so the tombstone GC can collect a chunk's
tombstones. The increments go to `<target>__deltas`, where each row's
generated `__part` is its group's merge partition: `hash_record_extended`
of the group, so equal groups (`1.5` and `1.50`, or every `NULL` group)
share one. It never writes a group row. A
merger claims one partition's delta rows oldest first through an index on
`(__part, __seq)` (`for update skip locked`), deletes them, and upserts their
sums per group with the page's upsert, in group order, then rewrites the
recomputed fields of the groups it wrote (#625 F5). Only one merger works
on a partition at a time: it takes a transaction-scoped advisory lock on the
partition without waiting, trying the partitions with rows in turn, and
skips the target only when another merger holds each of them (#625 F2b,
#717). Mergers of different partitions write different groups, so they run
side by side without queueing on each other's group rows. The delta table is a queue, so its statistics are unreliable: the merge
statement runs with nested loops and sequential scans off, and a merger
vacuums the table every 100,000 rows it merges. The delta rows are discarded
only with the ledger: by a truncate, a drop, or the one-pass build.

**Truncate.** A source `TRUNCATE` empties the ledger and the group deltas,
deletes every group row, and raises the target's truncate floor
(`ledger_truncate_floor`) to the truncate's `lsn` (#623 Q6). A 1-1 target
does the same to its ledger, and the page clears its rows. `TRUNCATE` takes
`ACCESS EXCLUSIVE`, so every earlier writer's trigger ran below that `lsn` and
every later writer's above it; a write later in the truncating transaction
itself is above it too, since the truncate's own ring row moves the WAL insert
position on. The floor is the second line of defence: the drain barrier
already orders the truncate's batch after every earlier one and before every
later one (no segment above an undrained truncate's is claimable,
`next_claimable_segments`; see [truncate-propagation-spec.md](truncate-propagation-spec.md)),
and the fold voids its own batch's rows from before it.

**Release and the orphan sweep.** Releasing a quarantined key
(`staging::release_key`) stages one `Recompute` of it and discards the parked
rows. A replayed row would carry the releaser's `row_txid`, not the source
transaction's. A catch-up discharge's orphan sweep finds live entries the
source no longer backs and Re-derives them in its own transaction (each comes
back a tombstone and leaves its group), stamping them with the newest segment.

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
entry live leaves it alone. Both ledgers index their tombstones by it
(`(__applied_seg) where __tombstone`), so a GC batch seeks the collectable
ones under a pinned index-scan plan (#738), however many tombstones wait
above the drained prefix, and an Apply or Re-derive that leaves an entry live
in its group changes no indexed column and can be HOT. A live entry's stale
stamp never has to protect anything: a delete's Apply applies only a change
its entry's `basis` doesn't see, so every change the `basis` does see
completed before the delete and is in the delete's batch or an earlier one,
which the Apply's stamp covers, and a Re-derive that deletes the key stamps
its own read's segment (`defs::ledger::tombstone_seg_sql`).

## The delta model

For a source key, maintaining a measure `f` over group `g` in one Phase-3
transaction, where the key's ledger entry holds the group `g(entry)` and the
contribution `f(entry)` it last applied:

| Op | Effect |
|---|---|
| INSERT | `g(new) += f(new)` |
| DELETE | `g(entry) -= f(entry)`; the entry becomes a tombstone |
| UPDATE, grain unchanged | `g += f(new) − f(entry)` — one group, net delta |
| UPDATE, grain changed | **grain migration**: `g(entry) -= f(entry)` *and* `g(new) += f(new)` — two groups |

The entry is then rewritten to the new side. So the drain needs one image per
folded record, its **new side** (none for a delete), and no old image: the old
side is the entry.

An Apply takes the new side from the **staged post-image at the claimed
position**, which keeps it scan-free. A Re-derive reads the source live
instead, which may see a *later* state than the batch accounts for. That is
safe because the entry records what it added: the read's snapshot becomes the
entry's `__basis`, so a later change the read already saw is refused by I2,
and the next write subtracts what the read wrote.

Composite measures fold their hidden partials, never themselves: `avg` maintains
`__{m}_sum` and `__{m}_count` and recomputes the visible ratio from them.

**Not every measure is delta-able**; the gate is explicit: only exact,
invertible folds are maintained by increments. `count(*)`, `count(col)`, and
`sum`/`avg` over an exact numeric argument are in. Every other field is
recomputed from the group's live entries (#623 D4): `min`/`max`, which are not
invertible (removing the current maximum tells you nothing about the next
one), `bool_and`/`bool_or`, a float `sum`/`avg`, whose increments would drift
because IEEE-754 addition is non-associative (and `Inf − Inf = NaN`), and a
composed field.

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
batches for one key draining out of order is closed separately, by the 1-1
ledger ([above](#absolute-writes-do-not-commute-the-1-1-ledger)).

**The fence runs first in Phase 3**, so a superseded batch rolls back before
touching a derived row. The fence set must include tables that are *evaluated* but
absent from the one-to-one write plan — an aggregate-only source, an equi-join
parent — or a grain change mid-batch slips through.

## Lock ordering, because parallel workers will overlap

Two workers whose batches touch overlapping keys or groups will contend on the
same rows. That is fine; deadlocking on them is not. A page takes its locks in
one consistent order: its 1-1 targets, then its aggregate targets, each in
target order, and per target **its entries, then its rows, each in one sorted
statement**. The entry lock is ordered by key
([the 1-1 ledger](#absolute-writes-do-not-commute-the-1-1-ledger),
[the aggregate ledger](#aggregate-groups-the-ledger)); a 1-1 target's rows are
then pre-locked in key order, and an aggregate's group upsert writes its
groups in group order. After its targets, a page locks every to-one
relationship projection row it writes, in one sorted statement per
relationship, in relationship order. Those are the rows whose generation it
bumps and the rows its reverse records guard and advance, and the order is
the one the reverse release uses. Left to themselves, the bump would write
its rows in whatever order its plan reads them (physical order under a
bitmap scan), and the reverse records would lock theirs one at a time in
fold order. Either one deadlocks against a page or a release that reaches
the same parents in another order. The sorted lock finds only the rows
that exist when it runs. A row it didn't find, because it doesn't exist yet
or was committed after the lock, is locked when a reverse record reads or
writes it, in record order, and a record inserting a key another page is
inserting waits on that page's insert. Two pages can still deadlock through
such a row, but only while a projection row both of them reach is being
created; Postgres aborts one of them, which retries.

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
  lock statements (the ledger's entry locks above, `FOR SHARE` on the version fence,
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
key a step above physically wrote in a target another definition reads, and,
for a relationship whose to-side changed, the from-side rows that join it. An
aggregate needs no old image for this: its ledger entry names the group a key
was in. The relationship paths still read the old image a capture trigger
stages ([01](01-capture-by-triggers.md)), the only place a key's old join
value still exists: a to-side **delete** or **join-key change** must refresh
the from-side rows that joined the old key, not only those that join the new
one ([04](04-claiming-and-the-fold.md#who-reads-the-old-image)). 
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
| **Halting schema diagnosis** | a source key the drain can't use (`NoPrimaryKey`, `UnsupportedPrimaryKeyType`); a tripped hop bound (`HopBoundExceeded`, a cross-table value cycle or a run-away wave); an aggregate target off the ledger (`AggregateOffLedger`); Postgres refusing the drain's role a read or write (`42501`: row-level security, which every Trellis session turns into an error with `row_security = off`, or a missing privilege) | **pause the closure it reaches, loudly**; never quarantine. Quarantining would blame one key for a failure every key reproduces, and turn a loud, actionable error into a key that blocks reads forever |
| **Claim lost** | a page's claim check or completion finds a held bucket's claim gone | surface; never isolate. The page rolled back, and whoever holds the buckets now resumes from the last committed cursor |
| **Everything else** | a genuinely poisonous change | isolate and charge — see [06](06-cleanup-and-reclaim.md) |

The halting class deserves emphasis: **failing that way pauses the definitions
the failure reaches, not the instance** (#663). Every key reproduces it, so the
page can never drain as it stands; instead the drain pauses a closure and retries
the page without it:

- for a key error, every definition that reads the table (as its source or as a
  relationship's to-side);
- for a hop bound outside a cycle, the readers of the table the wave ran away
  through — not the definition that wrote it; inside a cycle, every member;
- for an aggregate off the ledger, the definition that writes that target;
- for a refused read or write (#766), found from the catalog as the drain's
  own role, since Postgres's message names no key and only a bare table: the
  readers of each table they read that the role can't (`SELECT`, or policies
  that apply to it), and the writer of each target it can't write. The
  page's retries, and the isolation probes they run, then skip every table no
  unfrozen definition reads, as for a table whose key can't be used (#768):
  its own changes, and the lookup of a relationship's from-side rows a
  to-side change would recompute. The drain keeps a relationship's settled
  projection current from its to-side whatever its readers' status, and
  recomputes the from-side for the to-side's other readers' changes, so
  either read would be refused again. A resume refreshes the projections it
  reads;
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
table, [gap 11](../known-correctness-gaps.md#11-row-level-security-on-a-role-trellis-runs-as))
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
neither enumerates it nor refreshes its projections, and the marker discharges.
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
   (the `prev_lsn` guard, the truncate barrier). A ledger entry records its
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
