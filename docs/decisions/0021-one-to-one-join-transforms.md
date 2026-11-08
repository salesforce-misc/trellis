---
status: draft
date: 2026-10-24
deciders: Michael Ries
---

# 1-1 JOIN Transforms

Some queries join tables without changing the grain. They bring columns of
each into one row per row of the first, often to compute a value from them:

```sql
SELECT a.id, COALESCE(ag.total_words, 0) AS total_words,
       a.rating * COALESCE(ag.total_words, 0) AS scaled_words
FROM authors a LEFT JOIN comments_agg ag ON ag.author = a.id
```

The output is a 1-1 target of `a`, and the per-row target is the feature: a
reader can query and index `scaled_words` without repeating the join and the
calculation. A JOIN transform ([ADR-0019](0019-join-transforms.md)) always has a
`GROUP BY` and keeps no per-row state, so it can't serve this.

This ADR adds 1-1 JOIN transforms:

```text
TRANSFORM <target>
FROM <a> [INNER | LEFT] JOIN <b1> ON <a>.<col> = <b1>.<col> [AND ...]
        [[INNER | LEFT] JOIN <b2> ON <a>.<col> = <b2>.<col> [AND ...] ...]
SELECT <expressions over a's row and the matched row of each joined table>
```

It settles their scope, the hidden state behind them, and the locks and
ordering rules that keep them exact. It builds on
[ADR-0002](0002-async-data-flow.md)'s 1-1 ledger, which orders every per-row
write, and takes ADR-0019's side, qualification, join-kind and `ON` rules and
its `J` lock.

The root `a` is joined to one or more tables, a star. Each joined table has a
**position**, `1` for the first `JOIN` and up to `4`, and `n` is the number of
tables, the root included. Below, `b` names the joined table at any one
position, and a rule stated for `b` holds at every position. The two-table
JOIN is the one-position case of the same design: it takes the same locks
and has no rules of its own. A non-unique `b` key and `WHERE` are out of
scope.

## Decisions

### A 1-1 JOIN keeps one row per row of `a`, joined to at most one row of each joined table

- **No `GROUP BY`.** The target is keyed by `a`'s primary key. `INNER` drops an
  `a` row with no match; `LEFT` keeps it with `NULL` `b` columns. Each
  position has its own kind.
- **A star of at most 5 tables.** `a` is joined to up to 4 tables, each
  `INNER` or `LEFT`, and each `ON` compares columns of `a` to a unique key of
  that table. Define refuses, by name:
  - more than 5 tables;
  - a snowflake, an `ON` that reads a table joined earlier rather than `a`;
  - a table that appears twice;
  - `RIGHT`, `FULL`, `CROSS` and `LATERAL` joins;
  - a partitioned `a` or joined table.

  Correctness needs no cap: the argument below holds for any number of
  positions, since no rule relies on a lock of a third position. 5 is the
  largest `n` a model confirmed (#1459), and the cost per change grows with
  `n` (Consequences), so a later release can raise it without changing the
  design.
- **`b`'s `ON` columns are unique**: `b`'s primary key, a unique constraint,
  or the group key of an upstream aggregate or JOIN target. A non-unique `b`
  would give one row per pair, keyed by both sides, which is a different
  target. The staging worker's reconcile pass checks it by define's rule
  while the transform runs and pauses the transform when the key is gone, and
  `RESUME` refuses until it is back
  ([known correctness gaps](../known-correctness-gaps.md#a-duplicate-in-b-past-a-deferrable-key-under-session_replication_role--replica)
  has the one duplicate it can't see).
- **Fields** are any expression the 1-1 path admits, over `a`'s row and the
  matched row at every position, of any type a 1-1 field admits (floats and text included).
  A row is recomputed from its current inputs and never maintained by deltas,
  so none of the aggregate restrictions apply.
- Sides, qualification, join kinds and `ON` follow ADR-0019 at every
  position, except that ADR-0019's single join becomes up to 4.

### A side that is another transform's target is read from its change feed

A side that is another transform's target is read from its change feed, as in
[ADR-0019](0019-join-transforms.md#a-side-may-be-another-transforms-target-through-the-target-change-feed):
the feed's rows take the place of captured images, and every ordering rule
below holds for them as it does for a captured side.

- **A plain 1-1 target as `a`** is joined on any `ON` columns. They carry no
  uniqueness requirement, and the join-key rules (the allowed types, a
  deterministic collation, one type and collation on both sides) apply as for
  a captured table.
- **A plain 1-1 target as `b`** is joined on columns that include every column
  of its primary key. The `b` side's uniqueness relies on that key: Trellis
  orders changes per primary-key value only, so the primary key is the one
  column set of a target that is unique at every instant while its upstream
  drains. Changes to different keys apply in any order, so two rows can hold
  one value of any other column for a moment, and a unique index on that
  column refuses one of them and makes the upstream fail. A unique index
  added to the target never counts for this rule, though it satisfies the
  uniqueness rule above. A wider `b` gate needs another ordering
  mechanism: either the upstream orders changes per join-key value, or the JOIN
  tolerates transient duplicates and weakens its guarantee to "equal once the
  upstream has converged" ([#1489](https://github.com/salesforce-misc/trellis/issues/1489)).
- **An aggregate or JOIN target** as a side is held to the uniqueness rule
  alone.
- A target is never truncated as a change of its own: a `TRUNCATE` of its
  upstream's source reaches the JOIN as a delete of every row, in one
  upstream transaction. For a `b` side that is a change of `b` like any
  other, so it starts no rebuild, and it fans out over every `a` row those
  rows match, in chunks, as below.

### Each table keeps a Trellis-owned copy, ordered by ADR-0002's 1-1 ledger

- **`R`**, one entry per `a` row: the 1-1 ledger's ordering state, plus the
  row's join key at every position and the `a` columns the fields read. It is
  filled from captured images and has one index per position, on that
  position's join key and then the entry key, which is how a `b` change finds
  its rows and its chunks walk them. It is about the size of `a`, plus one
  key index per position.
- **`M`**, one per position, one entry per `b` row, keyed by `b`'s primary key
  and indexed on the join key: the ledger's ordering state plus the `b`
  columns the fields read. It is about the size of `b`. Keyed by `b`'s
  primary key rather than by join key, it applies a key's hand-off from one
  `b` row to another in either order. For the moment the two are applied out
  of order, a key can hold two live entries, and a read takes the newest.
- **`J`**, one shared lock table with one row per `(position, key)`, and the
  transform's truncate lock row, both as in ADR-0019. Its layout:
  - A `position` column, checked to lie between `1` and the number of
    positions, and one group of key columns per position, typed as that
    position's `ON` columns of `a` with no modifiers
    ([ADR-0020](0020-type-identity.md)). A row's columns of every other
    position are `NULL`. `J` has no primary key.
  - Per position, a partial unique index on that position's key columns
    `WHERE position = i`, and a check that a row of position `i` has no
    `NULL` key column. A partial unique index doesn't make its columns
    `NOT NULL`, and a `NULL` key would let two rows hold one key, so two
    writers would lock different rows. The check makes that an error rather
    than a silent second row, which is why it is chosen over `NULLS NOT
    DISTINCT`.
  - A page inserts its keys at every position in one statement, sorted by
    `(position, key)`, with `ON CONFLICT DO NOTHING` and no conflict target,
    so every position's unique index is an arbiter. It then locks them in one
    sorted `FOR UPDATE` over every position, each position's predicate using
    that position's index. Keys are cast per type, so keys equal by value
    share a row.
  - Every other reader of `J` (a fan-out chunk, a build chunk, `self_check`,
    `status()`) selects a position's rows by `position = i` and that
    position's key columns, so it reads through that position's index.

  One table rather than one per position puts every `J` lock in one sorted
  statement, so the lock order can't split by position, and it is one table
  to create and drop; one table per position was 1.32x slower at 8 workers
  (#1459). A key text form shared by every position was rejected: it adds an
  identity rule beside ADR-0020, and loses updates for any type it renders
  wrong (#1765).

`R` and `M` entries take the 1-1 ledger's lock pattern: a placeholder insert,
then one sorted `FOR UPDATE`, then the applied-position check. Without the
placeholder, an older page waiting on a newer page's uncommitted insert of the
same entry overwrote it. Without the position check, an out-of-order page put
an older state over a newer one.

Both `R` and every `M` take ADR-0002's full I2: an Apply runs only if its txid is
not in the entry's basis, its position is past the entry's applied position,
and it is above its table's truncate floor. Each check alone was wrong in
some interleaving (#1375). A Re-derive of any table is what every producer
stages (a build chunk, a rebuild, `request_backfill`, a quarantine release,
catch-up, and a page whose segment predates the build):

- Before its `J` lock, the page reads the row's live join keys, unlocked, and
  adds them to its key set alongside the keys the entry holds: every
  position's key for an `a` row, its own position's key for a `b` row.
- After its entry lock, one statement reads the live row and
  `pg_current_snapshot()`, which becomes the entry's basis. A snapshot taken a
  statement later was wrong in 22 families.
- If that read finds a key outside the locked set at any position (the row
  moved between the two reads), the page rolls back and retries: the late-key
  retry covers every key of the row. Not retrying was wrong in 910 of 1,716
  schedules for each side, and skipping it at one position of a star was
  wrong too (#1459). Taking the late key's `J` after the read was exact but
  deadlocked, because it breaks lock sorting.
- It reads any `M` only after its `J` lock.

### No writer reads another table's source, only its copy, under `J` and the row's `R` entry

- **An `a` change** locks `J` at every position, for its new key and the key
  `R` holds, sorted, insert-then-lock. It then locks its `R` entries and
  re-checks that every key each one holds is in the locked set, rolling back
  and retrying if a key moved. It reads each `M` only after the `J` lock, and
  writes `R` and the target row in one transaction.
- **A `b` change** locks its own position's `J` for its new key, its old key
  from the image, and the key `M` holds. It takes no other position's `J`.
  Then:
  1. It locks the `R` entries of every row on those keys, found through `R`'s
     index after the `J` lock, `FOR UPDATE` and sorted, in **one `R` lock
     statement over every position the page touches, together with the
     page's own `a` entries**. Any split of that statement, per position or
     into any grouping of positions, deadlocks: each statement is sorted, but
     the statements run in position order, not row order. A page that reaches
     row `r2` through position 1 and `r1` through position 2 takes `r2` first,
     a page that reaches them the other way round takes `r1` first, and the
     two pages hold different `J` rows, so nothing orders them before they
     meet on `R` (#1459, at `n = 3`, 4 and 5).
  2. Only then does it read any `M`, its own position's or another's.
  3. It locks and re-checks its `M` entries against the page's **fan-out
     keys** at its position: the keys whose `R` entries step 1 locked. A key
     in the page's `J` set only because an `a` change of the page holds it
     doesn't count, since its rows' `R` entries weren't locked for the
     fan-out. The re-check covers the key an entry holds at its lock and a
     Re-derive's live key, and a key outside the set rolls the page back to
     retry.
  4. It applies its `M` entries and recomputes the target row of every `R`
     entry on those keys.
- Every writer takes `J` `FOR UPDATE`. `FOR SHARE` on the `a` side is also
  exact, but it starved the `b` side: 24 `b` changes in 20 s against 1,925.
- The target may be written in a batch per position, as a fan-out's keyset
  batches are: the `R` entries already order every writer of a row, so the
  batches need no merging by root key.

**Why it is exact.** Three invariants hold, each enforced by locks that name
one position `i`:

- **(A)** A root row's key at position `i` changes only under `J` at
  position `i` for its old key and its new key: an `a` writer and a build
  chunk lock both and re-check, and a Re-derive retries on a late key at any
  position.
- **(B)** An `M` entry at position `i` on key `k` changes, and a row is
  recomputed through `k`, only under `J[i, k]` and then the `R` entries of
  every row whose key at position `i` is `k`. By (A), that set of rows can't
  change while `J[i, k]` is held, and the fan-out-key re-check makes `k` the
  key the entry really holds.
- **(C)** Every transaction that writes a target row holds the row's `R`
  entry `FOR UPDATE` from before its first `M` read until it commits.

So any two transactions that affect one target row (an `a` writer of the row,
a writer of an `M` entry on one of its keys, a recompute of it) both hold the
row's `R` entry across their reads and their write. The entry orders them,
and under READ COMMITTED the later one reads everything the earlier one
committed, so on every commit a target row is the join of its `R` entry with
the newest live `M` entry on its key at every position. The lock that orders
a pair names at most one position, never a third, so the argument doesn't
depend on the number of positions, and no writer needs another position's
`J`.

A model of the star (#1459) was exact under these rules and showed each one
necessary at every position by an ablation that removes it: 138,988
deterministic schedules per version at `n = 3` and 504,036 at `n = 4`, on
PG 16, 17 and 18, a spot check of 174,684 at `n = 5`, and soak runs at each,
for every combination of `INNER` and `LEFT`. A `b` writer that held only its
own `J` lost updates (1,988 of 18,748 schedules at `n = 3`), and so did one
that locked the target rows instead of the `R` entries (405, where an `INNER`
row doesn't exist yet), or read another `M` before its `R` lock. Locking
every other position's `J` for the rows a fan-out reaches is also exact, but
holds them for the whole fan-out (every `J` row of the other position over
a 1M-row fan-out), which stalls every `a` writer and every writer of that
position until it ends.

With one position the `R` entry lock is redundant, since `J[1, k]` already
orders the only two writer kinds: in a model on PG 16.8, 17.10 and 18.2,
8,064 deterministic interleavings per version and 240 randomized soak runs
of the two-table rules without it were exact (#1371). The two-table JOIN
takes it anyway, so one code path serves every `n`, at about 8% of its
fan-out's time.

**Lock order.** A page carrying changes and Re-derives of several tables
takes the lock classes in one global order, over the union of its key sets:
the truncate lock row; `J`, in one statement sorted by `(position, key)`;
the `R` entries, in one statement sorted by entry key; the `M` entries,
sorted by `(position, id)`, in statements that follow that order; then
the target rows. Placeholder inserts follow the same order, so no two
transactions take two rows of `J`, `R` or `M` in opposite orders. The target
rows need no such order: every writer of a target row holds its `R` entry
first (C), so two writers of one row queue on that entry before either
reaches the row.

A truncate page holds the lock row `FOR UPDATE`, empties its table's hidden
table and raises its floor. It empties the table with a `DELETE`: a build
step reads `R` or `M` before it takes the lock row, so a `TRUNCATE`, whose
lock waits for that read, would deadlock with it. ADR-0019's barrier orders
the page against other pages, over every table of the transform: a page
waits for every earlier truncate page of any of its tables, and a truncate
page for every earlier page of its own table. Without the barrier, four of
eight truncate families were wrong. With both rules, 60 families × 2 join
kinds (102,312 schedules per version on PG 16.8, 17.10 and 18.2) and 48
soak runs, with builds starting mid-run, were exact (#1375). The star's
model (#1459) applied a truncate as a per-entry floor and recomputed a
joined table's truncate in the page, so for more than two tables the
barrier and the rebuild below rest on this argument, not on a model.

A page settles its truncate before its changes, so a change after the truncate
in the same batch applies to the emptied table:

- **`TRUNCATE a`** empties `R` and raises its floor, and deletes every target
  row in the page. Every `M` and `J` is untouched.
- **`TRUNCATE b`**, at any position, empties that position's `M` and raises
  its floor. It changes every target
  row, so it starts a target rebuild rather than recomputing in the page. A
  rebuild's start takes the definition rows of everything it rebuilds, in id
  order, then their lock rows, then the segment pointer. So the page takes
  the definition rows of every target whose `b` it truncates `FOR UPDATE`, in
  id order, before its first lock row, and starts the rebuilds after it has
  marked its segments drained, since seal phase 1 takes a segment's row
  before the pointer. From the commit the transform reads `backfilling` and
  keeps applying. A `LEFT` row keeps the `b` columns it held until the
  build's chunk reaches it, an `INNER` row stays, and a change of `a` before
  then joins the emptied `M`. A definition that does not apply (paused since
  the page began) starts no rebuild, and its `RESUME` re-derives every row.
- A page that truncates several tables does each. However many joined tables
  it truncates, it takes the definition row once, before the lock row, and
  starts one rebuild.

A change of any table that was staged before a truncate and reaches a page
after it is below the table's floor and refused, by the same I2 check as a
change older than an entry's applied position.

### A `b` change's fan-out runs in chunks under a pending marker

A `b` change rewrites every target row on its key, each recomputed from `R`
and the newest live `M` entry at every position while the writer holds
`J[i, k]` and the rows' `R` entries, and an `a` change on that key waits for
`J[i, k]`. Fan-out in chunks that each take `J[i, k]` and their rows' `R`
entries again is exact (#1375 Y5, #1459), so no transaction recomputes more
than one chunk of 10k `R` entries:

- **The page** counts the entries on each key its `b` changes reach, at
  every position, against one budget for the page, summed over positions,
  each count stopping one past the budget left, and recomputes itself the
  keys that fit one chunk between them. For every other key it writes a
  pending marker `(position, k, gen, cursor)` in its own transaction, at the key's start, with
  the earliest origin of the page's `b` changes. A key the page recomputes
  whole loses its marker. A later page on a pending key sets the cursor back
  to the start and keeps the earlier origin: the rows behind the cursor joined
  the older `b` row, and every row must join the newer one.
- **A chunk** is one transaction, run by a drain worker as build work, after
  its segments and before any build chunk. In the page's lock order it takes
  every table's version fence, the definition row `FOR KEY SHARE` (a chunk
  may hold a row, as a page's fan-out may), the lock row `FOR SHARE`,
  `J[i, k]`, the marker, and then the `R` entries of its batch, before it
  reads any `M`. A chunk that recomputed its batch without re-taking those
  entries lost updates against a writer at another position (#1459). It
  recomputes the next chunk of live `R` entries after the cursor in key
  order, a range of `R`'s index on position `i`'s join key and then the
  entry key, and advances the cursor, or deletes the marker after the key's
  last entry. Its rows reach the target's readers through the seam, staged in
  the chunk's transaction with the marker's origin, as a page stages its
  fan-out's rows with its `b` changes' origin. A reader applies them as each
  chunk commits, so between two chunks it sees what a key's rows give while
  some join the old `b` row and some the new: the read contract, exact after
  `await_converged`, which the marker holds back. Readers don't wait for the
  marker. A chunk that dies commits nothing, its staged rows included, and
  the next resumes from the cursor the last commit left. A worker never
  queues for a `J` row: it skips a
  key another holds and takes the next marker, or does nothing. Queued behind
  another worker's chunk, it would put every `a` writer on the key behind one
  more chunk, and drain no segment while it waited.
- Every recompute reads `R` and each `M` under `J[i, k]` and its rows' `R`
  entries, so the result is exact
  whatever ran between two chunks: an `a` change writes its own row from the
  same state, and a row that joins the key behind the cursor, or leaves it,
  is written by its own page.
- A marker holds back every watermark token at or past its origin, as a
  parked row does, so a read after `await_converged` sees the fan-out done,
  and `self_check` re-checks a divergence of the target while one is pending.
  A row a chunk fails to write is held as a page's fan-out holds it, parked
  with the marker's origin and segment. A `TRUNCATE` of any table deletes
  the definition's markers at every position (a `TRUNCATE b` starts a
  rebuild, which recomputes every row whatever position its marker named,
  and a `TRUNCATE a` leaves no row to recompute), and so does `RESUME`, whose build
  recomputes every row.
- The index the chunks walk holds a position's `ON` columns and the entry
  key, so each `ON` of a 1-1 JOIN has at most 31 columns.
- **The fan-out never halts.** However many rows a key reaches, the page
  defers what doesn't fit its budget to chunks, so a position with few keys
  and many rows on each (a lookup table) is admitted at any position, with no
  threshold. Such a position turns one edit of its table into many target
  writes, and slows the root's apply while the chunks run; the
  [recommendations](../recommendations.md#join-small-often-edited-tables-at-query-time)
  weigh it against joining at query time.

The benchmark (`one-to-one-join-fan-out`, 1M `a` rows on one key, a `LEFT`
join) measures the page at 0.03 s and the whole fan-out at about 10.6 s, from
the `b` commit to the last chunk: about 10 µs per row, against 4.3 s in one
transaction, with the same 340 B of WAL per row. An `a` change on the key
lands in a median 0.17 s, against 4.4 s behind one transaction. An `INNER`
join's fan-out costs more, since each batch also deletes the target rows of
entries whose key has lost its last live `b` row.

- A `status()` warning names, per position, the position and the join key
  with the most `a` rows when it holds more than 250k, a key whose `b` change takes seconds to reach every
  row. The count is a sample of the position's index on `R`, not `pg_stats`, which
  is stale after a build.

### A row a page fails to write is held in the page

A field that fails with a data exception on a row a page writes is that `a`
row's failure, whichever write reaches the row: the write of the page's `a`
changes' rows, or a `b` change's fan-out. Each write runs under a savepoint.
A data exception rolls it back to the savepoint and bisects its rows under
savepoints down to the rows that fail, and the per-field probe names their
fields. The page holds each such row in quarantine in its own transaction, as
a key whose own change fails is held, and writes every other row, so the
page's other changes commit and `R` and `M` advance. An `a` change that fails
only against the same page's `b` change is held so too: probed alone against
the committed state, as the drain's isolation probes a record, neither change
fails, so isolation would charge nothing and the page would fail forever.

- The hold's `poison` row takes the definition row `FOR KEY SHARE` through
  its foreign key, so a page that may hold a row (one with `a` changes, or
  one that fans out) takes that lock before its first lock row, as a
  `TRUNCATE b` page takes the row `FOR UPDATE`: a rebuild's start holds it
  `FOR UPDATE` while it waits for the lock row. A writer that takes the
  definition row `FOR UPDATE` (a rebuild's start, a build step recording
  its failure) waits for such a page to commit.
- A row is held on its first failure, as a build chunk holds a key: the
  page reproduced it on the `R` and `M` it commits with. An `a` row is not
  left to the drain's isolation and its death threshold, which a plain 1-1
  transform's row keeps ([ADR-0003](0003-quarantine-storage-and-api.md)).
  Each failing field counts the row toward its column's fuse, and the held
  rows count toward the whole-transform fuse, both checked once the page has
  committed.
- The held row's parked Re-derive carries the earliest origin of the page's
  changes it holds back, so no watermark token past one of them converges
  while the row is held: the `b` changes for a fan-out's row; for an `a`
  change's row, its own changes and the page's `b` changes, since its target
  row reflects neither.
- A bisection that can't name every failing row within its attempts halts
  the transform rather than commit with a row unwritten.
- A held row is left out of later fan-outs and of the build, and its own
  changes are parked, so its target row stays as it was. A row held in its
  own change's page keeps that change in its `R` entry, and nothing writes
  the target from that entry until the release.
- Its release re-derives the row's `R` entry and target row from the live
  row in the release's transaction, under its `J` lock, before it deletes
  the hold, and stages a `Recompute` as well. A `b` change on its key that
  drained before the `Recompute` would otherwise read the `R` entry the hold
  left, and hold the row again.
- A pause of a field, by hand or by its fuse, and its resume bump every
  table's version fence, since the pages of any table write the fields.
  A 1-1 `JOIN` has no field build, so the resume rebuilds the target in
  place, which writes the field on the rows its pages left as they were.

### A build fills every `M`, then `R` and the target

A build re-derives every key of `b` ∪ `M` into `M` first, at every position: `b`'s rows in
batches of its primary key, those whose key has a `NULL` part (a `b` that is
an aggregate's target, keyed by a nullable group column) in batches of their
own, then `M`'s live entries whose `b` row is gone, which become tombstones.
Each batch is a page of `b` Re-derives with its fan-out left out: the chunks
that follow write every target row, none starts before the last batch
commits, and so none joins an `a` row to a `b` row that is gone. Every
position's `M` is filled before the first chunk of `R`. Each batch
commits with the build's cursor, so a batch that fails (a page holding a `J`
row longer than the batch waits) or a worker that dies resumes from the last
batch that committed. It then fills `R` and the target
in chunks of `a`'s primary-key range. Each chunk:

1. Holds the lock row `FOR SHARE`, and locks `J` at every position for the
   live keys in its range plus the keys `R` holds for its rows.
2. Inserts the missing entries in one statement that reads `a` live, with
   `pg_current_snapshot()` as their basis.
3. Locks and re-derives the remaining entries.
4. Writes each row's target row from `R` and the newest live entry of each
   `M` on its key at that position.
5. Retries if any read finds a key outside its locked set at any position.
   Without the retry, 378 of 1,716 schedules were wrong. A chunk that left
   out one position's keys would move a row's key at that position without
   its `J`, breaking (A).

A chunk's range is a row comparison, which is `NULL` wherever a `NULL` part
decides it, so no chunk reaches an `a` row whose primary key has one (an `a`
that is an aggregate's target). The plan job's last transaction stages a
Re-derive of each, and the page that drains it writes the row under its `NULL`
key like any other.

A chunk waits at most a second for a lock a page holds, then gives up and
retries, so a page is never queued behind a chunk that queues behind another
page. Pages whose segment predates the build re-derive. A build start takes
the lock row `FOR UPDATE` as it records that segment, so it waits for the
pages in flight. A start of several takes their lock rows in the order a page
does, by target name. A one-transaction build is not an option: at 100M `a`
rows it took 834 s, and a concurrent page waited 807 s behind it. In 10k-row
chunks on 4 workers the build took 446 s, and no page waited longer than
0.74 s (#1375 Y3).

A rebuild in place, and any build whose target has rows, ends with a sweep
after the last chunk. It re-derives every entry of `R` whose basis is null or
older than the build's start, so an entry whose row is gone becomes a
tombstone and its target row goes. Then its orphan pass walks
the target in its key order and re-derives, as an `a` Re-derive page, every
target row no live `R` entry backs. It never deletes one by statement: a row
re-inserted and applied between the read and the delete would be lost.

Each build transaction that writes the target (a chunk, a batch of either
sweep pass) stages its rows for the target's readers through the seam in
that transaction, with an unknown origin, as a 1-1 build's chunks do, and so
does a quarantine release's Re-derive. A `RESUME` and a `TRUNCATE` of `b`
reach the readers through the build they start; a `TRUNCATE` of `a` and the
Re-derives a producer stages, through the page that drains them.

### `self_check` compares the target, every copy and the uniqueness of each joined table

`self_check` reads each table as its copy has applied it, which is the source minus the ring rows the
copy hasn't applied yet, as the 1-1 and aggregate checks do (ADR-0013). It audits, in order:

- **The target**: each row against the full query, every `JOIN` of it: `a` as `R` has applied it,
  joined at every position to `b` as that position's `M` has applied it, on the newest live `M`
  entry of a key when there are several.
- **`R` and every `M`**: each against its source, in both directions: a row with no live entry or other
  values, and a live entry with no row.
- **A join key with more than one live entry in any `M`.** The target reads the newest entry of a key, which
  masks a duplicate, so this is where `b` losing its uniqueness shows while the transform runs. A
  hand-off of a key between two `b` rows applied out of order holds two entries
  for a moment as well, so what this check finds while `b`'s ring holds an unapplied row is
  re-checked.

It reports and never repairs.

### Pause and `RESUME`

`RESUME` recreates `R`, every `M`, `J` and the lock row at define's current
shape, after waiting out the chunks in flight, and keeps the target, so
readers see every row throughout. The build that follows re-derives every
joined table and every `a`
row that exists, and its sweep's orphan pass removes the target rows of `a`
rows deleted while the transform was paused. Until the sweep reaches them
those rows stay in the target, and the transform reads `backfilling`.

### A schema change of any table pauses the whole transform

A 1-1 `JOIN` takes an aggregate `JOIN`'s schema-change rules
([ADR-0019](0019-join-transforms.md)) on every table, the root and each
position's. The staging worker's
pass, or the drain on a write that marks a missing column, pauses the whole
transform with a `capture_failure` naming the table and the column when:

- a column the fields or an `ON` read from any table is dropped or renamed;
- a key column of any table (`a`'s primary key, an `ON` column) is
  retyped so the keys Trellis stored render differently, or one column of an
  `ON` pair is retyped so the pair no longer has one type;
- a column is retyped to a type the fields evaluate as another type
  (`text` to `integer`);
- a column Trellis keeps a typed copy of widens past the copy
  ([ADR-0020](0020-type-identity.md)). The copies are the target's key (`a`'s
  primary key), a field that names one column (typed as that column, with no
  modifier), a calculated field (typed by inference from the tables it reads), and the
  columns of `R`, each `M` and each position's group of `J` (typed as the
  table's column). The definition records each column it reads with its
  value type, so a field over several tables is reported by the pass of the table whose input changed value type
  (a widening, not a change of modifier), naming that table and column and
  every column the field is typed from with its type now; the other tables'
  passes leave it. Where no input changed value type, whichever pass runs
  first reports it. When the widened column is an upstream target's,
  re-typed by that upstream's `RESUME`, the pause records the upstream as its
  cause, so the transform resumes once the upstream is live.

Any of its tables dropped, renamed away, swapped or joining a hierarchy
pauses it too, as for any source. `RESUME` re-validates every table as define would
and refuses while define would. Otherwise the staging worker re-types the
target's drifted copies first, and the resume then creates `R`, `M` and `J`
again at the types define gives them now, so each position's group of `J` takes
its `ON` pairs' new types, and rebuilds as above.

## Consequences

- In a two-table 1-1 JOIN, an `a` change costs about 1.6–1.8x a plain 1-1
  change on one worker, and its hidden state is about 1.6x a plain 1-1
  transform's. It is cheaper than a `GROUP BY a.id` JOIN transform over the
  same tables on throughput, WAL, hidden state and build time.
- Parallel pages whose key sets overlap queue on `J`: in a two-table 1-1
  JOIN with 1,000-change pages over 100k keys, 8 workers gave 1.18x one
  worker's throughput. JOIN transforms share this.
- `J` keeps a row for every `(position, key)` any table has held, as in
  ADR-0019, and its rows are never deleted while the definition lives.
- **A star costs more per change as it grows.** An `a` change costs about
  1.61x, 2.22x and 2.92x a two-table change at `n` = 3, 4 and 5 (one worker,
  100-change pages), with a page of 100 changes locking
  `100 × (n − 1) + 50` `J` rows, and a fan-out row about 5.3, 6.7 and 7.9 µs
  against 3.9 µs (#1459's model). The hidden state grows by about 55 MB per
  position per 1M root rows and 100k keys at that position (an `R` key index
  plus that position's `M` and `J`). A star is cheaper than a chain of two-table
  JOINs over the same tables: at `n = 4`, 2.3x the chain's throughput and
  0.42x its hidden state.
