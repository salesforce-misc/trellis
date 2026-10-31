---
status: draft
date: 2026-10-24
deciders: Michael Ries
---

# 1-1 JOIN Transforms

Some queries join two tables without changing the grain. They bring columns of
both into one row per row of the first, often to compute a value from both:

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
FROM <a> [INNER | LEFT] JOIN <b> ON <a>.<col> = <b>.<col> [AND ...]
SELECT <expressions over a's row and the matched b row>
```

It settles their scope, the hidden state behind them, and the locks and
ordering rules that keep them exact. It builds on
[ADR-0002](0002-async-data-flow.md)'s 1-1 ledger, which orders every per-row
write, and takes ADR-0019's side, qualification, join-kind and `ON` rules and
its `J` lock. Three or more tables, a non-unique `b` key and `WHERE` are out of
scope.

## Decisions

### A 1-1 JOIN keeps one row per row of `a`, joined to at most one row of `b`

- **No `GROUP BY`.** The target is keyed by `a`'s primary key. `INNER` drops an
  `a` row with no match; `LEFT` keeps it with `NULL` `b` columns.
- **`b`'s `ON` columns are unique**: `b`'s primary key, a unique constraint,
  or the group key of an upstream aggregate or JOIN target. A non-unique `b`
  would give one row per pair, keyed by both sides, which is a different
  target. `RESUME` refuses a `b` that has lost that uniqueness since define;
  nothing checks it while the transform runs
  ([known correctness gaps](../known-correctness-gaps.md#bs-unique-key-dropped-under-a-1-1-join)).
- **Fields** are any expression the 1-1 path admits, over `a`'s row and the
  matched `b` row, of any type a 1-1 field admits (floats and text included).
  A row is recomputed from its current inputs and never maintained by deltas,
  so none of the aggregate restrictions apply.
- Sides, qualification, join kinds and `ON` follow ADR-0019.

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
  other, so it starts no rebuild, and its fan-out over every `a` row those
  rows match counts toward the cap below.

### Each side keeps a Trellis-owned copy, ordered by ADR-0002's 1-1 ledger

- **`R`**, one entry per `a` row: the 1-1 ledger's ordering state, plus the
  row's join key and the `a` columns the fields read. It is filled from
  captured images and indexed on the join key, which is how a `b` change finds
  its rows. It is about the size of `a`.
- **`M`**, one entry per `b` row, keyed by `b`'s primary key and indexed on the
  join key: the ledger's ordering state plus the `b` columns the fields read.
  It is about the size of `b`. Keyed by `b`'s primary key rather than by join
  key, it applies a key's hand-off from one `b` row to another in either
  order. For the moment the two are applied out of order, a key can hold two
  live entries, and a read takes the newest.
- **`J`**, one lock row per join key, and the pair's truncate lock row, both
  as in ADR-0019.

`R` and `M` entries take the 1-1 ledger's lock pattern: a placeholder insert,
then one sorted `FOR UPDATE`, then the applied-position check. Without the
placeholder, an older page waiting on a newer page's uncommitted insert of the
same entry overwrote it. Without the position check, an out-of-order page put
an older state over a newer one.

Both `R` and `M` take ADR-0002's full I2: an Apply runs only if its txid is
not in the entry's basis, its position is past the entry's applied position,
and it is above its side's truncate floor. Each check alone was wrong in
some interleaving (#1375). A Re-derive of either side is what every producer
stages (a build chunk, a rebuild, `request_backfill`, a quarantine release,
catch-up, and a page whose segment predates the build):

- Before its `J` lock, the page reads the row's live join key, unlocked, and
  adds it to its key set alongside the key the entry holds.
- After its entry lock, one statement reads the live row and
  `pg_current_snapshot()`, which becomes the entry's basis. A snapshot taken a
  statement later was wrong in 22 families.
- If that read finds a key outside the locked set (the row moved between the
  two reads), the page rolls back and retries. Not retrying was wrong in
  910 of 1,716 schedules for each side. Taking the late key's `J` after the
  read was exact but deadlocked, because it breaks lock sorting.
- It reads `M` only after its `J` lock.

### Neither side reads the other's source, only its copy under `J`

- **An `a` change** locks `J` for its new key and the key `R` holds, sorted,
  insert-then-lock. It then locks its `R` entries and re-checks that each one's
  key is in the locked set, rolling back and retrying if a key moved. It reads
  `M` only after the `J` lock, and writes `R` and the target row in one
  transaction.
- **A `b` change** locks `J` for its new key, its old key from the image, and
  the key `M` holds. It then locks and re-checks its `M` entries, applies them,
  and recomputes the target row of every `R` entry on those keys.
- Both sides take `J` `FOR UPDATE`. `FOR SHARE` on the `a` side is also exact,
  but it starved the `b` side: 24 `b` changes in 20 s against 1,925.

The race that defeats live reads of a second table can't arise: every writer
of either side holds `J[k]` exclusively while it reads the other side's copy
and writes. In a model on PG 16.8, 17.10 and 18.2, 8,064 deterministic
interleavings per version and 240 randomized soak runs were exact. Each
necessary rule was shown necessary by an ablation that removes it (#1371).

A page carrying changes and Re-derives of both tables takes one `J` lock
statement over the union of both sides' key sets. Its lock order is the
truncate lock row, `J`, then `R` entries, then `M` entries, then target rows,
each sorted. A truncate page holds the lock row `FOR UPDATE`, empties its
side's hidden table and raises its floor. It empties the table with a
`DELETE`: a build step reads `R` or `M` before it takes the lock row, so a
`TRUNCATE`, whose lock waits for that read, would deadlock with it.
ADR-0019's barrier orders the page against other pages; without the
barrier, four of eight truncate families were wrong. With both rules, 60
families × 2 join kinds (102,312 schedules per version on PG 16.8, 17.10 and
18.2) and 48 soak runs, with builds starting mid-run, were exact (#1375).

A page settles its truncate before its changes, so a change after the truncate
in the same batch applies to the emptied side:

- **`TRUNCATE a`** empties `R` and raises its floor, and deletes every target
  row in the page. `M` and `J` are untouched.
- **`TRUNCATE b`** empties `M` and raises its floor. It changes every target
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
- A page that truncates both sides does both.

A change of either side that was staged before a truncate and reaches a page
after it is below the side's floor and refused, by the same I2 check as a
change older than an entry's applied position.

### A `b` change's fan-out is linear in the `a` rows on its key

A `b` change rewrites every target row on its key while holding `J[k]`: about
2.7 µs and 318 B of WAL per row in the model, before the target-mutation
seam's images. An `a` change on that key waits for the whole fan-out. Fan-out
in chunks that each take `J[k]` again was exact.

In v1, fan-out runs in one transaction. It is about 3–4 s per million rows
with the seam, and that key's `a` writers wait for all of it. Three limits
keep it bounded:

- A `status()` warning names the join key with the most `a` rows when it holds more than 100k. The
  count is a sample of `R`'s join-key index, not `pg_stats`, which is stale after a build. The 100k is a
  placeholder knob that the benchmark of the SQL fan-out sets.
- A hard cap halts the transform rather than fan out past a configured row
  count.
- No transform may read a 1-1 JOIN target. The seam holds every changed key's
  prior image in memory until commit: about 300 MB per million rows with any
  reader, and more on the image feed.

Chunked fan-out with a durable per-key pending marker lifts these limits
later. In the model (#1375 Y5) it was exact under worker kills and a second
`b` change mid-way, and it bounded an `a` writer's wait to one chunk: 0.13 s
against 3.8 s. It took about 5x as long overall.

### A row a fan-out fails on is held, not the `b` change

A field that fails with a data exception on one row of a fan-out is that
`a` row's failure. The fan-out's batches are bisected under savepoints down
to the rows that fail, and the per-field probe names their fields. The page
holds each such row in quarantine in its own transaction, as a key whose own
change fails is held, and writes every other row, so the `b` change commits
and `M` advances.

- The hold's `poison` row takes the definition row `FOR KEY SHARE` through
  its foreign key, so a page that fans out takes that lock before its first
  lock row, as a `TRUNCATE b` page takes the row `FOR UPDATE`: a rebuild's
  start holds it `FOR UPDATE` while it waits for the lock row.
- A row is held on its first failure, as a build chunk holds a key: the
  page reproduced it on the `R` and `M` it commits with. Each failing field
  counts the row toward its column's fuse, and the held rows count toward
  the whole-transform fuse, both checked once the page has committed.
- A bisection that can't name every failing row within its attempts halts
  the transform rather than commit with a row unwritten.
- A held row is left out of later fan-outs and of the build, and its own
  changes are parked, so its target row stays as it was.
- Its release re-derives the row in the release's transaction, under its
  `J` lock, before it deletes the hold, and stages a `Recompute` as well. A
  `b` change on its key that drained before the `Recompute` would otherwise
  read the `R` entry the hold left, and hold the row again.
- A pause of a field, by hand or by its fuse, and its resume bump both
  tables' version fences, since the pages of either side write the fields.
  A 1-1 `JOIN` has no field build, so the resume rebuilds the target in
  place, which writes the field on the rows its pages left as they were.

### A build fills `M`, then `R` and the target

A build re-derives every key of `b` ∪ `M` into `M` first: `b`'s rows in
batches of its primary key, those whose key has a `NULL` part (a `b` that is
an aggregate's target, keyed by a nullable group column) in batches of their
own, then `M`'s live entries whose `b` row is gone, which become tombstones.
Each batch is a page of `b` Re-derives with its fan-out left out: the chunks
that follow write every target row, none starts before the last batch
commits, and so none joins an `a` row to a `b` row that is gone. Each batch
commits with the build's cursor, so a batch that fails (a page holding a `J`
row longer than the batch waits) or a worker that dies resumes from the last
batch that committed. It then fills `R` and the target
in chunks of `a`'s primary-key range. Each chunk:

1. Holds the lock row `FOR SHARE`, and locks `J` for the live keys in its
   range plus the keys `R` holds for its rows.
2. Inserts the missing entries in one statement that reads `a` live, with
   `pg_current_snapshot()` as their basis.
3. Locks and re-derives the remaining entries.
4. Writes each row's target row from `R` and the newest live `M` entry on its
   key.
5. Retries if any read finds a key outside its locked set. Without the retry,
   378 of 1,716 schedules were wrong.

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

### `self_check` compares the target, both copies and the uniqueness of `b`

`self_check` reads each side as its copy has applied it, which is the source minus the ring rows the
copy hasn't applied yet, as the 1-1 and aggregate checks do (ADR-0013). It audits, in order:

- **The target**: each row against `a` as `R` has applied it, joined to `b` as `M` has applied it,
  on the newest live `M` entry of a key when there are several.
- **`R` and `M`**: each against its source, in both directions: a row with no live entry or other
  values, and a live entry with no row.
- **A join key with more than one live `M` entry.** The target reads the newest entry of a key, which
  masks a duplicate, so this is where `b` losing its uniqueness shows while the transform runs. A
  hand-off of a key between two `b` rows applied out of order holds two entries
  for a moment as well, so what this check finds while `b`'s ring holds an unapplied row is
  re-checked.

It reports and never repairs.

### Pause and `RESUME`

`RESUME` recreates `R`, `M`, `J` and the lock row at define's current shape,
after waiting out the chunks in flight, and keeps the target, so readers see
every row throughout. The build that follows re-derives `b` and every `a`
row that exists, and its sweep's orphan pass removes the target rows of `a`
rows deleted while the transform was paused. Until the sweep reaches them
those rows stay in the target, and the transform reads `backfilling`.

### A schema change of either table pauses the whole transform

A 1-1 `JOIN` takes an aggregate `JOIN`'s schema-change rules
([ADR-0019](0019-join-transforms.md)) on both tables. The staging worker's
pass, or the drain on a write that marks a missing column, pauses the whole
transform with a `capture_failure` naming the table and the column when:

- a column the fields or `ON` read from either table is dropped or renamed;
- a key column of either table (`a`'s primary key, an `ON` column) is
  retyped so the keys Trellis stored render differently, or one column of an
  `ON` pair is retyped so the pair no longer has one type;
- a column is retyped to a type the fields evaluate as another type
  (`text` to `integer`);
- a column Trellis keeps a typed copy of widens past the copy
  ([ADR-0020](0020-type-identity.md)). The copies are the target's key (`a`'s
  primary key), a field that names one column (typed as that column, with no
  modifier), a calculated field (typed by inference from both tables), and the
  columns of `R`, `M` and `J` (typed as the side's column). Nothing records an
  expression's input types, so a field over both tables is reported by the
  pass of whichever table finds it first, naming every column it is typed
  from with its type now. When the widened column is an upstream target's,
  re-typed by that upstream's `RESUME`, and the other table's pass finds the
  field first, the pause records no upstream cause, so the transform waits
  for its own `RESUME` rather than resuming after the upstream
  ([#1539](https://github.com/salesforce-misc/trellis/issues/1539)).

Either table dropped, renamed away, swapped or joining a hierarchy pauses it
too, as for any source. `RESUME` re-validates both tables as define would
and refuses while define would. Otherwise the staging worker re-types the
target's drifted copies first, and the resume then creates `R`, `M` and `J`
again at the types define gives them now, so `J` takes an `ON` pair's new
type, and rebuilds as above.

## Consequences

- An `a` change costs about 1.6–1.8x a plain 1-1 change on one worker, and its
  hidden state is about 1.6x a plain 1-1 transform's. It is cheaper than a
  `GROUP BY a.id` JOIN transform over the same tables on throughput, WAL,
  hidden state and build time.
- Parallel pages whose key sets overlap queue on `J`: with 1,000-change pages
  over 100k keys, 8 workers gave 1.18x one worker's throughput. JOIN
  transforms share this.
- `J` keeps a row for every join key either table has held, as in ADR-0019.
