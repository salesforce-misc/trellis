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
  target. Losing that uniqueness after define pauses the transform.
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
each sorted. A truncate page holds the lock row `FOR UPDATE`, deletes its
side's hidden table and raises its floor. ADR-0019's barrier orders it
against other pages; without the barrier, four of eight truncate families
were wrong. With both rules, 60 families × 2 join kinds (102,312 schedules per
version on PG 16.8, 17.10 and 18.2) and 48 soak runs, with builds starting
mid-run, were exact (#1375).

`TRUNCATE b` changes every target row, so it starts a target rebuild rather
than recomputing in the page.

### A `b` change's fan-out is linear in the `a` rows on its key

A `b` change rewrites every target row on its key while holding `J[k]`: about
2.7 µs and 318 B of WAL per row in the model, before the target-mutation
seam's images. An `a` change on that key waits for the whole fan-out. Fan-out
in chunks that each take `J[k]` again was exact.

In v1, fan-out runs in one transaction. It is about 3–4 s per million rows
with the seam, and that key's `a` writers wait for all of it. Three limits
keep it bounded:

- A `status()` warning names keys with more than 100k `a` rows.
- A hard cap halts the transform rather than fan out past a configured row
  count.
- No transform may read a 1-1 JOIN target. The seam holds every changed key's
  prior image in memory until commit: about 300 MB per million rows with any
  reader, and more on the image feed.

Chunked fan-out with a durable per-key pending marker lifts these limits
later. In the model (#1375 Y5) it was exact under worker kills and a second
`b` change mid-way, and it bounded an `a` writer's wait to one chunk: 0.13 s
against 3.8 s. It took about 5x as long overall.

### A build fills `M`, then `R` and the target

A build re-derives every `b` row into `M` first, in batches of `b`'s primary
key. Each batch is a page of `b` Re-derives with its fan-out left out: the
chunks that follow write every target row, and none starts before the last
batch commits. It then fills `R` and the target in chunks of `a`'s
primary-key range. Each chunk:

1. Holds the lock row `FOR SHARE`, and locks `J` for the live keys in its
   range plus the keys `R` holds for its rows.
2. Inserts the missing entries in one statement that reads `a` live, with
   `pg_current_snapshot()` as their basis.
3. Locks and re-derives the remaining entries.
4. Writes each row's target row from `R` and the newest live `M` entry on its
   key.
5. Retries if any read finds a key outside its locked set. Without the retry,
   378 of 1,716 schedules were wrong.

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
after the last chunk. It re-derives every entry of `R`, then of `M`, whose
basis is null or older than the build's start, so an entry whose row is gone
becomes a tombstone and its rows are recomputed. Then its orphan pass walks
the target in its key order and re-derives, as an `a` Re-derive page, every
target row no live `R` entry backs. It never deletes one by statement: a row
re-inserted and applied between the read and the delete would be lost.

### Pause and `RESUME`

`RESUME` recreates `R`, `M`, `J` and the lock row at define's current shape,
after waiting out the chunks in flight, and keeps the target, so readers see
every row throughout. The build that follows re-derives `b` and every `a`
row that exists, and its sweep's orphan pass removes the target rows of `a`
rows deleted while the transform was paused. Until the sweep reaches them
those rows stay in the target, and the transform reads `backfilling`.

## Consequences

- An `a` change costs about 1.6–1.8x a plain 1-1 change on one worker, and its
  hidden state is about 1.6x a plain 1-1 transform's. It is cheaper than a
  `GROUP BY a.id` JOIN transform over the same tables on throughput, WAL,
  hidden state and build time.
- Parallel pages whose key sets overlap queue on `J`: with 1,000-change pages
  over 100k keys, 8 workers gave 1.18x one worker's throughput. JOIN
  transforms share this.
- `J` keeps a row for every join key either table has held, as in ADR-0019.
