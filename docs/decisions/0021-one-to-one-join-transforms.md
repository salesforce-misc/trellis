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

[OPEN: an entry's `basis` snapshot and the Re-derive producers (a build chunk,
a rebuild, `request_backfill`, a quarantine release, catch-up), and which
`J` locks a Re-derive of each side takes before its live read. Measured in
#1375 Y1. Recommendation: the ledger's full I2 on both `R` and `M`, with each
side's Re-derive taking `J` before its read.]

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

[OPEN: a page carrying changes of both tables takes one `J` lock statement
over the union of both sides' key sets. `TRUNCATE` of either side follows
ADR-0019's barrier. Measured in #1375 Y2.]

### A `b` change's fan-out is linear in the `a` rows on its key

A `b` change rewrites every target row on its key while holding `J[k]`: about
2.7 µs and 318 B of WAL per row in the model, before the target-mutation
seam's images. An `a` change on that key waits for the whole fan-out. Fan-out
in chunks that each take `J[k]` again was exact.

[OPEN: v1 is either one transaction with a `status()` warning on `a` rows per
key, or chunked fan-out with a durable pending-recompute marker per key.
Measured in #1375 Y4 and Y5.]

### A build fills `M`, then `R` and the target

[OPEN: chunked by `a` key under live pages, each chunk's entries taking its
read's snapshot as their basis, as a 1-1 Re-derive build does. Measured in
#1375 Y3. In the model, a one-transaction build of 10M `a` rows took 24.7 s
against 9.7 s for a plain 1-1 transform.]

## Consequences

- An `a` change costs about 1.6–1.8x a plain 1-1 change on one worker, and its
  hidden state is about 1.6x a plain 1-1 transform's. It is cheaper than a
  `GROUP BY a.id` JOIN transform over the same tables on throughput, WAL,
  hidden state and build time.
- Parallel pages whose key sets overlap queue on `J`: with 1,000-change pages
  over 100k keys, 8 workers gave 1.18x one worker's throughput. JOIN
  transforms share this.
- `J` keeps a row for every join key either table has held, as in ADR-0019.
