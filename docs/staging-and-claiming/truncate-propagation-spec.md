# TRUNCATE propagation

How a source `TRUNCATE` propagates through the staging ring to the derived
tables it feeds. The machinery lives in `staging::apply`, `staging::fold`, and
`staging::seal`.

## Semantics

- A source `TRUNCATE` clears **all derived/target rows produced from each named
  source relation**; post-truncate inserts then re-populate normally.
- Every captured table carries its own `AFTER TRUNCATE` capture trigger, and
  Postgres fires it for every table a `TRUNCATE` empties, `CASCADE` children
  included. So each truncated table that is captured stages its own truncate
  row; we do **not** traverse cascade ourselves. A cascaded child nothing
  captures has nothing to clear.

## The ordering hazard

A TRUNCATE is **whole-keyspace**, but drains are **per-bucket, parallel, and out
of `seg_seq` order** (`next_claimable_segment` does not enforce order). So a
truncate is a **two-directional drain barrier**:

- **Predecessors must drain first** — else an earlier insert applies *after* the
  truncate clears the target and wrongly survives.
- **Successors must not drain first** — else a later post-truncate insert is
  wiped when the truncate clears the whole target.

Enforcement:

1. **Single bucket.** A batch whose fenced window holds any `op='truncate'`
   row seals with `bucket_count = 1`, so one worker drains the whole-keyspace
   `DELETE` + any same-batch post-truncate writes. A batch over the drain cap
   pages, but the sentinel sorts first and the bucket's cursor orders the pages,
   so the clear still lands first.
2. **Barrier in `next_claimable_segment`.** `has_truncate` is recorded on the
   `segments` registry at seal. Let `B` = min `seg_seq` among undrained
   truncate-bearing segments; a worker may be handed segment `s` only when
   `s <= B`. Since the query returns the lowest undrained `s`, `B` is handed out
   only after every `s < B` drains — both directions from one clause.

**Both flags are decided over the fenced window, when the fence is published**
([03](03-sealing-and-the-fence.md), "The batch is sized when its fence is
published"). A truncate row belongs to whichever batch's fenced window holds it,
and that can be a batch the flip never saw it in:

- a writer that resolved slot *k* before the flip and commits its truncate after
  the flip but before `S_k` puts it in batch *k*;
- a writer still open at `S_k` puts it in batch *k+1*, through the predecessor
  half of the both-slots read.

So `has_truncate` and `bucket_count` are computed over both halves of the window,
in the same statement that publishes `S_k`, and `next_claimable_segment` only
hands out fenced segments. Deciding them from the slot at the flip would let a
straddling truncate escape the barrier.

Truncates are rare; fully serializing the drain around one is the right
correctness/throughput trade.

## Per-key fold correctness

The fold telescopes per `(src_table, key)` ordered by `(lsn, change_id)`. A
truncate between two changes to a key voids the earlier one, so the fold **voids
image-bearing keyed changes at a position ≤ the src_table's max truncate
position** in the fenced window:

```sql
truncates as materialized (
  select src_table, lsn, change_id from fenced where op = 'truncate'
)
-- ...
-- keep f unless a truncate for its src_table sits strictly above it
and not exists (
  select 1 from truncates t
  where t.src_table = f.src_table
    and (t.lsn, t.change_id) > (f.lsn, f.change_id)
)
```

The anti-join reads a materialized set of just the truncate rows, not `fenced`
itself. A `not exists` straight over `fenced` can plan as a nested loop that
rescans the whole window once per row, which makes the fold quadratic in batch
size (#492).

**Recompute rows (NULL `lsn`) are never filtered** — the comparison against a
NULL lsn is NULL, not true, so they survive regardless of truncate position.
That is correct: a recompute re-reads *live* source state, which already
reflects the truncate. Only image-bearing rows carry stale state and must be
voided below the truncate.

A truncate and inserts **in the same source transaction** stage in execution
order: each statement's capture trigger runs after the previous statement's, so
its rows carry a later `change_id` and an `lsn` no earlier than the previous
statement's, and truncate-then-insert orders correctly.

## Invariants to preserve

- Immutable claimed batch; apply ∪ mark-drained is one commit (see
  [05](05-apply-and-exactly-once-deltas.md)).
- A row's bucket is a total function of row+batch — the single-bucket truncate
  rule must not put a row in zero or two buckets.
- The fold must not filter `op` — the truncate sentinel must survive the fold.
