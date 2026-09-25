# TRUNCATE propagation

How a source `TRUNCATE` propagates through the staging ring to the derived
tables it feeds. The machinery lives in `staging::apply`, `staging::fold`, and
`staging::seal`.

## Semantics

- A source `TRUNCATE` clears **all derived/target rows produced from each named
  source relation**; post-truncate inserts then re-populate normally.
- Postgres pre-expands `CASCADE` server-side: the pgoutput TRUNCATE message's
  `relation_ids` already lists every truncated table **in the publication**. We
  iterate that list; we do **not** traverse cascade ourselves. A cascaded child
  outside the publication was never replicated, so nothing to clear.
- The `options` bits (bit0 CASCADE, bit1 RESTART IDENTITY) are informational; we
  act only on the listed relations.

## The ordering hazard

A TRUNCATE is **whole-keyspace**, but drains are **per-bucket, parallel, and out
of `seg_seq` order** (`next_claimable_segment` does not enforce order). So a
truncate is a **two-directional drain barrier**:

- **Predecessors must drain first** — else an earlier insert applies *after* the
  truncate clears the target and wrongly survives.
- **Successors must not drain first** — else a later post-truncate insert is
  wiped when the truncate clears the whole target.

Enforcement:

1. **Single bucket.** A batch with any `op='truncate'` row seals with
   `bucket_count = 1`, so one worker drains the whole-keyspace `DELETE` + any
   same-batch post-truncate writes as one atomic Phase-3 txn.
2. **Barrier in `next_claimable_segment`.** `has_truncate` is recorded on the
   `segments` registry at seal. Let `B` = min `seg_seq` among undrained
   truncate-bearing segments; a worker may be handed segment `s` only when
   `s <= B`. Since the query returns the lowest undrained `s`, `B` is handed out
   only after every `s < B` drains — both directions from one clause.

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

A truncate and inserts **in the same source transaction** share the commit
`lsn`; `change_id` (intake append order = execution order) breaks the tie, so
truncate-then-insert orders correctly.

## Invariants to preserve

- Immutable claimed batch; apply ∪ mark-drained is one commit (see
  [05](05-apply-and-exactly-once-deltas.md)).
- A row's bucket is a total function of row+batch — the single-bucket truncate
  rule must not put a row in zero or two buckets.
- The fold must not filter `op` — the truncate sentinel must survive the fold.
