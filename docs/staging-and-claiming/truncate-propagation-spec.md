# TRUNCATE propagation

How a source `TRUNCATE` propagates through the staging ring to the derived
tables it feeds. This is the design; the machinery lives in `staging::apply`,
`staging::fold`, and `staging::seal`.

## Semantics

- A source `TRUNCATE` clears **all derived/target rows produced from each named
  source relation**, then post-truncate inserts re-populate normally.
- Postgres pre-expands `CASCADE` server-side: the single pgoutput TRUNCATE
  message's `relation_ids` list already enumerates every truncated table **that
  is in the publication**. We iterate that list; we do **not** implement cascade
  traversal. A cascaded child not in the publication is simply absent (and was
  never replicated).
- The `options` bits (bit0 CASCADE, bit1 RESTART IDENTITY) are informational; we
  do not act on them beyond clearing the listed relations.

## The ordering hazard

A TRUNCATE is **whole-keyspace**, but drains are **per-bucket, parallel, and out
of `seg_seq` order** (`next_claimable_segment` does not enforce order). So a
truncate is a **two-directional drain barrier**:

- **Predecessors must drain first.** Otherwise an earlier batch's insert applies
  *after* the truncate clears the target and wrongly survives.
- **Successors must not drain first.** Otherwise a later batch's post-truncate
  insert is wiped when the truncate batch clears the whole target.

Enforcement (a single-bucket truncate batch, extended to a barrier):

1. **Single bucket.** A batch containing any `op='truncate'` row seals with
   `bucket_count = 1` — one worker drains the whole batch, so the whole-keyspace
   `DELETE` + any same-batch post-truncate writes are one atomic Phase-3 txn.
2. **Barrier in `next_claimable_segment`.** `has_truncate` is recorded on the
   `segments` registry at seal time. Let `B` = min `seg_seq` among undrained
   truncate-bearing segments. A worker may be handed segment `s` only when
   `s <= B` (never a segment past an undrained truncate). Because the query
   returns the lowest undrained `s`, `B` itself is handed out only once every
   `s < B` has drained. This gives both directions with one clause.

Truncates are rare; fully serializing the drain around one is the correct
correctness/throughput trade.

## Per-key fold correctness

The fold telescopes per `(src_table, key)` ordered by `(lsn, change_id)`. A
truncate landing between two changes to a key voids the earlier one. So the fold
**voids image-bearing keyed changes at a position ≤ the src_table's max truncate
position** in the fenced window:

```sql
-- keep f unless a truncate for its src_table sits strictly above it
and not exists (
  select 1 from fenced t
  where t.op = 'truncate' and t.src_table = f.src_table
    and (t.lsn, t.change_id) > (f.lsn, f.change_id)
)
```

Key subtlety — **recompute rows (NULL `lsn`) are never filtered**: the
row-comparison against a NULL lsn is NULL (not true), so recompute rows survive
regardless of truncate position. That is correct: a recompute re-reads *live*
current source state, which already reflects the truncate, so its position is
irrelevant. Only image-bearing rows (real `lsn`) carry stale state and must be
voided below the truncate.

A truncate and inserts **in the same source transaction** share the commit
`lsn`; `change_id` (intake append order = execution order) breaks the tie, so
truncate-then-insert within one txn orders correctly.

## Invariants to preserve

- Immutable claimed batch; apply ∪ mark-drained is one commit (see
  [05](05-apply-and-exactly-once-deltas.md)).
- A row's bucket is a total function of row+batch — the single-bucket truncate
  rule must not put a row in zero or two buckets.
- The fold must not start filtering `op` — the truncate sentinel must survive
  the fold.
</content>
</invoke>
