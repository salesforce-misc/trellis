# Implementation checklist

← [Convergence and await](07-convergence-and-await.md) · [Overview](README.md)

A condensed checklist. Each item links to the document that argues for it.

## What is essential vs. what is Postgres-specific

| Essential to the guarantees | Postgres-specific |
|---|---|
| Stage and watermark commit atomically; acknowledge the cursor only afterwards | logical replication, `pgoutput`, standby status updates |
| The staging area is append-only on the hot path | a *ring* of N tables, `TRUNCATE` as the retirement primitive |
| Batches are cut in visibility space, with a published, write-once fence | `txid_current()` defaults and `txid_snapshot` columns |
| A batch is immutable once sealed; every producer writes to the active batch | four producers, two of them SQL-side |
| Claim exclusivity comes from a unique constraint, not a lock | bucket count, share formula, `hashtext` routing |
| Apply ∪ mark-complete are one transaction | the bit-mask coverage of buckets |
| Merging happens at read time, under four fixed rules | ordered `array_agg` arg-extremes |
| Pending work is measured by asking storage, never a boundary aggregate | index + hand-written `ANALYZE` on statistics-free tables |
| Quarantined work still blocks the read-your-writes predicate | the three-table poison track |

## Foundations to lock down first

- [ ] **The source delivers committed changes in commit order, with an
      explicitly-acknowledged monotonic cursor** — Postgres logical replication.
      → [01](01-intake-and-lsn-confirmation.md)
- [ ] **The staging tables and the watermark share a transaction** — both live in
      the same Postgres database. Non-negotiable.
      → [01](01-intake-and-lsn-confirmation.md)
- [ ] **The staging tables and the *target* tables share a transaction** — same
      database — what makes exactly-once for non-idempotent effects possible.
      → [05](05-apply-and-exactly-once-deltas.md)
- [ ] **The fence is a store-assigned per-row writer identity (`txid_current()`)
      plus an inspectable transaction snapshot.**
      → [03](03-sealing-and-the-fence.md)
- [ ] **Enumerate which measures are invertible.** Everything else goes on a
      recompute path. Never approximate an inverse.
      → [05](05-apply-and-exactly-once-deltas.md)
- [ ] **An undrainable item stops the whole instance, on purpose** — write the
      metric at the same time, so "stopped" and "slow" stay distinguishable.
      → [06](06-cleanup-and-reclaim.md)

## Build order

1. **Staging table + blind append + the atomic stage/watermark/acknowledge
   sequence.** Prove no-loss under kill -9 at each of the three points before
   moving on. → [01](01-intake-and-lsn-confirmation.md), [02](02-the-staging-ring.md)
2. **Seal with a single bucket, and the fence.** One worker, no parallelism. Get
   the straddler and phase-gap tests green here — much harder to debug once
   buckets exist. → [03](03-sealing-and-the-fence.md)
3. **The fold.** Four rules plus the image-bearing discriminator. Test it against
   a from-scratch oracle. → [04](04-claiming-and-the-fold.md)
4. **Apply ∪ mark in one transaction**, still single-bucket. → [05](05-apply-and-exactly-once-deltas.md)
5. **The convergence predicate**, before parallelism — you need it to write every
   subsequent test. → [07](07-convergence-and-await.md)
6. **Retirement and reclaim** — now the system can run indefinitely. → [06](06-cleanup-and-reclaim.md)
7. **Buckets and multi-worker claims.** → [04](04-claiming-and-the-fold.md)
8. **Heartbeat, reclaim TTL, release-on-error.** → [04](04-claiming-and-the-fold.md)
9. **Quarantine.** Last, because it is the only part that can hide work if it is
   wrong. → [06](06-cleanup-and-reclaim.md)

## Tests that are not optional

These fail only if you write them deliberately; ordinary end-to-end tests pass against every one of these bugs.

- [ ] **The straddler.** Hold a writer's transaction open across a seal; assert its
      row is applied exactly once.
- [ ] **The phase-gap writer.** Resolve the pointer in the window between the
      flip's commit and the snapshot capture; assert exactly once.
- [ ] **The high-xid writer.** A writer that is the highest live transaction id
      when the snapshot is captured — the `xmax` case in
      [03](03-sealing-and-the-fence.md).
- [ ] **Buckets partition the batch exactly once** — assert the union of
      per-bucket folds equals the whole-batch fold, as a set identity.
- [ ] **Claim lost mid-drain.** Reclaim a worker's claim while it computes; assert
      its apply rolls back entirely.
- [ ] **A key born inside a batch** (insert-then-update) folds to a NULL old side,
      and an image-less trigger row never wins either arg-extreme.
- [ ] **Delta vs. oracle, byte-identical**, after every op and under randomized
      drain interleavings.
- [ ] **Retire-then-seal on the same slot.** Assert a re-seeded slot is never
      truncated by a stale reclaimer.
- [ ] **Quarantine release preserves the origin position**, so the predicate never
      reports converged across a parked key.
- [ ] **Crash in the seal's phase gap** wedges, and the age-gated recovery unwedges
      it without stomping a healthy in-flight seal.

## Anti-patterns to name in review

Each is a regression to something this design deliberately avoids:

- Merging staged rows at write time "to save the fold" — reintroduces mutable
  claimed batches, hence compare-and-delete and survivor rewriting.
- Locking the pointer so writers cannot read it stale — trades a read-time fence
  for per-append coordination.
- Recomputing the routing key at read time instead of storing it — unsound across
  any hash change.
- Sizing the bucket split from the live worker registry — samples a value that
  moves under a safety-critical partition.
- Narrowing the pending predicate to something cheaper — admits a false
  `converged` ([07](07-convergence-and-await.md)).
- Marking work complete in a separate transaction from the work — the exact thing
  this design exists to prevent.
- Quarantining an error that names a schema defect — converts a loud, fixable
  error into permanently hidden work.
- An unreferenced data-modifying or `FOR UPDATE` CTE — Postgres may plan it away,
  and it will lock nothing while looking like it does.
