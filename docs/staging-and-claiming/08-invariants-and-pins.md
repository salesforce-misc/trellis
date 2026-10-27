# Invariants and the tests that pin them

← [Convergence and await](07-convergence-and-await.md) · [Overview](README.md)

A condensed summary of what the design guarantees and the tests that pin each
guarantee. Each item links to the document that argues for it.

## What is essential vs. what is Postgres-specific

| Essential to the guarantees | Postgres-specific |
|---|---|
| A change is staged in the transaction that makes it | `AFTER … FOR EACH STATEMENT` triggers with transition tables |
| The staging area is append-only on the hot path | a *ring* of N tables, `TRUNCATE` as the retirement primitive |
| Batches are cut in visibility space, with a published, write-once fence | `txid_current()` defaults and `txid_snapshot` columns |
| A batch is immutable once sealed; every producer writes to the active batch | four producers, two of them SQL-side |
| Claim exclusivity comes from a unique constraint, not a lock | bucket count, share formula, `hashtext` routing |
| Apply ∪ mark-complete are one transaction | the bit-mask coverage of buckets |
| Merging happens at read time, under four fixed rules | ordered `array_agg` arg-extremes |
| Pending work is measured by asking storage, never a boundary aggregate | index + hand-written `ANALYZE` on statistics-free tables |
| Quarantined work still blocks the read-your-writes predicate | the three-table poison track |
| A non-commutative write commits only if its basis is current (I2), under a per-key lock | the `<target>__ledger` table (1-1) or the group's basis (aggregate), `pg_current_snapshot()` as the basis |

## Foundations

- **The staging tables and the source tables share a transaction** — capture
      triggers stage each change in the writer's own transaction, so a change is
      staged exactly when it commits. Same Postgres database. Non-negotiable.
      → [01](01-capture-by-triggers.md)
- **The staging tables and the *target* tables share a transaction** — same
      database — what makes exactly-once for non-idempotent effects possible.
      → [05](05-apply-and-exactly-once-deltas.md)
- **The fence is a store-assigned per-row writer identity (`txid_current()`)
      plus an inspectable transaction snapshot.**
      → [03](03-sealing-and-the-fence.md)
- **Enumerate which measures are invertible.** Everything else goes on a
      recompute path. Never approximate an inverse.
      → [05](05-apply-and-exactly-once-deltas.md)
- **Every 1-1 target keeps a per-key ledger** (last-applied position,
      basis snapshot, tombstone) and every non-commutative write checks it
      under the key's entry lock; an aggregate group keeps a basis and a
      stamp, and a page adds a change only if its basis hasn't counted it.
      → [05](05-apply-and-exactly-once-deltas.md#the-ledger)
- **No item may stay undrainable** — one would stop the whole instance. A
      schema error pauses the definitions it reaches and drains without them,
      and `status()` names the halt (`capture_failure` of kind `halt`), so
      "halted" and "slow" stay distinguishable.
      → [05](05-apply-and-exactly-once-deltas.md#failure-classification),
      [06](06-cleanup-and-reclaim.md)

## Tests that pin the guarantees

These fail only if written deliberately; ordinary end-to-end tests pass against every one of these bugs.

- **The straddler.** Hold a writer's transaction open across a seal; assert its
      row is applied exactly once.
- **The phase-gap writer.** Resolve the pointer in the window between the
      flip's commit and the snapshot capture; assert exactly once.
- **The high-xid writer.** A writer that is the highest live transaction id
      when the snapshot is captured — the `xmax` case in
      [03](03-sealing-and-the-fence.md).
- **Buckets partition the batch exactly once** — assert the union of
      per-bucket folds equals the whole-batch fold, as a set identity.
- **Claim lost mid-drain.** Reclaim a worker's claim while it computes; assert
      its apply rolls back entirely.
- **A key born inside a batch** (insert-then-update) folds to a NULL old side,
      and an image-less trigger row never wins either arg-extreme.
- **Delta vs. oracle, byte-identical**, after every op and under randomized
      drain interleavings.
- **Retire-then-seal on the same slot.** Assert a re-seeded slot is never
      truncated by a stale reclaimer.
- **Quarantine release preserves the origin position**, so the predicate never
      reports converged across a parked key.
- **Crash in the seal's phase gap** wedges, and the age-gated recovery unwedges
      it without stomping a healthy in-flight seal.
- **Two batches for one key, applied in reverse order.** Compute an older
      batch, drain a newer one for the same key, then apply the older one; assert
      a 1-1 target ends on the newer value. Cover a live recompute read, an older
      image, a stale write after a delete, and a stale delete after a re-insert.
- **I2 visibility.** A Re-derive whose snapshot saw a change refuses the
      older capture of that change and still applies a change it did not see;
      a Re-derive's tombstone outlives the changes its read saw.
      (`one_to_one_ledger_interleavings.rs`:
      `a_rederives_tombstone_outlives_the_changes_its_read_saw_*`)
- **Ledger GC holds back for out-of-order drains**: a tombstone is collected
      only at or below the contiguous drained prefix.
      (`one_to_one_ledger_interleavings.rs`: `an_out_of_order_drain_holds_gc_back_*`)
- **A Re-derive build interleaved with live Apply pages** converges to a
      from-scratch oracle after the last chunk and drain.
      (`rederive_build.rs`, `one_to_one_build_interleavings.rs`)
- **A schema change never fails the application's write**: a renamed or
      dropped captured column writes a `schema_changed` marker, the drain
      pauses the definitions that read it, and the others keep applying.
      (`capture_schema_change.rs`)

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
- A new write path that neither commutes nor checks its basis — it is correct
  only while its batches happen to drain in order
  ([05](05-apply-and-exactly-once-deltas.md#absolute-writes-do-not-commute-the-1-1-ledger)).
- An unreferenced data-modifying or `FOR UPDATE` CTE — Postgres may plan it away,
  and it will lock nothing while looking like it does.
