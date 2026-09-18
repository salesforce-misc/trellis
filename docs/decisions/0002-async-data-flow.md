---
status: accepted
date: 2026-08-15
deciders: Michael Ries
consulted: 
informed:
---

# Asynchronous Data Flow

Transformations can be applied synchronously or asynchronously, and the choice
shapes the whole user experience.

## Decision

Trellis applies transformations **asynchronously**. It uses Postgres logical
replication to detect source-data changes and applies derivations in batches,
outside the application's write path.

## Synchronous

Update derived tables (including transitive derivations) in the same transaction
that commits the source change, via triggers, plpgsql, or SQL functions.

### Pros

* **Consistent by construction.** Backfill at definition time; every later
  derivation runs in the transaction that modifies its source. All reads are
  strongly consistent.
* **Fast for shallow, same-row derivations.** Extra columns fill in the same
  write — very low overhead, 500k+ changes/sec on modest hardware.
* **Fails at write time**, so the application knows exactly which operation
  broke rather than discovering it after the fact.
* **Familiar tooling.** Triggers and functions live in the Postgres catalog,
  are well-known to DBAs, and keep all resource use in one pool — no replication
  slot, survives backup/restore, needs no connected client.

### Cons

* **Cross-relationship formulas are unsound.** Concurrent transactions don't see
  each other's changes until they land, leaving invalid data.
* **Disabling triggers strands stale data.** This includes logical replication
  (e.g. AWS DMS): you'd have to replicate every source *and* target table to
  stay consistent — easy to get wrong.
* **No batch recompute.** Triggers fire per row: 500 updates folding into one
  aggregate row run 500 functions. The async path composes them into a single
  write per affected group.
* **No clear path for chained updates** (source row → aggregate → second
  aggregate).
* **One failed derivation blocks the whole write.** No way to quarantine a
  single broken dependent while the other 99 proceed — the flip side of
  consistency.
* **Highly connected rows get slow.** Updating an account with millions of line
  items becomes a huge bulk transaction, and ordinary SQL costs get hard to
  reason about. Concurrency makes it worse: 20 updates folding into one regional
  total serialize on that row's write lock, and contention dominates.

## Asynchronous

Watch source tables via logical replication. On change (or on backfill of a new
definition), clients pull batches of changed rows and update the dependent
derived tables and columns.

### Pros

* **Batched for backfill and incremental updates alike.** Stale rows collapse
  into the minimum writes for the next derivation layer; transitive dependencies
  fold in by staging the rows a prior step modified. Aggregates benefit most —
  N source changes collapse to M target writes, sustaining 180k+ source updates
  per second indefinitely.
* **Failures quarantine.** Invalid data is already committed to the source; we
  pause only the failed dependents and let the rest flow.
* **Off the hot path.** Derivation cost and write latency leave the application's
  update path, and work can pause during busy times and resume later.
* **Backpressure trades latency for stability** — derived data lags without
  slowing source writes.
* **No concurrency tax.** Concurrent source updates don't contend on target rows.

### Cons

* **Eventual consistency must be designed around.** An `await(LSN, timeout)` can
  block until all transactions up to an LSN clear derivations, and latency is
  always monitorable.
* Calculated columns must live on a neighbor table, so we aren't notified about
  our own writes.
* **Replication slot cost.** Falling behind can block writes (mitigated by a
  staging area, so keep at least one Trellis client connected during writes);
  slots can consume a full CPU; failover needs design (easier on Postgres 17+).
* **Error API required** — clients must check for errors, since we don't catch
  them at write time.
* **Throughput is capped by logical replication.** Postgres decodes the WAL on a
  single CPU, a bottleneck under high write volume.
