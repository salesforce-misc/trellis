---
status: proposed
date: 2026-09-19
deciders: Michael Ries
---

# Pausing and Dropping a Transform

`Trellis` has `define` and `define_relationship` and no inverse — nothing retires a
definition or its target table. That is a plain API gap, and it *blocks* the
embeddable-clients epic (#140): ADR-0010 puts transform definitions in Rails/Ecto
migration files, and a migration needs a `down`. Without a drop path, a rolled-back
deploy leaves a live transform running against a table the migration just removed.

This ADR settles how a transform is retired. It answers every open question raised in
#142, and defines the pause/resume primitive it builds on. It deliberately shares
machinery with the redefinition design still open in #12 rather than inventing a
parallel one.

## The lifecycle: DEFINE → PAUSE → DROP

A transform's terminal path is two operator-visible verbs, not one. **PAUSE** freezes
a live transform reversibly; **DROP** reaps a paused one. There is no `undefine`.

`undefine` would be redundant. My earlier "soft-retire then reap" bundled these two
steps into one opaque call; splitting them is strictly better:

- **PAUSE is independently useful** — freeze a misbehaving transform during an
  incident, or stage a source-schema change, without losing the target data. That
  reuse is what earns it a verb; a bundled `undefine` has no use outside "remove it."
- **It unifies with the pause family Trellis already has.** The involuntary
  poison/halting pause (`halting_stops`, the `key_deaths` threshold) and column
  quarantine ([ADR-0003](0003-quarantine-storage-and-api.md)) are both "stop folding
  into this, hold the stale value." Voluntary whole-transform pause is the same idea
  at a third scope, reachable through the same status gate the fold path already
  applies (`transforms_for_source` filters `status = 'live'`).
- **Clearer failure semantics.** DROP is not transactional with the host migration
  (below); if a reap fails midway, the transform is still cleanly PAUSED — a
  well-defined state — not a half-undefined one.
- The `drop_target` flag (below) already covers the only other thing `undefine`
  might have meant: retire the definition but keep the data. So a fourth verb buys
  nothing, and cuts against [ADR-0012](0012-curate-public-api-demote-engine-modules.md)'s
  curated surface.

## Decisions

### PAUSE is reversible; RESUME recovers via a new backfill, not a catch-up

Pausing flips the definition to a `paused` status the claim-time fold already
excludes. It must **not** hold the ring from retiring — a paused transform that
pinned its ring segments would wedge the ring for its siblings, the same
lease-not-latch lesson that retired the pause-lease scaffolding (#191). So while a
transform is paused, its delta stream is drained away for the other transforms on the
same source and is *not recoverable by replay*.

Therefore **RESUME triggers a fresh backfill**, not a catch-up over buffered changes.
There are no buffered changes to catch up on; the target is reconciled from source
the same way `define` builds it. This is stated plainly because it is the surprising
part: a long pause is not free, and a caller who expects a cheap resume gets a full
rebuild. This is documented as the contract, not an implementation detail.

### DROP requires the transform to be paused

DROP acts only on a `paused` transform. Requiring the pause first makes quiescence an
explicit precondition, so the reap never has to reason about live fold dispatch, and
each step is independently atomic and idempotent. There is no `force` variant in v1.

For framework integration this is composed, not exposed as a burden: the Ruby and
Elixir migration helpers' `down` calls PAUSE then DROP (host-language convenience per
[ADR-0010](0010-embeddable-clients.md) decision 2's corollary — compose the public
calls, don't reach around them). A single-verb convenience on the crate that
internally pauses, waits for quiescence, then drops is a possible later addition; it
is not required for v1 and is left open below.

### DROP takes `drop_target`: retire the definition, optionally drop the data

`drop(target, drop_target)` retires the catalog definition either way. When
`drop_target` is true it also `DROP`s the physical target table; when false it leaves
the table as inert data. A migration `down` passes true — it mirrors the `up` that
created the table. A human retiring a transform defaults to false — keep the data,
stop deriving it. The target table is Trellis-owned, so dropping it does not violate
[ADR-0005](0005-source-schema-is-user-owned.md); that ADR governs *source* tables.

### Drops must be made in reverse dependency order — no cascade

If a live transform chains off the target being dropped (a `source` edge in
`schema_edges` from the target's node, found via `dependents_of`), the drop is
**refused** with an error naming the blocking dependents. Trellis does not cascade,
and does not leave a dependent silently broken. The operator drops in reverse
dependency order. This matches [ADR-0005](0005-source-schema-is-user-owned.md)'s
principle that the engine guides rather than reshapes: the error names the exact
transforms to retire first.

### In-flight work is quiesced by the status gate, never by deleting shared rows

Only `backfill_chunks` is keyed to a definition (`definition_id ... ON DELETE
CASCADE`); dropping the catalog row cascades those away, and any chunk a drain worker
holds is released on its heartbeat. Everything else touched by in-flight work — ring
segments, `seg_claims`, `poison`/`poison_held`/`key_deaths` — is **source-keyed and
co-owned by sibling transforms on the same source**, and is never deleted by a drop.

The pause status is what makes this safe: once the transform is `paused`, the fold
stops dispatching to it, so a sealed batch still draining for the source simply skips
the paused target while its siblings continue. The reap then deletes only rows the
dropped target owns and calls `reconcile_publication`, never touching shared staging
state out from under a running worker.

### Quarantine: delete the target's column rows, leave the shared poison band

The column-quarantine tables (`column_status`, `column_deaths`, `column_failures`,
`column_pause_cascades`) are keyed by the bare target-table name with no FK (V22
dropped it), so a drop clears them explicitly — they are target-specific and their
forensic value goes with the table. The whole-key `poison`/`poison_held`/`key_deaths`
tables are **source-keyed and shared with sibling transforms**, so a drop leaves them
untouched.

### The publication shrinks by reconcile, not by hand

A drop does not hand-edit the Postgres publication. After removing the catalog rows it
calls `reconcile_publication`, which recomputes the desired table set from
`all_source_tables` (the transitive closure over remaining definitions) and issues the
`ALTER PUBLICATION ... DROP TABLE` only if the source now backs zero definitions.
Correct by construction, and it runs inline rather than waiting for the maintenance
loop.

### PAUSE and DROP are idempotent and not transactional with the host migration

`define` runs on Trellis's own pool, not the host migration's transaction (#140's
central hazard), and so does its inverse. A rolled-back or replayed migration must be
safe in both directions, so **pausing an already-paused transform and dropping an
absent one are no-op successes**. This is the same idempotency question #140 raises
for `define` ("idempotent by text?"); the two are answered together — a replayed `up`
is a no-op, a replayed `down` is a no-op.

### Placement: Tier-1 facade, plain data across the boundary

`pause`, `resume`, and `drop` are methods on `Trellis`/`Client`/`BlockingTrellis`
(with the blocking mirror), returning plain data
([ADR-0012](0012-curate-public-api-demote-engine-modules.md),
[ADR-0010](0010-embeddable-clients.md) decision 4). Bindings call them; they never
reach around into staging with their own SQL.

### Shared mechanism with redefinition (#12)

Drop and in-place redefinition share three primitives — the live-dependent refusal
(`dependents_of`), the status-gated fold exclusion, and backfill-chunk cancellation.
Drop is the strictly simpler operation (whole definition, no versioning scheme) and it
is what unblocks #140 now, so it ships first and extracts those primitives so #12's
redefinition reuses them rather than co-designing both. The redefinition-specific
pieces — the stored-schema versioning scheme and per-column backfill — stay with #12.

## Consequences

- Operators gain a reversible PAUSE (a freeze that holds stale data) and a terminal
  DROP, with the physical table's fate an explicit `drop_target` choice.
- A long pause is not free: RESUME is a full backfill, because the delta stream is
  drained away for siblings while paused. This is the documented contract.
- Migration `down` in both bindings is PAUSE + DROP, idempotent in both directions,
  and honest about the fact that it is not transactional with the host's migration.
- The reap touches only definition-owned rows and reconciles the publication; shared
  source-keyed staging and poison state is never deleted under a running worker.
- #12's redefinition design inherits the dependency-refusal, fold-exclusion, and
  chunk-cancellation primitives rather than reinventing them.

## Open questions

- **A single-verb `retire` convenience** that internally pauses, waits for quiescence,
  then drops — worth adding to the crate, or is composing PAUSE + DROP in the
  migration helpers enough? Deferred until a binding needs it.
- **RESUME's backfill scope.** A full rebuild is always correct; whether a bounded
  resume (backfill only the key range that changed during the pause) is worth building
  depends on how long real pauses last, and is left to the redefinition/backfill work.
