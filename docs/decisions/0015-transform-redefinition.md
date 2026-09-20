---
status: accepted
date: 2026-09-19
deciders: Michael Ries
---

# Redefining a Transform's Columns

A transform's definition is three separable pieces — an immutable key-space, a set
of calculated fields, and an optional partial-data predicate. Defining, pausing, and
dropping a whole transform are settled. This ADR settles the missing middle: editing
the calculated fields of a transform that already exists, without redefining it and
cutting over.

## What is editable, and what is not

The **key-space is fixed at creation**. A change of granularity — 1-1, aggregate, or
cross-join, and the columns that key it — is a different table, not an edit, and stays
a new transform plus cutover. The grammar enforces this by construction: the edit
statement has no key-space clause to write, so a granularity change is not expressible
as an edit rather than rejected by a validator branch.

The **calculated fields are the editable surface** — added, dropped, or altered in
place. The partial-data predicate is out of scope here and stays fixed until a later
decision.

## Decisions

### Edits are a delta, not a re-specification

A transform with many columns must not be fully restated to change one. The edit
statement carries only the columns that change:

```text
ALTER TRANSFORM <target>
  ADD   <expr> AS <field>
  DROP  <field>
  ALTER <field> AS <expr>
  [, ...]
```

Each clause names one calculated field and the operation on it. `ADD` introduces a new
column, `DROP` removes one, `ALTER` replaces an existing column's formula. The key-space
is not named and cannot be changed.

A declarative form — restating the full field list and letting the engine diff it — is
a compatible future addition, not a competing one: it is another production the same
entrypoint discriminates, and both may be accepted at once. We ship the delta form first
because it is what the common case (one column on a wide table) wants.

### The edit is grammar through the one entrypoint

`ALTER TRANSFORM` is a statement in Trellis's grammar, submitted through the single
definition entrypoint the facade exposes — the same entrypoint and grammar that define,
pause, resume, and drop. It is not a typed `amend` method. The engine parses the statement
to decide the operation and composes it behind the facade.

### Added and dropped columns backfill in a single pass

An edit that adds columns backfills them by enumerating the source **once**, populating
every added column in that pass — never a backfill job per column. This is the same
single-pass contract a first definition already honors; a growing table pays for one
enumeration, not one per column. Single-column incremental backfill remains only an
optimization, never a correctness requirement.

While an added column is backfilling it holds no committed value and the rest of the
target stays live — the column-granularity form of the pause state a target already has.
A dropped column's data is removed, consistent with drop removing the data it explains.

### The stored schema versions monotonically; physical changes are additive

Each accepted edit bumps a monotonic definition version on the target's catalog row and
stores the resulting field schema against it. Physically, an added column is additive —
the column is created, backfilled, then made live — and a dropped column's data is
removed once nothing maintained still needs it. An edit never rewrites the whole target
table to change one column. A column-add backfill carries the new version as its fence,
so live change-application and the in-flight backfill do not race on a half-populated
column.

### Column edits validate against the dependency graph like any definition change

Adding or altering a column that references another table, a relationship, or a sibling
column is a graph-edge change, validated by the same whole-graph cycle detection a define
runs. An edit that would introduce a cycle, directly or transitively, is rejected before
it runs.

Dropping a column follows the drop rule already in force: **refuse, do not cascade.** If
any definition still references the column being dropped, the drop is refused and names
the referencing definitions; the operator retires them first, in dependency order. Trellis
implements no cascade. This requires column-granularity dependency edges — finer than the
table-granularity edges a whole-transform drop needs — so the refusal can name the exact
column a dependent reads.

Dropping a whole transform is this same check applied to every one of its columns, in
addition to the table-level dependency refusal: a transform cannot be dropped while any
definition reads it or any column it exposes.

### Edits are idempotent

Like the other definition operations, an edit runs on Trellis's own connections outside a
host migration transaction, so a replayed or rolled-back migration must be safe in both
directions. Re-adding a column that already exists with the same formula, altering a column
to the formula it already has, and dropping a column already absent are no-op successes.

## Consequences

- A transform's columns evolve in place; only a granularity change forces a new transform.
- Growing a table costs one source enumeration regardless of how many columns are added.
- The target table is never rewritten to edit a column — adds are additive, drops remove
  only the dropped column's data.
- Column-granularity dependency tracking is required, so a drop refusal can name the exact
  column a dependent reads.
- A framework migration that edits a transform is honest under replay and rollback, because
  each edit is idempotent in both directions.
