---
status: accepted
date: 2026-09-19
deciders: Michael Ries
---

# Redefining a Transform's Columns

A transform's definition is three separable pieces — an immutable key-space, a set
of calculated fields, and an optional partial-data predicate. Defining, pausing, and
dropping a whole transform are covered by their own decisions. This ADR covers the
middle: editing the calculated fields of a transform that already exists, without
redefining it and cutting over.

## What is editable, and what is not

The **key-space is fixed at creation**. A change of granularity — 1-1, aggregate, or
cross-join, and the columns that key it — is a different table, not an edit, and stays
a new transform plus cutover. The grammar enforces this by construction: the edit
statement has no key-space clause to write, so a granularity change is not expressible
as an edit rather than rejected by a validator branch.

The **calculated fields are the editable surface** — added, dropped, or altered in
place. The partial-data predicate is not editable: it is fixed at creation.

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

The clauses apply in order, and the edit is what they change together. A field added and
dropped in one edit cancels, and never gets a column. An `ALTER` of a field the same edit
added changes what it adds. `DROP z, ADD z` replaces `z`'s column, which is the way to
change its result type.

Only the delta form exists; it is what the common case (one column on a wide table)
wants. A declarative form (restating the full field list and letting the engine diff
it) would be another production the same entrypoint discriminates.

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

The edit returns before the backfill, which is a background *field build*. The edit's
transaction registers it (the definition moves `live -> backfilling`), the changed
fields apply to every live change from that commit, and background chunks rewrite just
those fields across the existing rows, one source read for all of them. The definition
reads `live` again once the chunks are done. An added column is not held paused while
it builds; `backfilling` on the definition says it isn't built yet. A field that reads
a source column capture doesn't image yet stays paused until it does, and its build
starts then. A column `RESUME` is the same kind of build. A dropped column's data is
removed, consistent with drop removing the data it explains.

### The stored schema versions monotonically; physical changes are additive

Each accepted edit bumps a monotonic definition version on the target's catalog row and
stores the resulting field schema against it. Physically, an added column is additive —
the column is created, backfilled, then made live — and a dropped column's data is
removed once nothing maintained still needs it. An edit never rewrites the whole target
table to change one column. A column-add backfill carries the new version as its fence,
so live change-application and the in-flight backfill do not race on a half-populated
column.

### `ALTER` refuses a genuine column-type change

The "never rewrites the whole target table" guarantee above is not automatic just because
`ALTER <field> AS <expr>` only touches one column's formula: if the new formula's result
type is not the type the column already physically has, Postgres cannot apply that as a
metadata-only change — it rewrites the *entire* physical heap under an `ACCESS EXCLUSIVE`
lock, blocking reads and writes to every other column too, not just the one being altered.
Shipping that would violate this ADR's own guarantee in exactly the case an operator is
least likely to expect it (an edit that reads like "just this column").

`ALTER TRANSFORM` therefore refuses, before any DDL or backfill runs, whenever an `ALTER`
clause's inferred result type differs from its column's current physical type — naming the
field and both types, and pointing at `DROP <field>` followed by `ADD <expr> AS <field>` as
the supported path instead. That pair pays the same backfill cost, so the refusal makes nothing
more expensive; it only stops a full-table rewrite posing as a single-column edit. A same-type `ALTER` (a formula change whose result type is unchanged) is unaffected and
still edits in place as described above.

A shadow column, single-pass backfill, and atomic rename-swap would avoid the restriction;
that is not implemented.

### Column edits validate against the dependency graph like any definition change

Adding or altering a column that references another table, a relationship, or a sibling
column is a graph-edge change, validated by the same whole-graph cycle detection a define
runs. An edit that would introduce a cycle, directly or transitively, is rejected before
it runs.

An edit validates the definition it leaves against the source's live columns, read in the
edit's own transaction after the version fence and the definition's row lock, with the
checks a define makes. The columns recorded when the definition was defined are not the
reference: a column the host has added since is readable, one named like a new field
refuses it, and one dropped, or retyped so that a field reading it no longer validates,
refuses an edit that leaves that field. So an edit never commits a definition define would
refuse. A field's builds compute it over the live types and its applies over the recorded
ones, so an edit records the type of each column it reads that the host added since, and
refuses to build a field reading a column whose type the host changed: `x` moved from
`integer` to `double precision` refuses `ADD x + y AS xy` until a resume re-types the
definition. Recording the new type instead would move every unedited field reading `x`
onto it, under columns created for the old type, so a `3.5` that fails to parse as the
recorded `integer`, and holds its key, would round silently into an `integer` column.

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
  only the dropped column's data, and an `ALTER` that would require a real type change is
  refused rather than honored as a whole-table rewrite; `DROP`+`ADD` remains the supported
  way to change a column's type.
- Column-granularity dependency tracking is required, so a drop refusal can name the exact
  column a dependent reads.
- A framework migration that edits a transform is honest under replay and rollback, because
  each edit is idempotent in both directions.
