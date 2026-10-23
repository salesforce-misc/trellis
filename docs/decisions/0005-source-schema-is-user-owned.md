---
status: accepted
date: 2026-09-02
deciders: Michael Ries
---

# Source Schema Is User-Owned

Trellis reads source tables (changes are captured by triggers it installs; see
[capture by triggers](../staging-and-claiming/01-capture-by-triggers.md)) and
derives target tables from them. As features grow, it's tempting to *improve* the
source schema on the user's behalf — add an index, add a `UNIQUE` constraint a
transform relies on. This ADR rejects that. It is project-wide, not
feature-specific: it governs how every definition type validates against source
tables — transforms and transform redefinition (see
[0015-transform-redefinition](0015-transform-redefinition.md)).

## Trellis never modifies the source schema

No indexes, no constraint changes, no DDL against a source table other than the
capture triggers themselves. The user owns those tables; Trellis is a reader.
Instead:

1. **Validate strongly at definition time.** Every definition is checked against
   the introspected source schema (`pg_catalog` / `information_schema`) before
   acceptance. If the schema can't support it, the definition is **rejected** —
   Trellis never silently produces wrong answers, and never reshapes the source
   to make a definition work.
2. **Guide the user to a fix.** Every rejection or warning names the exact change
   to make — e.g. a source index dropped after define is reported in `status()`
   with the statement that restores it, `create index on public.orders (region)`.

## A missing correctness prerequisite rejects; a missing performance one warns

Correctness vs. performance sets the severity:

* A missing correctness prerequisite (a required type, a source's primary key)
  is a **hard rejection**.
* A missing performance prerequisite (a source index dropped after define) is
  a **warning**: the definition stays correct without it.

## Consequences

* Definitions are only as capable as the schema the user actually built — by
  design. Trellis never leaves a schema in a state the user didn't author.
* Validation must introspect real constraints (keys, unique indexes, types),
  never assume them. Requirements are checked and surfaced, never applied.
* Error and warning messages are first-class product surface: the guidance *is*
  how a user learns to shape a schema Trellis can serve well.
