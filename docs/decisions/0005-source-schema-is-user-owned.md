---
status: accepted
date: 2026-09-02
deciders: Michael Ries
consulted:
informed:
---

# Source Schema Is User-Owned

Trellis reads source tables over logical replication and derives target tables
from them. As features grow, it's tempting to *improve* the source schema on the
user's behalf — add an index, add a `UNIQUE` constraint a transform relies on,
set `REPLICA IDENTITY FULL` so deletes carry an old image. This ADR rejects that.

## Decision

**Trellis never modifies the source schema.** No indexes, no constraint changes,
no replica-identity changes, no DDL against a source table. The user owns those
tables; Trellis is a reader. Instead:

1. **Validate strongly at definition time.** Every definition is checked against
   the introspected source schema (`pg_catalog` / `information_schema`) before
   acceptance. If the schema can't support it, the definition is **rejected** —
   Trellis never silently produces wrong answers, and never reshapes the source
   to make a definition work.
2. **Guide the user to a fix.** Every rejection or warning names the exact change
   to make — e.g. "`products.id` must be a primary key or `UNIQUE` to be a to-one
   target; add one, or go through an aggregate," or "no index on
   `order_line_items.product_id`; related-row updates will be slow — consider
   `CREATE INDEX ... ON order_line_items (product_id)`."

Correctness vs. performance sets the severity:

* A missing correctness prerequisite (a required type, a uniqueness guarantee a
  bare to-one relationship needs) is a **hard rejection**.
* A missing performance prerequisite (an index for cheap reverse propagation) is
  a **warning**: the definition succeeds and is correct without it.

## Consequences

* Definitions are only as capable as the schema the user actually built — by
  design. Trellis never leaves a schema in a state the user didn't author.
* Validation must introspect real constraints (keys, unique indexes, types,
  replica identity), never assume them. Requirements like `REPLICA IDENTITY FULL`
  for deletes/re-parents from one-to-many aggregates (see
  `staging-and-claiming/01-intake-and-lsn-confirmation.md`) are checked and
  surfaced, never applied.
* Error and warning messages are first-class product surface: the guidance *is*
  how a user learns to shape a schema Trellis can serve well.

## Scope

Project-wide, not feature-specific. Governs how every definition type validates
against source tables — transforms, relationships (see
[0006-relationships](0006-relationships.md)), and the transform-redefinition work
still being designed.
