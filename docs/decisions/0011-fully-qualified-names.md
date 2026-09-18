---
status: proposed
date: 2026-09-10
deciders: Michael Ries
consulted:
informed:
---

# Fully-Qualified Names Are The Persisted Identity

Trellis definitions name PostgreSQL tables and columns. A bare name like `posts`
is ambiguous: it resolves to different objects depending on the `search_path`.
Trellis has repeatedly hit bugs from resolving an unqualified name
against the wrong schema at a later step than the one that accepted the
definition (e.g. `relation "trellis.posts" does not exist`, the motivation for PR
#39, and the schema-qualification masking fixed by PR #69 for issue #65).

## Decision

**Trellis resolves every table reference to a fully-qualified `schema.table` name
once, when accepting a definition, and persists that qualified form as the
reference's identity. It never persists or re-resolves the bare spelling.**

1. **Definitions persist qualified names.** On acceptance, a bare or ambiguous
   name is resolved exactly once using PostgreSQL's own `search_path`
   semantics: the first schema in the search path that has a table by that name
   wins. The qualified result — not the bare spelling the user typed — is what
   gets persisted.

2. **The dependency graph keys on qualified identity.** A node is the object it
   actually names, not a bare label — `public.posts` and `archive.posts` are
   distinct nodes with independent edges, versions, and quarantine state. The
   graph is walked and diffed on qualified strings, never re-resolved.

3. **Every generated statement emits qualified names**, schema and table each
   quoted independently, never a bare name left to the executing session's own
   `search_path`. This covers replication/publication management, backfill
   enumeration, and target-table DDL alike.

4. **The grammar accepts, but does not require, an explicit `schema.table`**
   spelling on source and target references, for users who want to disambiguate
   up front. A qualified spelling resolves to that exact relation; a bare
   spelling is resolved once as in (1).

This applies to source tables, transform targets, and — as they gain persisted
identity — relationship endpoints (see [ADR-0006](0006-relationships.md)).

## Consequences

* **The schema-confusion bug class is closed by construction.** Resolution
  happens once, at acceptance time, in the session whose `search_path` the bare
  name is meant to read against. Nothing downstream re-resolves, so no later step
  can bind the same definition to a different object.

* **Same-named tables in different schemas are distinct nodes and definitions,**
  with no shared identity to confuse.

* **This does not survive renames or `SET SCHEMA`.** If an operator renames or
  moves a source table, the persisted qualified name no longer resolves and the
  definition fails loudly rather than silently following the object. This is a
  deliberate trade: OID-binding (see Alternatives) would *attempt* to follow
  renames, at the cost of breaking on the far more common restore / `pg_upgrade`
  / DR-failover case, where OIDs are reassigned but a restored `public.posts` is
  still `public.posts`. Recovering from a genuine rename is an explicit redefine,
  kept visible rather than silent, consistent with
  [ADR-0005](0005-source-schema-is-user-owned.md).

* **A one-time data migration is required.** Existing persisted references hold
  bare names and must be canonicalized to qualified form (each resolved against
  the schema it was created under), with uniqueness constraints updated to key
  on qualified identity.

* **Names remain human-readable and parameterizable.** Unlike OIDs, a
  `schema.table` string is a normal SQL identifier — usable directly in generated
  statements and in error/observability output, with no name-refresh path.

## Alternatives Considered

### Bind identity to a PostgreSQL OID at definition time

Explored on branch `source_table_resolution` (draft ADR
`0007-source-object-identity.md`): resolve each reference once and store the
underlying relation's OID as authoritative identity instead of its name.
**Rejected.** The bug this decision addresses is a *definition-time
name-resolution* bug — the replication path was never confused, since it
already carries authoritative schema identity from the WAL. OID-binding is
also self-defeating for the scenario operators most care about: OIDs are
**not** stable across restore/`pg_upgrade`/DR-failover, so every transform
would break after any source-DB restore unless a missing OID were tolerated —
which then reopens the ambiguity this decision closes. It further imposes
permanent dual-path complexity (name-based and OID-based identity coexisting
indefinitely) and was only ever partially applied.

### Resolve names on every operation

Re-applying `search_path` during backfill, publication reconciliation, and apply
makes behavior depend on the executing session and can bind one definition to
different objects over time — the failure mode PR #39 and PR #69 hit. Persisting
the qualified name avoids this and the repeated resolution work, extending to
identity the same resolve-once reasoning
[ADR-0005](0005-source-schema-is-user-owned.md) applies to validation.

## Scope

This governs how table references are spelled and persisted across all current
and future definition types — transforms, relationships
([ADR-0006](0006-relationships.md)), and transform-redefinition. It does not
relax [ADR-0005](0005-source-schema-is-user-owned.md): Trellis still issues no
DDL against source tables — it resolves, records, and reads their qualified
names. It introduces no new identity concept beyond the `schema.table` string
PostgreSQL itself uses.
