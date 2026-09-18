---
status: proposed
date: 2026-09-10
deciders: Michael Ries
consulted:
informed:
---

# Fully-Qualified Names Are The Persisted Identity

A bare name like `posts` resolves to different tables depending on `search_path`.
Trellis has repeatedly hit bugs from resolving an unqualified name against the
wrong schema at a later step than the one that accepted the definition (e.g.
`relation "trellis.posts" does not exist`, the motivation for PR #39, and the
masking fixed by PR #69 for issue #65).

## Decision

**Trellis resolves every table reference to a fully-qualified `schema.table` name
once, at definition-acceptance time, and persists that qualified form as the
reference's identity. It never persists or re-resolves the bare spelling.**

1. **Definitions persist qualified names.** A bare name is resolved exactly once
   using PostgreSQL's `search_path` semantics — first schema with a matching
   table wins — and the qualified result, not the bare spelling, is persisted.

2. **The dependency graph keys on qualified identity.** `public.posts` and
   `archive.posts` are distinct nodes with independent edges, versions, and
   quarantine state, walked and diffed on qualified strings, never re-resolved.

3. **Every generated statement emits qualified names,** schema and table quoted
   independently — for replication/publication, backfill enumeration, and
   target-table DDL alike.

4. **The grammar accepts, but does not require, an explicit `schema.table`.** A
   qualified spelling resolves to that exact relation; a bare one is resolved as
   in (1).

This applies to source tables, transform targets, and — as they gain persisted
identity — relationship endpoints (see [ADR-0006](0006-relationships.md)).

## Consequences

* **The schema-confusion bug class is closed by construction.** Resolution
  happens once, in the session whose `search_path` the bare name is meant to
  read against; nothing downstream re-resolves, so no later step can bind the
  same definition to a different object.

* **Same-named tables in different schemas are distinct definitions,** with no
  shared identity to confuse.

* **This does not survive renames or `SET SCHEMA`.** A renamed or moved source
  table no longer resolves, and the definition fails loudly rather than silently
  following the object. A deliberate trade: OID-binding (see Alternatives) would
  *attempt* to follow renames, at the cost of breaking on the far more common
  restore / `pg_upgrade` / DR-failover case, where OIDs are reassigned but a
  restored `public.posts` is still `public.posts`. Recovering from a genuine
  rename is an explicit redefine — visible, not silent — consistent with
  [ADR-0005](0005-source-schema-is-user-owned.md).

* **A one-time data migration is required.** Existing bare-name references must
  be canonicalized to qualified form (each resolved against the schema it was
  created under), with uniqueness constraints re-keyed on qualified identity.

* **Names remain human-readable and parameterizable.** Unlike OIDs, a
  `schema.table` string is a normal SQL identifier — usable directly in generated
  statements and in error/observability output, with no name-refresh path.

## Alternatives Considered

### Bind identity to a PostgreSQL OID at definition time

Explored on branch `source_table_resolution` (draft ADR
`0007-source-object-identity.md`): store the relation's OID as authoritative
identity instead of its name. **Rejected.** The bug addressed here is a
*definition-time name-resolution* bug — the replication path was never confused,
carrying authoritative schema identity from the WAL. OIDs are also **not** stable
across restore/`pg_upgrade`/DR-failover, so every transform would break after a
source-DB restore unless a missing OID were tolerated — which reopens the very
ambiguity this decision closes. It also imposes permanent dual-path complexity,
and was only ever partially applied.

### Resolve names on every operation

Re-applying `search_path` during backfill, publication reconciliation, and apply
makes behavior depend on the executing session and can bind one definition to
different objects over time — the failure mode PR #39 and PR #69 hit. Persisting
the qualified name avoids this and the repeated work, extending to identity the
resolve-once reasoning [ADR-0005](0005-source-schema-is-user-owned.md) applies to
validation.

## Scope

Governs how table references are spelled and persisted across all current and
future definition types — transforms, relationships
([ADR-0006](0006-relationships.md)), and redefinition. It does not relax
[ADR-0005](0005-source-schema-is-user-owned.md): Trellis still issues no DDL
against source tables, only resolves, records, and reads their qualified names.
It introduces no identity concept beyond the `schema.table` string PostgreSQL
itself uses.
