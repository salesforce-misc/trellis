---
status: accepted
date: 2026-09-04
deciders: Michael Ries
consulted:
informed:
---

# Source Object Identity Is Bound At Definition Time

Trellis definitions name PostgreSQL tables and columns, but a name is a mutable
label, not the object itself. Operators rename tables, move them between schemas,
rename columns, drop objects, and recreate objects under a former name. Logical
replication identifies relations by OID. We need one definition-time rule for
resolving names and one stable identity for the catalog, dependency graph, and
replication pipeline.

## Decision

**Trellis resolves every table and column reference once, when accepting a
definition, and binds it to the PostgreSQL object that resolution selected.**

A table reference records the relation OID. A column reference records the
owning relation OID, the column attribute number (`attnum`), and the type
information needed to validate and evaluate the definition. The original
definition text and resolved schema-qualified names are kept as user-facing
metadata, not authoritative identity.

Unqualified names resolve through the definition connection's `search_path`
using PostgreSQL's own rules; a qualified name resolves to that exact relation.
After resolution succeeds, every later operation uses the stored identity, never
the original spelling.

This applies to source tables, transform targets, relationship endpoints, and
future forms that name relations or columns.

## Consequences

* **Renames and schema moves preserve the definition.** `RENAME`, `SET SCHEMA`,
  and `RENAME COLUMN` retain the relation's OID and the column's `attnum`.
  Trellis keeps maintaining the same objects, resolving current names from
  `pg_class`, `pg_namespace`, and `pg_attribute` when it generates identifier
  SQL or a diagnostic.
* **A drop and recreate is not a rename.** A new `posts` table gets a new OID
  even if it reuses the dropped table's schema and name, and its columns, keys,
  replica identity, constraints, and contents can all differ. Trellis treats a
  missing bound object as a loud schema failure rather than silently
  transferring the definition; staged work follows the dropped-table purge in
  [stage 06](../staging-and-claiming/06-cleanup-and-reclaim.md), and recovery
  requires an explicit new or redefined transform.
* **Same-named objects in different schemas remain distinct.** `public.posts`
  and `archive.posts` have different OIDs and thus independent source versions,
  dependency-graph nodes, transform subscriptions, and quarantine state. The
  bare spelling `posts` is used only while accepting the definition.
* **Replication and catalog lookup use the same identity.** Incoming pgoutput
  relation IDs match on live relation identity, not on schema-stripped names.
  Relation metadata may refresh after DDL but never changes which catalog object
  a definition represents.
* **Incompatible source changes stop rather than corrupt.** Before evaluation,
  Trellis verifies every bound relation and column still exists with compatible
  type and key/constraint properties. Dropping and re-adding a same-named column
  gives it a new `attnum`, so it reads as a changed definition, not an
  interchangeable replacement.
* **OID values are internal and database-local.** They live only in the Trellis
  catalog, never in the transform DSL or public API. A restore or migration that
  recreates source objects may assign new OIDs, so attaching Trellis then
  requires explicit validation or rebinding, not an assumption that matching
  names mean matching objects.

## Alternatives Considered

### Resolve names on every operation

Reapplying `search_path` during backfill, publication reconciliation, and apply
makes behavior depend on session configuration and can bind one definition to
different objects over time; it also cannot distinguish a rename from a
drop-and-recreate. Canonical `schema.table` strings reduce the search-path
ambiguity but still silently rebind after a replacement appears at the same name.

### Follow a new object with the same name

A name-as-contract model makes an invisible, unsafe change: dropping
`public.posts` and creating another would make an existing transform consume
unrelated rows with no new definition or backfill decision. Users who intend
that must explicitly create or redefine the transform, keeping the cutover
visible.

### Store OIDs without resolved names

SQL identifiers can't be parameterized as OIDs in ordinary queries, and
operators need current names in errors and observability. Trellis keeps resolved
names as refreshable metadata while OIDs stay the source of truth.

## Scope

This decision defines object identity and DDL-evolution semantics, not the
transform-redefinition API, which remains open in
[open questions](../open-questions.md). It does not relax
[ADR-0005](0005-source-schema-is-user-owned.md): Trellis validates source objects
and reacts to incompatible changes but never modifies the user-owned source
schema.
