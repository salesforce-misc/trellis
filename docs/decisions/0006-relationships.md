---
status: accepted
date: 2026-09-02
deciders: Michael Ries
consulted:
informed:
---

# Named Relationships

This ADR settles how a named relationship is declared and referenced, how
cardinality constrains that reference, and how a change to a *related* row
propagates back to the rows that depend on it.

## Declaration

A relationship is a **standalone, named declaration**, not a clause inside a
transform's `FROM`, so one relationship is reusable across many transforms:

```text
RELATIONSHIP <name> FROM <from_table>.<fk_col> TO <to_table>.<pk_col>

RELATIONSHIP product FROM order_line_items.product_id TO products.id
RELATIONSHIP author  FROM posts.author_id            TO users.id
RELATIONSHIP editor  FROM posts.editor_id            TO users.id
```

The grammar follows [ADR-0004](0004-transform-definition-grammar.md): our own minimal
grammar, parsed at definition time into an AST over the shared lexer and
expression language.

### Naming and scope

* A relationship name is **unique per from-table**, not global. `posts` may
  declare both `author` and `editor` at `users` as independent relationships.
* Relationships may be declared in any order, as long as every endpoint
  resolves and the dependency graph stays acyclic.

### Endpoints

* Either endpoint may be a **source table or a transform target**, in any
  combination.
* **No FK auto-discovery.** Every relationship names its join key explicitly.

## Referencing a relationship

A calculated field references a relationship through a qualified path whose
**head is the relationship name** — not the target table and not a per-query
alias:

```text
product.category_name
```

`category_name` is a column on the to-side table; per-from-table uniqueness
guarantees `product` names exactly one relationship. How it may be referenced
depends on **cardinality**, paralleling how granularity constrains formula
shape in [transforms](../transforms.md#calculated-fields).

### To-one (at most one related row)

If the to-side column is a **primary key or `UNIQUE`**, the join resolves to at
most one row and enrichment columns are usable as **bare paths**
(`product.category_name`). A transform target always qualifies, its PK coming
from its key-space.

This invariant is enforced **at definition time** from the to-side constraint,
per [ADR-0005](0005-source-schema-is-user-owned.md): a bare to-one relationship
whose to-side is not provably unique is rejected with guidance (add `UNIQUE`, or
use the aggregate form). A runtime uniqueness violation quarantines the affected
key (existing per-key isolation), never resolving to an arbitrary row.

### To-many (many related rows)

If the to-side column is not unique, the relationship is **to-many** and its
columns may be referenced **only** wrapped in exactly one aggregate (`SUM`,
`MIN`, `MAX`, `AVG`, `COUNT`):

```text
sum(comments.word_count)
```

This reuses `GROUP BY` aggregate semantics and incremental-delta machinery,
keyed by the join value instead of a grouping tuple. A **bare** to-many
reference is a definition-time error.

### Relationship vs. cross-join

A relationship of either cardinality **keeps the referencing table's
granularity**, folding related data into scalar enrichment columns. A
**cross-join changes granularity**, producing one row per matching pair at the
pairing grain (`{a.pk, b.pk}`).

## Chaining

A path may cross more than one relationship, each segment naming a relationship
on the table the previous segment resolved to:

```text
post.author.name
```

Each hop resolves under its own cardinality rule. The chain is to-one only if
**every** hop is to-one, staying a bare reference; a single to-many hop makes
the whole path to-many — referenceable only wrapped in one outermost aggregate,
with no later to-one hop "undoing" it:

```text
sum(comments.post.author.post_count)
```

Chaining adds no new machinery: each hop is already an edge in the
[dependency graph](#dependency-graph-and-cycles).

## Nullability

* A to-one relationship with no matching row yields `NULL` enrichment columns
  (left-join semantics); it does not affect whether the referencing row exists.
* A to-many relationship with no related rows yields the aggregate's empty
  result (`COUNT` → `0`, `SUM` → `NULL`), matching PostgreSQL.

## Dependency graph and cycles

Relationship links are **edges in the same cross-table dependency graph** as
sources, joins, and chained transforms
([transforms](../transforms.md#chaining-and-cycle-detection)). Cycles — both
column-to-column and table-to-table — are rejected at definition time across the
whole graph, so evaluation order is well-defined and propagation terminates.

## Storage

Relationship definitions are persisted in Trellis's catalog as source text,
re-parsed on read, immutable once created — mirroring transforms. Source tables
are unchanged (see [ADR-0005](0005-source-schema-is-user-owned.md)).

## Incremental maintenance

Trellis maintains enriched columns incrementally, like any calculated field,
over the asynchronous staging/apply pipeline
([ADR-0002](0002-async-data-flow.md), [data-flow](../data-flow.md)). Two
directions:

* **Forward (a referencing row changes).** Its enrichment columns are re-derived
  in dependency order: to-one looks up the single related row by join key;
  to-many aggregates the related rows.
* **Reverse (a *related* row changes).** From the dependency graph, Trellis
  finds which relationships target the changed table and re-derives the
  referencing rows whose join key matches the changed row's key — read from the
  changed row's replica image. Both cardinalities therefore constrain replica
  identity, enforced at define time:

  * **To-many**'s join key is a *non-PK* to-side column, omitted from the
    default (PK) replica identity's delete/re-parent pre-images. Requires
    `REPLICA IDENTITY FULL`, or a replica-identity index covering the join
    column.
  * **To-one**'s join key is the to-side PK, which the default identity carries
    — but the key alone is not enough. Every to-one relationship gets an
    unconditional settled parent projection, whose reverse-applied advance needs
    the to-side row's *entire* old image to detect an update, delete, or re-key.
    So the to-side requires `REPLICA IDENTITY FULL`.
  * **To-one** additionally requires `REPLICA IDENTITY FULL` on its **from-side**
    (child) table. The from-side join column is an ordinary non-key column (the
    FK), so under the default identity an `UPDATE` that re-points the FK without
    touching the PK ships *no* pre-image — nothing to recover the prior parent
    from. The to-side gate cannot catch this: a from-side re-point never touches
    the to-side row.

  This reuses the existing "recompute" staging path, not a bespoke persisted
  reverse index. Finding the affected referencing rows is a lookup on the
  from-side join column — **correct without an index**; an index only makes it
  fast. Per [ADR-0005](0005-source-schema-is-user-owned.md), Trellis does not
  create it; it detects a usable one and, if absent, emits a performance warning
  naming the exact `CREATE INDEX`.

This is the case [ADR-0002](0002-async-data-flow.md) flagged: cross-relationship
formulas are unsound under naive synchronous triggers. The async
staging/apply/fence design makes them sound; reverse propagation is one more
producer feeding that machinery, not a new ordering regime.

## Redefinition

Relationships are **immutable once declared**, like transforms: to change a join
key, declare a new relationship and cut over. Editing the calculated columns
that *consume* a relationship is separate transform-redefinition work (see
[open-questions](../open-questions.md)).
