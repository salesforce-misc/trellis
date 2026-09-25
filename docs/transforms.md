# Defining Transforms

A transform describes how one derived (target) table is produced from one or
more source tables. Its definition is three separately-chosen pieces: the
**granularity** of the target's primary-key space, the **calculated fields**
that populate its non-key columns, and an optional **partial-data** predicate
restricting which source rows participate.

Trellis maintains these tables **incrementally** as source data changes, trading
spikey read load for a steady write load that keeps the table cheap to read (see
the README for fuller motivation).

## Source tables

**Trellis requires every source table to have a primary key.** A change arriving
over logical replication is identified by its primary key, and a 1-1 target
inherits that key as its own. A definition over a table with no primary key is
rejected when it is defined, with an error naming the table. Trellis never adds
the key itself: the source schema is the user's
([0005-source-schema-is-user-owned](decisions/0005-source-schema-is-user-owned.md)).
A table whose replica identity is `USING INDEX` over a unique index is accepted
too, since that index plays the primary key's part, but a primary key is the
rule to design to.

Some shapes need more than the key. An aggregate, and any read through a to-one
relationship, needs a deleted or re-keyed source row's whole old image, which
Postgres logs only under `REPLICA IDENTITY FULL`. That, too, is checked and
never applied; the rejection quotes the exact `ALTER TABLE` to run.

One consequence is worth stating plainly: **an aggregate target does not qualify
as a source table.** Its grouping columns may be `NULL`, so its identity is a
`UNIQUE NULLS NOT DISTINCT` constraint rather than a primary key. Chaining off
it still works inside the instance that owns it, because an instance hands each
write to one of its own targets on to that target's readers in the writing
transaction, never over logical replication (see
[Chaining and cycle detection](#chaining-and-cycle-detection)). That internal
path is what makes the chain possible, and it stops at the instance boundary: to
another Trellis instance, or to any other logical-replication consumer, an
aggregate target is an ordinary table with no primary key, and Trellis rejects
it as a source like any other. See [instance-identity](instance-identity.md)
for the cross-instance rules.

## Granularity

Granularity determines the target's primary-key space — what a single target row
represents relative to its source row(s). Trellis supports three.

### 1-1

Exactly one target row per source row, inheriting the source's primary key.
Insertion/deletion maps 1-1. Always a single source table: deriving a target
from a key-to-key join of two tables is a cross-join, not 1-1, even when the
join is one-to-one in practice.

### Aggregate (`GROUP BY`)

Each distinct combination of grouping values produces one target row, matching
the equivalent `GROUP BY` query against the source. The primary key is the tuple
of grouping columns; many source rows can map to one target row, and adding,
removing, or changing a source row can insert, delete, or update a target row.

Grouping-key columns may be referenced directly; any other source column must be
wrapped in exactly one of `SUM`, `AVG`, `MIN`, or `MAX` (numeric-only). `COUNT(*)`
counts rows in the group (#75); `COUNT(<column>)` is not yet implemented (see
ADR-0004).

```
TRANSFORM order_totals FROM order_line_items GROUP BY order_id
SELECT
  order_id AS order_id,
  SUM(amount) AS total_amount,
  AVG(amount) AS avg_amount,
  MIN(amount) AS min_amount,
  MAX(amount) AS max_amount
```

**Throughput depends on how many groups a batch touches, not on how few there
are.** Fewer groups do not buy more headroom, but they don't cost any either.
Issue #277 measured a single `SUM`/`COUNT(*)` aggregate offered 400k rows/sec
from 8 drain workers. It folded the same ~85k rows/sec at 10, 100 and 400
groups. At that end the cost is per source row (the claim-time fold and
decoding each staged row image are the largest sampled costs), and hot target
rows aren't what limits it. The shape that hurts
is many distinct groups receiving writes at once: 56–69k rows/sec at 4,000
groups, and ~2k rows/sec at 40,000. At 40,000 groups, one drain worker folded
more than eight did. Every group a batch touches costs an existence
probe against the source table, run while that group's target row is locked.
The probe can't use an index on the `GROUP BY` column, so drain workers whose
batches share groups queue behind each other for those rows. If you're planning
an aggregate with tens of thousands of groups written concurrently, benchmark
it first (`benchmark group-contention --groups <n>`).

### Cross-join

The primary-key space is the join of two source tables, mirroring the rows a
`JOIN` returns. Each unique pairing of source primary keys that satisfies the
join condition produces one target row, keyed by the pair `{a.pk, b.pk}` — both
kept because one row on either side may match many on the other. Source columns
are referenced through the side they come from (`a.column`, `b.column`). The
join type is chosen per transform:

* `INNER` — only matching pairs produce a row.
* `LEFT` / `RIGHT` — every row on the retained side produces a row even with no
  match; the unmatched side's columns (and its half of the key) read null.

## Relationships

Cross-join changes a table's granularity. Often you instead want to keep a
table's own granularity and merely *enrich* it with columns looked up from a
related table — order lines decorated with `product.category_name`.

A relationship is a named, directed link from one table to another, defined by a
join key (`order_line_items.product_id -> products.id`) and declared as its own
reusable statement. Either endpoint may be a source table or a transform
target, 1-1 or aggregate. A source-table endpoint needs a primary key (or
`REPLICA IDENTITY USING INDEX`), like any table Trellis reads over logical
replication. A target endpoint needs neither: Trellis's own writes to it reach
the relationship directly (issue #375), so it must be `live` when the
relationship is declared: its initial build writes it outside that path. Both
endpoints are written as bare table names and resolved once, through the
declaring connection's `search_path`; the relationship stays pinned to the two
tables found then, even if a same-named table later appears earlier on some
connection's `search_path`. `trellis status` shows each endpoint's schema. The
referencing table keeps its own primary
key and granularity — a 1-1 table stays 1-1 — and calculated fields on it
reference the related table's columns
through paths whose head is the relationship name. How depends on **cardinality**:

* **To-one** (the to-side join key is a primary key or `UNIQUE`): at most one
  related row. Referenced as a bare path (`product.category_name`); null if no
  related row matches.
* **To-many** (the to-side join key is not unique): many related rows may match.
  Referenced only when wrapped in exactly one aggregate (`sum(comments.word_count)`),
  computed over the related rows like a `GROUP BY` aggregate.

See [0006-relationships](decisions/0006-relationships.md) for the full design and
[0005-source-schema-is-user-owned](decisions/0005-source-schema-is-user-owned.md)
for how the cardinality/uniqueness prerequisites are validated (never imposed) on
the user's source schema.

## Calculated Fields

Every target table has a set of **calculated fields** — non-key columns populated
by a formula rather than copied from a source column. A formula may reference:

* Source columns feeding the calculated row, following the shape its granularity
  requires (see [Granularity](#granularity)).
* Related-table columns through a declared relationship — a bare path for to-one,
  wrapped in an aggregate for to-many (see [Relationships](#relationships)).
* Other calculated columns on the same target table.

Formulas may only use **immutable** functions and operators — those whose output
depends solely on their inputs. Anything depending on database state outside the
row (current time, timezone, collation, session settings, random values) is
disallowed: incremental maintenance requires that re-evaluating a formula on
unchanged inputs always yields the same result.

### Chaining and cycle detection

Calculated columns may reference each other, letting users build derivations out
of small named steps (a `margin` column defined in terms of `revenue` and `cost`).
Transforms also chain: a target table may itself be a source for another —
aggregated, cross-joined, or referenced through a relationship. Trellis builds a
dependency graph spanning columns within a table and tables across chained
transforms, and evaluates in dependency order.

A transform can only chain off a target once that target's own transform is
`live`. Defining it while the upstream is still building (for example, a plain
1-1 target whose chunked backfill hasn't finished) is refused with
`TransformNotLive`: wait for the upstream to go live, then define the chained
transform. Each write to a target reaches the transforms reading it inside the
same transaction as the write; a target is never part of the CDC publication
itself, not even one that is a relationship endpoint. That in-transaction hand-off
is why an aggregate target, which has no primary key, can be chained off at all
(see [Source tables](#source-tables)). It does not cross into another instance.

**Cycles are rejected at definition time across the whole graph.** A definition
that would introduce a cycle, directly or transitively, is invalid and rejected
before it runs, keeping evaluation order well-defined.

## Partial data

Any target table, regardless of granularity, may be defined over a *subset* of
its source rows via a row-level predicate. Like a partial index, materializing
only a narrow, high-value slice keeps the write load small; excluded rows never
enter the target or incur maintenance cost.

## Target tables are Trellis-owned

A target is materialized as a plain table in the target schema, and reading it
is the whole point: query it, join it, index it, grant it to whoever needs it.
Its contents and shape, though, belong to Trellis
([0014-pause-and-drop-a-transform](decisions/0014-pause-and-drop-a-transform.md)).
Trellis assumes it is the table's only writer. A change made behind its back is
not corrected, and whether it reaches anything chained off the table is
undefined. Treat a target as read-only, and specifically:

* **Do not insert, update or delete rows.** An edited row stays wrong until the
  next event that recomputes it, and a row added by hand is accounted for by
  nothing. `self_check`
  ([0013](decisions/0013-self-check-production-recompute-audit.md)) reports the
  divergence; it does not repair it. `PAUSE` then `RESUME` rebuilds the table
  from source.
* **Do not add, drop, rename or retype columns.** Trellis addresses its columns
  by name and type. Redefining the transform is how a column changes
  ([Changing a definition](#changing-a-definition)).
* **Do not change the key constraints or the replica identity.** A 1-1 target's
  primary key and an aggregate target's unique grouping constraint are how
  Trellis addresses a row when it upserts or deletes it. The replica identity
  matters once the table is in a publication, which is the case for a target
  that another instance reads as a source:
  `REPLICA IDENTITY NOTHING` makes Postgres refuse Trellis's own updates.
* **Do not `TRUNCATE` or `DROP` the table yourself.** `DROP TRANSFORM` removes
  the table and its definition together, and refuses while another definition
  still chains off it. A hand `TRUNCATE` leaves the table empty until the
  transform is paused and resumed.
* **Expect a partial table while `backfilling`, and after `RESUME`.** The
  [status](#status) says when the table is complete; a reader that needs
  completeness checks it.

Indexes, and grants to other roles, are yours to add. They live and die with the
table, so `DROP TRANSFORM` takes them with it. Another Trellis instance may read
a 1-1 target as a source, exactly as it would any table with a primary key. An
aggregate target may not be read that way (see [Source tables](#source-tables)).

## Status

Every defined transform carries an observable **status**:

* **`waiting_to_backfill`** — defined, but pre-existing source rows not yet read.
  Every new transform starts here: defining one returns before any source row
  is read ([data-flow — Capturing a table's existing rows](data-flow.md#capturing-a-tables-existing-rows)).
* **`backfilling`** — those rows have been read and the target is being built.
* **`catching_up`** — the build is done and live changes are applied, but the
  target may still be missing changes made while it was building. A catch-up
  re-reads the source and takes it `live`. A `live` transform comes back here
  for its own catch-up after an `ALTER TRANSFORM` adds columns or a
  quarantined column is resumed.
* **`live`** — the steady state: once a transform reports `live`, awaiting a
  watermark token taken after a commit guarantees its target reflects that
  commit ([embedding](embedding.md)).
* **`quarantined`** — broken and no longer maintained (the quarantine fuse tripped);
  resuming re-runs the backfill, returning it to `waiting_to_backfill`.
* **`paused`** — frozen deliberately, by an operator's `PAUSE` rather than by the
  fuse. The same frozen state `quarantined` is, reached by the other trigger: the
  target stops being written to and holds its current, now-stale value. Resuming
  likewise re-runs the backfill from `waiting_to_backfill`. Trellis also pauses
  transforms itself when the replication slot feeding them is lost, for
  example after the source database is restored from a backup. It logs which
  transforms it paused and why until each one is resumed
  ([intake failure modes](staging-and-claiming/01-intake-and-lsn-confirmation.md#failure-modes)).

An application can list defined transforms and read each one's status — enough to
tell a newly-defined transform is still populating, without a metrics pipeline.

A transform can sit in `waiting_to_backfill` while a long-lived cluster
transaction holds the backfill's fence open — a safe wait, explained with its
remedy under
[observability — Transform status lifecycle](observability.md#transform-status-lifecycle).

## Changing a definition

Every definition-changing operation is one statement of the same grammar a
definition itself is written in, run through one entrypoint —
`Trellis::apply(text)`, or `trellis apply '<statement>'` from a shell:

```text
TRANSFORM <target> FROM <source> [GROUP BY <keys>] SELECT <fields> [WHERE <predicate>]
RELATIONSHIP <name> FROM <from_table>.<fk_col> TO <to_table>.<pk_col>

PAUSE  TRANSFORM <target>[.<column>]
RESUME TRANSFORM <target>[.<column>]
DROP   TRANSFORM <target>
DROP   RELATIONSHIP [<schema>.]<from_table>.<relationship_name>
```

**Addressing.** A transform is named by its bare target table, and a dotted
address means `<transform>.<column>` — pausing or resuming a single calculated
field rather than the whole definition. A relationship, which only `DROP` names,
is always scoped to its from-table (`posts.author`, or `blog.posts.author`),
because a relationship name is unique only there. Two same-named tables in
different schemas are different from-tables, so `blog.posts` and `shop.posts`
can each declare an `author`; when both do, the bare `posts.author` is refused
as ambiguous and the address must name the schema.

**Semantics** are covered by
[0014-pause-and-drop-a-transform](decisions/0014-pause-and-drop-a-transform.md).
In short: `PAUSE` and `DROP` are idempotent; `RESUME` rebuilds by a fresh
backfill rather than catching up on changes that happened during the pause;
`DROP` removes the target table's data along with the definition, and is refused
— naming the blockers — while another registered definition still chains off the
subject, so a chain is retired from the leaves inward. A relationship with the
target at either end counts as one.

**Pause and resume apply to transforms only.** That list of statements is
complete — there is no `PAUSE RELATIONSHIP`/`RESUME RELATIONSHIP`. A
relationship is a reusable *component* of a transform, not something that does
work of its own, so it has nothing to suspend; pausing the transform that uses
it is what stops that work. Retiring a relationship is meaningful, though — it
is a definition — so `DROP RELATIONSHIP` exists.

`DROP TRANSFORM <target>.<column>` is likewise not a statement: removing one
calculated field is an `ALTER TRANSFORM` operation, not a drop.

## Scope

This document describes the logical model only, not the physical storage or
timing of derived data; see [data-flow](data-flow.md) for how changes propagate
and [0002-async-data-flow](decisions/0002-async-data-flow.md) for why that flow
is asynchronous.
