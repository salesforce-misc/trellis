# Defining Transforms

A transform describes how one derived (target) table is produced from one or
more source tables. Its definition is three separately-chosen pieces: the
**granularity** of the target's primary-key space, the **calculated fields**
that populate its non-key columns, and an optional **partial-data** predicate
restricting which source rows participate.

Trellis maintains these tables **incrementally** as source data changes, trading
spikey read load for a steady write load that keeps the table cheap to read (see
the README for fuller motivation).

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
reusable statement. Either endpoint may be a source table or a transform target.
The referencing table keeps its own primary key and granularity — a 1-1 table
stays 1-1 — and calculated fields on it reference the related table's columns
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

**Cycles are rejected at definition time across the whole graph.** A definition
that would introduce a cycle, directly or transitively, is invalid and rejected
before it runs, keeping evaluation order well-defined.

## Partial data

Any target table, regardless of granularity, may be defined over a *subset* of
its source rows via a row-level predicate. Like a partial index, materializing
only a narrow, high-value slice keeps the write load small; excluded rows never
enter the target or incur maintenance cost.

## Status

Every defined transform carries an observable **status**:

* **`waiting_to_backfill`** — defined, but pre-existing source rows not yet enumerated.
* **`backfilling`** — those rows are being enumerated and staged.
* **`live`** — backfill complete; tracking live changes only. The steady state.
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
because a relationship name is unique only there.

**Semantics** are covered by
[0014-pause-and-drop-a-transform](decisions/0014-pause-and-drop-a-transform.md).
In short: `PAUSE` and `DROP` are idempotent; `RESUME` rebuilds by a fresh
backfill rather than catching up on changes that happened during the pause;
`DROP` removes the target table's data along with the definition, and is refused
— naming the blockers — while another registered definition still chains off the
subject, so a chain is retired from the leaves inward.

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
