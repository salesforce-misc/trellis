# Defining Transforms

A transform describes how one derived (target) table is produced from one or
more source tables. Its definition is three separately-chosen pieces: the
**granularity** of the target's primary-key space, the **calculated fields**
that populate its non-key columns, and an optional **partial-data** predicate
restricting which source rows participate.

Trellis maintains these tables **incrementally** as source data changes, trading
spikey read load for a steady write load that keeps the table cheap to read (see
the README for fuller motivation).

## Supported sources and targets

Trellis reads tables it does not own ([0005](decisions/0005-source-schema-is-user-owned.md)),
so it validates them when a definition is applied and refuses what it can't
maintain correctly, with an error naming the table or column and the fix. These
refusals are the supported-subset contract, not bugs. Everything in the table
is checked for a transform's source, for each relationship endpoint and
to-side it reads through, and (where noted) for its target. Trellis never adds
a key, index or policy itself.

| Condition | Refused at define | If it appears after define | Remedy |
|---|---|---|---|
| A source with no primary key (or unique index standing in for one), a partitioned table, or a table in a partition or inheritance hierarchy | Yes. A statement trigger fires only for the table a statement names, so capture would miss writes made through the rest of the hierarchy. | A table that later joins a hierarchy (`ATTACH PARTITION`, `INHERIT`) is not refused. `self_check` reports a `capture` divergence (1-1 targets only; [gap 7](known-correctness-gaps.md#7-a-source-attached-as-a-partition-or-made-to-inherit-after-define)). A dropped key pauses the transforms that read it, with the reason in `status()`; a retyped one: [gap 3](known-correctness-gaps.md#3-retyping-or-re-collating-a-key-join-or-group-by-column). | Use a plain table with a primary key. After a later change, undo it, then `PAUSE`/`RESUME`. |
| An aggregate target used as a source (it has no primary key: its identity is a `UNIQUE NULLS NOT DISTINCT` constraint) | Yes, for another Trellis instance. Inside the owning instance it can be chained off, because writes to a target reach its readers in the writing transaction, never through capture triggers. | n/a | Group over a 1-1 transform, or chain within the instance ([instance-identity](instance-identity.md)). |
| Row-level security that applies to the Trellis role (#745) on a source, on a relationship's to-side, or (#765) on a target. Policies apply to a role when RLS is enabled and the role neither owns the table (directly or through membership) nor has `BYPASSRLS`, or owns it and the table is `FORCE ROW LEVEL SECURITY`. A superuser is exempt. | Yes, checked for the role that owns the ring, and for the defining role on a to-one to-side (defining reads it) and on a table that is another transform's target. | The staging worker pauses every transform that reads the table (or, for a target, the transform that writes it), with the reason in `status()`'s `capture_failure`. `self_check` reports a `capture` divergence. A login role that is never checked: [gap 11](known-correctness-gaps.md#11-row-level-security-on-a-login-role-trellis-doesnt-check). | Give the role `BYPASSRLS` (not inherited), or make it the table's owner without `FORCE ROW LEVEL SECURITY`; then `RESUME`. |
| A table a logical-replication subscription in the same database replicates into (#751) | Yes, in any sync state, including a disabled subscription. The subscription's apply worker fires only row-level triggers, so its changes never reach capture. Publishing a table to another database is fine. | Paused and reported exactly as for RLS. | Remove the table from the publication and `ALTER SUBSCRIPTION … REFRESH PUBLICATION`, or drop the subscription; then `RESUME`. |
| A nondeterministic collation (an ICU collation with `deterministic = false`) on a key column: a source primary key, a `GROUP BY` key (including a relationship path's to-side column), a relationship join column or an endpoint's primary key (#638). Also on a column passed to `STRPOS` or `REGEXP_COUNT`, directly or through `COALESCE`, another field or a relationship path. | Yes, for the definition, the relationship, and an `ALTER TRANSFORM` that adds or alters such a field. Trellis matches keys by exact text; such a collation's `=` would fold keys it keeps apart. `CHAR_LENGTH` and `OCTET_LENGTH` accept any column. | Not detected ([gap 3](known-correctness-gaps.md#3-retyping-or-re-collating-a-key-join-or-group-by-column)). | Keep the column's collation deterministic ([type-support](type-support.md#collation)). |
| Relationship join columns that differ in type, type modifier or collation, or whose type is off the join-key allowlist (#590) | Yes. Trellis never casts a join key. `integer` against `bigint`, `text` against `varchar`, and `varchar(50)` against `varchar(255)` are all refused. | Not detected ([gap 3](known-correctness-gaps.md#3-retyping-or-re-collating-a-key-join-or-group-by-column)). | Alter one column to match the other. |
| A relationship endpoint that is not keyable, has a key type off the primary-key allowlist, or (for one of this instance's targets) is not `live` (#429) | Yes. Every requirement is checked at declaration ([relationship-propagation](relationship-propagation.md#endpoint-requirements)). | A key dropped later pauses its readers; one retyped later: [gap 3](known-correctness-gaps.md#3-retyping-or-re-collating-a-key-join-or-group-by-column). | Fix the table, then declare the relationship. |
| Two `GROUP BY` keys that share a target column name (`buyer.name` and `seller.name`, or `name` beside `buyer.name`); keys can't be aliased | Yes. | n/a | Group over a 1-1 transform that selects them under distinct names (`buyer.name AS buyer_name`). |
| A field named after a 1-1 source key column (`id AS id`), or any field or `GROUP BY` column whose name starts with `__` (#566) | Yes. The target already carries the key columns, and `__` names are Trellis's hidden columns (such as an `AVG`'s running sum). `id AS order_id` is an ordinary column. | n/a | Rename the field. |
| `JOIN` | Yes (parse error). Cross-join is not supported. | n/a | Use a [relationship](#relationships). |
| A `WHERE` other than `TRUE` | Yes (parse error). Partial data is not supported ([#804](https://github.com/salesforce-misc/trellis/issues/804)). | n/a | None. |
| Chaining off a transform that is not `live` (`TransformNotLive`) | Yes. | n/a | Wait for the upstream to go `live`, then define. |

A refusal after define is a pause, not a loss: `RESUME` rebuilds the target from
source once the condition is fixed ([Status](#status)). `status()`'s
`capture_failure` carries the reason. Other ways a source can drift after
define are in [known correctness gaps](known-correctness-gaps.md).

Other writers are not affected by the subscription rule: a session with
`session_replication_role = replica` fires the capture triggers, which are
`ENABLE ALWAYS` ([stage 1](staging-and-claiming/01-capture-by-triggers.md)).
Changing a source key's collation to another deterministic one is safe, though
the rest of any build then scans the source and `self_check` reads the target
by scan rather than by index.

## Granularity

Granularity determines the target's primary-key space — what a single target row
represents relative to its source row(s). The design has three: 1-1 and
aggregate are supported; cross-join is not.

### 1-1

Exactly one target row per source row, inheriting the source's primary key.
Insertion/deletion maps 1-1. Always a single source table: deriving a target
from a key-to-key join of two tables is a cross-join, not 1-1, even when the
join is one-to-one in practice.

The target always carries the source's primary key columns, so a field can't
reuse a key column's name (see [Supported sources and targets](#supported-sources-and-targets)).

### Aggregate (`GROUP BY`)

Each distinct combination of grouping values produces one target row, matching
the equivalent `GROUP BY` query against the source. The primary key is the tuple
of grouping columns; many source rows can map to one target row, and adding,
removing, or changing a source row can insert, delete, or update a target row.

Grouping-key columns may be referenced directly; any other source column must be
wrapped in exactly one aggregate: `SUM`, `AVG`, `MIN`, `MAX`, `COUNT`,
`BOOL_AND`, `BOOL_OR`, `BIT_AND`, `BIT_OR` or `JSONB_AGG`. `COUNT(*)` counts
rows in the group, and `COUNT(<expr>)` counts the rows where `<expr>`
isn't null. Which column types each aggregate accepts is in the
[type-support matrix](type-support.md).

A grouping key can also be a to-one relationship path (`GROUP BY post.author`).
Each key becomes a target column named after its bare column, and keys can't be
aliased. An aggregate target also has hidden columns of Trellis's own, such as
the running sum behind an `AVG`. Name limits are in
[Supported sources and targets](#supported-sources-and-targets).

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

**Not supported:** a `JOIN` clause is refused with a parse error. This
section describes the intended design.

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
target, 1-1 or aggregate. A source-table endpoint needs a primary key, like any
table Trellis captures. A target endpoint doesn't: Trellis's own writes to it reach
the relationship directly, so it must be `live` when the
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

Each join column's type must be on the join-key allowlist in
[type-support.md](type-support.md) (a domain never is), and the two join
columns must have the same type, type modifier and collation, because Trellis
never casts a join key. The error names both columns and both types. Both
sides also need deterministic collations
([Supported sources and targets](#supported-sources-and-targets)).

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
`TransformNotLive`. Each write to a target reaches the transforms reading it inside the
same transaction as the write; Trellis never installs capture triggers on a
target, not even one that is a relationship endpoint. That in-transaction hand-off
is why an aggregate target, which has no primary key, can be chained off at all.
It does not cross into another instance.

**Cycles are rejected at definition time across the whole graph.** A definition
that would introduce a cycle, directly or transitively, is invalid and rejected
before it runs, keeping evaluation order well-defined.

## Partial data

**Not supported:** the grammar accepts only `WHERE TRUE`, and any other
predicate is a parse error. This section describes the intended design.

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
* **Do not change the key constraints.** A 1-1 target's primary key and an
  aggregate target's unique grouping constraint are how Trellis addresses a
  row when it upserts or deletes it.
* **Do not `TRUNCATE` or `DROP` the table yourself.** `DROP TRANSFORM` removes
  the table and its definition together, and refuses while another definition
  still chains off it. A hand `TRUNCATE` leaves the table empty until the
  transform is paused and resumed.
* **Don't let row-level security apply to the role that writes it.**
  Trellis writes a target as the role each worker connects as, and policies
  that apply to that role filter those writes: an update or delete skips the
  rows they hide, leaving them stale, and an insert fails their `WITH CHECK`.
  Trellis creates the target as the role that defines it, so that role owns
  it and is exempt unless the table is `FORCE ROW LEVEL SECURITY`. Enabling
  row-level security on a target for your application's readers is fine as
  long as the workers run as the table's owner (or a member of it) or have
  `BYPASSRLS`. See [Supported sources and targets](#supported-sources-and-targets)
  for what happens when that stops holding. A definition is also refused if
  DDL around its target's creation, such as an event trigger that forces
  row-level security on every new table, makes the policies apply to the
  defining role.
* **Expect a partial table while `backfilling`, and after `RESUME`.** The
  [status](#status) says when the table is complete; a reader that needs
  completeness checks it.

Indexes, and grants to other roles, are yours to add. They live and die with the
table, so `DROP TRANSFORM` takes them with it. Another Trellis instance may read
a 1-1 target as a source, exactly as it would any table with a primary key. An
aggregate target may not be read that way (see [Supported sources and targets](#supported-sources-and-targets)).

## Status

Every defined transform carries an observable **status**:

* **`waiting_to_backfill`** — defined, but pre-existing source rows not yet read.
  Every new transform starts here: defining one returns before any source row
  is read ([data-flow — Capturing a table's existing rows](data-flow.md#capturing-a-tables-existing-rows)).
* **`backfilling`** — those rows have been read and the target is being built.
  A plain aggregate (grouped by plain columns of a table, not of another
  transform's target, with no relationship and no `MIN`/`MAX` of text), and a
  plain 1-1 transform (no relationship, over a table rather than another
  transform's target), is built while its live changes are applied, and goes
  from here straight to `live`
  ([data-flow — The Re-derive build](data-flow.md#the-re-derive-build)).
  A `live` 1-1 transform comes back here while an `ALTER TRANSFORM` that adds
  or changes a field, or a resumed column, is rebuilt in the background
  ([Changing a definition](#changing-a-definition)).
* **`catching_up`** — the build is done and live changes are applied, but the
  target may still be missing changes made while it was building. A catch-up
  re-reads the source and takes it `live`.
* **`live`** — the steady state: once a transform reports `live`, awaiting a
  watermark token taken after a commit guarantees its target reflects that
  commit ([embedding — Reading your own writes](embedding.md#reading-your-own-writes)).
* **`quarantined`** — broken and no longer maintained (the quarantine fuse tripped);
  resuming re-runs the backfill, returning it to `waiting_to_backfill`.
* **`paused`** — frozen deliberately, by an operator's `PAUSE` rather than by the
  fuse. The same frozen state `quarantined` is, reached by the other trigger: the
  target stops being written to and holds its current, now-stale value. Resuming
  likewise re-runs the backfill from `waiting_to_backfill`. Trellis also pauses
  a transform itself when a source column it reads is renamed or dropped, with
  the reason on its status (`capture_failure`)
  ([stage 01](staging-and-claiming/01-capture-by-triggers.md#a-renamed-or-dropped-column)).

An application can list defined transforms and read each one's status — enough to
tell a newly-defined transform is still populating, without a metrics pipeline
([embedding — Poll to `live`, don't wait](embedding.md#poll-to-live-dont-wait)
has the polling pattern).

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

**`apply` only registers.** Every statement returns once the change is
recorded; none reads the transform's source rows. A new transform is built in the
background from `waiting_to_backfill`. An `ALTER TRANSFORM` that adds or
changes fields, and a `RESUME TRANSFORM <target>.<column>`, start a
*field build*: the transform goes `backfilling`, the changed fields are
computed for every new change from that moment on, and background chunks
rewrite them across the existing rows, reading the source once however many
fields changed. The transform reads `live` again once they are all done, so
poll its status before relying on the new fields
([embedding — Poll to `live`, don't wait](embedding.md#poll-to-live-dont-wait)).
Until then a row's new field can still be empty (an added field) or hold the
old formula's value (a changed or resumed one). A field whose formula reads a
source column the source's capture doesn't record yet waits, paused, until
capture records it before its build starts.

The one read inside the call is of a to-one relationship's to-side table, not of
a transform's source: declaring the relationship creates its projection and
seeds it from the to-side rows, and defining a transform that reads a column
through it widens that projection, in the same transaction
([embedding](embedding.md#what-the-staging-worker-needs-from-the-database)).

## Scope

This document describes the logical model only, not the physical storage or
timing of derived data; see [data-flow](data-flow.md) for how changes propagate
and [0002-async-data-flow](decisions/0002-async-data-flow.md) for why that flow
is asynchronous.
