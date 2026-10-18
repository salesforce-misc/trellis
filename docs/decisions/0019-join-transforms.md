---
status: draft
date: 2026-10-17
deciders: Michael Ries
---

# JOIN Transforms

The queries most worth precomputing aggregate many fine-grained rows (facts)
across attributes that live in other tables (dimensions): revenue by customer
region, line items by product category. A transform that reads one table can't
express them. Relationships ([ADR-0006](0006-relationships.md)) reached a second
table by reading it live while applying the first, and a fact write concurrent
with a dimension change shares no lock with it, so the two can each miss the
other. Keeping that exact needed per-row state and a growing set of
special-case rules.

This ADR adds JOIN transforms:

```text
TRANSFORM <target>
FROM <a> [INNER | LEFT] JOIN <b> ON <a>.<col> = <b>.<col> [AND ...]
GROUP BY <columns of a and/or b>
SELECT <group columns>, <aggregates over a and/or b>
```

It settles their scope and semantics, the hidden state that keeps them exact
without per-row state, the locks that make that state transactional, how they
build, truncate, chain and read partitioned sources, and what readers see. They
replace relationships, which leave Trellis. It extends
[ADR-0002](0002-async-data-flow.md)'s group-level basis, which every structure
here is maintained by, and the grammar of
[ADR-0004](0004-transform-definition-grammar.md). Three or more tables,
`RIGHT`/`FULL` joins, non-equi joins, a join with no `GROUP BY`, and `WHERE` are
out of scope.

## Decisions

### A JOIN transform is two tables, an equi-join and a `GROUP BY`

The rules are strict where loosening one later is additive and tightening one
would break a stored definition, which is stored as text and re-parsed.

- **Sides.** The two sides are two tables with different bare names, each a
  captured table with a primary key, a partitioned root, or another transform's
  target (which needs no primary key). A side is named by its bare table name;
  there are no table aliases, so two sides sharing a bare name (a self-join, or
  `shop.orders JOIN archive.orders`) are refused.
- **Side qualification.** Every source column is written `<side>.<col>`, in
  `ON`, `GROUP BY` and fields alike; a bare name means a field. Resolving a
  bare name to whichever side has it would let a column later added to the
  other side change or break a stored definition. A dotted name whose head
  isn't a side is an unknown table qualifier: a JOIN transform has no
  relationship paths.
- **Join kind.** `JOIN` and `INNER JOIN` are `INNER`; `LEFT JOIN` and
  `LEFT OUTER JOIN` are `LEFT`, and `a` is the side `LEFT` keeps. A definition
  stores and renders them as `INNER JOIN` and `LEFT JOIN`. Exactly one join.
- **`ON`.** One or more equalities joined by `AND`, each `a.col = b.col` (in
  either order) with one column from each side, and no column pair repeated, so
  composite keys such as `(tenant_id, id)` are admitted. `P`, `M` and `J` key on
  the tuple of join columns, in `ON` order. A row with a `NULL` in any join-key
  column never matches, so a `LEFT` row with no match lands in the group with a
  `NULL` `gb`.
- **Join-key types.** Each column pair has the same type, type modifier and
  deterministic collation, with no casting or widening, from the same set of
  text-stable types any join key admits, so that grouping by the key and
  joining on it agree on equality. A retype of one side pauses the transform
  until the other side matches.
- **Cardinality.** The join may be many-to-many: `b`'s join key needn't be
  unique, and `M` counts multiplicity, so the results match SQL's fan-out.
- **`GROUP BY`.** Required, with at least one key, from either side. Each key
  becomes a target column named after its bare column name, so two keys with
  the same bare name (`orders.region`, `customers.region`) are refused; a 1-1
  transform that renames the column on one side is the remedy.
- **`WHERE`.** `TRUE`-only, as on every transform; a future `WHERE` applies
  after the join, as in SQL.

What's admitted is the shape the design below is proven exact for. Refused at
define, by name: two sides sharing a bare table name; `RIGHT` (swap the sides
and use `LEFT`), `FULL`, `CROSS`, a comma join and a second `JOIN`; an
expression key, a non-equality condition, `OR`, a one-side predicate, an
equality between two columns of one side, `USING` and `NATURAL`; and a join
with no `GROUP BY`.
[Supported sources and targets](../transforms.md#supported-sources-and-targets)
lists each refusal and its remedy. A join with no `GROUP BY` produces one row per
matching pair, which is per-row state by definition, and a non-equi join (a
validity-date range, say) doesn't decompose by key at all.

### The admitted aggregates are the ones partials keep exact by deltas alone

A JOIN transform admits exactly the aggregates a plain aggregate admits under
ADR-0002's group basis: `COUNT(*)`, `COUNT(col)`, and `SUM(col)` and `AVG(col)`
where `col` is `int2`, `int4`, `int8` or `numeric(p,s)` with a declared scale.
An argument may be a column or an expression over one side's columns, as long
as its result type is admitted: it becomes a partial on that side, kept by
`f(new) − f(old)` per change exactly like a column. An argument that mixes both
sides (`SUM(a.x + b.y)`) is refused at define. A field may compose admitted
aggregates (`COALESCE(SUM(a.x), 0)`), computed from the target group's
partials whenever its row is written. Result types match Postgres:
`count` is `bigint`, `sum` over `int2`/`int4` is `bigint`, `sum` over
`int8`/`numeric` is `numeric`, and `avg` is `numeric`. Every other aggregate is
refused at define, as on a plain aggregate.

Each admitted aggregate is a function of a few per-group partials (the row
count, a non-null count, a `numeric` sum) that signed deltas keep exact, so
maintaining one never reads a source table. `numeric` must declare its scale
because Postgres displays a `numeric` sum at the scale of the most precise value
in the group: a running sum would keep `1.500` after its most precise row left,
where Postgres shows `1.5`. A `numeric(p,s)` column stores every value at scale
`s`, so a running sum and every product below carry exactly the scale Postgres
shows. A product across the sides (revenue as quantity times a dimension's
price) decomposes the same way but isn't admitted: the grammar has no `*` or
`/`, and an argument mixing the sides is refused.

### Each side is aggregated before the join, and the target is their combination

The transform keeps four hidden structures, and its target is the visible face
of the last:

- **`P(ga, k)`**: `a GROUP BY (a's group columns, join key)`, with `a`'s
  partials: the row count `n`, and a non-null count and `numeric` sum per
  aggregated column of `a`.
- **`M(k, gb)`**: the same for `b`, keyed by `(join key, b's group columns)`.
- **`J(k)`**: one row per join key, the lock for that key, holding
  `matches = Σ M.n` over the key.
- **`T(ga, gb)`**: the target's groups, holding the joined partials
  `Σ_k P[ga,k] ⊗ M[k,gb]`.

`P`, `M`, `J`, `T`'s partials and the pair's truncate lock row belong to one
definition, never shared with another, and live in hidden tables in the
instance's catalog schema, named by definition id, as ADR-0002 places a plain
aggregate's partials. The target is a
plain table holding only the declared columns, and each page writes a group's
hidden partials, then its visible row.

The product `⊗` multiplies each side's partials by the other side's row count,
because each `a` row in a cell joins every `b` row in the matching cell:

| Target partial | Matched term `P ⊗ M` | Unmatched term `P ⊗ U` (`LEFT` only) |
|---|---|---|
| row count | `P.n · M.n` | `P.n` |
| non-null count of `a.x` | `P.count_x · M.n` | `P.count_x` |
| sum of `a.x` | `P.sum_x · M.n` | `P.sum_x` |
| non-null count of `b.y` | `P.n · M.count_y` | 0 |
| sum of `b.y` | `P.n · M.sum_y` | 0 |

For `LEFT`, a `P` cell whose key has `matches = 0`, or whose key is `NULL`, adds
`P ⊗ U` to the group with a `NULL` `gb`, the row `LEFT JOIN` produces for an
unmatched fact. A `b` row with a `NULL` key joins nothing, so it has no `M` cell
and no `J` row. `COUNT(*)` is the row count, `COUNT(col)` its non-null count,
`SUM` the sum (`NULL` while the non-null count is 0), and `AVG` the sum over the
non-null count.

`P` and `M` are each a single-table aggregate, so ADR-0002's group basis keeps
them exact against their own source, with no per-row state. `T` is a function of
`P`, `M` and `J`, all of which Trellis owns, never of a live read of `a` or `b`,
so keeping `T` equal to `Σ_k P ⊗ M` is ordinary transactional bookkeeping under
the three rules below.

The state is the number of distinct `(ga, k)` in `a` plus distinct `(k, gb)` in
`b` plus distinct `k`, plus the target. For a 100M-row fact table joined to a
50k-row dimension on its key and grouped by a dimension attribute with 500
values, I measured 51 MB in all, against 15 GB for one materialized row per fact.

### A writer locks `J[k]` `FOR UPDATE`, reads the other side after the lock, and writes `T` in its own transaction

- **JR1, lock order.** Every writer of `P` or `M` (a page, a build step, a cell
  recompute, a leaf correction) locks `J[k]` `FOR UPDATE` for every non-null
  key it touches; a key with a `NULL` in any column joins nothing and takes
  no `J` row and no lock. A writer creates the `J` rows its keys lack with
  one sorted `insert … on conflict do nothing`, then locks all of them with
  one sorted `select … for update`. Every writer takes its locks in one
  order: build-chunk rows, the pair's truncate lock row, `J` rows sorted by
  key (by the tuple of join columns), `P` and `M` cells sorted, then `T`
  groups sorted, each class in one sorted statement.
- **JR2, read after the lock.** A writer reads the other side's partials and
  `J.matches` only in statements after its `J` lock statement. Pages and builds
  run under `READ COMMITTED`, so each of those statements sees every writer the
  lock waited for.
- **JR3, one transaction.** An `a`-side writer adds `ΔP ⊗ M`, plus `ΔP ⊗ U`
  where `matches = 0`, to `T` in the same transaction as its change to `P`. A
  `b`-side writer adds `P ⊗ ΔM`, and when `matches` crosses zero moves the key's
  `P ⊗ U` into or out of the `NULL` `gb` group, in the same transaction as its
  change to `M`.

The race that defeats relationships can't arise: neither side ever reads the
other's source table, only its applied partials, and only under `J[k]`, which
every writer of either side holds exclusively while it reads and writes.

I modelled this against real PostgreSQL 15 and 18, maintaining six targets at
once over shared `P`/`M`/`J` (`LEFT` and `INNER`, grouped by both sides, by `b`
only and by `a` only). 815 deterministic interleavings, each confirmed to reach
its interleaving by `pg_blocking_pids`, were exact on both versions against the
`JOIN … GROUP BY` oracle, and so was a randomized soak of about 535k writes.
The model's `a` side locked `J` `FOR SHARE`; `FOR UPDATE` only excludes more,
and every build and page of my cost runs, which used it on both sides,
matched the oracle. Each
rule is necessary: skipping the `a`-side lock, reading `M` before the lock,
reading the live `b` table, writing `T` in a second transaction, and taking the
locks unsorted each produced wrong answers or deadlocks. The `a`-side lock
matters even where two writers' `T` groups don't overlap: a fact in a new `ga`
concurrent with a second dimension row in a new `gb` loses the cross term
without it.

Why not lock `J` `FOR SHARE` from the `a` side, since facts on one key don't
conflict with each other? It gained 35% fact-side throughput on one shape and
nothing on another, created about one multixact per page, and starved the
dimension side: a concurrent dimension worker finished 1–2 pages a run, against
61 under `FOR UPDATE`, while `FOR UPDATE` kept 81–91% of the fact side's
throughput.

### A page takes one lock mode per key, and never upgrades

A page can carry changes of both tables. It locks each of its keys once, in its
one sorted `J` statement, and adds `ΔP ⊗ M_new + P_old ⊗ ΔM` plus the `LEFT`
terms. With `FOR UPDATE` on both sides every key's mode is the same, so the rule
holds by construction. A page that locked a key `FOR SHARE` and later upgraded it
deadlocked against a page doing the same.

The strict form of the `T` write is kept: a nonzero delta for a `T` group the
writer didn't lock is an error, not a silent insert, so a gap in the computed
lock set fails loudly instead of losing a delta.

### `TRUNCATE` of either side excludes writers of both, behind a two-sided barrier

Every page, build step and recompute of the transform takes the pair's truncate
lock row `FOR SHARE`, and a truncate page takes it `FOR UPDATE`, because a
truncate of either table rewrites structures the other side's writers use.
`TRUNCATE a` empties `P` and `T`. `TRUNCATE b` empties `M`, sets every
`J.matches` to 0, and rebuilds `T` from `P`: `P ⊗ U` for `LEFT`, nothing for
`INNER`. Each table keeps its own truncate floor, as ADR-0002's truncate rule
gives a single source.

The barrier is two-sided: a page waits for every earlier truncate page of either
table, and a truncate page waits for every earlier page of its own table. Without
it, 79 of 80 soak runs ended wrong; with it, none of 80 did. The barrier assumes
sealed segments are planned in order. A planner that planned segment s+1 before
s lost rows across a `TRUNCATE` in 7 of 220 runs with the race widened, so
sealed segments are planned strictly in order, or the barrier is evaluated when
a page is claimed rather than when it is planned.

### Sum partials are `numeric` everywhere

Every sum partial in `P`, `M` and `T` is `numeric`, whatever the argument's type.
`P.sum_x · M.n` overflows `bigint` at sizes where Postgres's own `sum()` over the
join, computed in `numeric`, doesn't, and an overflow inside a page would fail
the page rather than the query. The visible result is cast to Postgres's result
type, so a `sum` that overflows `bigint` fails where Postgres's would.

### Readers see a `T` group only while its row count is above 0, and exact values only after convergence

`P`, `M`, `J` and `T`'s partials are hidden from readers. The target shows a
group only while its row count is above 0, with the aggregates computed from its
partials. Pages apply in parallel and can commit out of order, so before
`await_converged` a visible group can hold a combination no source state
produces: `count(x) > count(*)`, a negative sum, a far-off `AVG`. In the model,
about half of mid-run reads under fast writers showed such a row, and every read
taken once all committed changes were applied was exact (6,765 of 6,765). After
`await_converged`, values are exact. That contract, shared with plain
aggregates, is stated in [data flow](../data-flow.md#reading-derived-data).

### A side may be another transform's target, through the target change feed

A side that is a transform's target is read from that target's change feed:
the exact old and new images per changed key, in write-token order, which
ADR-0002's mutation seam stages for any target with an aggregate or JOIN
reader. The side consumes them as it consumes captured images. An upstream build
or rebuild bypasses the seam, so it rebuilds the JOIN transform. A JOIN
transform's own target feeds its readers the same way as any aggregate target.

### Either side may be partitioned when its partial key holds every partition-key column

A partitioned root is admitted as a side under
[ADR-0017](0017-partitioned-sources.md)'s group-key rule, applied to that side's
partial key: `(a's group columns, join key)` for `a`, `(join key, b's group
columns)` for `b`. When it holds every partition-key column at every level, each
`P` (or `M`) cell lives in one leaf, so a leaf `DROP`, `DETACH`, `TRUNCATE` or
`ATTACH` is corrected by recomputing the cells that route to the leaf through
the `J`-locked writer, which carries the change into `T`. Otherwise define
refuses, naming the column. The join key counts toward the rule, so a fact table
partitioned on the column it joins by needs nothing more in its `GROUP BY`; one
partitioned on its own id is refused.

### A build fills `P` and `M` from each source and `T` from them

A build fills `P` and `M` with ADR-0002's group-basis build, each a chunk at a
time, and each chunk carries its cells' contribution into `T` under the same
`J` locks and in the same transaction as a page, so pages apply from the first
chunk. In the model, chunked builds of both sides were exact in every chunk
order and against pages, recomputes and the other side's chunks, on both
versions.

[OPEN: Chunked builds cost per (chunk, cell) pair, not per row: with many cells
they were 3–4x slower than one materialized row per fact, while one `GROUP BY`
per structure was 2–12x cheaper than chunks (84 s against 412–509 s on a 100M-row
fact table grouped by day and dimension attribute). Which locks make filling `P`
and `M` by one `GROUP BY` each, and `T` from `P ⋈ M` in one statement, exact
while both sides take writes, and whether pages can apply during that fill,
awaits the build-path correctness model in #1097, and the build itself is
decided in #1109. Recommendation: build that way if the model shows it exact,
and keep chunks otherwise.]

### Define warns when `P` approaches the fact table's size

When the join key is nearly unique per fact, `P` has about one cell per fact
row: the state is the joint distribution of the query, and it stays exact, but
the cost advantage is gone. On a 10M-row dimension with 10 facts per key,
grouped by day and attribute, `P` held 98.7M cells for 100M facts and fact-side
pages were slower than one materialized row per fact (13.4 s against 11.1 s per
100k changes). Define estimates `P`'s cell count from the planner's statistics
and warns, naming the estimate, when it approaches the fact table's row count.
It doesn't refuse: the transform is correct, only expensive.

### `self_check` audits JOIN targets and their hidden structures

`self_check` compares the target with the `JOIN … GROUP BY` query, and audits
the invariants `P = GROUP BY a`, `M = GROUP BY b`, `J.matches = Σ M.n` and
`T = Σ_k P ⊗ M`. It detects and reports, and never repairs; the scope and
semantics are in [ADR-0013](0013-self-check-production-recompute-audit.md).

### JOIN transforms replace relationships

Relationships are removed rather than kept beside JOIN transforms. Grouping by a
relationship path, their main use, is a JOIN transform grouped by the
dimension's column. Enrichment at the fact's own grain (copying a dimension
attribute onto every fact row) is not served: its output is per fact row, which
is per-row state by definition, and readers join the dimension at query time
instead.

Why not keep both? Relationships carry the per-row state and the propagation
rules this design exists to avoid, and keeping them would ship two ways to reach
a second table with different correctness rules.

## Example

`orders(id, customer_id, amount numeric(12,2))` has 100M rows, and
`customers(id, region)` has 50k.

```text
TRANSFORM revenue_by_region
FROM orders LEFT JOIN customers ON orders.customer_id = customers.id
GROUP BY customers.region
SELECT
  customers.region AS region,
  COUNT(*) AS order_count,
  SUM(orders.amount) AS revenue,
  AVG(orders.amount) AS avg_order
```

`P` is keyed by `customer_id` alone, since `orders` contributes no group column,
and holds each customer's order count and `numeric` sum of `amount`. `M` is keyed
by `(id, region)` with a row count per cell, and `J` has one row per customer id.
An order insert locks `J[customer_id]`, adds to its `P` cell, and adds that delta
to the customer's region in `T`, or to the `NULL` region if no customer matches.
A customer moving from `EMEA` to `APAC` locks the same `J` row, reads the
customer's `P` cell, and moves its count and sum from `EMEA` to `APAC` in `T`:
one cell, however many orders it summarises. The target has one row per region,
and orders with no customer appear under a `NULL` region, as the `LEFT JOIN`
query shows them.

## Consequences

- Aggregates across two tables are kept exact with state proportional to the
  distinct keys and groups, not the fact rows. In the shape that motivates them
  (a dimension joined on its key, grouped by its attributes), fact-side pages
  were 3.6–19x faster than one materialized row per fact with 6–59x less WAL, and
  a dimension change was 100–5,000x cheaper.
- Shapes whose join key is nearly unique per fact, or that group by a fine-grained
  `a` column, keep state near the fact table's size and lose that advantage.
  They are admitted with a warning.
- A dimension change on one key fans out to every `P` cell of that key under one
  exclusive `J` lock; with a fine-grained `a` group column that is many `T`
  writes in one transaction.
- Every writer of the pair takes the truncate lock row `FOR SHARE`, which
  creates a multixact whenever two overlap.
- Zero `P` and `M` cells are collected once the drained prefix passes them, as
  ADR-0002's zero groups are; on a hot key, uncollected zero cells made
  fact-side pages 16x slower.
- `J` rows are never collected while the transform runs. JR1's insert-then-lock
  pair of statements is exact only because no `J` row is ever deleted: a
  writer queued behind a delete would carry on holding no lock. So `J` keeps
  a row, about 40–50 bytes, for every distinct non-null key either table has
  ever held. That is small for a dimension joined on its key, and unbounded
  where the join key churns, as under retention on the joined table. `RESUME`
  rebuilds the hidden tables, which reclaims it, and so does dropping the
  transform.
- Before `await_converged`, readers can see impossible combinations in a JOIN
  target; after it, values are exact.
- The admitted aggregate set is `COUNT`, `SUM` and `AVG` over integers and
  scaled `numeric`, with arguments over one side. Each other aggregate, an
  argument mixing the sides, and the out-of-scope
  join shapes are refused until designed.
- Relationships, their reverse propagation and their dedicated tests leave
  Trellis, and a transform that copied a dimension attribute onto every fact row
  has no replacement inside Trellis.
