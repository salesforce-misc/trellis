---
status: draft
date: 2026-10-09
deciders: Michael Ries
---

# Partitioned Tables as Transform Sources

Very large child tables are often declaratively partitioned, and they are where a
cheap per-parent count or sum pays off most. Capture is a set of statement
triggers ([ADR-0002](0002-async-data-flow.md#capture-by-statement-triggers)), and
a statement trigger fires only for the table the statement names, so triggers on
one table of a partition tree miss writes named on the others. Partition
maintenance (`ATTACH`, `DETACH`, `DROP`, a leaf `TRUNCATE`, a new leaf written
directly) also changes the parent's rows without firing any trigger on it.

The bar for supporting them: counts may be wrong for about one reconcile pass
only when Trellis then detects the change and corrects it by itself, with no
operator step, and every supported operation has a documented path that avoids
the window altogether.

This ADR extends [ADR-0002](0002-async-data-flow.md)'s capture and truncate
decisions to partitioned sources. It settles which partition trees an aggregate
or a to-many relationship can read, how capture covers them, and how the
reconcile pass detects and corrects partition maintenance. Partitioned targets,
inheritance hierarchies, a partition named as a source, and trees of more than
1,000 leaves are out of scope.

## Decisions

### A partitioned root is a source when its group key holds every partition-key column

A declaratively partitioned root (`RANGE`, `LIST` or `HASH`, any depth, with or
without a `DEFAULT`) may be an aggregate's `FROM` or a to-many relationship's
to-side, on PostgreSQL 15+, when its `GROUP BY` (or the relationship's to-side
join key) contains every partition-key column at every level. Targets stay plain
tables.

With every partition-key column in the group key, each group lives in exactly
one leaf, so a lifecycle change touches only the groups routing to one node and
the catalog says which those are. Without it, a `DROP` needs a full source
rescan (hours at multi-TB per retention drop) or per-leaf group bookkeeping. The
motivating shape, `HASH` on the parent id with 32 leaves, satisfies it, so the
restriction costs little and has no fallback.

I tested every decision here against real clusters with Trellis's generated
capture and an oracle diff after each step: the trigger and lifecycle behaviour
on PostgreSQL 14 through 18, routing and bound evaluation on 15 through 18, and
the detach check, the cheap cleanup forms, to-many targets and the cleanup race
on 16 through 18. Apart from PostgreSQL 16 refusing to attach a table that still
carries capture triggers, no result differed by version.

### Trees of up to 1,000 leaves are supported; larger ones are unsupported, not refused

No build statement prunes: each one plans, locks and opens every leaf, about two
locks per leaf and 19–27 ms of planning at 1,000. That is acceptable at this
size and is tested there. The default lock table runs out for one statement
somewhere above that, so a larger tree waits until someone needs it.

### Every node carries capture, staging under the root's identity

The root, each middle and each leaf carry the five capture triggers, all calling
the root's one function set and staging under the root's identity.

I ran 27 DML scenarios. Triggers on the parent missed writes aimed at a leaf,
triggers on the leaves missed writes through the parent, and root plus leaves
missed writes named on a middle. With every node, each change was captured
exactly once, and a row moved between partitions by an `UPDATE` through a
parent staged as a matched delete and insert. The generated bodies are
name-based, so one function set serves leaves whose attnums or dropped columns
differ. Capture cost the same per write as on a plain table at 10, 100 and
1,000 leaves.

### Up to 100 nodes install in one transaction; a larger tree installs node by node

A larger tree installs the root first, then one node per transaction.
Installing in one transaction blocked every writer for about 0.3 ms per leaf:
34–44 ms at 100 leaves and 271–305 ms at 1,000, and the existing 50 ms
`lock_timeout` bounds the wait for the lock, not how long it is held. Node by
node, the worst writer commit I saw was 7–56 ms at every size, for about twice
the total install time. 100 nodes keeps the one-transaction stall within the
roughly 50 ms writer wait a plain table's install already accepts.

### Capture stages nothing for a table that has left the tree

Each capture function of a partitioned source first compares
`pg_partition_root(TG_RELID)` with the source root and stages nothing when they
differ. The pass drops capture triggers from any table that has left the tree,
as cleanup only.

A table that leaves the tree keeps its triggers until the pass, and a write to
it in that interval must never count. A `pg_inherits` lookup in the trigger is
stale for a `REPEATABLE READ` or `SERIALIZABLE` writer whose snapshot predates
the `DETACH`: I saw it stage the write. `pg_partition_root` reads the catalog
snapshot instead: it returns NULL after a plain `DETACH` and the leaf itself
while it is detach-pending, takes no locks, doesn't block behind uncommitted
DDL, and cost about 1 µs per statement against about 80 µs for the capture
statement itself. While the source root is attached under another table the
check also drops the source's own writes, which the rebuild on resume repairs,
so I chose it over an ancestor walk. A writer with an old snapshot can still
write through the parent into a detach-pending leaf, which is why that leaf's
cleanup waits until the detach completes.

### The reconcile pass detects tree changes by two persisted fingerprints

The pass computes a lifecycle fingerprint (each node's `relpartbound`,
`inhparent`, `inhdetachpending` and `pg_inherits.xmin`) and a compliance
fingerprint (the source's own identity and its `pg_inherits` rows in both
directions), both stored with the definition. A deep check runs when either
changes, when none is stored, and after a restart.

The two cost 2.3–2.9 ms per pass at 1,000 leaves and gave no false positive
under DML, every `VACUUM` variant, `ANALYZE`, `CREATE INDEX` or Trellis's own
install. The lifecycle fingerprint alone misses a source that joins a hierarchy
or gains a level two deep, which the compliance fingerprint sees.
`pg_inherits.xmin` is what reveals a table detached and re-attached under the
same bound within one pass; without it, every trial of a detach, modify and
re-attach with the triggers dropped ended wrong. Persisting them means a restart
can't forget a change. Adding the source's own `pg_class.xmin` would also fire
on a `GRANT`, an owner change, a reloption, `VACUUM FULL` or `CLUSTER`, and each
firing would force a full rebuild.

Why not an event trigger, which would see DDL sooner? It needs a superuser, some
hosts don't offer one, and I found it fires neither for an interrupted
`DETACH CONCURRENTLY` nor for `TRUNCATE`. Nothing here depends on one.

### Target rows route to nodes by each leaf's recorded bound, under pinned settings

The pass records each leaf's `pg_get_partition_constraintdef` text with the key
columns' types and collations, fetching only for new or changed leaves and for a
`DEFAULT` whose siblings changed. Cleanup routes a target row to a node by
evaluating that text over the row, each group column cast to the source key
column's type and collation. Every fetch and evaluation runs under
`search_path = pg_catalog, pg_temp`, `DateStyle ISO, YMD` and `TimeZone UTC`, in
short transactions under a short `lock_timeout` that retry on a lock timeout.

The catalog loses a leaf's bound when it is dropped or detached, so the pass
records it while it can. With the cast I found 0 disagreements in about 3.4M
comparisons per version, while a `HASH` text raises on a differently typed
column. A text fetched under a non-ISO `DateStyle` errored or misrouted up to
32,981 keys. Under Trellis's session search path, three objects in `public`
silently misrouted rows: an enum in a `RANGE` bound, an ICU collation shadowing a
built-in name, and a custom operator. A `HASH` text embeds its parent's oid, so a
dropped middle's leaves can only be routed through the middle's own text. A
`DEFAULT`'s text changes with its siblings, and using a recorded one left stale
groups, so it is re-aggregated instead. Reading 1,000 texts holds 1,005 locks to
commit and blocks behind uncommitted DDL, hence short transactions and changed
leaves only.

### Each changed node is corrected in the pass that sees it, with no full rescan

- A new, attached or re-attached leaf: install capture, remove the target rows
  routing to it, then re-aggregate that leaf alone.
- A `DEFAULT` whose sibling set changed: treated as new.
- A dropped or detached subtree: remove the rows routing to its topmost gone
  node, by that node's recorded text, with no source read. A detach-pending leaf
  is cleaned up only once its `pg_inherits` row is gone and the ring has been
  folded.
- Then the no-leaf sweep below.

For a to-many target, "remove" is "recompute the routed parents" through the
normal Recompute path: no target row is ever deleted.

Deleting a leaf's routed rows, then re-aggregating it, closed the oracle diff in
every lifecycle cell I measured on `HASH`, `RANGE` and `LIST`, including direct
writes into a new leaf before its capture existed, which a "recompute the groups
found in the leaf" rule never converged on. Every window closed within 5.2 s
under load. Rows an operation adds are undercounted during the window, rows it
removes are overcounted, and writes through the parent have no window at all.
For a to-many target, deleting rows lost them for good in 180 of 189 trials;
recomputing the routed parents reached zero diff in every cell, kept parents
with no children left at a count of 0, and closed within 2.4–3.6 s.

Why not refuse the operations that open a window? Trellis can't refuse DDL
without event triggers, so it would only make the docs more alarming. Applying a
leaf's per-group totals inside the DDL's own transaction cost about 100 ms per
1M rows in a prototype and would close the window, but needs a new entry point.

[OPEN: how removing an aggregate's routed rows maps onto its ledger entries, and
how it orders against a concurrent Apply page. Recommendation: none yet.]

[OPEN: whether a definition reports `live` or build progress while a large
attached leaf is re-aggregated. Recommendation: none yet.]

### Every pass sweeps rows that route to no current leaf, by the bounds' complement

For `HASH` the sweep deletes over the missing remainders; for `RANGE` and `LIST`,
over the gaps between the current bounds, read from `pg_get_expr(relpartbound)`
and ordered by the key column's collation. A `DEFAULT` under the root means no
statement. The full routing form runs, limited to the groups the fold changed
since the last successful check, only on a pass where a recorded node is gone
and its text is unusable. The sweep runs in its own `READ COMMITTED`
transaction: the pass reads the lifecycle catalog before the constraint texts,
re-reads it in a new statement after the `DELETE`, and rolls back and retries if
the leaf set changed.

The sweep catches a leaf created and dropped between two passes, which no
fingerprint sees. Its full routing form cost 4.1–7.3 s per pass at 1,000 leaves
and about 1M target rows, nearly the whole interval. The complement forms cost
0 ms for `HASH` with nothing missing and 38 ms with two remainders missing,
under 1 ms for `RANGE`, and 4–8 ms for `LIST`; reading the gaps took 1.9 ms. The
changed-group fallback brought a never-recorded leaf's groups to zero diff, but
only when the changed set survives a skipped check and the pass clears only the
ids it read. Without the re-read, a leaf created between the catalog read and
the `DELETE` lost its rows for a pass; under about 20 DDL statements a second I
counted 264–308 wrongful deletes a minute without it and none in about 2,000
passes with it. Reading the constraint texts before the lifecycle catalog lost
rows even with the re-read, so the order is part of the decision.

### A leaf or middle `TRUNCATE` clears only the target rows routing to it

A root `TRUNCATE` clears the whole target, once.

[OPEN: whether the scoped clear is a routed truncate floor or per-entry
tombstones, and how its exactness argument is restated. Recommendation: none
yet.]

### `pg_upgrade` re-baselines the recorded state without re-aggregating

A changed `system_identifier` makes the pass re-record the `xmin`s and
`relpartbound` texts and compare the leaf set by oid and bound. `pg_upgrade`
resets every `pg_inherits.xmin` and `pg_class.xmin` to one value, and from 16 to
17 it rewrites `relpartbound`'s internal text, so without this every leaf would
read as new and trigger a full re-aggregate. Oids, constraint texts and the hash
opclass survive it. A dump and restore gives new oids, which break the recorded
`HASH` texts, so it needs a rebuild.

[OPEN: whether the pass detects a restore itself and rebuilds, or the operator
rebuilds. Recommendation: none yet.]

### Define refuses a shape routing can't serve, and the pass pauses one that appears later

Define refuses, naming the column: a group key or to-many join key missing a
partition-key column at some level; a key level that resolves to no columns (an
expression or whole-row key); a partition-key column whose type is not
`smallint`, `integer`, `bigint`, `numeric`, `text`, `varchar`, `uuid`, `date`,
`timestamp`, `timestamptz` or `boolean`, that uses a non-default partition
opclass, or whose collation is nondeterministic. A partition (leaf or middle)
named as a source, an inheritance hierarchy, a partitioned target and a root
that is itself a partition stay refused, and so do a 1-1 transform's `FROM` and a
to-one relationship's to-side.

After define, the pass pauses a definition whose source stops complying (a new
sub-level whose key isn't in the group key, or the root attached under another
table), with the reason in `capture_failure`, and `RESUME` refuses until it
complies. Resuming from such a pause rebuilds.

The accepted key types are the ones I tested. `char(n)` is the one shape where
the text misroutes without the cast (25,410 of 40,006 comparisons), so it waits
with the other types until an experiment shows they route exactly.

[OPEN: whether a 1-1 `FROM` on a partitioned table is in scope; it needs the 1-1
build chunk rewritten to join on `unnest` and a per-leaf 1-1 cleanup, neither
designed here. Recommendation: none yet.]

## Example

`order_line_items` is partitioned `BY HASH (order_id)` into 32 leaves, and

```text
TRANSFORM order_totals FROM order_line_items GROUP BY order_id
SELECT
  order_id AS order_id,
  COUNT(*) AS line_count,
  SUM(amount) AS total_amount
```

reads it. Every leaf carries capture, and a write named on the root or on any
leaf stages once under `order_line_items`. When an operator runs
`ALTER TABLE order_line_items DETACH PARTITION order_line_items_p7`, the next
pass sees the lifecycle fingerprint change, drops capture from the detached
table, and deletes the `order_totals` rows whose `order_id` satisfies that
leaf's recorded `satisfies_hash_partition(…, 32, 7, order_id)` text, without
reading the source. Until then those orders are overcounted. The same transform
grouped `BY product_id` is refused at define, naming `order_id`.

## Consequences

- Large partitioned child tables can feed aggregates and to-many counts, with
  capture costing no more per write than on a plain table.
- Partition maintenance needs no operator step, but counts can be wrong for
  about one interval plus the work on the leaf it touched. An operator who needs
  exact counts throughout moves rows with DML through the parent instead, which
  I measured at about 4–5 s and 0.5–0.6 GB of WAL per 1M-row leaf; creating an
  empty leaf and writing through the parent costs nothing.
- The reconcile pass carries per-source catalog state (recorded nodes and two
  fingerprints) and correction work, a few milliseconds per pass at 1,000
  leaves, and capture gains one catalog call per statement on a partitioned
  source.
- Installing capture stalls writers up to about 50 ms, or, on a tree of more
  than 100 nodes, takes about twice as long node by node.
- Build statements don't prune, so their planning and locking grow with the
  leaf count, and trees beyond 1,000 leaves wait until someone needs them.
- The supported shapes are narrow by design: a group key that omits a partition
  key, or an untested key type, is refused rather than handled slowly.
- A dump and restore of a partitioned source needs a rebuild.
- Three cases are left as [known correctness gaps](../known-correctness-gaps.md)
  rather than designed around, because no capture shape or fingerprint sees
  them: "A `MERGE` that moves a row to another partition", "A `DEFAULT`
  partition beside a leaf created and dropped within one pass" and "A
  partitioned source attached under another table and detached within one
  pass".
