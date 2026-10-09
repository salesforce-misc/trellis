---
status: draft
date: 2026-10-09
deciders: Michael Ries
---

# Partitioned Tables as Transform Sources

## Context

Very large child tables are often declaratively partitioned, and they are where a
cheap per-parent count or sum pays off most. Trellis refuses a partitioned table
as a source because capture is a set of statement triggers
([ADR-0002](0002-async-data-flow.md#capture-by-statement-triggers)), and a
statement trigger fires only for the table the statement names. Partition
maintenance (`ATTACH`, `DETACH`, `DROP`, a leaf `TRUNCATE`, a new leaf written
directly) also changes the parent's rows without firing any trigger on it.

The correctness bar for supporting them: counts may be wrong for about one
reconcile pass only when Trellis then detects the change and corrects it by
itself, with no operator step, and every supported operation has a documented
path that avoids the window altogether. Detection is the reconcile pass. An event
trigger would see DDL sooner, but it needs a superuser, some hosts don't offer
one, and I found it fires neither for an interrupted `DETACH CONCURRENTLY` nor for
`TRUNCATE`. Nothing here depends on one.

I tested every element below against real clusters with Trellis's generated
capture and an oracle diff after each step: the trigger and lifecycle behaviour
on PostgreSQL 14 through 18, routing and bound evaluation on 15 through 18, and
the detach check, the cheap cleanup forms, to-many targets and the cleanup race
on 16 through 18. Apart from PostgreSQL 16 refusing to attach a table that still
carries capture triggers, no result differed by version.

## Decision

1. **Scope.** A declaratively partitioned root (`RANGE`, `LIST` or `HASH`, any
   depth, with or without a `DEFAULT`) may be an aggregate's `FROM` or a to-many
   relationship's to-side, on PostgreSQL 15+, when its `GROUP BY` (or the
   relationship's to-side join key) contains every partition-key column at every
   level. Targets stay plain tables. Up to 1,000 leaves are supported and tested;
   more is unsupported, not refused.
2. **Capture on every node.** The root, each middle and each leaf carry the five
   capture triggers, all calling the root's one function set and staging under the
   root's identity. Up to 100 nodes install in one transaction; a larger tree
   installs the root first, then one node per transaction.
3. **A per-statement detach check.** Each capture function of a partitioned source
   first compares `pg_partition_root(TG_RELID)` with the source root and stages
   nothing when they differ. The pass drops capture triggers from any table that
   has left the tree, as cleanup only.
4. **Detection by two persisted fingerprints.** The pass computes a lifecycle
   fingerprint (each node's `relpartbound`, `inhparent`, `inhdetachpending` and
   `pg_inherits.xmin`) and a compliance fingerprint (the source's own identity and
   its `pg_inherits` rows in both directions). Both are stored with the definition.
   A deep check runs when either changes, when none is stored, and after a
   restart.
5. **Recorded bounds and routing.** The pass records each leaf's
   `pg_get_partition_constraintdef` text with the key columns' types and
   collations, fetching only for new or changed leaves and for a `DEFAULT` whose
   siblings changed. Cleanup routes a target row to a node by evaluating that text
   over the row, each group column cast to the source key column's type and
   collation. Every fetch and evaluation runs under `search_path = pg_catalog,
   pg_temp`, `DateStyle ISO, YMD` and `TimeZone UTC`, in short transactions under
   a short `lock_timeout` that retry on a lock timeout.
6. **Per-node cleanup, in the pass that sees the change.** No full rescan:
   - a new, attached or re-attached leaf: install capture, remove the target rows
     routing to it, then re-aggregate that leaf alone;
   - a `DEFAULT` whose sibling set changed: treated as new;
   - a dropped or detached subtree: remove the rows routing to its topmost gone
     node, by that node's recorded text, with no source read. A detach-pending
     leaf is cleaned up only once its `pg_inherits` row is gone and the ring has
     been folded;
   - then step 4, "remove rows that route to no current leaf", described below.

   For a to-many target, "remove" is "recompute the routed parents" through the
   normal Recompute path: no target row is ever deleted.
   [OPEN: how removing an aggregate's routed rows maps onto its ledger entries,
   and how it orders against a concurrent Apply page.]
7. **Step 4 is cheap every pass, with a full-form fallback.** For `HASH` it
   deletes over the missing remainders; for `RANGE` and `LIST`, over the gaps
   between the current bounds, read from `pg_get_expr(relpartbound)` and ordered
   by the key column's collation. A `DEFAULT` under the root means no statement.
   The full routing form runs, limited to the groups the fold changed since the
   last successful check, only on a pass where a recorded node is gone and its
   text is unusable. Step 4 runs in its own `READ COMMITTED` transaction: the
   pass reads the lifecycle catalog before the constraint texts, re-reads it in a
   new statement after the `DELETE`, and rolls back and retries if the leaf set
   changed.
8. **Truncate is scoped.** A `TRUNCATE` of a leaf or middle clears only the target
   rows routing to that node; a root `TRUNCATE` clears the whole target, once.
   [OPEN: whether the scoped clear is a routed truncate floor or per-entry
   tombstones, and how its exactness argument is restated.]
9. **`pg_upgrade` re-baselines without re-aggregating.** A changed
   `system_identifier` makes the pass re-record the `xmin`s and `relpartbound`
   texts and compare the leaf set by oid and bound, which survive the upgrade. A
   dump and restore needs a rebuild. [OPEN: whether the pass detects a restore
   itself and rebuilds, or the operator rebuilds.]
10. **Refused, and paused if it appears later.** Define refuses, naming the
    column: a group key or to-many join key missing a partition-key column at
    some level; a key level that resolves to no columns (an expression or
    whole-row key); a partition-key column whose type is not `smallint`,
    `integer`, `bigint`, `numeric`, `text`, `varchar`, `uuid`, `date`,
    `timestamp`, `timestamptz` or `boolean`, that uses a non-default partition
    opclass, or whose collation is nondeterministic. A partition (leaf or middle)
    named as a source, an inheritance hierarchy, a partitioned target and a root
    that is itself a partition stay refused. A 1-1 transform's `FROM` and a to-one
    relationship's to-side stay refused. [OPEN: whether a 1-1 `FROM` on a
    partitioned table is in scope; it needs the 1-1 build chunk rewritten to join
    on `unnest` and a per-leaf 1-1 cleanup, neither designed here.] After define,
    the pass pauses a definition whose source stops complying (a new sub-level
    whose key isn't in the group key, or the root attached under another table),
    with the reason in `capture_failure`, and `RESUME` refuses until it complies.
    Resuming from such a pause rebuilds.

### Why

**The group-key restriction.** With every partition-key column in the group key,
each group lives in exactly one leaf, so a lifecycle change touches only the
groups routing to one node and the catalog says which those are. Without it, a
`DROP` needs a full source rescan (hours at multi-TB per retention drop) or
per-leaf group bookkeeping. The motivating shape, `HASH` on the parent id with 32
leaves, satisfies it, so the restriction costs little and has no fallback.

**Capture on every node.** I ran 27 DML scenarios. Triggers on the parent
missed writes aimed at a leaf, triggers on the leaves missed writes through the
parent, and root plus leaves missed writes named on a middle. Every
node captured each change exactly once, and a row moved between partitions by an
`UPDATE` through a parent stages as a matched delete and insert. The generated
bodies are name-based, so one function set serves leaves whose attnums or
dropped columns differ. Capture cost the same per write as on a plain table at
10, 100 and 1,000 leaves.

**The install threshold.** Installing in one transaction blocked every writer for
about 0.3 ms per leaf: 34–44 ms at 100 leaves and 271–305 ms at 1,000, and the
existing 50 ms `lock_timeout` bounds the wait for the lock, not how long it is
held. Node by node, the worst writer commit I saw was 7–56 ms at every size, for
about twice the total install time. 100 nodes keeps the one-transaction stall
within the roughly 50 ms writer wait a plain table's install already accepts.

**The detach check.** A table that leaves the tree keeps its triggers until the
pass, and a write to it in that interval must never count. I found that a
`pg_inherits` lookup in the trigger is stale for a `REPEATABLE READ` or
`SERIALIZABLE` writer whose snapshot predates the `DETACH`, and staged the write.
`pg_partition_root` reads the catalog snapshot instead: it returns NULL after a
plain `DETACH` and the leaf itself while it is detach-pending, takes no locks,
doesn't block behind uncommitted DDL, and cost about 1 µs per statement against
about 80 µs for the capture statement itself. While the source root is attached
under another table the check also drops the source's own writes, which the
rebuild on resume repairs, so I chose it over an ancestor walk. A writer with an
old snapshot can still write through the parent into a detach-pending leaf, which
is why that leaf's cleanup waits until the detach completes and the ring is
folded.

**The fingerprints.** The two cost 2.3–2.9 ms per pass at 1,000 leaves and gave no
false positive under DML, every `VACUUM` variant, `ANALYZE`, `CREATE INDEX` or
Trellis's own install. The lifecycle fingerprint alone misses a source that joins
a hierarchy or gains a level two deep, which the compliance fingerprint sees.
`pg_inherits.xmin` is what reveals a table detached and re-attached under the
same bound within one pass; without it, every trial of a detach, modify and
re-attach with the triggers dropped ended wrong. Persisting them means a restart
can't forget a change. I rejected adding the source's own `pg_class.xmin`: it
would also fire on a `GRANT`, an owner change, a reloption, `VACUUM FULL` or
`CLUSTER`, and each firing would force a full rebuild.

**Recorded texts, the cast and the pinned settings.** The catalog loses a leaf's
bound when it is dropped or detached, so the pass records it while it can. Each
text is evaluated with the target's columns cast to the source key types: with
the cast, I found 0 disagreements in about 3.4M comparisons per version, while a
`HASH` text raises on a differently typed column. A text fetched under a non-ISO
`DateStyle` errored or misrouted up to 32,981 keys. Under Trellis's session
search path, three objects in `public` silently misrouted rows: an enum in a
`RANGE` bound, an ICU collation shadowing a built-in name, and a custom operator.
A `HASH` text embeds its parent's oid, so a dropped middle's leaves can only be
routed through the middle's own text. A `DEFAULT`'s text changes with its
siblings, and using a recorded one left stale groups, so it is re-aggregated
instead. Reading 1,000 texts holds 1,005 locks to commit and blocks behind
uncommitted DDL, hence short transactions and changed leaves only. The accepted
key types are the ones I tested; `char(n)` is the one shape where the text
misroutes without the cast (25,410 of 40,006 comparisons), so it waits with the
other types until an experiment shows they route exactly.

**Per-node cleanup and the window.** Deleting a leaf's routed rows, then
re-aggregating it, closed the oracle diff in every lifecycle cell I measured on
`HASH`, `RANGE` and `LIST`, including direct writes into a new leaf before its
capture existed, which a "recompute the groups found in the leaf" rule never
converged on. Every window closed within 5.2 s under load. Rows an operation adds
are undercounted during the window, rows it removes are overcounted, and writes
through the parent have no window at all. For a to-many target, deleting rows
lost them for good in 180 of 189 trials; recomputing the routed parents reached
zero diff in every cell, kept parents with no children left at a count of 0, and
closed within 2.4–3.6 s.

**Step 4.** It catches a leaf created and dropped between two passes, which no
fingerprint sees. Its full routing form cost 4.1–7.3 s per pass at 1,000 leaves
and about 1M target rows, nearly the whole interval. The complement forms cost
0 ms for `HASH` with nothing missing and 38 ms with two remainders missing, under
1 ms for `RANGE`, and 4–8 ms for `LIST`; reading the gaps took 1.9 ms. The
changed-group fallback brought a never-recorded leaf's groups to zero diff, but
only when the changed set survives a skipped check and the pass clears only the
ids it read. Without the re-read, a leaf created between the catalog read and the
`DELETE` lost its rows for a pass; under a stress of about 20 DDL statements a
second I counted 264–308 wrongful deletes a minute without it and none in about
2,000 passes with it. Reading the constraint texts before the lifecycle catalog
lost rows even with the re-read, so the order is part of the decision.

**`pg_upgrade`.** It resets every `pg_inherits.xmin` and `pg_class.xmin` to one
value, and from 16 to 17 it rewrites `relpartbound`'s internal text, so every leaf
would read as new and trigger a full re-aggregate. Oids, constraint texts and the
hash opclass survive it. A restore gives new oids, which break the recorded
`HASH` texts.

**The 1,000-leaf range.** No build statement prunes: each one plans, locks and
opens every leaf, about two locks per leaf and 19–27 ms of planning at 1,000. That
is acceptable at this size and is tested there. The default lock table runs out
for one statement somewhere above that, so a larger tree waits until someone
needs it.

**What stays out of the design.** A `MERGE` that moves a row between partitions
loses or duplicates it in the transition tables Postgres hands to capture; no
capture shape can see it, so it is the known gap "A `MERGE` that moves a row to
another partition", not designed around. "A `DEFAULT` partition beside a leaf
created and dropped within one pass" and "A partitioned source attached under
another table and detached within one pass" are known gaps too
([known correctness gaps](../known-correctness-gaps.md)): no fingerprint sees
either, so they are documented rather than checked for. Refusing the operations
that undercount would only make the docs more alarming, since Trellis can't
refuse DDL without event triggers. Applying a leaf's per-group totals inside the
DDL's own transaction cost about 100 ms per 1M rows in a prototype and would
close the window, but needs a new entry point; it isn't part of this design.

## Consequences

- Large partitioned child tables can feed aggregates and to-many counts, with
  capture costing no more per write than on a plain table.
- Partition maintenance needs no operator step: the pass corrects every
  lifecycle change within about one interval plus the work on the leaf it
  touched. An operator who needs exact counts throughout moves rows with DML
  through the parent instead, which I measured at about 4–5 s and 0.5–0.6 GB of
  WAL per 1M-row leaf; creating an empty leaf and writing through the parent
  costs nothing.
- The reconcile pass gains per-source catalog state (recorded nodes and two
  fingerprints) and correction work, and capture gains one catalog call per
  statement on a partitioned source.
- The supported shapes are narrow by design: a group key that omits a partition
  key, or an untested key type, is refused rather than handled slowly.
- [OPEN: whether a definition reports `live` or build progress while a large
  attached leaf is re-aggregated.]
