---
status: accepted
date: 2026-10-17
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

The bar for supporting them: counts may be wrong for about one reconcile pass,
plus the work of correcting the node the change touched, only when Trellis then
detects the change and corrects it by itself, with no operator step, and every
supported operation has a documented path that avoids the window altogether.

This ADR extends [ADR-0002](0002-async-data-flow.md)'s capture and truncate
decisions to partitioned sources. It settles which partition trees an
aggregate, a side of a JOIN transform
([ADR-0019](0019-join-transforms.md)) or a to-many relationship can read, how
capture covers them, and how the
reconcile pass detects and corrects partition maintenance. Partitioned targets,
inheritance hierarchies, a partition named as a source, and trees of more than
1,000 leaves are out of scope.

## Decisions

### A partitioned root is a source when its group key holds every partition-key column

A declaratively partitioned root (`RANGE`, `LIST` or `HASH`, any depth, with or
without a `DEFAULT`) may be an aggregate's `FROM`, either side of a JOIN
transform, or a to-many relationship's to-side, on PostgreSQL 15+, when its
group key contains every partition-key column at every level. The group key is
an aggregate's `GROUP BY`; for a JOIN side, it is that side's partial key, the
side's own group columns plus the join key; for a relationship, its to-side join
key. Targets stay plain tables.

With every partition-key column in the group key, each group lives in exactly
one leaf, so a lifecycle change touches only the groups routing to one node and
the catalog says which those are. Under [ADR-0002](0002-async-data-flow.md)'s
group-level basis, a node's correction recomputes exactly those groups from the
source; on a JOIN side, those groups are the side's partial cells, recomputed
under the join-key locks so the change carries into the target. Because the
join key counts, a fact table partitioned on the column it joins by needs
nothing more in its `GROUP BY`, while one partitioned on its own id is refused.
Without the rule, a `DROP` needs a full source rescan (hours at multi-TB per
retention drop) or per-leaf group bookkeeping. The motivating shape, `HASH` on
the parent id with 32 leaves, satisfies it, so the restriction costs little and
has no fallback.

Why not lift the rule by keying the partials by a hidden leaf column, so a leaf
event deletes that leaf's cells and propagates the change? It costs up to
leaves × cells in state and touches every group-basis structure, and stamping
each captured image with its leaf is unmeasured. Why not recompute every group
from the source on any leaf event? That is a full source `GROUP BY` per leaf
event, one a day under daily retention drops, and it is untested. Either would
lift a define-time refusal later without changing what is admitted now.
Time-range-partitioned tables grouped coarser than their partition key (by day
over `created_at` partitions) are refused until then.

I tested every decision here against real clusters with Trellis's generated
capture and an oracle diff after each step: the trigger and lifecycle behaviour
on PostgreSQL 14 through 18, routing and bound evaluation on 15 through 18, and
the detach check, the cheap cleanup forms, to-many targets and the cleanup race
on 16 through 18. No result differed by version, except one that differs by
minor release: PostgreSQL 16.8 refuses to `ATTACH` a table that still carries
capture triggers (`trigger "trellis_capture_delete" prevents table … from
becoming a partition`), taking the statement triggers' transition tables for a
row trigger's. 15.19, 16.15, 17.10, 17.11, 18.2 and 18.6 attach it. On a
server that refuses, the triggers are dropped before the attach.

### Trees of up to 1,000 leaves are supported; larger ones are refused

No build statement prunes: each one plans, locks and opens every leaf, about two
locks per leaf. At 1,000 leaves a statement holds about 2,000 locks, 29% of the
lock table's 6,912 slots (the default `max_locks_per_transaction` of 64
times `max_connections` 100 plus the test cluster's `max_prepared_transactions`
8), and plans in 25–28 ms, or 59–63 ms for a build chunk's
statement. Define, capture install and the build of a 1,000-leaf tree complete
inside the default lock table on PostgreSQL 17.11. One statement first ran out
of shared memory at about 5,500 leaves. Define refuses a larger tree, and a
tree that grows past the limit after define pauses its readers with a capture
failure that names it. A larger tree waits until someone needs it.

### Every node carries capture, staging under the root's identity

The root, each middle and each leaf carry the five capture triggers, all calling
the root's one function set and staging under the root's identity.

I ran 27 DML scenarios. Triggers on the parent missed writes aimed at a leaf,
triggers on the leaves missed writes through the parent, and root plus leaves
missed writes named on a middle. With every node, each change was captured
exactly once, and a row moved between partitions by an `UPDATE` through a
parent staged as a matched delete and insert. The generated bodies are
name-based, so one function set serves leaves whose attnums or dropped columns
differ. Capture cost the same per write at 10, 100 and 1,000 leaves as
at none: at 1,000 leaves it added about 0.2–0.4 ms to a write, as it does to a
plain table.

### Up to 100 nodes install in one transaction; a larger tree installs node by node

A larger tree installs the root first, then one node per transaction.
Installing in one transaction blocked every writer for about 0.3 ms per leaf:
34–44 ms at 100 leaves and 271–305 ms at 1,000, and the existing 50 ms
`lock_timeout` bounds the wait for the lock, not how long it is held. Node by
node, the slowest application-writer commit during the one-time install of a
1,000-leaf tree was typically 12–65 ms, with occasional spikes of 0.2–1.5 s
that varied between runs on a quiet box. The install took 1.6–4.6 s in total,
against 0.2–0.7 s in one transaction (5–10 times as long). 100 nodes keeps the
one-transaction stall within the roughly 50 ms writer wait a plain table's
install already accepts.

### Each node's triggers are recorded, and maintenance is not a disruption

A trigger disabled, dropped, recreated or altered outside Trellis, or a capture
function replaced by hand, pauses a partitioned source's readers exactly as it
does a plain table's (`capture::triggers`). Every install operation records the
oid, `xmin` and enabled mode of the triggers it made, on the root and on each
node, in its own transaction, so a larger tree's node-by-node install leaves a
record for exactly the nodes that carry triggers. The pass checks the recorded
nodes that are still in the tree. A node that is attached, created, detached or
dropped is not a recorded node in the tree, so maintenance pauses nothing, and
the install that adds capture to a new node records it. The rows of a node that
left the tree are forgotten; a node that returns carrying all five triggers
`ENABLE ALWAYS` is recorded as found.

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

The two cost 2.3–2.9 ms per pass at 1,000 leaves (2.56–2.61 ms in the
1,000-leaf sweep) and gave no false positive
under DML, every `VACUUM` variant, `ANALYZE`, `CREATE INDEX` or Trellis's own
install. The lifecycle fingerprint alone misses a source that joins a hierarchy
or gains a level two deep, which the compliance fingerprint sees.
`pg_inherits.xmin` is what reveals a table detached and re-attached under the
same bound within one pass; without it, every trial of a detach, modify and
re-attach with the triggers dropped ended wrong. Persisting them means a
restart can't forget a change. Adding the source's own `pg_class.xmin` would also fire
on a `GRANT`, an owner change, a reloption, `VACUUM FULL` or `CLUSTER`, and each
firing would force a full rebuild.

Why not an event trigger, which would see DDL sooner? It needs a superuser, some
hosts don't offer one, and I found it fires neither for an interrupted
`DETACH CONCURRENTLY` nor for `TRUNCATE`. Nothing here depends on one.

### Target rows route to nodes by each leaf's recorded bound, under pinned settings

The pass records each leaf's `pg_get_partition_constraintdef` text with the key
columns' types and those types' collations, fetching only for new or changed
leaves and for a `DEFAULT` whose siblings changed. A correction routes a group to
a node by evaluating that text over the group key, each group column cast to the
source key column's type, under the type's own collation. A text names a level's
collation on a bound only when it differs from the type's, so every comparison
runs under its own level's collation, even when the levels of one tree differ.
Every fetch and evaluation runs under `search_path = pg_catalog, pg_temp`,
`DateStyle ISO, YMD` and `TimeZone UTC`, in short transactions under a short
`lock_timeout` that retry on a lock timeout.

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

- A new, attached or re-attached leaf: install capture, then correct that leaf
  alone.
- A `DEFAULT` whose sibling set changed: treated as new.
- A dropped or detached subtree: correct its topmost gone node, routing by that
  node's recorded text. A detach-pending leaf's correction starts only once its
  `pg_inherits` row is gone.
- Then the no-leaf sweep below.

Every detected operation queues its own correction, whatever its kind
(`new_leaf`, `gone_node`, `complement`), unless an entry of the same node, kind
and text is queued that no reader has started. A reader that has started an
entry (it has a progress row, in any phase) may already have walked past the
rows the new operation changed, and one that finished it never reads it again,
so a started entry never absorbs a later operation; the reader re-derives the
node once more for it. Every engine commits its progress row before it reads
the node's keys or rows, so an operation an unstarted entry absorbs is one each of
its readers reads. Two inserters that race past the test both queue, which
costs work and never correctness.

On an aggregate, a node's correction is a Re-derive of a key set through the
normal page path. The set is the keys of the live ledger entries whose group
routes to the node by its recorded text, found through the ledger's `GROUP BY`
index, plus, for a node in the tree, the keys of its current rows, read from
the node in keyset chunks as a build does. A gone node's rows all satisfied its
recorded text, so the ledger walk finds their entries; only a gone node without
a usable text has its detached table's rows read instead. Each key takes its entry lock, reads
the source row live through the root, and rewrites or tombstones its entry, so
its group moves by a delta. No target row and no ledger entry is ever deleted
directly. A group is the sum of its entries, so deleting a group row would leave
the entries live, and deleting entries would drop the tombstones that refuse a
ring change staged before the DDL, which would then resurrect the row. A
partitioned source's aggregate always has the ledger's `GROUP BY` index. The
pending correction is durable, so a restart resumes it, and the pass records its
fingerprints once the correction is queued, not once it has drained. A
correction works in pages of one build chunk's size, each page one transaction
that commits its keys' entries and its progress together, so a restart resumes
after the last committed page: the ledger walk in `(group, key)` order, then the
node's rows in primary-key order.

On an aggregate kept on [ADR-0002](0002-async-data-flow.md)'s group basis, a
node's correction recomputes the groups that route to the node, never zeroes
them: a `TRUNCATE` and an `INSERT` into the leaf in one transaction leave a row
that a zero would erase and whose change the zero's basis would then refuse.
The set is read in one statement: the target's groups that route to the node by
its recorded text, the groups of the images of the source's changes still in
the ring that route to it, and, for a node in the tree, the groups of its
current rows. The ring's groups are there because a change committed before
the DDL and not yet applied may name a group the target doesn't have, and its
page would otherwise create that group with a row the DDL removed. One
statement, because a page that applies such a change and has its ring rows
retired between two reads would be in neither. The correction then locks the
set in one sorted statement and rewrites each group's partials from a
`GROUP BY` of its rows, with that statement's snapshot as the group's basis,
in one transaction that records the correction done. Every change the DDL
removed was committed before the reader recorded its start, so the basis sees
it and its page refuses it: no `build_seg` and no fence. The rows are read through the
root, or, while the whole tree is as recorded (every node under the same
parent by the same `pg_inherits` row, none detach-pending, none added), by one
`GROUP BY` of the node under an `ACCESS SHARE` lock; every row of a group that
routes to the node is then in it. The node's own row isn't enough: a sibling
attached beside a `DEFAULT` takes some of its groups, whose rows a read of the
`DEFAULT` alone would zero. Every operation that narrows the node's region
after the check locks the node, so it waits for the correction. The capture
install that adds a new node to the tree rebuilds none of the readers a
correction covers (an aggregate over the source, on the group basis or the
ledger, and a definition reading it through a to-many relationship): the
node's own correction covers its rows, including those written into it
directly before its capture existed. That install parks no marker on the root
when no other reader needs a catch-up, so no discharge stages a recompute of
every key of the source. An install that also changes the capture functions,
for a reader registered in the same pass, parks the marker as any install
does, since its capture gate holds that reader until the rows the old
functions staged have drained. Measured on the engine (a debug build, one
runner), the correction of a `DROP`, `DETACH` or `TRUNCATE` of a leaf of 1M or
of 10M rows over 1,560 groups takes 27–34 ms and about 0.6 MB of WAL, whatever
the leaf's rows; the correction of an `ATTACH` of such a leaf reads it twice, once for its groups and
once for their partials, 0.5–0.6 s at 1M rows and 3.9–4.7 s at 10M.

A correction runs for each definition that reads the source, an aggregate over
it, a JOIN transform reading it as either side or a definition reading it
through a to-many relationship, as a row of the
build-chunk queue, which the pass that queues the correction enqueues. The drain workers claim it as
they claim a build chunk, behind the ring's backlog, and give it back after
about a second of pages. The claim makes one runner per correction and
definition at a time, the stale-claim sweep frees the row of a runner that
died, and the claim's fence keeps a runner whose claim was reclaimed, or that
held it across a pause and resume, from writing. A failing correction records
its error on its row, which `status()` reports, and backs off. Charged
failures pause its definition, whose resume rebuilds it from the source, and
until that resume the row and the correction stay queued as the record of why.
No other correction, definition or source waits on it. A `catching_up`
definition gets its row once it is `live`, and the correction stays queued
until then.

A change staged before the DDL can still be in the ring when the correction
runs. The correction starts by stamping the definition's `build_seg`, as a
rebuild does: a page with a batch at or below it re-derives its keys from the
source instead of applying their changes, so an unapplied insert into a dropped
leaf becomes a tombstone rather than a row. It then waits for the transactions
open at the stamp to end, so no page that read the old `build_seg` can still
write a key the walk has no entry for. Waiting for the ring to fold through a
position would never finish on a busy ring, whose active segment is never empty.

For a to-many target, the correction is "recompute the routed parents" through
the normal Recompute path: no target row is ever deleted. A parent routes to
the node when the node's recorded text holds over its join value, presented as
the to-side's join column and cast to its recorded type and collation. Every
row of a node holds the text, so the routed parents are every parent of a row
the node holds or held, and a node in the tree needs no read of its own rows.
The correction walks the from-side in primary-key order, in pages that each
stage a recompute of their parents (the one a child's change stages) and
commit the progress with it. The drain reads each parent's children live, so
a parent whose last child went away reads 0, and a child's change staged
before the DDL and drained after recomputes from the live rows too: the
correction stamps no `build_seg` and waits on no fence. A gone node without a
usable text has its detached table's rows read instead, and their parents
recomputed. Staging a recompute does not write the target, and a to-many target
has no group deltas for the build's completion to wait on, so the last page
records the segment active at its commit, read under the share lock that keeps
the seal from moving it, and the correction's row is done only once every
segment through that one has drained. Until then the definition reports
`backfilling`, and a `keys_remaining` of 0 means every recompute is staged, not
yet applied. The wait is for a fixed segment, which seals and drains as the ring
rotates, not for an empty ring. When that segment is still the active one and
holds nothing, with no straggler of the one before it, the last page staged
nothing after the earlier pages' segment sealed, and only the segments before
it are waited for: an idle ring never seals an empty segment.

My cleanup measurements deleted a leaf's routed rows from a model target, a
virtual sum with no ledger and no real Apply, then re-aggregated the leaf. That
closed the oracle diff in every lifecycle cell I measured on `HASH`, `RANGE` and
`LIST`, including direct writes into a new leaf before its capture existed,
which a "recompute the groups found in the leaf" rule never converged on, and
every window closed within 5.2 s under load. Those figures show the routing is
exact, not what a correction costs. On the real path the work scales with the
node's rows. Measured on the ledger build in the 1,000-leaf sweep (a release
build, PostgreSQL 17.11, the cluster on disk), the correction of a dropped leaf
of 1,000,000 rows takes 13.0 s in the engine: 1,000,000 keys in 101 pages and
489 MB of WAL, about 490 bytes a key. Each key costs an entry lock, the entry's
rewrite as a tombstone (not HOT, so an insert into the key index and the
tombstone index) and the full-page images those touch. The pass that detects
the drop takes 256 ms, and folding the correction's group deltas takes the
merger 0.35 s and 23 MB. At that rate a retention `DROP` of 100M rows takes
about 22 minutes and about 49 GB of WAL, then the garbage collector's deletes of
those tombstones; that figure is a linear extrapolation from a leaf whose
ledger stays in cache, and it is a floor. Past the cache, finding each key
through the ledger's `GROUP BY` index reads a random heap page and a random
index page: a model of that walk (#1085) ran at 17.5–21.5k keys a second and
9–12 KB of WAL a key, which projects to 78–95 minutes and 0.9–1.2 TB of WAL for
a leaf of 100M rows. Rows an operation adds
are undercounted until its correction drains, rows it removes are overcounted,
and writes through the parent have no window at all. For a to-many target,
deleting rows lost them for good in 180 of 189 trials; recomputing the routed
parents reached zero diff in every cell, kept parents with no children left at a
count of 0, and closed within 2.4–3.6 s.

Why not tombstone the routed entries without reading the source when the gone
bound is uncovered? It saves only the primary-key lookups, since the writes are
the main cost, and it adds a second mechanism. Why not a truncate floor per
target and node? It adds a term to every Apply's refusal check and needs its own
garbage collection, and only `TRUNCATE` has an lsn to set one at, so it covers
neither `DROP` nor `DETACH`.

Why not refuse the operations that open a window? Trellis can't refuse DDL
without event triggers, so it would only make the docs more alarming. Applying a
leaf's per-group totals inside the DDL's own transaction cost about 100 ms per
1M rows in a prototype and would close the window, but needs a new entry point.

### A definition under correction reports `backfilling`, with a `correcting` field

From the pass that detects a lifecycle change until the correction's key set has
drained, the definition reports `backfilling`, as it does while
`request_backfill` re-derives it, then returns to `live`. `status()` carries a
`correcting` field with one entry per node being corrected: `node`
(schema-qualified), `since` (when the pass queued it) and `keys_remaining`. In
the interval of at most one pass between the DDL and its detection the status
still reads `live`. A restart in the middle of a correction resumes it and
reports `backfilling` again.

Staying `live` with only the field would make the `live` guarantee exclude
partition maintenance for every caller, so a correction is reported as the
backfill it is.

### Every pass sweeps rows that route to no current leaf, by the bounds' complement

For `HASH` the sweep finds target groups over the missing remainders; for
`RANGE` and `LIST`, over the gaps between the current bounds, read from
`pg_get_expr(relpartbound)` and ordered by the key column's collation. A
`DEFAULT` under the root means no statement. On an aggregate, the keys of the
live ledger entries in the groups it finds are corrected through the normal page
path, as a node's are; on a JOIN side, the sweep searches the side's partial
cells, whose key holds every partition-key column where the target's may not,
and the cells it finds are recomputed under their join-key locks; on a to-many
target, those parents are recomputed. The
sweep runs in its own `READ COMMITTED` transaction: the pass reads the lifecycle
catalog before the constraint texts, re-reads it in a new statement after
queuing the hits' correction, and rolls back and retries if the leaf set
changed. Its bound reads wait at most a short `lock_timeout` behind a lock on
a node, and a pass whose sweep times out leaves the next pass to sweep.

A recorded node can be gone without a usable text: a node detached or dropped
between the pass's tree read and its text fetch is recorded with none, and its
correction routes nothing by it. Nothing else is needed for its groups. The
group key holds every partition-key column, so each target group routes to
exactly one region of the current tree, and the gone node's region ends in one
of three states, each corrected: **uncovered**, which the sweep finds; **taken
by the `DEFAULT`**, whose changed sibling set queues its correction; or **taken
by a new or re-attached leaf**, which has its own correction. No unchanged leaf
can take the region: Postgres refuses overlapping bounds, and a bound changes
only by a detach and an attach, which the pass reads as re-attached. The full
routing form over the groups the fold changed would find a subset of what the
sweep finds, so no pass runs it.

The sweep catches a leaf created and dropped between two passes, which no
fingerprint sees. Its full routing form cost 4.1–7.3 s per pass at 1,000 leaves
and about 1M target rows, nearly the whole interval. The complement forms cost
0 ms for `HASH` with nothing missing and 38 ms with two remainders missing,
under 1 ms for `RANGE`, and 4–8 ms for `LIST`; reading the gaps took 1.9 ms. At 1,000 leaves the sweep is one statement per
pass: 8.0–9.2 ms to read the bounds and 1.6–1.7 ms to search.
Against Postgres 15 to 18, a node detached between the tree read and the text
fetch, by a plain or a concurrent detach, and then dropped reached zero diff one
pass after the pass that recorded it, in each of the three states, also beside
mixed `HASH` moduli, a partitioned `DEFAULT`, a `DEFAULT` under a middle, a
middle created again under its name, and several operations in one interval.
Against the model target, without the re-read a leaf created between the
catalog read and the cleanup lost its rows for a pass; under about 20 DDL
statements a second I counted 264–308 wrongful deletes a minute without it and
none in about 2,000 passes with it. Reading the constraint texts before the
lifecycle catalog lost rows even with the re-read, so the order is part of the
decision.

### A leaf or middle `TRUNCATE` queues that node's correction

A node truncate's ring row names the truncated node. Without the name, the
truncate trigger of a leaf would empty the whole ledger and target and raise the
truncate floor, losing every other leaf's rows until a rebuild. The row queues
that node's correction under the same rule as any other operation (an
unstarted entry of the node absorbs it), with no floor and no barrier, so the other nodes' entries
and groups are untouched. A root `TRUNCATE` is unchanged: it clears the whole
target, once.

### `pg_upgrade` re-baselines the recorded state; a restore pauses for a rebuild

Each recorded source also stores the cluster's `system_identifier` and the oid
of Trellis's own `partition_sources` table, written with the row. A restore has
to recreate that table to bring the rows back (a dropped and recreated
database, `pg_restore --clean`, a restore into another cluster, a data-only
load into freshly created tables), so the stored oid no longer names it. DDL
on a source can't change it; `VACUUM FULL`, `CLUSTER` and pg_repack change the
file node, not the oid; `pg_upgrade`, promoting a replica, point-in-time
recovery and `CREATE DATABASE … TEMPLATE` keep every oid. Rows copied into a
new table look exactly like a restore, so a Trellis migration never rebuilds
`partition_sources` (or `capture_baselines`) by create-copy-rename, only with
`ALTER`.

A deep check compares them in this order:

1. The stored catalog oid differs from the live one: a restore.
2. Otherwise a changed `system_identifier` compares the recorded nodes by oid
   and bound. When every recorded oid is present with the same bound it is
   `pg_upgrade`; anything else is a restore.
3. Otherwise the ordinary plan runs. A root dropped and recreated, renamed
   away, moved to another schema or swapped in by renames keeps both values,
   so it is no restore. It is table maintenance, not partition maintenance:
   the pass pauses every reader before any deep check, like a plain table that
   is gone or swapped ([capture by
   triggers](../staging-and-claiming/01-capture-by-triggers.md#capture-changed-outside-trellis)),
   and corrects and reinstalls nothing. A resume re-validates against the
   table now at the name, forgets the recorded tree and rebuilds. Only a
   leaf's or a middle's maintenance is corrected per node.

On `pg_upgrade` the pass re-records the `xmin`s and `relpartbound` texts.
`pg_upgrade` resets every `pg_inherits.xmin` and `pg_class.xmin` to one value,
and from 16 to 17 it rewrites `relpartbound`'s internal text, so without this
every leaf would read as new and trigger a full re-aggregate. Oids, constraint
texts and the hash opclass survive it.

A dump and restore gives new oids and breaks the recorded `HASH` texts, which
embed the parent's oid. It is one case of the general rule for a logical
restore ([ADR-0002](0002-async-data-flow.md), I2): the pass pauses every
definition of the database, whatever its kind and source, with a
`capture_failure` naming the restore, discards the ring, and runs no sweep, no
correction and no routing from the recorded state. That includes corrections
queued when the dump was taken, which came back with it: the pass removes them
in the transaction that pauses, and until a pass has classified the change the
correction engine sees no correction of a source whose stored catalog oid or
identifier differs from the live one. The pass then records the live tree as a
first sight. `RESUME` resets the definition's ordering state, then rebuilds as
every resume does and re-records the nodes, texts and fingerprints; nothing
about the definition is non-compliant, so it doesn't refuse. A changed
`system_identifier` over recorded nodes that did not all survive is a restore
too, and answers the same way. Rebuilding automatically would take the choice of when to pay for a
full rebuild away from the operator.

### Define refuses a shape routing can't serve, and the pass pauses one that appears later

Define refuses, naming the column: a group key, a JOIN side's partial key or a
to-many join key missing a partition-key column at some level; a key level that resolves to no columns (an
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

A 1-1 `FROM` and a to-one to-side stay refused until partitioned targets exist,
because a 1-1 target over a partitioned source would likely need its own
partitioning.

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
table, queues a correction of the `order_totals` groups whose `order_id`
satisfies that leaf's recorded `satisfies_hash_partition(…, 32, 7, order_id)`
text, and re-derives those orders' keys from the live ledger entries. The
definition reports `backfilling`, with a `correcting` entry for the leaf, until
the keys drain. Until then those orders are overcounted. The same transform
grouped `BY product_id` is refused at define, naming `order_id`.

## Consequences

- Large partitioned child tables can feed aggregates and to-many counts, with
  capture costing no more per write than on a plain table.
- Partition maintenance needs no operator step, but counts can be wrong for
  about one interval plus a correction that costs O(rows in the node), not
  O(groups): on the ledger build, 13.0 s and 489 MB of WAL for a dropped leaf of
  1M rows (measured), and for a retention `DROP` of 100M rows at least 22
  minutes and 49 GB of WAL (extrapolated), and as much as 78–95 minutes and
  0.9–1.2 TB once the ledger outgrows the cache (#1085's model). An operator who needs exact counts throughout moves rows with DML
  through the parent instead, which I measured at about 4–5 s and 0.5–0.6 GB of
  WAL per 1M-row leaf; creating an empty leaf and writing through the parent
  costs nothing.
- The reconcile pass carries per-source catalog state (recorded nodes and two
  fingerprints), a few milliseconds per pass at 1,000 leaves (a whole steady
  pass there takes 233–252 ms), and a durable
  queue of pending corrections, and capture gains one catalog call per
  statement on a partitioned source.
- Installing capture stalls writers up to about 50 ms. On a tree of more than
  100 nodes it installs node by node, which takes 5–10 times as long in total
  (1.6–4.6 s at 1,000 leaves), with a slowest writer commit typically of 12–65
  ms and occasionally 0.2–1.5 s.
- Build statements don't prune, so their planning and locking grow with the
  leaf count (about 2,000 locks and 25–28 ms of planning at 1,000 leaves), and
  trees beyond 1,000 leaves wait until someone needs them.
- A `DROP` or `DETACH` of a leaf can deadlock against a per-leaf rebuild or a
  page that holds the leaf's groups. Postgres aborts one transaction with
  `40P01`: the job and the page retry, and the DDL may be the one aborted, so
  the operator retries it. The error is visible and nothing is lost.
- The supported shapes are narrow by design: a group key that omits a partition
  key, or an untested key type, is refused rather than handled slowly.
- A retention `DROP` takes the definition out of `live` for as long as its
  correction runs, which can be minutes for a large leaf.
- After any dump and restore the pass pauses every definition on a
  partitioned source, holding stale values
  until an operator runs `RESUME`, which rebuilds.
- Four cases are left as [known correctness gaps](../known-correctness-gaps.md)
  rather than designed around, because no capture shape or fingerprint sees
  them: "A `MERGE` that moves a row to another partition", "The source renamed
  or re-parented and back within one pass", "A short-lived leaf beside a
  `DEFAULT` partition" and "A leaf detached and attached again around a
  `pg_upgrade`".
