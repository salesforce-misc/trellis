# Stage 1 — Capture by triggers

← [Overview](README.md) · next → [The staging ring](02-the-staging-ring.md)

**What this stage owns:** turning a source change into a durable row in the
staging ring.

**The guarantee:** *a change's ring rows commit in the writer's own
transaction.* A change is in the ring exactly when the write that made it
committed. A crash at any instant loses nothing and replays nothing, because
there is no second transaction to lose or replay.

[ADR-0002](../decisions/0002-async-data-flow.md) records why capture works
this way. The code is in `trellis/src/capture/`.

## The capture function

Each table some definition reads carries four `AFTER … FOR EACH STATEMENT`
triggers: insert, update and delete with transition tables, and truncate. Each
calls a function generated for that table and event
(`capture::sql`), which appends the statement's changes to the active ring
segment with one `INSERT … SELECT`:

- **Keys.** Every ring row is keyed by the table's primary key, in declared
  order, joined with `\x1f` for a composite key. A table without a primary
  key, a partitioned table, or a table in a partition or inheritance
  hierarchy is refused (see
  [supported sources and targets](../transforms.md#supported-sources-and-targets)).
  A table can join a hierarchy after define (`ATTACH PARTITION`, `INHERIT`),
  or be dropped and recreated as one. The reconcile pass checks every table
  it captures with the same check define runs (`defs::hierarchy`), before
  anything else: it pauses every definition that reads one, with its
  `capture_failure`, and installs or changes no capture on it. A resume
  refuses until the table is plain again. A table that joins and leaves a
  hierarchy between two passes is never seen
  ([known correctness gaps — Capture disrupted and restored between two passes](../known-correctness-gaps.md#capture-disrupted-and-restored-between-two-passes)).
  A relationship's to-side is captured while some definition reads the
  relationship's from-side, whether or not one reads through the
  relationship, so the pass can find a to-side in a hierarchy with nobody to
  pause. Its settled projection then misses the writes routed through the
  hierarchy. Every definition that reads through the relationship is paused
  or refused while the table is in the hierarchy, and the first one to read
  through it again, by a define or a resume, refreshes the projection from
  the to-side in its own transaction (`catalog::relationships_to_refresh`,
  #768). Pinned by `unread_to_side_hierarchy.rs`.
- **Images.** `old_image` and `new_image` hold the primary key plus every
  column some reader of the table needs (`capture::columns`), rendered with
  `format('%s', col)` under the same five pinned output settings as every
  Trellis session (`DateStyle`, `TimeZone`, `IntervalStyle`, `bytea_output`,
  `extra_float_digits`), set as the function's own `SET` clauses. So an image
  doesn't depend on the writing session's settings. `old_image` comes from
  the OLD transition table, which carries the whole row, detoasted.
- **`new_image` is the live row.** When another write to the table ran during
  the statement, capture re-reads each row by primary key (see "When capture
  re-reads" below); otherwise it images the transition table. The re-read
  needs `SELECT` on the table, which the Trellis role holds as the table's
  owner; the capture audit reports it missing. A table whose row-level
  security hides rows from the Trellis role is refused (see
  [supported sources and targets](../transforms.md#supported-sources-and-targets)).
- **Updates** pair the OLD and NEW transition tables by primary key. A row
  whose key changed has no partner, so it becomes a delete of the old key and
  an insert of the new one.
- **An update that changed nothing imaged stages nothing.** A paired row
  whose imaged columns all hold the values they had, compared with
  `record_image_ne` over the typed values, is dropped. That covers an update
  of only columns no reader reads, and one that sets a column to its own
  value. A binary comparison works for every type, including those with no
  equality operator (`json`, `point`, `xml`), and is never looser than the
  images: an equal value that renders differently (`numeric` `1.5` to `1.50`)
  still stages. Every relationship join column (`from_col`, `to_col`) is
  imaged, so a join move alone always stages. A statement trigger can't take
  a `WHEN` clause when it has transition tables, so the filter is in the
  function.
- **`group_key`** is the union of every outbound relationship's `from_col`
  across OLD, NEW and the live row.
- **`row_txid`** is the ring's default, `pg_current_xact_id()`: the source
  commit's own `xid8` (exact identity, invariant I0).
- **`lsn` and `origin_lsn`** are `pg_current_wal_insert_lsn()` when the
  trigger runs, below the commit's position.
- **`src_changed`** is `clock_timestamp()` when the statement's trigger runs:
  change time, not commit time.
- **The slot.** The function reads the active ring slot from
  `ring_slot_mirror` in the same expression that assigns the writer's xid,
  so the seal's fence ([03](03-sealing-and-the-fence.md)) holds for
  application writers at every isolation level, prepared transactions
  included.
- **Truncate** stages one image-less `truncate` row keyed by the sentinel
  `\x1ftrellis-truncate-sentinel`. The seal records it
  (`segments.has_truncate`), and the drain empties the table's ledgers and
  targets and raises their truncate floor.

### A partitioned source's truncate

Every node of a partitioned source's tree carries the root's triggers, which
call the root's functions, so every row stages under the root's
`src_table` ([ADR-0017](../decisions/0017-partitioned-sources.md)). A
`TRUNCATE` fires the truncate trigger of each table it empties: each table it
names, then the partitions under that table, a parent before its children.

- **The root's truncate** stages the `truncate` sentinel, as a plain table's
  does: the target is cleared once.
- **A leaf's or middle's truncate** stages a `node_truncate` marker instead
  (key `\x1ftrellis-node-truncate`, `new_image` `{"node":
  "<schema>.<table>", "oid": <oid>}`, no `old_image` or `group_key`). It
  raises no truncate floor, is no barrier, and the fold leaves it out. The
  seal queues a `truncated` correction of the node for each marker in the
  segment's fenced window, in the statement that publishes the fence
  ([03](03-sealing-and-the-fence.md)), unless an unstarted correction of the
  node is already queued. The correction re-derives the keys whose group
  routes to the node and those of its current rows
  (`staging::partition_correction`), so other nodes' groups are untouched.
- **A table under one already truncated stages nothing.** The function keeps
  a transaction-local list of the tables of the tree truncated since capture
  last staged a row (`trellis_capture.t_<hash>`, next to the statement-span
  state). A node whose ancestor, or itself, is on the list stages nothing:
  nothing captured has been written to the source since that truncate. So a
  root `TRUNCATE` stages one sentinel and a middle's one marker, not one row
  per table. A `TRUNCATE` naming several nodes, or emptying several by
  `CASCADE`, stages one marker per node. A leaf whose trigger fires before
  its root's (the leaf named first, or a `CASCADE` through a foreign key the
  root's partitions inherit, which adds the tables in oid order) stages its
  marker as well as the root's sentinel: one correction too many, no more.

The functions are `SECURITY DEFINER`, owned by the role that owns the ring (the
role that ran the migrations), with `search_path` pinned. That need not be the
schema's owner: a DBA can pre-create the schema as another role. The
application needs no privilege on Trellis's schema. The triggers are
`ENABLE ALWAYS`, so a session in `session_replication_role = replica` is
captured too. A logical-replication subscription's apply worker fires only
row-level triggers, so a table a subscription replicates into is refused (see
[supported sources and targets](../transforms.md#supported-sources-and-targets)).
Nothing in the capture path issues `NOTIFY`, because `NOTIFY` takes a
database-wide lock at commit.

What is installed lives only in Postgres's catalog: the triggers in
`pg_trigger`, and each function's column set in its `COMMENT ON FUNCTION`.
`pg_dump` carries both.

## Installing, widening, narrowing and uninstalling

The staging worker's maintenance loop brings every table's capture to what the
catalog needs, on every reconcile pass, before it discharges backfill markers
(`capture::reconcile`):

| Change | Operation | Table lock |
|---|---|---|
| A definition reads a table nothing captures | install, with the table's join marker in the same transaction | `SHARE ROW EXCLUSIVE` |
| A new reader needs a column the functions don't image | widen: replace the functions | `SHARE ROW EXCLUSIVE` |
| The last reader of a column is dropped | narrow: replace the functions | none |
| The last reader of the table is dropped | uninstall: drop the triggers and functions | `ACCESS EXCLUSIVE` |

Every locking attempt waits at most 50 ms for the table, so an application
writer never queues behind one for longer (ADR-0002 I6), and is retried. A
pass spends at most one second on locked tables in all; the first attempt on
each table always runs, so one blocked table doesn't delay another's join.
Nothing cancels a lock holder, an autovacuum included: a table
whose lock stays held is left for the next pass. While a definition waits on
such a table, its status carries `capture_wait`, naming the sessions holding
the lock.

Defining a transform never waits for any of this. `apply` validates and
records the definition; the install happens in the background.

## The join fence

An install parks the table's join marker in the transaction that creates the
triggers, so the marker commits exactly when capture starts, and the table
lock means no writer of the table is in flight at that moment. A write
committed before the install is in the backfill's enumeration; one after it
runs the trigger. The discharge fences the marker only after reading it
committed.

## Which definitions a discharge may dispatch

A waiting definition starts applying ring rows as soon as a discharge
dispatches it, so it must not meet a row whose image lacks a column it reads.
Two things could put one in front of it, and the pass rules out both:

1. **Rows the replaced function staged.** A widen records a capture gate on
   its marker, the insert position read while it holds the table lock. The
   marker's discharge waits until no change to the table at or below the gate
   is pending (deferred relationship reverses included).
2. **A table it reads through a relationship.** A definition sourced from `U`
   that reads a column of `T` waits on `U`'s marker, which `T`'s gate doesn't
   hold. So a definition is dispatched only once every table it reads is
   captured and current, and no relationship to-side it reads still has a
   gated marker pending. That marker also refreshes `T`'s settled projections,
   repairing any column the old images wrote as `NULL`.

The pass hands the discharge the list of definitions it found ready, from the
same catalog snapshot it built the column sets from, so a definition
registered after the pass read the catalog waits for the next pass.

An `ALTER TRANSFORM` field is the same problem for a definition that is
already applying. The edit pauses the fields it adds or changes while its
backfill runs. When a field reads a source column the definition didn't read
before, that pause stays when the backfill ends (`column_status
.awaiting_capture`). The edit's catch-up marker then waits: its discharge
unpauses the field only once the installed capture images the column, and
only after the widen's gate, so every row the old function staged has drained
past the paused field. The same discharge's enumeration re-derives every row
with the field unpaused. A field that reads only columns the definition
already read unpauses when the backfill ends.

## Order

Per-key order is `(lsn, change_id)`. A second writer of a key runs its
trigger only after the first commits, because it waits on the row lock, so its
rows sort after the first's. Cross-key order is not promised. In
particular, an `ON DELETE CASCADE` child's capture runs before its parent's
statement trigger, the reverse of the WAL's order; nothing depends on it.

## Convergence

Because a change's ring rows commit with the change, every commit at or below
a watermark token already has its rows in the ring when the token is read.
The read-your-writes predicate ([07](07-convergence-and-await.md)) only asks
the ring, and a waiter writes nothing.

## A renamed or dropped column

A function's inserts name every column it images, so once one of them is
renamed or dropped they no longer plan, and without a guard every write to
the table would fail. Instead, the insert, update and delete functions count
their columns first, in one `pg_attribute` probe that rides in the
empty-statement guard's query (`present := case when exists (select 1 from
<transition table>) then (select count(*) from pg_attribute …) else -1
end`). PL/pgSQL plans a statement only when it first runs it, so on a miss
the stale inserts are never planned. The check is per statement, and sound,
because `ALTER TABLE … RENAME` and `DROP COLUMN` take `ACCESS EXCLUSIVE`: no
captured statement runs across one. It costs about 3.4–5 µs per single-row
statement and nothing measurable past a few rows per statement (
`trigger+column-check-guard`). There is no `EXCEPTION` block, so no
subtransaction.

On a miss the function:

1. appends a `schema_changed` marker for the table (key
   `\x1ftrellis-schema-changed`, `new_image` `{"missing": [...],
   "key_missing": bool}`);
2. if the primary key is intact, images the statement's rows over the
   columns that are left, with the same `SELECT` as the static insert built at
   run time and run with `EXECUTE` (PL/pgSQL registers the transition tables
   for the whole trigger call, so dynamic SQL reads them). If a key column is
   gone, it writes the marker only.

The seal records whether a segment's fenced window holds a marker
(`segments.has_schema_change`). A drain that claims such a segment reads its
markers over the whole window, every bucket, before it folds anything
(`staging::schema_change`), and pauses every definition that reads a
missing column (`capture::columns::readers_of`; with `key_missing`, every
definition that reads the table), recording why in `capture_failures`. That
commits before the drain's compute picks the definitions to apply, so no
page of the segment, on any worker, applies a partial image to a paused
definition, on the ledger path or any other. A marker and the partial images
after it come from one trigger call, so every segment with such a row has
its marker. The fold leaves markers out.

`Trellis::status` reports the pause as `DefinitionStatus::capture_failure`
(the table, the columns, a message, and when it was found). While the
record exists, capture doesn't count the definition's columns, so the next
reconcile pass narrows the functions to what the other readers need and the
markers stop; a column only an unused relationship or a projection still
names is left out the same way. `RESUME TRANSFORM` re-validates the
definition against the live schema as define would, and refuses while a
column it reads is still missing; otherwise it deletes the record and
rebuilds: the pass widens capture again under the join fence before the
rebuild is dispatched.

The reconcile pass doesn't wait for a marker either. Before it regenerates a
table's functions, it pauses the same way every definition not yet paused
that reads a column the table lacks: a column the installed functions image
(what their next write's marker would name), or one an active definition
reads. That covers two orderings no marker would. A pass that runs between a
primary-key rename and the table's next write would otherwise regenerate the
functions keyed by the new name, so no write would ever mark the change and
every drain would fail on rows keyed by a column the definitions don't know.
And a definition whose column goes missing between its resume (or
registration) and the next pass would otherwise wait to backfill forever,
with the table's capture failing every pass and nothing on its status;
instead it pauses again with its `capture_failure`. A pass that pauses leaves the table for the next pass.

A drain page doesn't wait for the pass on a redefined primary key either.
The installed functions go on imaging the old key until the pass
regenerates them, so no write marks the change, and a page would read their
rows by the new key: an arity change (`(id)` to `(id, g)`) fails the page,
and a same-arity one (`(id)` to `(g)`) reads the wrong rows. So before a
page computes anything, it compares the key each of its tables' installed
functions record with the table's primary key now, one catalog read per
page, and on a mismatch runs the pass's pause for that table
(`staging::schema_change::pause_readers_of_redefined_keys`). The page's
compute then applies none of those rows, and nothing is charged to
quarantine for them.

## A re-typed key column, or an outgrown copy

`ALTER COLUMN ... TYPE` rewrites the table but keeps its triggers, and the
capture functions name columns in dynamic SQL, so capture goes on working
over the new type: nothing marks the change. The rows the rewrite changes
fire no trigger at all.

So the same pass also checks, for every definition that reads the table,
the columns it keys by there (its source key, its `GROUP BY` keys, a
`JOIN`'s `ON` columns, and each relationship it reads through's join
columns and to-side key) and the
columns Trellis created from them with a type that comes from the source
(`defs::copies`: a 1-1 target's key, passthrough and calculated columns, an
aggregate's `GROUP BY` columns in its target, ledger and group-delta table,
its field columns and its ledger's contribution columns, a `JOIN`'s
`GROUP BY` and `ON` columns in its target and hidden tables and its field
columns, and a to-one relationship projection's key and its column for each to-side column read
through the relationship). A column typed by an expression is compared
with the type define's inference gives the expression over the live
schema. Before
it regenerates anything, it pauses the definition, with its
`capture_failure`, when

1. a key column now has a type or collation define would refuse for that use
   (a join column made `character(n)`, a nondeterministic `COLLATE` on a
   `GROUP BY` column);
2. a relationship it reads through no longer joins two columns of the same
   type and modifier (one join column widened to `bigint`, the other not
   yet);
3. a key column's type changed in a way that renders the values already
   stored differently: `timestamp` to `timestamptz`, `date` to `timestamp`,
   `text` to `uuid`, a narrower `numeric` scale or temporal precision (the
   rewrite rounds), or a narrower `varchar(n)` (the rewrite strips trailing
   spaces past the new length). It compares against the type recorded in
   `definition_key_types` when the definition was accepted or last resumed,
   or when a pass last accepted a change to the column without a pause; or
4. a column Trellis created can't hold every value of the type define would
   give it now: `integer` to `bigint` under a 1-1 target's key, a `GROUP BY`
   key, a calculated field (`qty + 1`) or a `SUM` or `MIN` (whose ledger
   contribution is typed as the argument), `integer` or `bigint` to
   `numeric` under the same, `real` to `double precision` under a
   passthrough. The column's next value that doesn't fit would fail its
   write.

The reason names each column, its old and new type, and each copy, and says
what to do. Nothing clears it but a deliberate `RESUME`, or a drop.

A modifier never makes the fourth check pause (ADR-0020, #1207, #1288): every
column Trellis creates from a source column has the source column's type with
no modifier, and its collation. It holds whatever the source column holds
after any change of a modifier (a string bound, a `numeric` precision or
scale, a temporal or `interval` precision, a `bit` length), and `text` and
`varchar(n)` source columns are one family to the check. So a widening of a
modifier needs no action on the copies, and a value past the old modifier is
stored as it is, with no pass in between. Only an integer or float width
outgrows a copy.

Changes that need neither pause nothing: widening a key no copy holds (an
aggregate's source key, which its ledger keys by text), a `GROUP BY` key's
`varchar` widening or move to `text` (its copy is `text`), a string column's
bound or move between `text` and `varchar` (its copies are unbounded), a `numeric`
precision change or wider scale, or a wider timestamp precision, on a
`GROUP BY` key (its copy is unconstrained), a narrowing every copy still
holds, a change to a column read only as a field that every column typed
from it still holds (a `varchar` widening under a calculated field, whose
column is `text`), and a change between deterministic collations (byte
equality is unchanged, even across a join pair; define and a resume still
require a pair's collations to match). A change to a column read as a field
that isn't a pure widening, a narrowing included, is the next section's: it
rebuilds the readers, or pauses the ones it leaves invalid.

A resume re-validates the definition as define would and refuses while the
first two hold, naming the columns and what to change. Otherwise, if any
column it created no longer has the type define would give it from the live
schema, the resume returns at once and leaves it paused with a resume
request (`resume_requests`): the next pass re-types each such column (`ALTER
... TYPE`, one table per transaction, under `ACCESS EXCLUSIVE`, which
rewrites the table for `integer` to `bigint`) and then completes the resume,
which records the live key types and rebuilds. A crash between the two
leaves the request, and the next pass finishes it. So does a re-type that
fails transiently (a lock it can't get in time, a deadlock, a serialization
failure, a cancelled statement, a lost connection): the request stays, with
no failure reported, and the next pass tries again. The re-type of a
definition's target records which resume re-typed each column
(`retype_causes`), so when the pass then pauses a definition chained off
that target for those columns alone, its pause records the upstream as its
cause (`capture_failures.caused_by`) and its `capture_failure` names the
upstream resume. At the start of each pass, the pass resumes a definition
with such a cause once that upstream is `live` and the definition has no
resume request, the way an operator's `RESUME` would, so one `RESUME` of the
head of a chain carries the chain, each rebuild after the one above it. One
whose resume is refused stays paused with the refusal and no cause.

The pause doesn't stop a drain that reads a source key whose type is off the
key allowlist: `staging::apply::compute` introspects every staged table's key
before it asks which definitions apply, and halts on that type (#663). The
halt and the capture pass's pause both record a `capture_failure`, and the
first one stands.

## A column changed or rewritten without a trigger

Two more kinds of DDL change what a read column holds while its name stays
the same, and fire no DML trigger (#703):

- **A column dropped and re-added** (`DROP COLUMN plan; ADD COLUMN plan
  text`): every row holds the new column's default, under a new `attnum`.
  **A column retyped** other than by a pure widening: a narrowing, a
  `numeric` scale change (`1.50` becomes `1.5000`, or rounds), a move to
  another type family (`integer` to `double precision`). A pure widening (a
  wider integer, a longer `varchar` or `text`, more `numeric` precision at
  the same scale, a wider temporal precision) keeps every value's text, and
  is left to the previous section's checks of the columns Trellis created.
- **A same-type rewrite**: `ALTER COLUMN b TYPE integer USING b * 10` on an
  `integer` column changes every value and leaves the name, `attnum`, type
  and modifier as they were.

So each captured table keeps a baseline in `capture_baselines`: per column
its capture images, on the table itself or on each leaf of a partitioned
root (the root has no storage of its own), its `attnum`, type and modifier,
the `xmin` of its `pg_attribute` row, a hash of the rest of that row with
its default expression, and the node's file node. An install, widen or
narrow records it in the transaction that installs the functions, for the
columns and leaves it newly covers. Each capture pass compares the live
catalog with it before it regenerates the table's functions, and writes what
it read back in the transaction of its response, so a change seen once is
acted on once (`capture::baseline`). It costs two catalog reads per table.

- **A changed `attnum`, or a type change that isn't a pure widening, is a
  changed column.**
- **A same-type rewrite is the row's `xmin` and the node's file node both
  moved, and nothing else about the column changed.** Each moves alone in
  routine work: `SET DEFAULT`, `SET NOT NULL`, `SET STATISTICS` and column
  `GRANT`s move the row, and `VACUUM FULL`, `CLUSTER`, pg_repack, `SET
  LOGGED` and a volatile `ADD COLUMN` rewrite the table. A framework
  migration often does one of each in a transaction (`add_column ...
  default: -> { 'gen_random_uuid()' }` beside a `change_column_default`), so
  the rewrite counts only when the rest of the column's row, its default
  expression included, is what it was. A catalog freeze leaves the `xmin` a
  query reads as it was.

The response follows whether the definitions are still valid as written:

- **Rebuilt automatically, pausing nobody.** A same-type rewrite, and a
  changed column every definition reading it is still valid over. The pass
  re-reads the table for its applying readers, exactly as
  `Trellis::request_backfill` does: a reader the Re-derive build takes
  reports `backfilling`, every other `catching_up`, until the re-read is
  done. It logs a `WARN` naming the table, the cause and the readers, and
  counts `capture_auto_rebuilds_total{source_table, cause}`, `cause`
  `value_rewrite` or `column_changed`
  ([observability](../observability.md#metrics)).
- **Paused, with a `capture_failure` naming the column and the change.** A
  changed column where define would now refuse a definition that reads it,
  where the definition would evaluate it as another type (`integer` to
  `double precision`, so the types it recorded no longer hold), or where a
  `numeric` column Trellis created from it must be re-typed to the new
  scale. `RESUME` re-validates, re-types and rebuilds, or refuses while
  define would. A key column's type change is left to the previous
  section's checks; a key column dropped and re-added rebuilds.

### Capture changed outside Trellis

A statement trigger captures only what it sees, so an operator who removes,
swaps, disables or recreates Trellis's capture triggers leaves writes that
the table's current state can't recover. The pass therefore never repairs
that by itself. It pauses every definition that reads the table, with a
`capture_failure` naming what it found and saying that changes made
meanwhile may be missing, so correctness can't be guaranteed, and waits for a
`RESUME`.

Each install, widen, narrow and repair records, in the transaction that makes
it, the oid, `xmin` and enabled mode of each of the five capture triggers of a
plain table, or of a partitioned source's root and each of its tree's nodes
(`capture_triggers`, one set of rows per node), and each capture function's comment records a
hash of the source Trellis generated. Trellis's own changes therefore never
read as a disruption, a crash in the middle of an install included. A widen
or narrow changes no trigger and keeps the rows it finds. The pass compares
the live triggers and functions with them (`capture::triggers`) and pauses
on any of:

- the table is gone from its name: dropped, renamed away or moved to another
  schema, or a view, materialized view or foreign table put there in its
  place. This covers a captured source, a `JOIN`'s joined table, a
  relationship's to-side, and another instance's target read as a source;
- a recorded trigger is gone from the table at that name: a swap of the
  table, a drop and recreate, the triggers dropped;
- a trigger's oid changed: recreated by hand or by a schema tool;
- a trigger isn't `ENABLE ALWAYS`: disabled, `ORIGIN` or `REPLICA`.
  `ENABLE TRIGGER ALL|USER`, which `pg_restore --disable-triggers`, Rails
  fixtures' `disable_referential_integrity` and hand-written bulk loads end
  with, leaves `ORIGIN`;
- a trigger's `xmin` moved under the same oid: disabled and enabled again by
  name, even within one transaction. `ENABLE ALWAYS TRIGGER` takes a trigger
  name only, so a restore to exactly `ALWAYS` between two passes is done by
  name, and this catches it;
- a capture function's source differs from the hash its own comment
  records: it was replaced by hand.

A table's records live only while some definition reads it. The transaction
that drops the last reader (`DROP TRANSFORM`, or `DROP RELATIONSHIP` for a
relationship's to-side) deletes the table's trigger, column and partition-tree
records, so a definition made afterwards, on a table dropped and created again
before the next pass, is judged against nothing: it never read the old table.
A paused or quarantined definition still reads its tables, so while one is
left the records stay and a real disruption pauses a new definition beside it
too. Uninstalling the triggers stays the pass's job, and the pass forgets any
record of a table nothing reads as well.

Routine maintenance moves none of these: `VACUUM FULL`, `CLUSTER`,
`REINDEX`, pg_repack, a catalog freeze, a rename and back within one
transaction, and a migration that leaves the triggers alone. Dumps and
restores are told apart by the rows themselves: they carry the cluster's
`system_identifier` and the oid of `capture_triggers`, and a stored value
that isn't the live one means the rows came from another database (a restore
or `pg_upgrade`, which recreates every trigger under a new oid). The pass
forgets them and records the live triggers. New trigger oids under the same
values are a recreation in this database: that pauses.

The pause is recorded in `capture_failures` with `kind` `disruption`, so a
tool can tell it from an operator's `PAUSE` and from every other failure. A
definition already paused by the operator gets the same record with `kind`
`capture`: it stays the operator's to resume, and the record is what makes
that `RESUME` forget the table's recorded capture. `Trellis::detect_capture_disruptions` runs this check
(`capture::reconcile::detect_disruptions`) on the caller's connection and
pauses the same readers, installing and repairing nothing, so a caller that
has just loaded data needn't wait for the next pass.

The pause is immediate (one pass, no threshold) and holds until `RESUME`,
which refuses while the table is missing, re-validates the definition against
the table now at that name (it may be a different table), forgets the record,
and lets the next pass reinstall capture, record it and rebuild. The pass
installs nothing on the table while its readers are paused. A table swap, a
bulk load with triggers disabled or a fixture load therefore needs a
`RESUME` afterwards ([embedding](../embedding.md)).

A partitioned source's pass checks each recorded node that is still in the
tree, and the root's functions once; the finding names the node. Partition
maintenance pauses nothing, because it never changes a recorded node: a leaf
that is attached, created (a `DEFAULT` and a `HASH` re-split included),
detached or dropped is either not in the record or no longer in the tree, and
the tree's corrections are ADR-0017's. The pass forgets the rows of a node
that left the tree, installs capture on a new one (recording it in the same
transaction), and records a node that comes back carrying all five triggers
`ENABLE ALWAYS` as it finds it. A tree of 100 tables or more installs root
first and then one table per transaction, each recording its own tables, so a
pass that stops between two of them leaves a record for exactly the tables
that carry triggers. A node that is still in the tree and lost, disabled or
recreated its triggers is a disruption like a plain table's. So is the root
itself dropped, renamed away, moved to another schema, swapped in by renames or
dropped and recreated (empty, or with leaves attached): that is table
maintenance, not partition maintenance, so the pass pauses the readers and
corrects and reinstalls nothing; `RESUME` forgets the recorded tree and
rebuilds.

### What rebuilds, what pauses, and what's missed

Changes no capture trigger sees, and what the staging worker does about
them:

- **Repaired on its own, pausing nobody** (a re-read's readers report
  `backfilling` or `catching_up` until it's done):
  - a same-type `USING` rewrite of a read column: re-read;
  - a read column dropped and re-added, or retyped other than by a pure
    widening, where every definition reading it is still valid: re-read;
  - a widening of a modifier that changes only the catalog: nothing to do,
    since no copy has a modifier;
  - a capture function an older Trellis generated (its source still is what
    its own comment hashes): replaced by a widen.
- **Pauses, until a deliberate `RESUME`:**
  - capture changed outside Trellis, on a plain table or a partitioned source
    (the next section);
  - a read column renamed or dropped, or the primary key redefined, by a
    marker, the pass or a drain page;
  - a key column retyped or re-collated so define would refuse it, or so the
    stored keys render differently; a relationship's join columns no longer
    matching; a column Trellis created outgrown by a widening;
  - a read column changed so define would refuse a definition reading it, so
    it would evaluate the column as another type, or so a `numeric` column
    Trellis created must be re-typed;
  - row-level security applying to Trellis's roles, a subscription into a
    source, or a source joining a partition or inheritance hierarchy.
- **Missed, and documented** in
  [known correctness gaps](../known-correctness-gaps.md#a-columns-values-rewritten-in-place-by-alter-column-type):
  a `USING` rewrite with a metadata change to the same column in the same
  window; one on a column added with a constant default whose table no
  rewrite has touched since (the rewrite clears the default the column's row
  keeps for older rows, as `VACUUM FULL` does); one on a column with a
  compression method, a storage mode or a non-default collation set (the
  `ALTER` resets each); a pure widening no copy outgrows; and a column read
  by `MIN` or `MAX` re-collated, since a field column's collation isn't part
  of what the pass compares. Between two passes
  ([known correctness gaps](../known-correctness-gaps.md#capture-disrupted-and-restored-between-two-passes)):
  a table attached to a hierarchy, or given an inheritance child, and
  detached or the child dropped; a capture function replaced and restored by
  hand; a disruption a restore or `pg_upgrade` records before a pass sees
  it; a subscription that gains the table and loses it; capture changed
  outside Trellis on a partitioned source.

A few routine operations rebuild for nothing, at the cost of one re-read: a
rewrite that leaves the values as they were (`USING b + 0`, or a column
retyped and back between two passes), and an identical `SET DEFAULT`
re-applied in the same window as a table rewrite.

## Nested writes to the same key

When an application `AFTER ROW` trigger, or a self-referencing cascade,
rewrites a row its own statement wrote, the nested statement's capture runs
first, so the outer statement's ring row has the higher `lsn`. The outer
row's `new_image` is the live row, the final one, so a `GROUP BY` or 1-1
target keeps the final values (`tests/capture_join.rs`,
`tests/ledger_interleavings.rs`). Its `old_image` is the statement's OLD row;
the fold keeps the earliest old image anyway. A nested write that re-keys a
row is covered by the [known correctness gaps](../known-correctness-gaps.md).

### Every transition row counts once

A reader that sums each ring row's `f(new) − f(old)`, as a group basis does,
needs a transaction's rows to telescope to the final row minus the first. The
live `new_image` breaks that: when a trigger rewrites `a → b` to `c`, the
nested capture stages `(b, c)` and the outer one `(a, c)`, which sums to
`2c − a − b`. A 1-1 target still needs the live row at the highest `lsn`, so
the re-read stays, and a re-read row also records the statement's own
transition rows wherever its images aren't them:

- `transition_images` is `{"old": …, "new": …}`, each side the transition
  row's image or JSON `null`. It is set where the live row is gone (staged as
  a delete, whose `old_image` is the transition NEW row if the statement
  inserted it), where the live row differs from the transition NEW row, and
  where a delete finds its key put back (staged as an update). Otherwise it is
  `NULL` and `old_image`/`new_image` are the transition rows. A capture that
  doesn't re-read never sets it, so the common path writes nothing new.
- Each transition row is in exactly one ring row. An update's re-read pairs
  OLD and NEW rows one to one per key, so a key with two versions on a side
  pairs each once.

A summing reader takes a row's sides from `transition_images` when it is set.
A statement's rows then sum to `Σ f(NEW) − Σ f(OLD)` (a dropped no-op pair
cancels), and a transaction's telescope. 1-1 readers ignore the column.
`tests/capture_transition_images.rs` checks the sum, and each key's final
state, for every nested-write shape.

## When capture re-reads

`new_image` is the live row: the one the transaction holds once the
statement and its own `AFTER ROW` triggers are done, not the transition
table's version. When another write to the table ran during the statement
(below), each row the statement wrote is re-read from the table by primary
key. If the key is gone, a nested write deleted or re-keyed it, and the row
staged is a delete. A delete's key is re-read too: a nested write that put
the key back stages an update to the live row.

The re-read never sees another transaction's change: each row it joins was
written or deleted by this statement, so this transaction holds its row lock
(or, for a new key, its unique-index entry) until commit. The probe keeps only
a version this transaction wrote (`age(xmin) <= 0`), because under
`REPEATABLE READ` and `SERIALIZABLE` a version another transaction deleted
after the snapshot stays visible beside the one this statement re-created. It
also requires the version's key to render as the row's own, because the ring
keys by text and a type's `=` can be looser (`numeric` `1.0 = 1.00`). The
probe is a `LATERAL … LIMIT 1` per row, and the function runs with
`enable_seqscan` off: PL/pgSQL plans once per session, often against a table
that was empty then, and a cached sequential scan would read the whole table
for every captured row once it grows (`tests/capture_reread.rs`, at every
isolation level).

Under `SERIALIZABLE` the re-read's index probe takes a predicate (SIREAD)
lock on the key's btree leaf page, and concurrent serializable writers on
neighbouring keys then form the read-write conflict chains Postgres cancels
with `40001`. Every insert of an auto-increment id lands on the rightmost
leaf, so with an unconditional re-read 50% of single-row serializable
inserts failed at 4 writers and 87% at 8 and 16, where none fail without
capture (`benchmark` scenario `ssi-tax`).

So capture re-reads only when the live row can differ from the transition
row: when some other write to the same table changed it after the
statement's row change and before its capture. Each such write is itself a
statement on the table (a nested statement from an application trigger, an
FK cascade or a function the statement calls, or a sibling event of the
same statement: a writable CTE, `MERGE`, `INSERT … ON CONFLICT DO UPDATE`),
so its own capture runs first. A fifth trigger, `<schema>_capture_begin`
(`BEFORE INSERT OR UPDATE OR DELETE … FOR EACH STATEMENT`), marks where each
statement's span starts in a transaction-local setting, and a capture
re-reads only if another capture of the table staged rows since then.
Otherwise it images the transition tables and reads no relation, so a
statement nothing else touched takes no predicate lock on the table. The
inexact cases all err towards re-reading: a disabled or missing begin
trigger makes every capture re-read (and the capture audit reports it).

The re-read runs under the writing session's `row_security`, so a policy
that comes to apply to the ring's owner hides rows from it, and a hidden row
reads as gone: the capture stages a delete for a row that still exists.
Setting `row_security` in the capture functions would turn the policy into a
failed application write instead. A target that reads the table directly
doesn't show the wrong delete: a 1-1 or ledger apply re-reads a delete's row
live as the drain's role, which is either exempt from the policy and reads
the row, or refused, which halts the definition, and the group basis sums
the statement's own transition rows. A to-one relationship's projection
applies the delete, though, so until the next pass a 1-1 field read through
the relationship reads the parent as gone
([known correctness gaps](../known-correctness-gaps.md#a-to-side-row-hidden-from-capture-by-a-new-policy)).
The next reconcile pass pauses the table's readers
([transforms — Supported sources and targets](../transforms.md#supported-sources-and-targets)),
and their `RESUME` refreshes the projection.

One write isn't a statement of its own: the update a foreign key's action
makes (`ON UPDATE CASCADE`, `SET NULL` or `SET DEFAULT`, or `ON DELETE SET
NULL` or `SET DEFAULT`). Postgres runs it inside the trigger query level of
the statement that fired it, where it fires no `BEFORE` statement trigger
once one has fired, and its rows join the transition tables of the table's
update already queued there: one capture call covers both. A row both
updated (a self-referencing key that cascades, or two cascading keys on one
row) is in the transition tables twice, and pairing by key can image the
intermediate version last. A row that references itself through a
self-referencing `ON UPDATE CASCADE` can repeat a key among the new rows
only, where pairing by key counts one old row twice. So an update capture
also re-reads when a key occurs twice on either side, and the re-read pairs
one to one.
`tests/capture_ssi.rs` pins the gate, and `capture::sql`'s `function_body`
has the argument.

## What it costs the writer

The capture runs inside the application's transaction, so the writer pays for
it: about 16 µs per single-row statement on tmpfs, and 1.3–1.7× one
expression index's CPU per row for batched writes, with about 300 bytes of
WAL per row (`benchmark` scenario `write-tax`). The begin
trigger and the span bookkeeping add about 4.4 µs per statement,
and nothing per row. An update's two repeated-key checks, which group its old
and new rows by the typed key, add about 2.5 µs per statement each and well
under 1 µs per row (`write-tax` shapes `upd1x1`, `upd1000x1`). When a statement does
re-read, the probe adds about 0.65–1.0 µs per row in batched writes and
3–10 µs per single-row statement. An application trigger that rewrites every
row it inserted makes every statement re-read and stage transition images:
captured, `nest100x1` keeps 8.5% of its uncaptured rate.

## Trellis migrations and the write path

A Trellis migration that alters a ring table (`seg_0`–`seg_3`) takes
`ACCESS EXCLUSIVE` on it, and every captured writer's trigger inserts into
the ring. So while such a migration runs, every application write to a
captured table waits for it. Run those migrations in a quiet window.
