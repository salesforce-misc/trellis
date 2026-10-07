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

## A re-typed key column, or an outgrown copy

`ALTER COLUMN ... TYPE` rewrites the table but keeps its triggers, and the
capture functions name columns in dynamic SQL, so capture goes on working
over the new type: nothing marks the change. The rows the rewrite changes
fire no trigger at all.

So the same pass also checks, for every definition that reads the table,
the columns it keys by there (its source key, its `GROUP BY` keys, and each
relationship it reads through's join columns and to-side key) and the
columns Trellis created from them with a type that comes from the source
(`defs::copies`: a 1-1 target's key, passthrough and calculated columns, an
aggregate's `GROUP BY` columns in its target, ledger and group-delta table,
its field columns and its ledger's contribution columns, and a to-one
relationship projection's key and its column for each to-side column read
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

The fourth check has one exception, which pauses nothing (#824). When every
widened column of a table Trellis created widened by changing only the
catalog, the pass re-types that table's columns itself. Only a 1-1 target's
copies and a relationship projection's can: Trellis types every other
column by value family or by an expression, with no length or precision. The widenings that qualify
are exactly `varchar(n)` to a longer `varchar(m)`, `varchar(n)` or
`varchar` to `text`, `varchar(n)` to `varchar`, and `numeric(p,s)` to a
larger precision at the same scale. `character(n)` changes and `text` to
`varchar(n)` don't. Postgres then rewrites nothing, changes no value and
keeps each index, so no definition pauses or rebuilds. A `varchar` key's
widening is one of them: the key's copy is re-typed and its new type
recorded. It runs one table per transaction, under the same lock timeout
as a resume's re-type. A table whose re-type fails transiently (its lock
not got in time, a deadlock, a statement timeout) is left as it is,
nothing pauses, and the next pass tries again. A table where some column
also needs a rewrite (`integer` to `bigint`) pauses as above. A table
whose re-type fails for another reason (a view on the column) pauses too
when some column there outgrew its type. When none did (`varchar` to
`text`), its writes still succeed, so it pauses nothing and keeps its old
types. Either way the worker doesn't try that re-type again, and so
doesn't take the table's lock for it every pass, until the source's type
changes again, a definition with a column there is defined or dropped, or
the worker restarts.

A value written after the source widened and before the pass re-typed the
column failed its write with `22001` (too long) or `22003` (numeric
overflow), and its key may be held. One whose excess characters are all
spaces was stored truncated instead, which nothing repairs ([known
correctness gaps, entry
23](../known-correctness-gaps.md#23-a-value-padded-with-spaces-past-a-widened-varchars-old-length-drained-before-trellis-re-types-its-copy)).
The re-type's transaction records a release request (`retype_releases`) for
each definition with a column on the table. After the pass, the staging
worker releases every key such a definition holds whose failure had that
SQLSTATE (`poison.sqlstate`), through the same per-key release as
`Trellis::release_key`, so the key's parked work is applied again, and does
so after each pass for a minute more, for a key whose eviction was still
committing. A key held for any other failure stays held.

Changes that need neither pause nothing: widening a key no copy holds (an
aggregate's source key, which its ledger keys by text), a `GROUP BY` key's
`varchar` widening or move to `text` (its copy is `text`), a `numeric`
precision change or wider scale, or a wider timestamp precision, on a
`GROUP BY` key (its copy is unconstrained), a narrowing every copy still
holds, a change to a column read only as a field that every column typed
from it still holds (a `varchar` widening under a calculated field, whose
column is `text`), and a change between deterministic collations (byte
equality is unchanged, even across a join pair; define and a resume still
require a pair's collations to match). A field's move to another type
family (`integer` to `double precision`) pauses nothing either, though its
columns may not hold the new values
([known correctness gaps, entry 22](../known-correctness-gaps.md#22-a-column-read-only-as-a-field-moved-to-another-type-family)).

A resume re-validates the definition as define would and refuses while the
first two hold, naming the columns and what to change. Otherwise, if any
column it created no longer has the type define would give it from the live
schema, the resume returns at once and leaves it paused with a resume
request (`resume_requests`): the next pass re-types each such column (`ALTER
... TYPE`, one table per transaction, under `ACCESS EXCLUSIVE`, which
rewrites the table for `integer` to `bigint`) and then completes the resume,
which records the live key types and rebuilds. A crash between the two
leaves the request, and the next pass finishes it. So does a re-type that
fails transiently (its lock not got in time, a deadlock, a serialization
failure, a statement timeout, a lost connection): the request stays, with
no failure reported, and the next pass tries again. The re-type of a
definition's target records which resume re-typed each column
(`retype_causes`), so when the pass then pauses a definition chained off
that target for those columns alone, its pause records the upstream as its
cause (`capture_failures.caused_by`) and its `capture_failure` names the
upstream resume.

The pause doesn't stop a drain that reads a source key whose type is off the
key allowlist: `staging::apply::compute` introspects every staged table's key
before it asks which definitions apply, and halts on that type (#663). The
halt and the capture pass's pause both record a `capture_failure`, and the
first one stands.

## Nested writes to the same key

When an application `AFTER ROW` trigger, or a self-referencing cascade,
rewrites a row its own statement wrote, the nested statement's capture runs
first, so the outer statement's ring row has the higher `lsn`. The outer
row's `new_image` is the live row, the final one, so a `GROUP BY` or 1-1
target keeps the final values (`tests/capture_join.rs`,
`tests/ledger_interleavings.rs`). Its `old_image` is the statement's OLD row;
the fold keeps the earliest old image anyway. A nested write that re-keys a
row is covered by the [known correctness gaps](../known-correctness-gaps.md).

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
capture (`benchmark` scenario `ssi-tax`; `local_docs/pr/623-d8a.md`).

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

One write isn't a statement of its own: the update a foreign key's action
makes (`ON UPDATE CASCADE`, `SET NULL` or `SET DEFAULT`, or `ON DELETE SET
NULL` or `SET DEFAULT`). Postgres runs it inside the trigger query level of
the statement that fired it, where it fires no `BEFORE` statement trigger
once one has fired, and its rows join the transition tables of the table's
update already queued there: one capture call covers both. A row both
updated (a self-referencing key that cascades, or two cascading keys on one
row) is in the transition tables twice, and pairing by key can image the
intermediate version last. So an update capture also re-reads when a key
occurs twice among its old rows.
`tests/capture_ssi.rs` pins the gate, and `capture::sql`'s `function_body`
has the argument.

## What it costs the writer

The capture runs inside the application's transaction, so the writer pays for
it: about 16 µs per single-row statement on tmpfs, and 1.3–1.7× one
expression index's CPU per row for batched writes, with about 300 bytes of
WAL per row (`local_docs/bench/622-baseline.md`). The begin
trigger and the span bookkeeping add about 4.4 µs per statement,
and nothing per row. An update's repeated-key check adds about 2.7 µs per
statement and 0.3 µs per row (a rough in-transaction measurement, not
`write-tax`, which only inserts). When a statement does re-read, the probe
adds about 0.65–1.0 µs per row in batched writes and 3–10 µs per single-row
statement.

## Trellis migrations and the write path

A Trellis migration that alters a ring table (`seg_0`–`seg_3`) takes
`ACCESS EXCLUSIVE` on it, and every captured writer's trigger inserts into
the ring. So while such a migration runs, every application write to a
captured table waits for it. Run those migrations in a quiet window.
