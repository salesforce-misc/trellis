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
  hierarchy, is refused when a definition is applied. (A statement trigger
  fires only for the table a statement names: one on a partitioned parent
  misses a write aimed at a partition directly, and one on a partition or an
  inheritance child misses a write made through its parent.) `self_check`
  reports a source that joins such a hierarchy later
  (`staging::capture_audit`).
- **Images.** `old_image` and `new_image` hold the primary key plus every
  column some reader of the table needs (`capture::columns`), rendered with
  `format('%s', col)` under the same five pinned output settings as every
  Trellis session (`DateStyle`, `TimeZone`, `IntervalStyle`, `bytea_output`,
  `extra_float_digits`), set as the function's own `SET` clauses. So an image
  doesn't depend on the writing session's settings. `old_image` comes from
  the OLD transition table, which carries the whole row, detoasted.
- **`new_image` is the live row** (#623 D8a). When another write to the
  table ran during the statement (see "When capture re-reads" below), each
  row the statement wrote is
  re-read from the table by primary key, and `new_image` images that row,
  the one the transaction holds once the statement and its own `AFTER ROW`
  triggers are done, not the transition table's version (see "Nested writes
  to the same key" below). If the key is gone from the table, a nested
  write deleted or re-keyed it, and the row staged is a delete. A delete's
  key is re-read too: a nested write that put the key back stages an update
  to the live row. The re-read never sees another transaction's change: each
  row it joins was written or deleted by this statement, so this transaction
  holds its row lock, or for a new key its unique-index entry, until commit,
  and the key's live version is one this transaction wrote. The probe keeps
  only such a version (`age(xmin) <= 0`): under `REPEATABLE READ` and
  `SERIALIZABLE` a version another transaction deleted after the snapshot
  stays visible beside the one this statement re-created. It also requires
  the version's key to render as the row's own, because the ring keys by
  text and a type's `=` can be looser (`numeric` `1.0 = 1.00`). The re-read
  is a `LATERAL … LIMIT 1` probe per row, and the function runs with
  `enable_seqscan` off: PL/pgSQL plans once per session, often against a
  table that was empty then, and a cached sequential scan would read the
  whole table for every captured row once it grows
  (`tests/capture_reread.rs`, at every isolation level).
- **The re-read needs `SELECT` on the table**, which the Trellis role holds
  as the table's owner; the capture audit reports it missing. On a table
  with `FORCE ROW LEVEL SECURITY` whose policies hide a row from the
  Trellis role, the re-read can't find it and stages a delete.
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
  across OLD, NEW and the live row (#133).
- **`row_txid`** is the ring's default, `pg_current_xact_id()`: the source
  commit's own `xid8` (exact identity, invariant I0).
- **`lsn` and `origin_lsn`** are `pg_current_wal_insert_lsn()` when the
  trigger runs, below the commit's position.
- **`src_changed`** is `clock_timestamp()` when the statement's trigger runs:
  change time, not commit time (#622 plan Q8).
- **The slot.** The function reads the active ring slot from
  `ring_slot_mirror` in the same expression that assigns the writer's xid,
  so the seal's fence ([03](03-sealing-and-the-fence.md)) holds for
  application writers at every isolation level, prepared transactions
  included.

The functions are `SECURITY DEFINER`, owned by the role that owns the ring (the
role that ran the migrations), with `search_path` pinned. That need not be the
schema's owner: a DBA can pre-create the schema as another role (issue #701).
The application needs no privilege on Trellis's schema. The triggers are
`ENABLE ALWAYS`, so a session in `session_replication_role = replica` is
captured too. Nothing in the capture path issues `NOTIFY`, because `NOTIFY`
takes a database-wide lock at commit.

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
Nothing cancels a lock holder, an autovacuum included (#622 plan Q1): a table
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
committed, as it always has.

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
already read unpauses when the backfill ends, as before.

## Order

Per-key order is `(lsn, change_id)`. A second writer of a key runs its
trigger only after the first commits, because it waits on the row lock, so its
rows sort after the first's (#565 E4). Cross-key order is not promised. In
particular, an `ON DELETE CASCADE` child's capture runs before its parent's
statement trigger, the reverse of the WAL's order; nothing depends on it.

## Convergence

Because a change's ring rows commit with the change, every commit at or below
a watermark token already has its rows in the ring when the token is read.
The read-your-writes predicate ([07](07-convergence-and-await.md)) only asks
the ring, and a waiter writes nothing.

## A renamed or dropped column (#622 C6)

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
statement and nothing measurable past a few rows per statement (#622 C4's
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
names is left out the same way. `RESUME TRANSFORM` deletes the record and
rebuilds: once the column is back, the pass widens capture again under the
join fence before the rebuild is dispatched.

The reconcile pass doesn't wait for a marker either. Before it regenerates a
table's functions, it pauses the same way every definition not yet paused
that reads a column the table lacks: a column the installed functions image
(what their next write's marker would name), or one an active definition
reads. That covers two orderings no marker would. A pass that runs between a
primary-key rename and the table's next write would otherwise regenerate the
functions keyed by the new name, so no write would ever mark the change and
every drain would fail on rows keyed by a column the definitions don't know.
And a definition resumed (or registered) while its column is still missing
would otherwise wait to backfill forever, with the table's capture failing
every pass and nothing on its status; instead it pauses again with its
`capture_failure`. A pass that pauses leaves the table for the next pass.

Regenerating from an event trigger, inside the DDL's own transaction, is not
built (#622 plan Q2(b)).

## Nested writes to the same key (#680)

Suppose an application `AFTER ROW` trigger, or a self-referencing cascade,
rewrites a row its own statement wrote. The nested statement's capture runs
first, so the outer statement's ring row has the higher `lsn`. Its OLD and
NEW transition tables still hold the outer statement's own versions, so
before #623 D8a it staged the older values last, and a `GROUP BY` or 1-1
target kept them. The outer row's `new_image` is now the live row, the final
one (`tests/capture_join.rs`, `tests/ledger_interleavings.rs`). Its
`old_image` is still the outer statement's OLD row: the fold keeps the
earliest old image anyway, and only the relationship readers read it (until
#624).

## When capture re-reads (#623 D8a)

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
occurs twice among its old rows (found in review).
`tests/capture_ssi.rs` pins the gate, and `capture::sql`'s `function_body`
has the argument.

## What it costs the writer

The capture runs inside the application's transaction, so the writer pays for
it: about 16 µs per single-row statement on tmpfs, and 1.3–1.7× one
expression index's CPU per row for batched writes, with about 300 bytes of
WAL per row (#622 C4, `local_docs/bench/622-baseline.md`). The begin
trigger and the span bookkeeping (#623 D8a) add about 4.4 µs per statement,
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
