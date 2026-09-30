# Stage 1 — Capture by triggers

← [Overview](README.md) · next → [The staging ring](02-the-staging-ring.md)

**What this stage owns:** turning a source change into a durable row in the
staging ring.

**The guarantee:** *a change's ring rows commit in the writer's own
transaction.* A change is in the ring exactly when the write that made it
committed. A crash at any instant loses nothing and replays nothing, because
there is no second transaction to lose or replay.

This replaced capture by logical replication (the stream, the slot and the
publication) in #622 C5, as [ADR-0002](../decisions/0002-async-data-flow.md)
decided. The code is in `trellis/src/capture/`.

## The capture function

Each table some definition reads carries four `AFTER … FOR EACH STATEMENT`
triggers: insert, update and delete with transition tables, and truncate. Each
calls a function generated for that table and event
(`capture::sql`), which appends the statement's changes to the active ring
segment with one `INSERT … SELECT`:

- **Keys.** Every ring row is keyed by the table's primary key, in declared
  order, joined with `\x1f` for a composite key. A table without a primary
  key, or a partitioned table, is refused when a definition is applied. (A
  statement trigger on a partitioned parent misses a write aimed at a
  partition directly, and a partition attached later has no triggers.)
- **Images.** `old_image` and `new_image` hold the primary key plus every
  column some reader of the table needs (`capture::columns`), rendered with
  `format('%s', col)` under the same five pinned output settings as every
  Trellis session (`DateStyle`, `TimeZone`, `IntervalStyle`, `bytea_output`,
  `extra_float_digits`), set as the function's own `SET` clauses. So an image
  doesn't depend on the writing session's settings. A transition table
  carries the whole row, detoasted, so an image never lacks a column it names.
- **Updates** pair the OLD and NEW transition tables by primary key. A row
  whose key changed has no partner, so it becomes a delete of the old key and
  an insert of the new one.
- **`group_key`** is the union of every outbound relationship's `from_col`
  across OLD and NEW (#133).
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

The functions are `SECURITY DEFINER`, owned by the role that owns the Trellis
schema, with `search_path` pinned. The application needs no privilege on
Trellis's schema. The triggers are `ENABLE ALWAYS`, so a session in
`session_replication_role = replica` is captured too. Nothing in the capture
path issues `NOTIFY`, because `NOTIFY` takes a database-wide lock at commit.

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
The read-your-writes predicate ([07](07-convergence-and-await.md)) needs no
"intake has staged past the token" condition, and a waiter writes nothing.

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
join fence before the rebuild is dispatched. If it is still missing, the
pass reports the table's capture as failed and the definition stays
`waiting_to_backfill`.

Regenerating from an event trigger, inside the DDL's own transaction, is not
built (#622 plan Q2(b)).

## Known limitation: nested writes to the same key (#680)

Suppose an application `AFTER ROW` trigger, or a self-referencing cascade,
rewrites a row its own statement wrote. The nested statement's capture runs
first, so the ring holds the newer image at the lower `lsn`, and the outer
statement's older image after it. A `GROUP BY` target then subtracts and adds
the wrong images and can leave the row in the wrong group. A 1-1 target is
unaffected, because its drain compares the image with the live row. Under
today's OLD+NEW images no image shape fixes this; D's NEW-only apply with a
re-read image (#623) does. `tests/capture_join.rs` pins the case as an
ignored test.

## What it costs the writer

The capture runs inside the application's transaction, so the writer pays for
it: about 16 µs per single-row statement on tmpfs, and 1.3–1.7× one
expression index's CPU per row for batched writes, with about 300 bytes of
WAL per row (#622 C4, `local_docs/bench/622-baseline.md`). The slot deferred
that cost to intake and capped it at intake's staging rate.

## Trellis migrations and the write path

A Trellis migration that alters a ring table (`seg_0`–`seg_3`) takes
`ACCESS EXCLUSIVE` on it, and every captured writer's trigger inserts into
the ring. So while such a migration runs, every application write to a
captured table waits for it. Run those migrations in a quiet window.
