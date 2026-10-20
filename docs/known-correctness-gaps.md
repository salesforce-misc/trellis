# Known correctness gaps

Trellis keeps each target equal to what its definition computes from the
source tables. This page lists the known ways a user or operator can break
that guarantee, and leave a target stale or wrong, *without* Trellis repairing
it on its own.

Things Trellis detects and repairs by itself aren't listed; those are just how
it works (see [What isn't on this list](#what-isnt-on-this-list)). Each entry
here needs an operator to act, either because Trellis can't see the problem or
because it only pauses or fails and waits for you.

## Preconditions

The guarantee holds when all of these do. Each entry below is a way one of
them gets broken without Trellis noticing.

* No one changes Trellis's artifacts by hand: its schema (the catalog and ring
  tables, by DML, DDL or a data load), the target tables and the hidden tables
  Trellis keeps beside them, and the capture functions and triggers it
  installs. The install survives a whole-database backup and restore as a
  unit; any manual change inside it can break the guarantee without Trellis
  noticing (entry 8).
* The capture triggers stay installed, enabled and `ENABLE ALWAYS`, and a
  table stays out of a partition or inheritance hierarchy, between two
  reconcile passes. Changing them on a plain table pauses its readers, which a
  `RESUME` rebuilds
  ([capture by triggers](staging-and-claiming/01-capture-by-triggers.md#capture-changed-outside-trellis));
  what the pass can't see is entry 5.
* No DDL rewrites the values of a column a definition reads beyond the
  changes Trellis detects (entry 1). A column dropped and re-added, retyped,
  or rewritten in place to the same type rebuilds its readers, or pauses the
  ones define would no longer accept as written. Retyping or re-collating a
  key, join or GROUP BY column, and widening any column Trellis creates a
  column from with its type, pause the definitions concerned, except a
  widening that changes only the catalog, which Trellis applies to its own
  columns in place (see [What isn't on this list](#what-isnt-on-this-list),
  and entry 23).
* No row-level security policy applies to a role Trellis logs in as, or to
  the role that owns its ring (entry 11).
* No application trigger re-keys a relationship's join column within the
  statement that wrote it ([an application trigger re-keying a parent's
  join column within the
  statement](#9-an-application-trigger-re-keying-a-parents-join-column-within-the-statement)).
* No trigger or constraint refuses a change only when several of the
  definitions reading it apply it together, in combinations no one of them is
  in every one of, such as two separate pairs (entry 24).

Each entry gives:

* **Trigger**: what causes it.
* **Effect**: what goes wrong, and whether it's silent.
* **Detected?**: whether Trellis notices, automatically or through
  `self_check`. Only 1-1 and `JOIN` targets can be audited (entry 16), and
  each "Detected?" line names the targets its `self_check` covers.
* **Planned work**: issues, branches and decisions that would close the gap.
* **Repair**: what you can do today.

## The repair tools

These are the tools the entries refer to:

* **`PAUSE TRANSFORM <target>` then `RESUME TRANSFORM <target>`** rebuilds the
  target from the source. Run it through `Trellis::apply` or
  `trellis apply '<statement>'`. A resume first re-runs define's validation
  against the live schema, and refuses, leaving the definition paused, while
  define would refuse it. It also refuses in four cases define doesn't: a
  1-1 target's source key was redefined, a definition's ledger table is
  missing, or an aggregate's group-delta table is missing (entry 8)
  or has different running-sum or recompute columns from the ones define would
  create now (a `SUM` or `AVG` argument moved between an exact type and
  floating point). It then brings every column
  Trellis created for the definition with a type from the source (its key,
  passthrough and GROUP BY copies, its calculated, aggregate and ledger
  contribution columns, and its relationship projections' columns) to the
  type define would give it now, re-reads every current source row and
  deletes target rows the source no longer backs
  ([ADR-0014](decisions/0014-pause-and-drop-a-transform.md)). It doesn't replay
  buffered changes, so it repairs anything that's wrong because a change was
  missed. It also releases every key the definition holds in quarantine, and
  only that definition's.
* **`request_backfill(source_table)`** (Rust, Ruby and Elixir) queues a re-read
  of one source table for every definition that reads it, without pausing them.
  It's the cheapest repair when you know which table missed changes.
* **`DROP TRANSFORM <target>`, then define it again** is the repair when you
  keep a schema change the definition can't be rebuilt over: a resume refuses
  while define would, naming the column and what to change, and a 1-1
  definition whose source key was redefined, an aggregate whose `SUM` or
  `AVG` argument moved between an exact type and floating point, or whose
  ledger or group-delta table was dropped (entry 8), can only be
  defined again.
  It's also the repair when a resume can't convert the values of a column
  Trellis created to its new type (a key moved from `text` to `uuid` by a
  `USING` that isn't a cast). The definition stays paused and its target
  keeps its rows: Trellis doesn't empty a target the application reads on its
  own.
* **`self_check(target, …)`** detects divergence but never repairs it. It
  starts a background job that audits the target's capture triggers, then
  compares the target with a recompute of it in Postgres, a page of keys at a
  time ([ADR-0013](decisions/0013-self-check-production-recompute-audit.md));
  `self_check_job(id)` reads its progress and verdict. **It only audits 1-1
  and `JOIN` targets** (entry 16).

## Summary

| # | Trigger | Silent? | Detected? | Planned work |
|---|---|---|---|---|
| 1 | `ALTER COLUMN … TYPE … USING` that rewrites values, with a metadata change to the same column in the same pass window, on a column added with a constant default whose table hasn't been rewritten since, on a column with a compression method set, or as a widening no copy outgrows | yes | `self_check` (1-1 targets only) | none; documented |
| 3 | A source key re-collated while a `self_check` job runs | the job can skip or repeat keys | no | none filed |
| 5 | A table attached to a hierarchy and detached, or a capture function replaced and restored, between two reconcile passes; capture changed outside Trellis on a partitioned source, once partitioned sources are supported (#897) | yes | `self_check` (1-1 targets only) | #707 (partitioned sources); otherwise none; documented |
| 8 | A manual change inside Trellis's artifacts: a target or hidden table edited or dropped, DML, DDL or a data load into Trellis's schema, or a restore of that schema alone | yes | `self_check` (1-1 and `JOIN` targets, and a `JOIN`'s hidden tables); a missing ledger or group-delta table refuses a resume; a change to Trellis's schema: no | none; documented |
| 9 | An application trigger re-keying a parent's join column within the statement | yes | no | #1115 removes relationships |
| 10 | `REGEXP_COUNT` on `"C"`-collated data or with Postgres-only regex syntax | yes | `self_check` (1-1 targets only) | #643, with #575 |
| 11 | Row-level security applying to a role Trellis runs as | only capture, for the ring owner, until the next reconcile pass; elsewhere reads and writes fail | drain, discharge, catch-up and build merge: pause what they reach; one the catalog can't pin shows on `drain_failure` (drain) or `backfill_failure` (discharge, catch-up), or, for a build merge, pauses on its fifth charge with the error on `capture_failure`; build chunk: `backfill_failure` | none |
| 12 | A crash empties an unlogged source table | yes | `self_check` (1-1 targets only) | none filed |
| 13 | Partial restore, or a schema-only load (`db:schema:load`, `ecto.load`) | partial restore silent; schema load loud | schema load of a plain dump: on define; partial restore: `self_check` (1-1 targets only) | none |
| 15 | A from-side change pending across a to-side `TRUNCATE` | yes | no | #1115 removes relationships |
| 16 | `self_check` audits 1-1 targets only | yes | n/a | #1104 (aggregates); #1115 removes relationships, whose `JOIN` replacements `self_check` already compares |
| 17 | `DROP TYPE` of an enum a live definition references | no (later introspection fails) | at failure only | none filed |
| 18 | `jsonb_agg`, `array_agg` and `string_agg` element order differs between recomputes | yes | no | none filed |
| 19 | A paused column doesn't pause the aggregates that read it | yes | on the upstream only | none filed |
| 21 | A to-one relationship field keeps a superseded parent value under concurrent writes (rare) | yes | no | #1115 removes relationships |
| 23 | A value padded with spaces past a widened `varchar`'s old length, drained before Trellis re-types its copy | yes | `self_check` (1-1 and `JOIN` targets) | none filed |
| 24 | A trigger or constraint refusing a change only when two separate pairs of definitions apply it | no (the drain fails) | `drain_failure` and `self_check` | none |
| 25 | A `MERGE` moving a row to another partition, once partitioned sources are supported (#897) | yes | `self_check` (1-1 targets only) | none; reported to Postgres (#942) |
| 26 | A partitioned source renamed away and back within one pass, once partitioned sources are supported (#897) | yes | no | none filed |
| 27 | A `DEFAULT` partition beside a leaf created and dropped within one pass, once partitioned sources are supported (#897) | yes | no | none filed |
| 28 | A leaf detached, written and attached again under the same bound around a `pg_upgrade`, with no pass between, once partitioned sources are supported (#897) | yes | no | none filed |

## 1. A rewriting `ALTER COLUMN TYPE … USING` that the capture pass can't tell from routine work

**Trigger:** a `USING` that rewrites a read column's values, in one of
four shapes:

```sql
-- A metadata change to the same column in the same reconcile window
-- (5 s by default), before or after the rewrite:
ALTER TABLE orders ALTER COLUMN amount SET DEFAULT 0;
ALTER TABLE orders ALTER COLUMN amount TYPE integer USING amount * 10;

-- A column added with a constant default, on a table no rewrite
-- (`VACUUM FULL`, `CLUSTER`, a rewriting `ALTER TABLE`) has touched since:
ALTER TABLE orders ADD COLUMN fee integer DEFAULT 0;
ALTER TABLE orders ALTER COLUMN fee TYPE integer USING fee + 1;

-- A column with a compression method set, rewritten for the first time
-- since it was set:
ALTER TABLE orders ALTER COLUMN note SET COMPRESSION pglz;
ALTER TABLE orders ALTER COLUMN note TYPE text USING upper(note);

-- A widening that changes values, where no column Trellis created from
-- `amount` outgrows the new type:
ALTER TABLE orders ALTER COLUMN amount TYPE bigint USING amount * 10;
```

**Effect:** the statement rewrites the table without firing any DML
trigger, so capture never sees the new values. Every target that reads
`amount` keeps its old values until each row is written again. Nothing
pauses or rebuilds, and status shows nothing.

**Detected?** Not automatically. The capture pass rebuilds the readers of a
column rewritten to the same type when the column's `pg_attribute` row and
the table's file node both moved and nothing else about the column changed
([capture by triggers](staging-and-claiming/01-capture-by-triggers.md#a-column-changed-or-rewritten-without-a-trigger)).
A `SET DEFAULT`, `SET STATISTICS` or other metadata change to the same column
in the same window looks like routine work that happened to meet a table
rewrite, which framework migrations do all the time, so the pass leaves it.
So does the first rewrite of a column added with a constant default: the
column's row keeps that default for the rows that predate it until the
table is rewritten, and any rewrite, `VACUUM FULL` included, clears it.
So does the first `ALTER COLUMN … TYPE` of a column with a compression
method set, which resets the method to the default.
A pure widening (`integer` → `bigint`, a longer `varchar`) never acts on
its own, since it keeps every value's text; only a column Trellis created
that it outgrows pauses its readers. `self_check` reports the cell
differences on 1-1 targets.

**Planned work:** none; documented. Event triggers, which would see every
rewrite, need superuser (#699). Logical decoding has the same blind spot:
`pgoutput` never sends a rewrite's rows.

**Repair:** right after the `ALTER`, run `request_backfill('orders')`, or
`PAUSE`/`RESUME` every transform that reads the table.

## 3. A source key re-collated while a `self_check` job runs

**Trigger:** `ALTER COLUMN id TYPE text COLLATE "C"` (or any change between
deterministic collations) on a source's key column while a `self_check`
job of a 1-1 target pages through it.

**Effect:** the data stays right: byte equality doesn't change, so nothing
pauses. But `self_check` pages the source and the target under the source
key's collation, and the job's saved cursor continues in the new order, so a
job that straddles the change can skip or repeat keys. It also reads each
page of the target by a scan instead of the target's key index (#782). A
build that straddles the change is pinned to the collation it planned under
(#769) and doesn't lose rows, but its remaining range reads scan the source.

**Detected?** No.

**Planned work:** none filed.

**Repair:** start a new `self_check` after the change.

## 5. Changes the capture pass can't see between two passes

Trellis pauses the readers of a plain table whose capture triggers or
functions were changed outside Trellis, or whose table is gone from its name,
as soon as a pass sees it
([capture by triggers](staging-and-claiming/01-capture-by-triggers.md#capture-changed-outside-trellis)).
It compares the live triggers' oids, `xmin`s and enabled modes with what it
recorded when it installed them, so a disable and re-enable by name between
two passes is seen. Three cases are still silent.

**Trigger:**

* A table attached to a partition or inheritance hierarchy and detached again
  between two passes, with writes routed through the hierarchy's parent in
  between: those fire the parent's triggers, not the table's.

  ```sql
  BEGIN;
  ALTER TABLE orders_all ATTACH PARTITION orders FOR VALUES FROM (MINVALUE) TO (MAXVALUE);
  UPDATE orders_all SET status = 'archived' WHERE created_at < '2025-01-01';
  ALTER TABLE orders_all DETACH PARTITION orders;
  COMMIT;
  ```

* A capture function replaced by hand and put back between two passes. The
  trigger rows don't change, and the function is whole again when the pass
  looks.
* Capture changed outside Trellis on a **partitioned source**, once
  partitioned sources are supported (#897). Its tree's nodes come and go with
  partition maintenance and aren't recorded, so the pass reinstalls a missing
  or disabled trigger (and queues the corrections ADR-0017 defines for a new
  leaf), but pauses nobody for it.

**Effect:** the writes made while capture was off never reach Trellis.
Targets stay wrong until each affected row is written again, with nothing on
status.

**Detected?** Not by the pass. `self_check`'s comparison reports the stale
rows on 1-1 targets.

**Planned work:** #707 records and checks a partitioned source's triggers
too. None for the other two cases; documented.

**Repair:** `request_backfill` the table, or `PAUSE`/`RESUME` its readers.
Better still, don't disable the capture triggers
([embedding — Leave the capture triggers alone](embedding.md)).

## 8. Operator changes inside Trellis-owned artifacts

**Trigger:** a manual change to anything Trellis installed or keeps:

* Any `INSERT`, `UPDATE`, `DELETE` or `TRUNCATE` on a target table, changing
  its columns or key constraints, or dropping a table Trellis keeps beside it:
  `<target>__ledger`, an aggregate's `<target>__deltas`, or a hidden table in
  the catalog schema (a group-basis aggregate's partials table `agg_<id>`; a
  `JOIN`'s `P`, `M`, `J` and `T` tables and its one-row lock table, named
  `join_<id>_p`, `_m`, `_j`, `_t` and `_l`).
* Replacing a capture function and putting it back between two passes, or
  disabling or dropping the capture triggers on a partitioned source
  ([entry 5](#5-changes-the-capture-pass-cant-see-between-two-passes)). A
  function replaced and left that way, and the triggers of a plain table,
  pause the readers.
* DML, DDL or a data load into Trellis's own schema, such as loading another
  database's `partition_sources` rows into the catalog.
* Restoring only the Trellis schema.

**Effect:**

* **Target and side tables.** Trellis assumes it's the only writer and doesn't
  correct the change. An edited row stays wrong until its key is recomputed
  for another reason, and a truncated target stays empty. A resume refuses
  while the definition's ledger table is missing, or an aggregate's
  group-delta table is, naming the table, and the definition stays paused. A
  build whose start fails because its ledger is gone (dropped while the
  definition waited to start) pauses that definition and what reads its
  target, with the error as its `capture_failure` of kind `halt`; the staging
  worker's pass goes on to start the other definitions' builds. A live
  definition whose ledger is dropped fails each drain page that applies a
  change for it: the keys those pages touch are held once their retries run
  out, and the definition is quarantined once enough are, while the other
  definitions on its source carry on. Its resume then refuses the same way.
  An edited or truncated group-basis or `JOIN` hidden table leaves the
  groups or cells it held wrong until they are recomputed for another reason.
* **Trellis's schema.** The catalog and the ring are Trellis's record of what
  every target has applied, so a change to them is believed.
  * The restore signal compares the stored OID of the catalog table with the
    live one (#1052, #1067). A physical copy forked *before* Trellis was
    installed, with Trellis then installed on both sides and one side's
    catalog rows loaded into the other, gets equal OIDs. No restore is
    detected, and the pass routes by recorded `HASH` texts that name the wrong
    OIDs.
  * Restoring only the Trellis schema rewinds the catalog and the ring
    without the targets, so they disagree about what has been applied.
    Restoring the application's schemas alone is
    [entry 13](#13-partial-restores-and-schema-only-loads).

**Detected?**

* **Target and side tables.** `self_check` reports an edited 1-1 or `JOIN`
  target, and compares each of a `JOIN`'s `P`, `M`, `J` and `T` tables
  against the source tables. For aggregate and relationship-enriched
  targets, see [entry 16](#16-self_check-audits-1-1-targets-only).
* **Trellis's schema.** Not detected.

**Planned work:** none for the edits. This is a documented rule
([transforms — Target tables are Trellis-owned](transforms.md#target-tables-are-trellis-owned)).

**Repair:**

* **Target and side tables.** `PAUSE`/`RESUME` the transform. A resume drops
  and recreates a `JOIN`'s hidden tables, so it also repairs one that was
  edited or dropped. It doesn't recreate the other side tables: if the
  target's columns or constraints were changed, or its `<target>__ledger`,
  `<target>__deltas` or `agg_<id>` table was dropped, `DROP TRANSFORM` and
  define it again. An edited `agg_<id>` is repaired by `request_backfill` of
  the source, which recomputes every group's partials and visible row from
  it.
* **Trellis's schema.** Restore the whole database from a backup taken before
  the change, or `DROP TRANSFORM` every transform and define it again, which
  rebuilds each target from its sources. A target the changed catalog has no
  definition for is a plain table to Trellis, and defining it again fails
  with a `conflict` error until you drop it.

## 9. An application trigger re-keying a parent's join column within the statement

**Trigger:** an application `AFTER ROW` trigger (or a self-referencing
cascade) that changes a parent's non-key join column again inside the
statement that changed it. For example, `parents.code` goes `'a'` → `'x'`, then
the trigger changes it to `'y'`. A key join column isn't affected, because the
ring key names every key a row had.

**Effect:** the nested statement's capture runs first, so the key's earliest
ring row is `('x', 'y')` and the outer one is `('a', 'y')` (#680). The fold
keeps the earliest old image, the uncommitted `'x'`, so nothing names the
committed `'a'`. The children that joined through `'a'` are never re-derived:
aggregates over them keep stale values, and a to-one projection of `'a'` isn't
cleared, so 1-1 fields read through it stay stale too.

**Detected?** No. Two ignored tests in `ledger_interleavings.rs` pin the
behaviour:
`a_nested_rekey_of_a_non_key_to_col_re_derives_the_committed_value`
(aggregate) and
`a_nested_rekey_of_a_non_key_to_col_clears_the_committed_projection` (1-1).

**Planned work:** #1115 removes relationships, which removes this gap.

**Repair:** `PAUSE`/`RESUME` the definitions that read through the
relationship, or `request_backfill` the child table. Avoid triggers that re-key
a join column inside the statement that wrote it.

## 10. `REGEXP_COUNT` semantics

**Trigger:** a transform using `REGEXP_COUNT` with any of:

* a character-class pattern (`\w`, `[[:alpha:]]`) over a `"C"`-collated column
  or a C/`SQL_ASCII` database;
* syntax that only Postgres's regex engine accepts (back-references, `\m`/`\M`,
  embedded options).

**Effect:** Trellis evaluates the pattern with Rust's regex engine. For
example, `regexp_count('é' COLLATE "C", '\w')` is 0 in Postgres and 1 in
Trellis. That's a silently different value. Syntax that Rust's regex engine
rejects fails at define instead.

**Detected?** `self_check` on 1-1 targets. Not on aggregate targets.

**Planned work:** #643, decided. It closes when #575 moves expression
evaluation into Postgres.

**Repair:** stick to literal patterns or UTF-8 collated data until #575 lands.
Afterwards, `PAUSE`/`RESUME` the affected transforms.

## 11. Row-level security on a role Trellis runs as

**Trigger:** a row-level security policy that applies to a role a drain
worker, the staging worker or a build logs in as, or that comes to apply to
the role that owns the ring after define. A privilege one of those roles
lacks (`permission denied for table ...`) is handled the same way.

**Effect:** every connection Trellis opens runs with `row_security = off`, so
Postgres refuses (`42501`) a read or write the policies would filter, rather
than filtering it. What follows depends on what was refused:

* **A drain** pauses the definitions the refusal reaches, with kind `halt`:
  the readers of each table its role can't read, the writer of each target it
  can't write, and everything downstream of them. `status()`'s
  `capture_failure` names the table, the role, the fix and Postgres's own
  error. The refusal charges no key, and the rest of the page commits; a key
  that fails beside it is still charged to the definition it fails in. The
  drain finds those tables from the catalog, as its own role, so it pauses
  every unfrozen definition whose tables that role can't use, not only those
  on the refused page. A `42501` the catalog can't pin on a table (a column
  the role isn't granted while it holds a grant on another, a function's
  `EXECUTE`, one of Trellis's own tables) pauses nothing and charges no key.
  The drain records it as a drain holdup on the page: `status()` reports it
  as `drain_failure` on every definition that isn't paused or quarantined and
  reads a table on the page, directly or through a relationship, and
  `self_check` reports it for the instance. Every pass retries the page, and
  the holdup clears when the page commits.
* **A build chunk** is retried, and its fifth charged attempt pauses its
  definition, with the error on `backfill_failure`. It's never narrowed to a
  key.
* **A build's group-delta merge** halts like a drain: it pauses what the
  refusal reaches with kind `halt`. One the catalog can't pin is retried with
  backoff, and its fifth charge on one drain worker pauses the definition, and
  everything downstream of its target, with kind `halt` and the error on
  `capture_failure`. Until then it shows only in that worker's warn log. The
  charges are held in the worker's memory, so a restart starts them again.
* **A backfill discharge or go-live catch-up** halts the same way, whether
  the refusal comes while it plans a build, before any chunk exists, or while
  a catch-up re-reads the source. It reads the catalog as the discharge's own
  role, pauses what the refusal reaches with kind `halt` and the same reason,
  charges no key, and retries the marker at once without them. A frozen
  definition doesn't count as a reader of a table, so once every reader of
  the refused table is paused the marker discharges without reading it. A
  `42501` the catalog can't pin pauses nothing here either: the marker
  retries with backoff, with the error on `backfill_failure`.
* **Capture** runs inside the application's own write, under that session's
  `row_security`, so the capture functions don't set it. Setting it there
  would turn a policy on the ring's owner into a failed application write.
  So when policies come to apply to the ring's owner, the capture functions'
  re-read of the source silently skips the rows they hide, until the next
  reconcile pass pauses the table's readers.

**Detected?** A refusal of a drain, a discharge or a catch-up pauses its
definitions and shows in `status()`, except a `42501` the catalog can't pin,
which pauses nothing: a drain's shows on `drain_failure` and in
`self_check`, and a discharge's or catch-up's on `backfill_failure`. A build
chunk's shows on `backfill_failure`. A build merge's pauses what it reaches
too, and one the catalog can't pin shows on `capture_failure` once its fifth
charge pauses the definition. For the ring's owner, the
reconcile pass pauses the readers and `self_check` reports a `capture`
divergence (#745, #765). Define and declare refuse policies that already apply
([transforms — Supported sources and targets](transforms.md#supported-sources-and-targets)).

**Planned work:** none.

**Repair:** grant `BYPASSRLS` to every role Trellis logs in as and to the
ring's owner (it isn't inherited), or make the role the table owner without
`FORCE ROW LEVEL SECURITY`, and grant any missing privilege. Then resume the
paused definitions, which rebuilds them. A drain page held on a refusal the
catalog couldn't pin commits on its next pass, which clears its
`drain_failure`, and a discharge or catch-up backing off on one succeeds on
its next attempt.

## 12. A crash empties an unlogged source table

**Trigger:** an `UNLOGGED` source table, followed by a crash or an immediate
shutdown. Crash recovery resets unlogged tables to empty.

**Effect:** the reset fires no trigger, so targets keep every row derived
from the table's old contents. This is silent. It's inferred from Postgres's
behaviour and Trellis not checking `relpersistence`; it isn't tested, and no
issue is filed.

**Detected?** `self_check` on 1-1 targets. Not on aggregate targets.

**Planned work:** none.

**Repair:** after a crash, `request_backfill` each unlogged source, or
`PAUSE`/`RESUME` its readers. Better, don't use unlogged tables as sources.

## 13. Partial restores and schema-only loads

**Trigger:**

* Restoring only some schemas or tables, or restoring them to different
  points in time.
* Building a database from a schema dump instead of migrations:
  `rails db:schema:load`, `db:prepare` on a fresh database, or
  `mix ecto.load`.

**Effect:**

* A partial restore is silent. Trellis's catalog, its ring and the targets have
  to come from the same point as the sources. A whole-database or
  whole-cluster restore keeps them consistent and needs no special
  handling (#236). Restoring only the Trellis schema is
  [entry 8](#8-operator-changes-inside-trellis-owned-artifacts).
* A schema load of a plain dump is loud. It carries the target tables but not
  their definitions, so nothing maintains them, and defining again fails with a
  `conflict` error because the target exists. A Rails app's `structure.sql`
  and an Ecto app's dump made through `mix trellis.dump` carry the
  definitions, so a load of either isn't affected. An app that doesn't wire
  the Ecto alias (`"ecto.dump": "trellis.dump"`) gets a plain dump with no
  warning, and Rails refuses to dump a `schema.rb`.
* A definition that was paused or quarantined when dumped loads as
  `waiting_to_backfill`, so a load clears the pause or quarantine and the
  worker builds it.

**Detected?** A partial restore isn't detected by Trellis (`self_check` can
tell on 1-1 targets only). A schema load of a plain dump is detected when you
define again. An Ecto app that skipped the alias isn't warned at dump time.

**Planned work:** none filed for the dump hooks. The engine's
`dump_definitions_sql` returns the `INSERT`s that both hooks append to a
schema-only `pg_dump` of the Trellis schema and the schemas it maintains
([ADR-0010](decisions/0010-embeddable-clients.md) decision 5).

**Repair:**

* After a partial restore, `PAUSE`/`RESUME` every transform whose sources or
  targets were restored.
* For a schema-loaded database without the definitions, drop the target tables
  and run the defining migrations ([embedding](embedding.md)). A dump made through
  either hook loads with its definitions, and the worker
  builds them from the source rows.

## 15. A from-side change pending across a to-side `TRUNCATE`

**Trigger:** an aggregate that groups by a column read through a relationship
(`GROUP BY region, buyer.name`). The to-side table (`users`) is truncated, and
a from-side row (`orders`) changes a grouping column while its change is still
undrained, so the two land in different batches:

```sql
TRUNCATE users;
UPDATE orders SET region = 'us' WHERE id = 10;
```

**Effect:** the truncate re-derives the from-side rows from their live values,
so its image names the new group `(us, a)`, never the old `(eu, a)`. The later
change resolves its old image's `buyer` against the cleared projection, which
names `(eu, NULL)`. Nothing names `(eu, a)`, so that group keeps its old
value. This is silent.

**Detected?** No. The test that pins it is ignored:
`a_from_side_change_pending_across_a_to_side_truncate_leaves_no_stale_group`
(`trellis/tests/apply_relationships.rs`).

**Planned work:** #1115 removes relationships, which removes this gap.

**Repair:** `PAUSE`/`RESUME` the aggregate.

## 16. `self_check` audits 1-1 targets only

**Trigger:** any divergence of an aggregate target or a relationship-enriched
1-1 target from what its definition computes, from any cause above.

**Effect:** `self_check` on an aggregate target is refused with
`UnsupportedKeySpace`, and on a 1-1 target with a field read through a
relationship with `UnsupportedExpr`, when the job is started. Neither kind of
target is compared with a recompute, so a stale value in one is silent.

**Detected?** Not applicable. Each entry's "Detected?" line says `self_check`
only for plain 1-1 targets.

**Planned work:** #1104 adds `self_check` for aggregate targets. #1115
removes relationships, and with them relationship-enriched 1-1 targets.
`self_check` already compares the `JOIN` targets that replace them, and
their hidden tables, against the two source tables.

**Repair:** compare the target with your own query of the source, or
`PAUSE`/`RESUME` it when you suspect a divergence.

## 17. `DROP TYPE` of an enum a live definition references

**Trigger:** `DROP TYPE` (with the column dropped or retyped) on an enum type
that a live definition's source column uses.

**Effect:** Trellis looks an enum up by its live `pg_type` row. Once the type
is gone, the column is no longer recognized, and later introspection of the
definition fails. Nothing notices the drop itself, and no pause is recorded.

**Detected?** Only when a later read of the column's type fails.

**Planned work:** none filed. Nothing in the crate notices a referenced type or
table disappearing from under a live definition.

**Repair:** `DROP TRANSFORM` and define it again against the new schema.

## 18. `jsonb_agg`, `array_agg` and `string_agg` element order

**Trigger:** an aggregate field `JSONB_AGG(...)`, `ARRAY_AGG(...)` or
`STRING_AGG(...)`.

**Effect:** the call has no `ORDER BY`, so two recomputes of the same group
aren't guaranteed to agree on element order, though the set of elements is
always right. A comparison with your own query of the source that is
order-sensitive can show a difference that isn't staleness. This is how the
function is defined, more than a bug.

**Detected?** No. `self_check` doesn't audit aggregates (entry 16).

**Planned work:** none filed. An `ORDER BY` inside the call would need a
grammar extension.

**Repair:** compare the elements as a set, or sort them when you read them.

## 19. A paused column doesn't pause the aggregates that read it

**Trigger:** a column of a 1-1 target is paused (its column fuse tripped, or an
upstream column it reads was paused), and an aggregate transform reads that
column, whether it was defined before the pause or after it.

**Effect:** the pause cascades to downstream 1-1 readers but not to
aggregates (`column_dependents` filters to 1-1 definitions, and the aggregate
write path has no per-column pause). The aggregate keeps applying, over the
paused column's frozen values. It agrees with the upstream target as it stands,
so it isn't wrong against it, but it's stale against the source and nothing on
its status says so.

**Detected?** No, on the aggregate. The upstream target's status lists the
paused column.

**Planned work:** none filed. It's a deliberate scoping of column pauses to
1-1 targets
([ADR-0003](decisions/0003-quarantine-storage-and-api.md)).

**Repair:** resume the upstream column. Its rebuild writes through to the
aggregate. If the aggregate's value matters before then, treat it as stale
while the column is paused.

## 21. A to-one relationship field keeps a superseded parent value under concurrent writes

**Trigger:** a 1-1 definition with a field read through a to-one relationship
(`author.name AS author_name`), and concurrent writes to both tables: a child
joins a parent (inserted under it, inserted again after a delete, or moved
onto it) while a page is applying a change to that parent, and a page or a
build chunk writes the child before that page commits. The parent change can
be an update, a delete or a `TRUNCATE` of the parent table. It is rare: no
generated case is known to fail on it.

**Effect:** the child keeps the value its parent held before the change
(after a `TRUNCATE`, the truncated parent's value). This is silent. The
relationship projection holds the parent's new value, so any later write to
the child corrects it.

**Detected?** No. `self_check` doesn't compare a 1-1 target with a field read
through a relationship (entry 16).

**Planned work:** #1115 removes relationships, which removes this gap.

**Repair:** any later write to the child. `request_backfill` of the parent
table also repairs it: its catch-up refreshes the relationship projection
from the table, then re-derives every child from it.

## 23. A value padded with spaces past a widened `varchar`'s old length, drained before Trellis re-types its copy

**Trigger:** a source `varchar(n)` column widened by changing only the
catalog (to a longer `varchar`, to `text` or to `varchar`), and a value
whose characters past `n` are all spaces (`'abcdefghij   '` after
`varchar(10)` became `varchar(40)`) written to it and drained before the
staging worker's next capture pass re-types the column Trellis copied it
into, up to one `reconcile_interval` later. The copies are a 1-1 target's
key and passthrough columns, a `JOIN` target's `GROUP BY` columns and its
hidden tables' `GROUP BY` and `ON` columns, and a to-one relationship
projection's key and its column for each to-side column read through it.

**Effect:** silent. Postgres stores a value whose excess characters are all
spaces in a `varchar(n)` column truncated to `n`, rather than failing it, so
the copy holds `'abcdefghij'`. Every other value too long for the copy fails
its write, and its key is held and then released after the re-type (see
[What isn't on this list](#what-isnt-on-this-list)). The re-type changes no
value and rebuilds nothing, so the copy keeps the truncated value until its
row is written again. A 1-1 key truncated this way leaves its target row
under the truncated key: the source's later changes to the row write the
full key, and nothing deletes the truncated row. A `JOIN` counts the row in
a group under the truncated value the same way: its later changes are counted
under the full value, and nothing takes it out of the truncated one.

**Detected?** Only by `self_check`, on a 1-1 or a `JOIN` target.

**Planned work:** none filed.

**Repair:** `PAUSE`/`RESUME` every transform that copies the column, after
the capture pass has re-typed it.

## 24. A change only two separate pairs of definitions fail on together

**Trigger:** a user trigger or constraint that refuses a write only when
several definitions' targets are written in one transaction, in combinations
of the definitions that read one table directly that no one definition is in
every one of: two separate pairs (`a` and `b` refused together, and `c` and
`d` too), or any two of three.

**Effect:** isolation can't charge the change's key to anyone. Each
definition applies it alone, and leaving any one of them out still leaves
another combination failing. The drain fails on the page on every pass, and every
target reading the page's tables stops short of it, along with every
watermark token taken since. It isn't silent: each pass surfaces the error.
A single failing pair is charged to both of its definitions, which hold the
key (see [Repair caveats](#repair-caveats)).

**Detected?** Yes. The page is a drain holdup: `status()` reports it as
`drain_failure` on every definition that isn't paused or quarantined and
reads a table on the page, directly or through a relationship, and
`self_check` reports it as `drain_failures`.

**Planned work:** none.

**Repair:** fix the trigger or constraint, and the next pass commits the page.
Or pause a definition in a failing combination (one of either pair): once
some definition is in every combination left, isolation charges the ones it
pins, which hold the key until it's released or they're resumed.

## 25. A `MERGE` that moves a row to another partition

**Trigger:** a `MERGE` on a partitioned source whose `UPDATE` action
(including `WHEN NOT MATCHED BY SOURCE … UPDATE`) changes a row's partition,
wherever Trellis installs capture in the partition tree. A `MERGE` that keeps
every row in its partition, and a plain `UPDATE` that moves one, are
unaffected. Trellis refuses a partitioned table or a partition as a source
today, so nobody can hit this yet: it applies once partitioned sources are
supported (epic #897).

**Effect:** silent. The cause is in Postgres, not Trellis: the transition
tables Trellis's capture reads mishandle the moved row. It came in with
Postgres commits `c0bfdaf2b` (15.6) and `06a546382` (16.2), which fixed an
unrelated row-trigger failure. It's reported upstream as a bug, and Trellis
is waiting to hear whether Postgres accepts and fixes it.

* **15.6+, 16.2+, 17 and 18:** the row's images never reach the `UPDATE`
  tables. Its old image goes to the `DELETE` table only if the `MERGE` has a
  `DELETE` action, and its new image to the `INSERT` table only if it has
  an `INSERT` action. With only an `UPDATE` action, the row's change is
  lost and the target keeps it as it was. With an `INSERT` action but no
  `DELETE` action, the target has it in both its old and new place (an
  aggregate's count is one too high). With a `DELETE` action but no
  `INSERT` action, the target loses it (one too low). Only a `MERGE` with
  both is exact.
* **15.0–15.5 and 16.0–16.1:** a `MERGE` with only an `UPDATE` action is
  exact. An `INSERT` or `DELETE` action also puts the row's new or old
  image in its own table, so that image is applied twice: an `INSERT`
  action adds the row to its new place twice, and a `DELETE` action takes
  it out of its old place twice. Observed on 15.5, and on 16 just before
  `06a546382`; the other minors are inferred.

**Detected?** Not when it happens: the capture triggers receive empty or
partial transition tables, and can't tell a `MERGE` from an `UPDATE`.
`self_check` shows the drift on a 1-1 target.

**Planned work:** none. Epic #897 doesn't design around it, and #942 has the
findings and the upstream report.

**Workaround:** change the partition key with `UPDATE`, or with `DELETE` and
`INSERT`, not with a `MERGE`. A `MERGE` with both `DELETE` and `INSERT`
actions isn't a safe form: it's wrong before 15.6 and 16.2.

**Repair:** `PAUSE`/`RESUME` the transforms that read the table, or
`request_backfill` it.

## 26. A partitioned source renamed away and back within one pass

**Trigger:** `ALTER TABLE orders RENAME TO orders_old` and, before the
staging worker's next reconcile pass, `ALTER TABLE orders_old RENAME TO
orders`, on a partitioned source. It applies once partitioned sources are
supported (epic #897). A rename that spans a pass is seen: the pass finds the
recorded root under another name and, when the name resolves again, queues a
correction of every leaf.

**Effect:** silent. Each capture function of a partitioned source stages
nothing unless the table's root is the table the source's name resolves to,
so every write made while the root had another name is not staged, and
nothing in the catalog differs afterwards: neither fingerprint of the pass
covers the root's `pg_class.xmin`, which a `GRANT`, an owner change or a
`VACUUM FULL` would move too. The groups the missed writes touched stay as
they were.

**Detected?** No.

**Planned work:** none.

**Workaround:** rename a source only while its transforms are paused, then
resume them.

**Repair:** `PAUSE`/`RESUME` the transforms that read the table, or
`request_backfill` it.

## 27. A `DEFAULT` partition beside a leaf created and dropped within one pass

**Trigger:** on a partitioned source, a leaf created beside a `DEFAULT`
partition (under the same parent), written through the parent, and dropped
or detached before the staging worker's next reconcile pass. It applies once
partitioned sources are supported (epic #897). A leaf that is still there
when a pass runs is recorded, and its later drop or detach is corrected.
Without a `DEFAULT` beside it, every pass's sweep for groups that route to no
partition finds the groups such a leaf leaves behind and corrects them.

**Effect:** silent. The removed rows stay counted in their groups. Neither
fingerprint of the pass sees a leaf that came and went between two passes,
and the sweep has nothing to search: the `DEFAULT` takes every row its
siblings don't, so no group routes to no partition.

**Detected?** No.

**Planned work:** none.

**Workaround:** delete a short-lived leaf's rows through the parent before
dropping or detaching it, or keep the leaf until a reconcile pass has run.

**Repair:** `PAUSE`/`RESUME` the transforms that read the table, or
`request_backfill` it.

## 28. A leaf detached and attached again around a `pg_upgrade`

**Trigger:** on a partitioned source, a leaf detached, written while
detached, and attached again under the same parent and bound, with no
reconcile pass completing a deep check between that and a `pg_upgrade`:
after the last pass on the old cluster, or after the upgrade and before the
first pass on the new one completes. It applies once partitioned sources are
supported (epic #897). Outside an upgrade, the pass sees such a leaf by its
new `pg_inherits.xmin` and corrects it.

**Effect:** silent. A detached leaf's capture functions stage nothing, and
`pg_upgrade` resets every `xmin`. The pass recognizes the upgrade by a
changed `system_identifier` with every recorded oid present under its
recorded bound, and records the new `xmin`s instead of comparing them, so the
leaf reads as unchanged. The groups the missed writes touched stay as they
were.

**Detected?** No.

**Planned work:** none.

**Workaround:** let a reconcile pass run after partition maintenance before
shutting the cluster down for the upgrade, and after the upgrade don't detach
or attach a source's partitions until a pass has run.

**Repair:** `PAUSE`/`RESUME` the transforms that read the table, or
`request_backfill` it.

## Repair caveats

* **A resume that re-types copies holds `ACCESS EXCLUSIVE` on them.** After a
  widening, the resume's `ALTER … TYPE` on the target (and its ledger, or the
  relationship's projection) waits for every reader of the table, and readers
  queue behind it. The capture pass's in-place re-type of a widening that
  changes only the catalog takes the same lock, for at most two seconds
  while it waits and briefly once it has it. Either re-type changes the
  table's row type, so a client that prepared a statement returning one of
  its columns before the re-type gets `cached plan must not change result
  type` the next time it runs it, once per prepared statement. `integer` to
  `bigint` rewrites the table, so at a billion rows reads of the target
  block for minutes, and the capture pass waits with it. Trellis sets no
  `statement_timeout` and honours the one on its role or database: a rewrite
  that outlasts it is cancelled, rolled back and retried on the next pass,
  blocking readers up to the timeout each time, and the third cancellation
  ends the request. The definition stays `paused`, its `capture_failure`
  naming both remedies: raise the timeout for Trellis's role or database,
  restart Trellis (the setting reaches only new connections) and `RESUME`
  again, or `DROP TRANSFORM` and define it again. The `RESUME` itself
  returns at once; the definition stays `paused`, its `capture_failure`
  saying it's resuming, until the staging worker has re-typed the copies and
  started the rebuild. A rebuild follows every
  widening of a copied column.
* **A resume that re-types a target pauses the definitions chained off it,
  and they rebuild one after another.** A definition reading the target
  whose own columns were typed from the old type pauses, its
  `capture_failure` naming the upstream resume. The capture pass resumes it
  once the upstream is `live` again, so one `RESUME` of the head carries the
  chain. Its resume re-types its own target, which pauses the next
  definition down the same way. Each rebuild starts only after the one above
  it has finished, and each is a full rebuild, so a chain of depth k stays
  paused below the head for k rebuilds in a row. A definition waiting on an
  upstream that doesn't go live (paused, quarantined, refused its own
  resume) keeps waiting. So does a definition an operator resumes while an
  upstream isn't live, or reads a definition that isn't: the `RESUME`
  returns at once and the rebuild waits for the upstream, so a stack resumed at once rebuilds one level at a time
  in the same way. A `PAUSE` of a waiting definition withdraws its resume.
  One whose resume is refused when its upstream is live, because the schema
  changed after the pause, stays paused with the refusal, and isn't tried
  again; resume it by hand once that is fixed, or drop it and define it
  again.
* **Quarantined keys stay held until they're released.** A key that's
  quarantined after repeated apply failures is held for the definition whose
  apply failed, with its parked work and its poison row, and that
  definition's target row for it stays stale. A failure in a relationship's
  reverse work holds the key for that relationship's readers. One that fails
  only when several definitions apply it together holds it for each
  definition in every failing combination (both of a failing pair), though
  only one of their writes may be at fault. Every other definition reading
  the same source keeps applying the key, so two definitions can disagree on
  it. A widening of a column Trellis copies with its type causes such
  failures when an oversized value drains before the capture pass pauses
  the definition. After a widening that changes only the catalog, the capture pass
  re-types Trellis's column and then releases the definition's keys held
  for `22001` or `22003` itself, after that pass and each pass for a minute
  more. A key whose eviction commits later than that, from a failure
  reproduced before the re-type, stays held. The definition's `status`
  reports its held keys (`held_keys`: how many, and since when) whatever its
  status, `live` included, and `self_check` reports them with every audit.
  What clears a held key:
  * `Trellis::release_key(transform, source_table, key)` (`trellis release`
    on the CLI, `release_key` in the bindings) releases one key, once its
    cause is fixed: every definition reading it re-derives it from its
    current row. A key whose cause is still there is quarantined again.
  * `RESUME TRANSFORM` deletes every key the resumed definition holds, and
    only its own, and its rebuild re-derives them from the source. A key
    whose cause is still there fails again and is quarantined again.
  * `DROP TRANSFORM` deletes the definition's held keys with it, so defining
    it again starts with none.
* **Resuming a field that's awaiting capture (#687).** A `RESUME` of a field
  whose status shows `capture_wait` unpauses it before its capture is widened,
  so its rows fail with `MissingColumn`. Let the wait finish first.

## What isn't on this list

Trellis handles these cases itself, either by repairing them or by
refusing them up front:

* **DML under `session_replication_role = replica`.** The capture triggers are
  `ENABLE ALWAYS`, so they fire. This is not a gap.
* **A capture trigger left disabled, dropped, or not `ALWAYS`, a capture
  function replaced, or a source table dropped, renamed away, swapped or
  recreated.** The next reconcile pass pauses the table's readers with a
  `capture_failure` naming what it found, and a `RESUME` rebuilds them.
* **`TRUNCATE` of a source**, including `CASCADE`.
* **A read column renamed or dropped, or the primary key redefined.** Trellis
  pauses the readers with a `capture_failure`, and you resume once the schema
  is right
  ([capture by triggers](staging-and-claiming/01-capture-by-triggers.md)).
  A drain page that runs before the capture pass sees a redefined key pauses
  them itself, before it applies anything, and charges nothing to
  quarantine.
* **A read column dropped and re-added under the same name, retyped other
  than by a pure widening, or rewritten in place to the same type (`ALTER
  COLUMN b TYPE integer USING b * 10` on an `integer`).** The capture pass
  rebuilds the table's applying readers, as `request_backfill` would,
  logging a `WARN` and counting `capture_auto_rebuilds_total`. A definition
  define would now refuse, one whose evaluated type changed (`integer` to
  `double precision`), or one whose `numeric` copy must be re-typed pauses
  instead, with a `capture_failure` naming the column and the change, and
  its resume re-validates, re-types and rebuilds
  ([capture by triggers](staging-and-claiming/01-capture-by-triggers.md#a-column-changed-or-rewritten-without-a-trigger)).
  Entry 1 is what this misses.
* **A key, join or GROUP BY column retyped or re-collated, or a column
  Trellis creates a column from with its type widened.** The staging
  worker's capture pass pauses the definitions concerned, with a
  `capture_failure` naming each column, its old and new type, and what to
  do, when define would now refuse the column or a relationship's join
  columns no longer match, when the change renders the stored keys
  differently or rounds them (`timestamp` to `timestamptz`, `text` to
  `uuid`, a narrower `numeric` scale or `varchar(n)`), and when a column
  Trellis created from the source can no longer hold every value of the type
  define would give it now (`int` to `bigint` under a 1-1 target's key,
  passthrough or calculated field, a GROUP BY key, a `SUM` or `MIN` and its
  ledger contribution, or a relationship projection's key or column;
  `bigint` to `numeric` under a `MIN`). A resume refuses until define would
  accept the definition again (and for the refusals only a resume makes, see
  [The repair tools](#the-repair-tools)); otherwise it re-types those
  columns and rebuilds
  ([transforms — Supported sources and targets](transforms.md#supported-sources-and-targets)).
  A value a copy can't hold that drains before the pass is quarantined, and
  the resume releases it. A change between deterministic collations needs
  nothing (but see [a source key re-collated while a `self_check` job
  runs](#3-a-source-key-re-collated-while-a-self_check-job-runs)).
* **A widening that changes only the catalog of a column Trellis copies with
  its type.** `varchar(n)` to a longer `varchar`, to `text` or to
  `varchar`, and `numeric(p,s)` to a larger precision at the same scale,
  under a 1-1 target's key or passthrough or a to-one relationship
  projection's key or column. The capture pass re-types Trellis's columns in
  place, with no pause and no rebuild, and then releases the keys held for
  a value the old type couldn't hold (`22001`, `22003`) in between
  ([capture by triggers](staging-and-claiming/01-capture-by-triggers.md)).
  A table where another column needs a rewrite pauses as above. A value
  padded with spaces past the old length doesn't fail, and is stored
  truncated (entry 23).
* **A source's primary key dropped, or retyped off the supported types.** The
  drain pauses every definition it reaches, and what is downstream of them,
  with a `capture_failure` naming the cause, and the rest of the page commits.
  Resume once the key is right, which rebuilds them.
* **Row-level security on the ring owner or defining role, a
  logical-replication subscription into a source, and a source in a partition
  or inheritance hierarchy.** These are refused at define, paused by the pass,
  and reported by `self_check`
  ([transforms — Supported sources and targets](transforms.md#supported-sources-and-targets)).
  A source joins a hierarchy after define through `ATTACH PARTITION`,
  `INHERIT`, a child table created `INHERITS` it, or by being dropped and
  recreated as a partitioned table, a partition or an inheritance child. The
  pass installs no capture on such a table, and a resume refuses until it's
  a plain table again. Writes made through the hierarchy before the pass sees
  it are missed, or, on an inheritance parent, a child's rows are staged as
  the parent's own; the resume's rebuild re-reads the table and repairs
  both. A table that joins and leaves a hierarchy between two passes is
  never seen, which is entry 5. Capture's
  window before the pass sees row-level security is entry 11. Once
  partitioned sources are supported (#897), a `MERGE` that moves a row
  between partitions is entry 25, a partitioned source renamed away and
  back between two passes is entry 26, and a leaf created and dropped
  between two passes beside a `DEFAULT` partition is entry 27.
* **A capture function's privilege revoked, or the function made
  `SECURITY INVOKER`.** This is loud rather than silent: every write to the
  captured table fails, naming the capture function, so no change is lost.
  `self_check` names the cause on 1-1 targets.
* **Session settings** (`DateStyle`, `TimeZone`, `extra_float_digits`, …).
  Trellis pins them on its connections and in its capture functions. Its
  connections also run with `row_security = off`, so a read or write that
  row-level security would filter fails instead of silently missing rows
  (entry 11).
* **A `default_transaction_isolation` of `repeatable read` or
  `serializable`** on the server, the database or a role. Trellis's
  connections default to `read committed`, which its reads after a lock
  depend on, whatever the setting. The application's own sessions keep
  their level: capture works at every level.
* **A whole-database backup and restore, or PITR.** Sources and Trellis state
  roll back together.
