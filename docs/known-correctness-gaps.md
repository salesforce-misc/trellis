# Known correctness gaps

Trellis keeps each target equal to what its definition computes from its
sources. This page lists every known way a target can end up different from
that **while Trellis reports it healthy**: the definition reads `live` (or is
on its way there), no failure field is set, and `await_converged` returns. If
you find one that isn't here, this page has a bug.

Anything Trellis reports isn't a gap: a refusal at define, a pause with a
`capture_failure`, a held key, a drain holdup, a failing build. Those are
stops, and [operations](operations.md) says what to do about each one. A gap
leaves this page when Trellis repairs it, or starts reporting it.

An aggregate keeps each group's partials (its row count and running sums) and
applies each captured change to them as a delta, and a `JOIN` does the same for
each table's partials.
A delta is computed from the change's own images, so a group that missed a
change stays wrong until it's rebuilt: `request_backfill` of the source, or
`PAUSE`/`RESUME`. A later write to the same rows doesn't repair it.

## Summary

Every gap below is silent. "Detected by" says whether `self_check` finds the
wrong rows, on the targets it compares
([What `self_check` can't see](#what-self_check-cant-see)).

| Gap | Detected by | Ends with |
|---|---|---|
| **Schema changes Trellis can't tell from routine work** | | |
| [A column's values rewritten in place by `ALTER COLUMN TYPE`](#a-columns-values-rewritten-in-place-by-alter-column-type) | `self_check` | [#1206](https://github.com/salesforce-misc/trellis/issues/1206) (study) |
| [An enum label renamed and added again](#an-enum-label-renamed-and-added-again) | `self_check` | documented |
| **Writes capture never sees** | | |
| [Capture disrupted and restored between two passes](#capture-disrupted-and-restored-between-two-passes) | `self_check` | [#1206](https://github.com/salesforce-misc/trellis/issues/1206) (study), otherwise documented |
| [An unlogged source emptied by a crash or failover](#an-unlogged-source-emptied-by-a-crash-or-failover) | `self_check` | documented |
| [A `MERGE` that moves a row to another partition](#a-merge-that-moves-a-row-to-another-partition) | `self_check` | a Postgres fix |
| [The source renamed or re-parented and back within one pass](#the-source-renamed-or-re-parented-and-back-within-one-pass) | `self_check` | [#1210](https://github.com/salesforce-misc/trellis/issues/1210) |
| [A short-lived leaf beside a `DEFAULT` partition](#a-short-lived-leaf-beside-a-default-partition) | `self_check` | documented |
| [A leaf detached and attached again around a `pg_upgrade`](#a-leaf-detached-and-attached-again-around-a-pg_upgrade) | `self_check` | [#1210](https://github.com/salesforce-misc/trellis/issues/1210) |
| **Changes made outside Trellis to what it owns** | | |
| [Rows written into a target, a hidden table or Trellis's schema](#rows-written-into-a-target-a-hidden-table-or-trelliss-schema) | `self_check`, except Trellis's schema | [#1209](https://github.com/salesforce-misc/trellis/issues/1209) |
| [Part of the database restored from another point in time](#part-of-the-database-restored-from-another-point-in-time) | `self_check` | [#1209](https://github.com/salesforce-misc/trellis/issues/1209) |
| **Values computed differently from the definition** | | |
| [Text functions in a 1-1 field](#text-functions-in-a-1-1-field) | `self_check` | [#575](https://github.com/salesforce-misc/trellis/issues/575) |

## Preconditions

The guarantee rests on these rules. Most entries below are a way one of them
gets broken without Trellis noticing; the rest are ways Trellis goes wrong
while every rule holds.

* **Only Trellis writes what it owns:** the target tables, the tables it
  keeps beside them, and its own schema (the catalog and the ring). Grant
  application roles `SELECT` on targets and nothing more
  ([recommendations](recommendations.md#application-roles-read-targets-and-nothing-else)).
* **Leave the capture triggers and functions alone.** Trellis pauses the
  readers of a table whose capture was changed, but not when the change is
  undone before the next reconcile pass sees it
  ([embedding — What the staging worker needs from the database](embedding.md#what-the-staging-worker-needs-from-the-database)).
* **Restore or upgrade the database as a whole: a physical backup, PITR,
  `pg_upgrade` or a logical dump.** A physical backup, PITR and `pg_upgrade`
  need nothing. A logical dump (`pg_dump`, `pg_restore`) pauses every
  definition until it is resumed
  ([operations](operations.md#runbooks)). Restoring part of the database
  breaks the guarantee.
* **Use logged tables as sources.** Postgres empties an unlogged table on a
  crash, a failover or a physical restore.
* **On a partitioned source, change a partition key with `UPDATE`** (or
  `DELETE` and `INSERT`), not `MERGE`; rename or re-parent the source only
  while its transforms are paused; and let a reconcile pass run between
  creating or attaching a leaf beside a `DEFAULT` partition and removing it,
  and between partition maintenance and a `pg_upgrade`
  ([below](#a-merge-that-moves-a-row-to-another-partition)).
* **Repair after DDL that rewrites a read column's values** in one of the
  shapes [listed below](#a-columns-values-rewritten-in-place-by-alter-column-type).

## Schema changes Trellis can't tell from routine work

The staging worker's reconcile pass compares each captured table's catalog
with what it recorded, and rebuilds or pauses the readers when a read column
changed ([capture by triggers — What rebuilds, what pauses, and what's
missed](staging-and-claiming/01-capture-by-triggers.md#what-rebuilds-what-pauses-and-whats-missed)).
These changes look like routine maintenance to it.

### A column's values rewritten in place by `ALTER COLUMN TYPE`

**When:** an `ALTER COLUMN … TYPE … USING` changes the values of a column a
definition reads, in one of these shapes:

```sql
-- A metadata change to the same column within one reconcile interval
-- (5 s by default), before or after the rewrite:
ALTER TABLE orders ALTER COLUMN amount SET DEFAULT 0;
ALTER TABLE orders ALTER COLUMN amount TYPE integer USING amount * 10;

-- A widening that changes values:
ALTER TABLE orders ALTER COLUMN amount TYPE bigint USING amount * 10;
```

or a same-type `USING` rewrite of a column that has:

* a constant default it was added with, on a table no rewrite (`VACUUM
  FULL`, `CLUSTER`, a rewriting `ALTER`) has touched since;
* a compression method or a storage mode set (`SET COMPRESSION`, `SET
  STORAGE`), rewritten for the first time since;
* a non-default collation, rewritten without restating `COLLATE`.

**What goes wrong:** the statement rewrites the table without firing a
trigger, so every target reading the column keeps the old values. A 1-1
target's row is repaired when its source row is next written; an aggregate or
`JOIN` target stays wrong until it's rebuilt.

**Detected by:** `self_check`.

**Repair:** right after the `ALTER`, `request_backfill` the table.

**Ends with:** [#1206](https://github.com/salesforce-misc/trellis/issues/1206),
a false-positive study of signals that would catch these shapes. Event
triggers would see every rewrite but need superuser, and logical decoding has
the same blind spot: `pgoutput` never sends a rewrite's rows.

### An enum label renamed and added again

**When:** a change is staged with an enum label, the label is renamed away
and a new value is added with the old label, and the capture pass can't
pause the readers for the rename before the change is applied, because:

* the renamed value was itself added since the pass before, so the pass has
  no record of its OID and takes it for a new value:

  ```sql
  ALTER TYPE tone ADD VALUE 'x';
  INSERT INTO orders (id, tone) VALUES (2, 'x');
  ALTER TYPE tone RENAME VALUE 'x' TO 'y';
  ALTER TYPE tone ADD VALUE 'x';  -- all before the next pass
  ```

* or the change's transaction was open across the rename and commits only
  after the readers' `RESUME`.

**What goes wrong:** Trellis keeps the staged value as its label text, and
the text now names the new value. An aggregate, a `JOIN`, and a 1-1 beside
either on the same table apply the new value instead of the renamed one, with
no error. Without the re-add, the label fails to cast and the key is held
([capture by triggers — An enum value relabelled](staging-and-claiming/01-capture-by-triggers.md#an-enum-value-relabelled)).

**Detected by:** `self_check`.

**Repair:** `request_backfill` the table.

**Ends with:** documented.

## Writes capture never sees

### Capture disrupted and restored between two passes

The reconcile pass pauses the readers of a table whose capture triggers or
functions were changed outside Trellis, or that is gone from its name, as
soon as it sees it
([capture by triggers — Capture changed outside Trellis](staging-and-claiming/01-capture-by-triggers.md#capture-changed-outside-trellis)).
A trigger disabled and enabled again by name is seen. These aren't:

**When:**

* the table is attached as a partition or inheritance child, or a child is
  created `INHERITS` it, and it's detached or the child dropped again before
  the next pass, with writes through the parent in between:

  ```sql
  BEGIN;
  ALTER TABLE orders_all ATTACH PARTITION orders FOR VALUES FROM (MINVALUE) TO (MAXVALUE);
  UPDATE orders_all SET status = 'archived' WHERE created_at < '2025-01-01';
  ALTER TABLE orders_all DETACH PARTITION orders;
  COMMIT;
  ```

* a capture function is replaced by hand and put back before the next pass;
* capture is disrupted and put back, and the database is then restored from a
  dump, or upgraded with `pg_upgrade`, before a pass sees it: a restore
  records the triggers it finds;
* a logical-replication subscription gains the table and loses it again
  before the next pass. The apply worker fires no statement trigger.

**What goes wrong:** the writes made in between never reach Trellis. A 1-1
target's row stays wrong until its source row is written again; an aggregate
or `JOIN` target stays wrong until it's rebuilt. A child's rows written
through an inheritance parent are staged as the parent's own, and nothing
removes them.

**Detected by:** `self_check`.

**Repair:** `request_backfill` the table, or `PAUSE`/`RESUME` its readers.

**Ends with:** [#1206](https://github.com/salesforce-misc/trellis/issues/1206)
studies a signal for the attach and inherit shapes. The others are documented:
a replaced function's footprint also moves on routine `GRANT`s and owner
changes, and a restore leaves nothing to compare.

### An unlogged source emptied by a crash or failover

**When:** a source table is `UNLOGGED` (created so, or `SET UNLOGGED` after
define), and Postgres empties it: crash recovery, an immediate shutdown, a
failover to a standby, a restore of a physical backup or PITR, or a restore
of a dump taken with `--no-unlogged-table-data`.

**What goes wrong:** the reset fires no trigger, so targets keep every row
derived from the old contents, and changes captured before a crash still
apply over the empty table.

**Detected by:** `self_check`.

**Repair:** after any of those events, `request_backfill` each unlogged
source. Better, don't use unlogged tables as sources: define accepts them.

**Ends with:** nothing filed.

### A `MERGE` that moves a row to another partition

**When:** a `MERGE` whose `UPDATE` action (`WHEN NOT MATCHED BY SOURCE …
UPDATE` included) changes a row's partition. A plain `UPDATE` that moves a
row, and a `MERGE` that keeps every row in its partition, are unaffected.

**What goes wrong:** a Postgres bug in the transition tables Trellis's
capture reads:

* **15.6+, 16.2+, 17 and 18:** the moved row's images never reach the
  `UPDATE` tables. With only an `UPDATE` action the change is lost. With an
  `INSERT` action but no `DELETE` action the row is counted in both its old
  and new place, and with a `DELETE` action but no `INSERT` action in
  neither. Only a `MERGE` with both is exact.
* **15.0–15.5 and 16.0–16.1:** an `INSERT` or `DELETE` action also puts the
  moved row's new or old image in its own table, so that image is applied
  twice.

**Detected by:** `self_check`.

**Repair:** `PAUSE`/`RESUME` the transforms that read the table, or
`request_backfill` it. To avoid it, change a partition key with `UPDATE`, or
with `DELETE` and `INSERT`.

**Ends with:** a Postgres fix; it's reported upstream.

### The source renamed or re-parented and back within one pass

**When:** before the next reconcile pass, the source is renamed away and back
(`ALTER TABLE orders RENAME TO orders_old`, then back), or attached under
another partitioned table and detached again. A pass that sees the source
renamed away, or attached elsewhere, pauses its readers.

**What goes wrong:** capture stages a write only while the source's name
resolves to the root of the tree being written, so every write made in
between is lost, including writes named on the source and its leaves.
Nothing in the catalog differs afterwards.

**Detected by:** `self_check`.

**Repair:** `PAUSE`/`RESUME` the transforms that read the table, or
`request_backfill` it. To avoid it, rename or re-parent a source only while
its transforms are paused.

**Ends with:** [#1210](https://github.com/salesforce-misc/trellis/issues/1210).

### A short-lived leaf beside a `DEFAULT` partition

**When:** a leaf is created or attached beside a `DEFAULT` partition, written
through the parent, and dropped or detached before the next reconcile pass. A
leaf still there at a pass is recorded, and without a `DEFAULT` beside it each
pass's sweep finds the rows it leaves behind.

**What goes wrong:** the removed rows stay counted in their groups. The
`DEFAULT` takes every row its siblings don't, so no group routes to no
partition for the sweep to find.

**Detected by:** `self_check`.

**Repair:** `PAUSE`/`RESUME` the transforms that read the table, or
`request_backfill` it. To avoid it, delete a short-lived leaf's rows through
the parent before removing it, or keep it until a pass has run.

**Ends with:** documented; accepted in the design.

### A leaf detached and attached again around a `pg_upgrade`

**When:** a leaf is detached, written while detached, and attached again under
the same parent and bound, with no reconcile pass completing a deep check
between that and a `pg_upgrade`. Outside an upgrade, the pass sees the leaf's
new `pg_inherits` row and corrects it.

**What goes wrong:** a detached leaf's capture stages nothing, and the
upgrade resets every `xmin`, so the pass records the leaf as unchanged. The
groups the missed writes touched stay as they were.

**Detected by:** `self_check`.

**Repair:** `PAUSE`/`RESUME` the transforms that read the table, or
`request_backfill` it. To avoid it, let a pass run after partition
maintenance before an upgrade, and after the upgrade before detaching or
attaching a partition.

**Ends with:** [#1210](https://github.com/salesforce-misc/trellis/issues/1210).

## Changes made outside Trellis to what it owns

### Rows written into a target, a hidden table or Trellis's schema

**When:** anything but Trellis writes to what it owns:

* `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE` or a data load into a target, or
  into a table Trellis keeps beside one: a 1-1 target's `<target>__ledger`,
  an aggregate's `agg_<id>`, or a `JOIN`'s `join_<id>_*` tables;
* a target dropped and created again by hand with the same columns;
* a trigger or rule added to a target or one of those tables that changes
  the rows Trellis writes, or the table set `UNLOGGED`, which a crash or
  failover empties;
* DML, DDL or a data load into Trellis's own schema, such as another
  database's catalog rows.

Some hand changes are loud instead: a target dropped, one of its columns
dropped, renamed or re-typed, or its key constraint dropped fails the next
drain or pauses the definition, and a resume refuses while the target is
missing.

**What goes wrong:** Trellis assumes it's the only writer and believes what it
finds. An edited target row stays wrong until its key is recomputed for
another reason, and a truncated target stays empty. Trellis's catalog and
ring are its record of what every target has applied, so it acts on whatever
a change to them says.

**Detected by:** `self_check` reports a target's wrong rows, and a hidden
table's. A change to Trellis's schema isn't detected.

**Repair:** first drop a trigger or rule added by hand, or `SET LOGGED`.

* **A 1-1 target:** `PAUSE`/`RESUME`, or `request_backfill` of its source,
  repairs rows edited, deleted, added or re-keyed by hand and a truncate: a
  rebuild deletes the target rows no source row backs. A target dropped by
  hand: `DROP TRANSFORM` and define it again.
* **An aggregate target:** `request_backfill` of its source rewrites every
  group of the target and `agg_<id>`, and `PAUSE`/`RESUME` drops and
  recreates `agg_<id>` and rewrites every group.
* **A `JOIN` target:** `PAUSE`/`RESUME` repairs the target's rows and every
  `join_<id>_*` table: the resume recreates the hidden tables, rebuilds them
  from both sources and keeps the target, removing a row no source row backs.
  So does `PAUSE`, `DROP TRANSFORM` and define it again. `request_backfill`
  of either source repairs target rows edited by hand, but not a hand edit
  of a `join_<id>_*` table.
* **Trellis's schema:** restore the whole database from a backup taken
  before the change, or `PAUSE`, `DROP TRANSFORM` and define every transform
  again. A target the changed catalog has no definition for is a plain table
  to Trellis: drop it by hand before defining it again.

**Ends with:** [#1209](https://github.com/salesforce-misc/trellis/issues/1209)
weighs refusing or detecting such writes. Until then it's a rule
([transforms — Target tables are Trellis-owned](transforms.md#target-tables-are-trellis-owned)).

### Part of the database restored from another point in time

**When:** a restore rewinds part of what Trellis keeps in step, without the
rest:

* Trellis's schema alone;
* Trellis's schema and the sources, without the targets;
* the targets alone;
* a data-only reload into a target.

Restoring the sources alone is loud: it recreates their tables, so their
readers pause until `RESUME`. A data-only reload of a source goes through its
capture triggers.

**What goes wrong:** the catalog, the ring and the targets disagree about
what has been applied. A restore of Trellis's schema loses the changes the
ring held.

**Detected by:** `self_check`, on the targets.

**Repair:** restore the whole database from one backup, or `PAUSE`, `DROP
TRANSFORM` and define each affected transform again.

**Ends with:** [#1209](https://github.com/salesforce-misc/trellis/issues/1209).

## Values computed differently from the definition

### Text functions in a 1-1 field

**When:** a 1-1 transform's field calls `REGEXP_COUNT` with a pattern that
uses any regex syntax beyond plain characters, or a column read by `STRPOS`
or `REGEXP_COUNT` is given a nondeterministic collation after define. Define
refuses such a collation, because Postgres refuses both functions under one,
and nothing checks it afterwards.

**What goes wrong:** the drain evaluates the pattern with Rust's `regex`
engine, which differs from Postgres's even on ordinary patterns:
`regexp_count('aab', 'a|aa')` is 1 in Postgres and 2 in Trellis, `.` matches
a newline only in Postgres, and `\w` and `\d` disagree on non-ASCII text and
under `"C"` collation. A build evaluates in Postgres, so the target holds
Postgres's value on rows a build wrote and Rust's on rows the drain wrote
since. The drain's evaluator has no collation to consult, so under a
nondeterministic one it keeps writing values Postgres would refuse to
compute. An aggregate transform's arguments evaluate in Postgres and aren't
affected. A pattern Rust's engine rejects (back-references, `\m`, embedded
options) is refused at define.

**Detected by:** `self_check`. Under a nondeterministic collation, its
recompute fails with Postgres's error, as a rebuild does.

**Repair:** use patterns of plain characters. `PAUSE`/`RESUME` rewrites
every row with Postgres's value, until each row's next write.

**Ends with:** [#643](https://github.com/salesforce-misc/trellis/issues/643),
which closes when [#575](https://github.com/salesforce-misc/trellis/issues/575)
moves 1-1 evaluation into Postgres.

## What `self_check` can't see

`self_check(target)` starts a background job that audits the target's
capture, then compares the target with a recompute of it in Postgres, a page
of keys at a time
([embedding — `self_check` is a background job](embedding.md#self_check-is-a-background-job),
[ADR-0013](decisions/0013-self-check-production-recompute-audit.md)). It
detects; it never repairs. It's how you find the gaps above, so its own
limits belong here:

| Target | `self_check` | Until |
|---|---|---|
| 1-1, including one over another target | compared | — |
| Aggregate, `JOIN` | compared, hidden tables included | — |

* Each audit also reports the definition's held keys and the instance's drain
  holdups. They aren't gaps, but they explain a target that stops short.

## Repairing a gap

Each entry names its repair.
[Operations — Repair tools](operations.md#repair-tools) says what each tool
fixes and what it costs:

* **`request_backfill(source_table)`** re-reads one source table for every
  definition that reads it, without pausing them. It's the cheapest repair
  when you know which table missed changes.
* **`PAUSE TRANSFORM` then `RESUME TRANSFORM`** rebuilds one definition from
  its sources.
* **`PAUSE`, `DROP TRANSFORM` and define again** builds the target from empty.
  It's the repair when a rebuild keeps something wrong.

## Not gaps

* **Everything Trellis refuses at define, or pauses on when it appears
  later:** a source without a primary key, row-level security on a role
  Trellis runs as,
  a logical-replication subscription into a source, a source
  in a hierarchy
  ([transforms — Supported sources and targets](transforms.md#supported-sources-and-targets)),
  and a column renamed, dropped or retyped, a primary key redefined, or
  capture changed outside Trellis
  ([capture by triggers — What rebuilds, what pauses, and what's missed](staging-and-claiming/01-capture-by-triggers.md#what-rebuilds-what-pauses-and-whats-missed)).
* **Stops that need you** (a pause, a quarantine, held keys, a drain holdup,
  a failing build): Trellis reports them, and the target stops short instead
  of going wrong ([operations](operations.md)).
* **DML under `session_replication_role = replica`.** The capture triggers
  are `ENABLE ALWAYS`, so they fire.
* **Session settings** (`DateStyle`, `TimeZone`, `extra_float_digits`, …).
  Trellis pins them on its connections and in its capture functions
  ([type support — Text rendering](type-support.md#text-rendering)).
* **A `default_transaction_isolation` of `repeatable read` or
  `serializable`.** Trellis's connections use `read committed` whatever the
  setting, and capture works at every level.
* **A whole-database physical backup and restore, PITR or `pg_upgrade`.**
  Sources and Trellis's state stay in step, except an unlogged source's data
  after a physical restore or PITR
  ([above](#an-unlogged-source-emptied-by-a-crash-or-failover)).
* **A whole-database logical dump and restore.** The ordering state in the
  dump belongs to the original cluster's WAL and transaction counter, so the
  capture pass pauses every definition, discards the changes staged before it
  and waits for a `RESUME` of each, which resets that state and rebuilds the
  target from the restored source. Nothing is skipped, and nothing applied
  before the pass stays: a drain worker that starts after the restore waits
  for the pass, and the rebuild overwrites what one already running applied
  ([operations](operations.md#runbooks)).
