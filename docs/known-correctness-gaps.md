# Known correctness gaps

Trellis keeps each target equal to what its definition computes from the
source tables. This page lists the known ways a user or operator can break
that guarantee, and leave a target stale or wrong, *without* Trellis repairing
it on its own.

Things Trellis detects and repairs by itself aren't listed; those are just how
it works (see [What isn't on this list](#what-isnt-on-this-list)). Each entry
here needs an operator to act, either because Trellis can't see the problem or
because it only pauses or fails and waits for you.

Each entry gives:

* **Trigger**: what causes it.
* **Effect**: what goes wrong, and whether it's silent.
* **Detected?**: whether Trellis notices, automatically or through
  `self_check`.
* **Planned work**: issues, branches and decisions that would close the gap.
* **Repair**: what you can do today.

## The repair tools

These are the tools the entries refer to:

* **`PAUSE TRANSFORM <target>` then `RESUME TRANSFORM <target>`** rebuilds the
  target from the source. Run it through `Trellis::apply` or
  `trellis apply '<statement>'`. A resume re-reads every current source row and
  deletes target rows the source no longer backs
  ([ADR-0014](decisions/0014-pause-and-drop-a-transform.md)). It doesn't replay
  buffered changes, so it repairs anything that's wrong because a change was
  missed.
* **`request_backfill(source_table)`** (Rust, Ruby and Elixir) queues a re-read
  of one source table for every definition that reads it, without pausing them.
  It's the cheapest repair when you know which table missed changes.
* **`DROP TRANSFORM <target>`, then define it again** is the only repair when the
  definition itself no longer matches the schema. It's needed because a resume
  doesn't re-validate the definition (see [Repair caveats](#repair-caveats)).
* **`self_check(target, …)`** detects divergence but never repairs it. It
  audits the target's capture triggers, then compares the target with a
  recompute of it in Postgres
  ([ADR-0013](decisions/0013-self-check-production-recompute-audit.md)).
  **It only supports 1-1 targets today.** Aggregate targets return
  `UnsupportedKeySpace` and relationship-enriched fields return
  `UnsupportedExpr`, so neither gets the capture audit or the comparison.

## Summary

| # | Trigger | Silent? | Detected? | Planned work |
|---|---|---|---|---|
| 1 | `ALTER COLUMN … TYPE … USING` that rewrites values | yes | `self_check` (1-1) only | #703, study pending |
| 2 | A column dropped and re-added under the same name | yes | `self_check` (1-1) only | #703, study pending |
| 3 | Retype or re-collate of a key, join or GROUP BY column | mostly | no | #760, fix held for sign-off |
| 4 | Widening a column Trellis keeps a typed copy of (`int` → `bigint`) | no (writes fail) | at failure only | #767, decided, not built |
| 5 | Capture switched off and back on between two reconcile passes | yes | no | #707, study pending |
| 6 | A capture function body replaced by hand | yes | not by the audit | #707, study pending |
| 7 | A source attached as a partition, or made to inherit, after define | yes | `self_check` (1-1) only | #707, study pending |
| 8 | Hand edits to a target table | yes | `self_check` (1-1) only | none; documented |
| 9 | An application trigger re-keying a parent's join column within the statement | yes | no | #788, decision pending |
| 10 | `REGEXP_COUNT` on `"C"`-collated data or with Postgres-only regex syntax | yes | `self_check` (1-1) only | #643, with #575 |
| 11 | Row-level security applying to a Trellis login role that isn't checked | yes | no | #766, decision pending |
| 12 | A crash empties an unlogged source table | yes | no | none filed |
| 13 | Partial restore, or a schema-only load (`db:schema:load`, `ecto.load`) | partial restore silent; schema load loud | schema load: on define | #644 |
| 14 | A source's primary key dropped, or retyped to an unsupported type | no (drain loops) | logs only | #663, decided, not built |
| 15 | A field alias that shadows a source column read by an aggregate | target never builds | logs only | #695 |

## 1. A rewriting `ALTER COLUMN TYPE … USING`

**Trigger:**

```sql
ALTER TABLE orders ALTER COLUMN amount TYPE bigint USING amount * 10;
-- or the same type, values rewritten in place:
ALTER TABLE orders ALTER COLUMN amount TYPE integer USING amount * 10;
```

**Effect:** the statement rewrites the table without firing any DML
trigger, so capture never sees the new values. Logical decoding doesn't see
them either. Every target that reads `amount` keeps its old values until each
row is written again. Nothing pauses and status shows nothing.

**Detected?** Not automatically. Capture compares column *names*, not types,
attnums or values. `self_check` reports the cell differences on 1-1 targets.

**Planned work:** #703 proposes recording each read column's identity
(attnum, type, typmod), plus a filenode/`xmin` baseline that would catch a
same-type rewrite. Either would pause the readers. Both are gated on a
false-positive study that hasn't run yet. If a rule is too noisy, this case
stays documented as a limitation. Event triggers were rejected because they
need superuser (#699).

**Repair:** right after the `ALTER`, run `request_backfill('orders')`, or
`PAUSE`/`RESUME` every transform that reads the table. If the type changed
and a key, join or GROUP BY column is involved, see entries 3 and 4 first.

## 2. A column dropped and re-added under the same name

**Trigger:**

```sql
ALTER TABLE users DROP COLUMN plan;
ALTER TABLE users ADD COLUMN plan text DEFAULT 'free';
```

Both statements can run before the next reconcile pass sees the drop.

**Effect:** every existing row now has the new column's value. No trigger
fires, and the name still matches, so there's no schema-change marker and
no pause. Targets keep values derived from the old column.

**Detected?** Not automatically, because detection is by name. `self_check`
reports it on 1-1 targets. (A drop that a pass or a captured write sees
first *is* detected; see [What isn't on this list](#what-isnt-on-this-list).)

**Planned work:** #703 (the same column-identity rule as entry 1).

**Repair:** the same as entry 1: `request_backfill`, or `PAUSE`/`RESUME` the
readers.

## 3. Retyping or re-collating a key, join or GROUP BY column

**Trigger:** an `ALTER COLUMN … TYPE` or `… COLLATE` on a column a
definition uses as its key, a relationship join column, or a GROUP BY column,
after that definition exists. The checks that would have refused it run only
at define. Examples:

* `varchar` → `character(n)` on a join column. `bpchar`'s `::text` drops the
  padding that the captured image keeps, so key comparisons never match.
* A change to a nondeterministic collation. Postgres's `=` then folds keys
  that Trellis keeps apart (#590, #638).
* A narrower numeric scale on a GROUP BY column, e.g. `numeric(10,3)` →
  `numeric(10,2)`. It rounds `1.555` and `1.556` both to `1.56` with no
  trigger, leaving two groups where the source has one.
* `timestamp(6)` → `timestamp(3)`, `timestamp` ↔ `timestamptz` (which moves
  the instant when run in a non-UTC session), `date` → `timestamp`, or
  `text` → `uuid`/`int` with `USING`.
* `uuid`, `int` or an enum → `text` on a 1-1 source key. Every later drain then
  fails with `operator does not exist: uuid = text`.

**Effect:** usually silent, with stale or split groups and unmatched
relationship rows. Sometimes loud, with drain failures that charge keys to
quarantine (see [Repair caveats](#repair-caveats)).

**Detected?** No. Routine changes are safe and need nothing:
`varchar(n)` → `varchar(m>n)`, `varchar` → `text`, a wider numeric precision or
scale, and a change between deterministic collations. `int` → `bigint` is
safe for the values but not for Trellis's copies; see entry 4.

**Planned work:** #760. A fix is built, reviewed and passing on branch
`fix/issue-760-revalidate-type-collate`, held unmerged until the
false-positive study in the issue is signed off. The fix pauses the
definition's readers with a `capture_failure` naming the column, in two
cases:

* the new type or collation would be refused at define;
* the change re-renders stored keys, compared against the type recorded at
  define.

It ships with #767 and #759 as one PR.

**Repair:** revert the column's type or collation, then `PAUSE`/`RESUME`
the readers. If you're keeping the new type, `DROP TRANSFORM` and define it
again so it's validated against the new schema. A resume alone won't
re-validate (#708).

## 4. Widening a column Trellis keeps a typed copy of

**Trigger:** a routine widening such as `ALTER COLUMN id TYPE bigint`
(common in Rails and Ecto migrations), a `varchar(n)` widening, or a wider
numeric or timestamp precision. This applies to any column Trellis copies with
its type:

* a 1-1 target's key and passthrough columns;
* an aggregate's GROUP BY key;
* the ledger's group column;
* a relationship projection's key.

**Effect:** loud, but not self-healing. Trellis's copies keep the old type,
so the first value that doesn't fit fails every write to that row with
`22003` (out of range) or `22001` (too long). The key is poisoned and stays
held (see [Repair caveats](#repair-caveats)). Rows written before that are
fine.

A one-sided widening of a join pair (`line_items.product_id` to `bigint`
while `products.id` stays `integer`) also breaks the same-type rule that
relationships are defined under.

**Detected?** Only when the first oversized value fails.

**Planned work:** #767, decided 2026-10-05 and not yet built. It goes on #760's
branch. The capture pass will compare each copy's type with the source and
pause the owning definitions. Resume will then widen the copies
(`ALTER … TYPE`, under `ACCESS EXCLUSIVE`) and rebuild.

**Repair:** `DROP TRANSFORM` and define it again. `PAUSE`/`RESUME` doesn't help
today, because the rebuild writes into the same narrow tables. Keys that are
already quarantined may stay held (see [Repair caveats](#repair-caveats)).

## 5. Capture switched off and back on between two reconcile passes

**Trigger:** a capture trigger that's disabled, dropped, or switched out of
`ENABLE ALWAYS`, and restored before the staging worker's next reconcile pass
(every 5 s) looks:

```sql
BEGIN;
ALTER TABLE orders DISABLE TRIGGER ALL;
UPDATE orders SET status = 'archived' WHERE created_at < '2025-01-01';
ALTER TABLE orders ENABLE ALWAYS TRIGGER ALL;
COMMIT;
```

The same applies to bulk-load tools that disable triggers around a load and
restore them to `ALWAYS`.

**Effect:** the writes made while capture was off never reach Trellis.
Targets stay wrong until each affected row is written again, with nothing
on status.

**Detected?** No. The pass and the `self_check` audit both see only the
current trigger state, and it looks healthy again.

If the pass *does* see the broken trigger, it isn't a gap. Trellis reinstalls
it and re-reads the table for its readers, so it repairs itself. Examples are a
trigger left disabled, `ENABLE TRIGGER` restoring it as `ORIGIN` rather than
`ALWAYS` (which `pg_restore --disable-triggers` does), or a dropped and
recreated table. The same holds while no staging worker is running: nothing is
repaired until a worker starts, and then its pass repairs it.

**Planned work:** #707. The candidate signal is the `xmin` of Trellis's
`pg_trigger` rows, which a disable/enable cycle rewrites. The work is gated on
a false-positive study that hasn't run yet; cases it can't detect without
routine false positives will be documented here instead.

**Repair:** `request_backfill` the table, or `PAUSE`/`RESUME` its readers.
Better still, don't disable the capture triggers
([embedding — Leave the capture triggers alone](embedding.md)).

## 6. A capture function body replaced by hand

**Trigger:** `CREATE OR REPLACE FUNCTION` on one of Trellis's capture
functions, with a body that drops or alters what it stages.

**Effect:** the writes it mis-stages are lost or wrong. The pass sees the body
is stale and rewrites it. That repair is a "widen", which doesn't re-read the
table, so the writes made in between stay lost.

**Detected?** The function is restored, but the lost writes aren't detected.

**Planned work:** #707 (case 2). One option there is to have this repair
re-read the table the way a reinstall does.

**Repair:** `request_backfill` the table, or `PAUSE`/`RESUME` its readers.

## 7. A source attached as a partition, or made to inherit, after define

**Trigger:** `ALTER TABLE parent ATTACH PARTITION orders …`, or
`ALTER TABLE orders INHERIT parent` (or the reverse), after a definition reads
`orders`. Define refuses such tables, but nothing refuses the later change.

**Effect:** a write through the parent fires the parent's statement triggers,
not the child's, so the change is missed or attributed to the wrong table.
This is silent.

**Detected?** The reconcile pass doesn't check for it, so nothing pauses. The
`self_check` audit reports it as a `capture` divergence on 1-1 targets.

**Planned work:** #707 (case 3).

**Repair:** detach the table (or `NO INHERIT`) so it's a plain table again,
then `PAUSE`/`RESUME` its readers.

## 8. Hand edits to a target table

**Trigger:** any `INSERT`, `UPDATE`, `DELETE` or `TRUNCATE` on a target
table, or changing its columns or key constraints.

**Effect:** Trellis assumes it's the only writer and doesn't correct the
change. An edited row stays wrong until its key is recomputed for another
reason, and a truncated target stays empty.

**Detected?** `self_check` reports it on 1-1 targets. Nothing reports it on
aggregate targets.

**Planned work:** none. This is a documented rule
([transforms — Target tables are Trellis-owned](transforms.md#target-tables-are-trellis-owned)).

**Repair:** `PAUSE`/`RESUME` the transform. If its columns or constraints
were changed, `DROP TRANSFORM` and define it again.

## 9. An application trigger re-keying a parent's join column within the statement

**Trigger:** an application `AFTER ROW` trigger (or a self-referencing
cascade) that changes a parent's non-key join column again inside the
statement that changed it. For example, `parents.code` goes `'a'` → `'x'`, then
the trigger changes it to `'y'`.

**Effect:** capture records `'x'` as the old value, which was never committed.
Nothing names the committed `'a'`, so the children that joined through `'a'`
are never re-derived. Aggregates and to-one projection fields over those
children keep stale values.

**Detected?** No. Ignored tests in `ledger_interleavings` pin the
behaviour.

**Planned work:** #788 needs a design decision. The options are:

* have the fold pick its old image from the outer write;
* have capture record the outer pre-image;
* leave it to milestone E (#624);
* document it as unsupported.

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

**Detected?** `self_check` on 1-1 targets.

**Planned work:** #643, decided. It closes when #575 moves expression
evaluation into Postgres.

**Repair:** stick to literal patterns or UTF-8 collated data until #575 lands.
Afterwards, `PAUSE`/`RESUME` the affected transforms.

## 11. Row-level security on a login role Trellis doesn't check

**Trigger:** a row-level security policy that applies to the role a drain
worker logs in as, when that isn't the ring owner or the defining role.

**Effect:** reads that build or recompute a target silently skip the rows
the policy hides.

**Detected?** No. Define refuses, the capture pass pauses and `self_check`
reports RLS that applies to the ring owner or the defining role (#745, #765).
The check doesn't cover other login roles.

**Planned work:** #766 needs a design decision: set `row_security = off` on
Trellis connections so a filtered read raises instead.

**Repair:** grant `BYPASSRLS` to every role Trellis logs in as (it isn't
inherited), or make the role the table owner without
`FORCE ROW LEVEL SECURITY`. Then `PAUSE`/`RESUME` the readers
([transforms — Source tables](transforms.md#source-tables)).

## 12. A crash empties an unlogged source table

**Trigger:** an `UNLOGGED` source table, followed by a crash or an immediate
shutdown. Crash recovery resets unlogged tables to empty.

**Effect:** the reset fires no trigger, so targets keep every row derived
from the table's old contents. This is silent. It's inferred from Postgres's
behaviour and Trellis not checking `relpersistence`; it isn't tested, and no
issue is filed.

**Detected?** `self_check` on 1-1 targets.

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
  handling (#236).
* A schema load is loud. The dump carries the target tables but not their
  definitions, so nothing maintains them, and defining again fails with a
  `conflict` error because the target exists.

**Detected?** A partial restore isn't detected (`self_check` on 1-1 targets
only). A schema load is detected when you define again.

**Planned work:** #644 is decided: dump the definitions with the schema, the
way Rails dumps `schema_migrations`. It isn't built yet.

**Repair:**

* After a partial restore, `PAUSE`/`RESUME` every transform whose sources or
  targets were restored.
* For a schema-loaded database, drop the target tables and run the defining
  migrations ([embedding](embedding.md)).

## 14. A source's primary key dropped or moved to an unsupported type

**Trigger:** `ALTER TABLE orders DROP CONSTRAINT orders_pkey`, or a key
column retyped off the supported list, while definitions read the table.

**Effect:** it's loud but doesn't stop. The docs promise the instance halts.
In fact the worker re-claims the failing batch every poll interval
(200 ms) forever and logs every attempt. Other definitions' changes coalesced
into that batch are held up with it, and the targets go stale. A redefined
key (`DROP CONSTRAINT …, ADD PRIMARY KEY (id, region)`) is paused by the
next reconcile pass (#687). Before that pass, drains can fail with
`MalformedCompositeKey` and charge keys to quarantine (#703).

**Detected?** In logs only. Status has no halted state.

**Planned work:** #663, decided and not built (it depends on #623). Only the
dependent definitions will be parked, shown as `halted` with a reason, and
resume will rebuild them.

**Repair:** restore a supported primary key, then `PAUSE`/`RESUME` the
table's readers. If the key changed for good, `DROP TRANSFORM` and define the
transforms again.

## 15. A field alias that shadows a source column

**Trigger:** an aggregate whose field alias is the name of a source column
another field aggregates, e.g.
`SELECT grp, SUM(val) AS val, MIN(val) AS lo …`. `lo` then reads as
`MIN(SUM(val))`.

**Effect:** define accepts it, but every build attempt fails. The transform
stays `waiting_to_backfill` forever, with no reason on status. No wrong data
is written, but the target never fills.

**Detected?** Warn logs only.

**Planned work:** #695 is open, with no fix yet.

**Repair:** `DROP TRANSFORM`, then define it again with an alias that doesn't
shadow a source column.

## Repair caveats

* **Resume doesn't re-validate (#708).** `RESUME TRANSFORM` rebuilds with the
  definition as it was checked at define. After a type, key or column change,
  it can fail row by row or write values of the wrong type. When the schema
  changed under a definition, `DROP TRANSFORM` and define it again. #708 is
  decided (resume will run define's validation first) and not yet built.
* **Quarantined keys stay held (#759).** A key that's quarantined after
  repeated apply failures keeps its parked work and its poison row. Entries
  3, 4 and 14 can cause such failures.
  `RESUME TRANSFORM` doesn't clear them today, so that key's target row stays
  stale. There's no supported release in production: `release_key` is
  test-only. Quarantine is keyed by source table, so dropping and redefining
  the transform isn't known to clear it either. Resume is decided to clear
  every poisoned key and rebuild, in the #760/#767 PR.
* **Resuming a field that's awaiting capture (#687).** A `RESUME` of a field
  whose status shows `capture_wait` unpauses it before its capture is widened,
  so its rows fail with `MissingColumn`. Let the wait finish first.
* **The bindings don't show pause reasons yet (#687).** Ruby, Elixir and
  embedded status show `paused` but not `capture_failure` or `capture_wait`.
  Read `Trellis::status` from Rust, or the logs, for the reason.
* **`self_check` covers 1-1 targets only.** For aggregate and
  relationship-enriched targets, the only check is comparing against your own
  query of the source.

## What isn't on this list

Trellis handles these cases itself, either by repairing them or by
refusing them up front:

* **DML under `session_replication_role = replica`.** The capture triggers are
  `ENABLE ALWAYS`, so they fire. This is only a gap through entry 5.
* **A capture trigger left disabled, dropped, or not `ALWAYS`, or a source
  table dropped and recreated.** The next reconcile pass reinstalls capture and
  re-reads the table for its readers.
* **`TRUNCATE` of a source**, including `CASCADE`.
* **A read column renamed or dropped, or the primary key redefined (once the
  pass sees it).** Trellis pauses the readers with a `capture_failure`, and
  you resume once the schema is right
  ([capture by triggers](staging-and-claiming/01-capture-by-triggers.md)).
* **Row-level security on the ring owner or defining role, and a
  logical-replication subscription into a source.** These are refused at define,
  paused by the pass, and reported by `self_check`
  ([transforms — Source tables](transforms.md#source-tables)).
* **Session settings** (`DateStyle`, `TimeZone`, `extra_float_digits`, …).
  Trellis pins them on its connections and in its capture functions.
* **A whole-database backup and restore, or PITR.** Sources and Trellis state
  roll back together.
