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

* Trellis is the only writer to its target tables (entry 8).
* The capture triggers stay installed, enabled and `ENABLE ALWAYS`, and the
  capture functions keep their bodies (entries 5 and 6).
* The source tables stay plain tables, outside any partition or inheritance
  hierarchy (entry 7).
* No DDL rewrites a column a definition reads, or retypes one it reads only
  as a field, beyond the changes Trellis detects and pauses for (entries 1, 2
  and 4). Retyping or re-collating a key, join or GROUP BY column, and
  widening a column Trellis keeps a typed copy of, pause the definitions
  concerned (see [What isn't on this list](#what-isnt-on-this-list)).
* No row-level security policy applies to a role Trellis logs in as, or to
  the role that owns its ring (entry 11).
* No application trigger re-keys a relationship's join column within the
  statement that wrote it (entry 9).

Each entry gives:

* **Trigger**: what causes it.
* **Effect**: what goes wrong, and whether it's silent.
* **Detected?**: whether Trellis notices, automatically or through
  `self_check`. Only 1-1 targets can be audited (entry 16), so "`self_check`"
  below always means a plain 1-1 target.
* **Planned work**: issues, branches and decisions that would close the gap.
* **Repair**: what you can do today.

## The repair tools

These are the tools the entries refer to:

* **`PAUSE TRANSFORM <target>` then `RESUME TRANSFORM <target>`** rebuilds the
  target from the source. Run it through `Trellis::apply` or
  `trellis apply '<statement>'`. A resume first re-runs define's validation
  against the live schema, and refuses, leaving the definition paused, while
  define would refuse it. It then brings Trellis's typed copies of the
  definition's key, passthrough, GROUP BY and projection-key columns to their
  sources' live types, re-reads every current source row and deletes target
  rows the source no longer backs
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
  definition whose source key was redefined can only be defined again. It is
  also the repair for entry 4.
* **`self_check(target, …)`** detects divergence but never repairs it. It
  audits the target's capture triggers, then compares the target with a
  recompute of it in Postgres
  ([ADR-0013](decisions/0013-self-check-production-recompute-audit.md)).
  **It only audits 1-1 targets** (entry 16).

## Summary

| # | Trigger | Silent? | Detected? | Planned work |
|---|---|---|---|---|
| 1 | `ALTER COLUMN … TYPE … USING` that rewrites values | yes | `self_check` (1-1 targets only) | #703, study pending |
| 2 | A column dropped and re-added under the same name | yes | `self_check` (1-1 targets only) | #703, study pending |
| 3 | A source key re-collated while a `self_check` sweep runs | the sweep can skip or repeat keys | no | #782 |
| 4 | Widening a column read only as a field that Trellis copies with its type (`SUM(qty)` with `qty` `int` → `bigint`) | no (writes fail) | at failure only | none filed |
| 5 | Capture switched off and back on between two reconcile passes | yes | no | #707, study pending |
| 6 | A capture function body replaced by hand | yes | not by the audit | #707, study pending |
| 7 | A source attached as a partition, or made to inherit, after define | yes | `self_check` (1-1 targets only) | #707, study pending |
| 8 | Hand edits to a target table | yes | `self_check` (1-1 targets only) | none; documented |
| 9 | An application trigger re-keying a parent's join column within the statement | yes | no | #788, decision pending |
| 10 | `REGEXP_COUNT` on `"C"`-collated data or with Postgres-only regex syntax | yes | `self_check` (1-1 targets only) | #643, with #575 |
| 11 | Row-level security applying to a role Trellis runs as | only capture, for the ring owner, until the next reconcile pass; elsewhere reads and writes fail | drain, discharge and catch-up: pause what they reach; one the catalog can't pin only logs (drain) or shows on `backfill_failure` (discharge); build chunk: `backfill_failure` | #817 |
| 12 | A crash empties an unlogged source table | yes | no | none filed |
| 13 | Partial restore, or a schema-only load (`db:schema:load`, `ecto.load`) | partial restore silent; schema load loud | schema load: on define | #644 |
| 15 | A from-side change pending across a to-side `TRUNCATE` | yes | no | #528, test ignored |
| 16 | `self_check` audits 1-1 targets only | yes | n/a | none filed |
| 17 | `DROP TYPE` of an enum a live definition references | no (later introspection fails) | at failure only | none filed |
| 18 | `jsonb_agg` element order differs between recomputes | yes | no | none filed |
| 19 | A paused column doesn't pause the aggregates that read it | yes | on the upstream only | none filed |
| 20 | A column paused and resumed on a 1-1 definition that reads a to-one relationship, while both tables take writes | yes | no | #832 (suspected cause #831) |
| 21 | A to-one relationship field keeps a superseded parent value under concurrent writes (rare) | yes | no | #838 |

## 1. A rewriting `ALTER COLUMN TYPE … USING`

**Trigger:**

```sql
ALTER TABLE orders ALTER COLUMN amount TYPE bigint USING amount * 10;
-- or the same type, values rewritten in place:
ALTER TABLE orders ALTER COLUMN amount TYPE integer USING amount * 10;
```

**Effect:** the statement rewrites the table without firing any DML
trigger, so capture never sees the new values. Every target that reads `amount` keeps its old values until each
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
`PAUSE`/`RESUME` every transform that reads the table. A type change to a
key, join or GROUP BY column pauses its readers on its own (see
[What isn't on this list](#what-isnt-on-this-list)); a field's widening is
entry 4.

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

## 3. A source key re-collated while a `self_check` sweep runs

**Trigger:** `ALTER COLUMN id TYPE text COLLATE "C"` (or any change between
deterministic collations) on a source's key column while a `self_check`
sweep of a 1-1 target pages through it.

**Effect:** the data stays right: byte equality doesn't change, so nothing
pauses. But `self_check` pages the source and the target under the source
key's collation, and its `next_after` cursor continues in the new order, so a
sweep that straddles the change can skip or repeat keys. It also reads each
page of the target by a scan instead of the target's key index (#782). A
build that straddles the change is pinned to the collation it planned under
(#769) and doesn't lose rows, but its remaining range reads scan the source.

**Detected?** No.

**Planned work:** #782.

**Repair:** start a new sweep after the change.

## 4. Widening a column read only as a field that Trellis copies with its type

**Trigger:** a routine widening, such as `ALTER COLUMN qty TYPE bigint` or a
`varchar(n)` widening, of a column a definition reads only as a field, where
Trellis created a column of the field's type from it:

* an aggregate's contribution column in its ledger, and its target column,
  for `SUM(qty)`, `MIN(qty)` and `MAX(qty)` over an integer column;
* a 1-1 calculated field over an integer column (`qty + 1`);
* a to-one relationship projection's column for a to-side field read through
  it (`author.name`), which copies the to-side column's type exactly. The same
  column read as a `GROUP BY` key (`GROUP BY author.country`) is a key copy,
  not this entry.

A key, passthrough, GROUP BY or projection-key column is not this entry:
widening one pauses its definitions, and a resume re-types Trellis's copies
(see [What isn't on this list](#what-isnt-on-this-list)).

**Effect:** loud, but not self-healing. Trellis's column keeps the old type,
so the first value that doesn't fit fails every write to its row with
`22003` (out of range) or `22001` (too long). The key is quarantined for each
definition whose column is too narrow, and stays held there, while the other
definitions reading it keep applying it (see
[Repair caveats](#repair-caveats)). Rows written before that are fine.

**Detected?** Only when the first oversized value fails.

**Planned work:** none filed. The capture pass's widening check covers the
copies of key, passthrough, GROUP BY and projection-key columns only; a
field's columns are #703's R1, which leaves columns read only as fields out.

**Repair:** `DROP TRANSFORM` and define it again. `PAUSE`/`RESUME` doesn't
help: the resume doesn't re-type these columns, so the rebuild fails the same
way. The drop deletes the definition's quarantined keys with it, so the new
definition starts with none.

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
aggregate or relationship-enriched targets.

**Planned work:** none. This is a documented rule
([transforms — Target tables are Trellis-owned](transforms.md#target-tables-are-trellis-owned)).

**Repair:** `PAUSE`/`RESUME` the transform. If its columns or constraints
were changed, `DROP TRANSFORM` and define it again.

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
  `EXECUTE`, one of Trellis's own tables) pauses nothing: the drain retries
  the page and surfaces the error on every pass, charges no key, and puts
  nothing in `status()`. The logs and a stalled watermark are the only signs.
* **A build chunk** is retried, and its fifth charged attempt pauses its
  definition, with the error on `backfill_failure`. It's never narrowed to a
  key.
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
definitions and shows in `status()`, except a `42501` the catalog can't pin:
a drain's only logs, and a discharge's or catch-up's shows on
`backfill_failure`. A build chunk's shows on `backfill_failure`. For the
ring's owner, the reconcile pass pauses the readers and `self_check` reports
a `capture` divergence (#745, #765). Define
and declare refuse policies that already apply
([transforms — Supported sources and targets](transforms.md#supported-sources-and-targets)).

**Planned work:** #817 decides what a refusal the halt can't pin on a table
should do, for the drain and the discharge alike.

**Repair:** grant `BYPASSRLS` to every role Trellis logs in as and to the
ring's owner (it isn't inherited), or make the role the table owner without
`FORCE ROW LEVEL SECURITY`, and grant any missing privilege. Then resume the
paused definitions, which rebuilds them. A discharge or catch-up backing off
on a refusal the catalog couldn't pin succeeds on its next attempt.

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
  handling (#236).
* A schema load is loud. The dump carries the target tables but not their
  definitions, so nothing maintains them, and defining again fails with a
  `conflict` error because the target exists.

**Detected?** A partial restore isn't detected by Trellis (`self_check` can tell on 1-1
targets only). A schema load is detected when you define again.

**Planned work:** #644 is decided: dump the definitions with the schema, the
way Rails dumps `schema_migrations`. It isn't built yet.

**Repair:**

* After a partial restore, `PAUSE`/`RESUME` every transform whose sources or
  targets were restored.
* For a schema-loaded database, drop the target tables and run the defining
  migrations ([embedding](embedding.md)).

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

**Planned work:** #528.

**Repair:** `PAUSE`/`RESUME` the aggregate.

## 16. `self_check` audits 1-1 targets only

**Trigger:** any divergence of an aggregate target or a relationship-enriched
1-1 target from what its definition computes, from any cause above.

**Effect:** `self_check` on an aggregate target returns `UnsupportedKeySpace`
before it audits anything. On a 1-1 target with a field read through a
relationship it audits capture, and reports a fault there, but the comparison
returns `UnsupportedExpr`. Neither kind of target is compared with a recompute,
so a stale value in one is silent.

**Detected?** Not applicable. Each entry's "Detected?" line says `self_check`
only for plain 1-1 targets.

**Planned work:** none filed. ADR-0013 names aggregates and relationships as
the next scope.

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

## 18. `jsonb_agg` element order

**Trigger:** an aggregate field `JSONB_AGG(...)`.

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
column.

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

## 20. A column paused and resumed on a 1-1 definition that reads a to-one relationship

**Trigger:** a 1-1 definition with a field read through a to-one relationship
(`author.name AS author_name`) and other fields of its own. One of those other
columns is paused and resumed (`PAUSE TRANSFORM posts.title`, then
`RESUME TRANSFORM posts.title`) while the from-side table (`posts`) and the
to-side table (`authors`) keep taking writes.

**Effect:** the relationship field of some rows ends wrong: `NULL` where the
parent has a value, a value where the parent is gone, or an older value of
the parent. The resumed column itself is right. This is silent, and a row
stays wrong until it's written again. The generative suite's steady-load tier
reproduces it in between 1 in 20 and 8 in 30 runs of its pinned cases, with
page and build chunk stalls widening the window, and not once in 30 runs of
a pinned case with the column pause and resume taken out.

**Detected?** No. `self_check` doesn't compare a 1-1 target with a field read
through a relationship (entry 16).

**Planned work:** #832. The first lead is #831, a suspected race in which a
resume commits while a parent change's propagation skips re-deriving the
resumed definition's rows, leaving them with the old parent value. Neither is
confirmed.

**Repair:** `PAUSE`/`RESUME` the whole transform, which rebuilds every row,
ideally while the to-side table is quiet: if #831's race is the cause, any
resume of the definition can hit it under concurrent writes. Check the
relationship field against your own query of the source afterwards.

## 21. A to-one relationship field keeps a superseded parent value under concurrent writes

**Trigger:** a 1-1 definition with a field read through a to-one relationship
(`author.name AS author_name`), and concurrent writes to both tables: child
rows inserted, deleted and inserted again while their parent's value changes
several times. No pause, resume, build or other action is involved. It has
been seen once, in about 135 runs of one generated case under the
steady-load tier's page and build chunk stalls.

**Effect:** every child of one parent kept a value the parent held only
briefly, after two later updates replaced it (61, then 17, then `NULL`; the
children kept 61). This is silent, and a child stays wrong until it's written
again.

**Detected?** No. `self_check` doesn't compare a 1-1 target with a field read
through a relationship (entry 16).

**Planned work:** #838. The cause is unknown. It isn't the shape of #763's
to-one projection ordering holes (an orphaned or missing projection key), and
it needs no resume, unlike entry 20. Milestone E (#624) replaces the
relationship projection a 1-1 target reads its to-one values from.

**Repair:** `PAUSE`/`RESUME` the transform.

## Repair caveats

* **A resume that re-types copies holds `ACCESS EXCLUSIVE` on them.** After a
  widening, the resume's `ALTER … TYPE` on the target (and its ledger, or the
  relationship's projection) waits for every reader of the table, and readers
  queue behind it. `integer` to `bigint` rewrites the table, so at a billion
  rows reads of the target block for minutes, and the capture pass waits with
  it. The `RESUME` itself returns at once; the definition stays `paused`, its
  `capture_failure` saying it's resuming, until the staging worker has
  re-typed the copies and started the rebuild. A rebuild follows every
  widening of a copied column.
* **Quarantined keys stay held until a resume or a drop (#759).** A key
  that's quarantined after repeated apply failures is held for the definition
  whose apply failed, with its parked work and its poison row, and that
  definition's target row for it stays stale. Every other definition reading
  the same source keeps applying the key, so two definitions can disagree on
  it. Entry 4 can cause such failures, and so can a widening of a copied
  column whose oversized value drains before the capture pass pauses the
  definition. There's no supported per-key
  release in production: `release_key` is test-only until #759. What clears a
  held key:
  * `RESUME TRANSFORM` deletes every key the resumed definition holds, and
    only its own, and its rebuild re-derives them from the source. A key
    whose cause is still there fails again and is quarantined again.
  * `DROP TRANSFORM` deletes the definition's held keys with it, so defining
    it again starts with none.
* **Resuming a field that's awaiting capture (#687).** A `RESUME` of a field
  whose status shows `capture_wait` unpauses it before its capture is widened,
  so its rows fail with `MissingColumn`. Let the wait finish first.
* **The bindings don't show pause reasons yet (#687).** Ruby, Elixir and
  embedded status show `paused` but not `capture_failure` or `capture_wait`.
  Read `Trellis::status` from Rust, or the logs, for the reason.

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
* **A key, join or GROUP BY column retyped or re-collated, or a column
  Trellis keeps a typed copy of widened.** The staging worker's capture pass
  pauses the definitions concerned, with a `capture_failure` naming each
  column, its old and new type, and what to do, when define would now refuse
  the column or a relationship's join columns no longer match, when the
  change renders the stored keys differently or rounds them (`timestamp` to
  `timestamptz`, `text` to `uuid`, a narrower `numeric` scale or
  `varchar(n)`), and when a 1-1 target's key or passthrough, a GROUP BY key
  (including a relationship projection's column for one read through the
  relationship) or a projection's key can no longer hold its source's values
  (`int` to `bigint`). A resume refuses until define would accept the definition again;
  otherwise it re-types the copies and rebuilds
  ([transforms — Supported sources and targets](transforms.md#supported-sources-and-targets)).
  A value a copy can't hold that drains before the pass is quarantined, and
  the resume releases it. A change between deterministic collations needs
  nothing (but see entry 3).
* **A source's primary key dropped, or retyped off the supported types.** The
  drain pauses every definition it reaches, and what is downstream of them,
  with a `capture_failure` naming the cause, and the rest of the page commits.
  Resume once the key is right, which rebuilds them.
* **Row-level security on the ring owner or defining role, and a
  logical-replication subscription into a source.** These are refused at define,
  paused by the pass, and reported by `self_check`
  ([transforms — Supported sources and targets](transforms.md#supported-sources-and-targets)).
  Capture's window before the pass sees row-level security is entry 11.
* **A capture function's privilege revoked, or the function made
  `SECURITY INVOKER`.** This is loud rather than silent: every write to the
  captured table fails, naming the capture function, so no change is lost.
  `self_check` names the cause on 1-1 targets.
* **Session settings** (`DateStyle`, `TimeZone`, `extra_float_digits`, …).
  Trellis pins them on its connections and in its capture functions. Its
  connections also run with `row_security = off`, so a read or write that
  row-level security would filter fails instead of silently missing rows
  (entry 11).
* **A whole-database backup and restore, or PITR.** Sources and Trellis state
  roll back together.
