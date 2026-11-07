---
status: accepted
date: 2026-11-06
deciders: Michael Ries
---

# Source Keys

Trellis keys every table it reads by a key: capture writes each changed row
into the ring under it, a drain re-reads rows by it, and a 1-1 target carries
its columns as its own primary key. Until now the key was the table's primary
key or, for a table without one, the oldest unique index that could stand in
for one. That fallback was added so a transform could chain off an aggregate
target, whose identity is a `UNIQUE NULLS NOT DISTINCT` group key. It was
never decided for user tables, and it is a guess: which index is oldest
changes under `REINDEX CONCURRENTLY`, a dump restore creates constraints in
name order, and a user may add a unique index to any table at any time.

This ADR settles how a definition's key for each table it reads is chosen,
validated and kept. The principle: a strict rule with no ambiguity and no
guessing, while `RESUME` stays available whenever the table still meets what a
new definition would require, so an operator never has to drop and define a
chain of transforms again when resuming is safe.

## Every source has a key

A user table is keyed by its primary key, unless the definition names another
key with `KEY (<column>, …)` directly after the table it names:

```text
TRANSFORM t FROM orders KEY (customer_id, id) SELECT …
TRANSFORM t FROM a JOIN b KEY (b_id, n) ON a.f = b.b_id GROUP BY a.g SELECT …
```

Every transform kind that reads a table takes it, on its `FROM` table and on a
`JOIN`'s joined table. A table with neither a primary key nor a `KEY` is
refused at define, and the refusal names both remedies: `KEY (…)`, or
`ALTER TABLE … ADD PRIMARY KEY`. Trellis never keys a user table by a unique
index it picked itself. Neither a primary key nor a `KEY` may be deferrable: a
deferrable key can hold duplicates inside a transaction, which writes keyed by
it can't rely on.

`KEY` is part of Trellis's statement language, like `GROUP BY` and `ADOPT
EXISTING TABLE`, not of the expression grammar, so
[ADR-0022](0022-grammar-mirrors-postgres.md) doesn't govern it. Postgres
doesn't reserve the word, so `KEY` not followed by `(` is still an alias. The
canonical rendering writes it back, so `ALTER TRANSFORM` and a schema dump
keep it. A definition using it carries the floor of the release it arrived in
([ADR-0024](0024-compatibility-floors-and-rollback.md)).

## A `KEY` is backed by a unique index, matched by shape

A `KEY` must be backed by a unique index whose key columns are exactly the
named columns, in any order (`INCLUDE` columns are not key columns),
that is valid, immediate, not partial and over no expression, with each
column under its type's default operator class and the column's own
collation, and every column `NOT NULL` or the index `NULLS NOT DISTINCT`. An
index under another operator class or collation could compare keys more
finely than Trellis does and admit two rows with one key. Its column types go
through the key-type allowlist a primary key's do.

Any equivalent index backs it: the match is by shape, never by an index's oid
or name. The user may build a replacement and drop the old index at any time;
the key only has to stay enforced unique while it is used. The key's column
order is the order written, whatever the order of the index backing it, so a
replacement in another column order changes nothing a transform stores.

## Another transform's target takes its key from Trellis

A table that is another transform's target is keyed by the key Trellis gave
it: an aggregate's or `JOIN`'s `UNIQUE NULLS NOT DISTINCT` group key, from the
definition's group columns and never from scanning its indexes, or a 1-1
target's primary key. A `KEY` on such a table is refused. A user's unique
index or constraint on a target never becomes its key.

## One key per table, explicit at define

Capture keys a table one way for all its readers, whether they read it as a
`FROM` or a `JOIN` side. Define refuses a key, explicit or default, that
differs from the key the table's existing definitions use, and the refusal
names the key to write. A key is never inherited from another definition: a
second transform over a `KEY` table repeats its `KEY`.

## Keeping the key

Trellis records the key of each table the first time a definition keys by it,
tied to the relation it was taken from, and keys the table by the index over
exactly those columns for every definition that reads or writes it.

Any change to the key a transform uses pauses it with a message: a default
reader's primary key redefined, or the table created again with another one.
A change to a key the transform does not use pauses nothing: a primary key
added, redefined or dropped on a table whose readers use `KEY`.

A `KEY`'s backing index going missing pauses every reader of the table, with
no grace. That detector is built only once a false-positive study of the
key-rule pauses against routine maintenance and migrations is posted and
decided.

`RESUME` re-validates everything define validates, against the table as it is
now, and if that passes, re-records the key and rebuilds. An aggregate or
`JOIN` reader of a default-keyed table takes its primary key now; a 1-1
transform is amended to carry the new key. A `KEY` definition's
columns never change: its `RESUME` needs an index backing them to exist again.

## Definitions an earlier release kept over a table with no primary key

At attach, a released definition over a user table with no primary key gets
the index the earlier release's rule chose for it (its oldest qualifying
unique index, excluding expression indexes, key columns only) written into its
stored definition as an explicit `KEY`, when that index backs a key under the
rule above, and the key is recorded and the definition keeps running, with no
pause. Otherwise it pauses with a `capture_failure` naming the table and the
remedies: add a primary key and `RESUME`, or `DROP TRANSFORM` and define it
again with a valid `KEY`.

## Rejected

- **Keeping a unique-index fallback for user tables**, ranked by age, by
  constraint, or refused when ambiguous: each keys a table by a guess that
  routine maintenance can change.
- **Coupling the key to `REPLICA IDENTITY USING INDEX`**: replica identity
  has its own uses, the WAL contents other replication consumers read.
- **Always requiring `DROP TRANSFORM` on a key change**: it drops the target
  with the user's indexes, grants and views, and every transform chained off
  it, for no correctness gain on aggregates and `JOIN`s.

## Consequences

- A user table without a primary key needs `KEY (…)` (or `ADD PRIMARY KEY
  USING INDEX`), and a nullable key works only through `KEY` on a
  `NULLS NOT DISTINCT` index.
- Another instance's aggregate target, which has no primary key, can be read
  as a source when the definition names its group key with `KEY`.
- A second transform over a `KEY` table must repeat its `KEY`.
- Every key change costs a pause, a `RESUME` and a full rebuild of the
  readers.
- `docs/transforms.md` "Supported sources and targets" holds the refusals,
  what happens after define, and the remedies.
