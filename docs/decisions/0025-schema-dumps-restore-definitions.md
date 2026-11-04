---
status: accepted
date: 2026-11-03
deciders: Michael Ries
---

# Schema Dumps Restore Definitions

This ADR supersedes decision 5 of [ADR-0010](0010-embeddable-clients.md),
which dumped Trellis's schema and appended its catalog rows.

A Rails or Phoenix application checks its schema dump into the repository and
builds every test database from it. A transform is a row in Trellis's own
tables, which a schema-only `pg_dump` doesn't write, so the dump has to carry
the definitions some other way. Appending the catalog rows to a dump of
Trellis's schema works, but the file then holds every internal table, capture
function and trigger. I measured it at 100 definitions: about 95% of the
file's bytes were Trellis internals. The file changed whenever the catalog
layout did, it differed between two databases with the same definitions and
different histories, and it could only be loaded by a release with the same
catalog layout.

This ADR settles what an embedded client's schema dump carries and how a
database loaded from it gets its transforms. A dump with data, or a physical
backup, carries the Trellis schema as before and is out of scope.

## A dump carries the application's schema, the targets and one row per definition

`pg_dump` runs over the application's schemas and the target schema, and
leaves out the Trellis schema and each 1-1 target's ledger by exact name. It
can't leave out one trigger, so the capture triggers on source tables are
taken out of its output by one engine function both clients call, tested
against `pg_dump` 15 through 18. A missed trigger fails the load loudly,
because the function it runs isn't in the file.

After the dump comes an appendix that creates a restore queue in the Trellis
schema and inserts one row per definition, one row per line. The rows are
ordered so each reader comes after its upstream, and otherwise in the order
the definitions were made. A row records the qualified source, target and
`JOIN` table, the text as written, its `grammar_version`
([ADR-0018](0018-grammar-versioning.md)) and its floor
([ADR-0024](0024-compatibility-floors-and-rollback.md)). It records no ids,
OIDs, timestamps or runtime state. The same definitions give the same file,
whatever their history, and regenerating it changes only the lines of
definitions that changed.

For example, with `TRANSFORM order_totals FROM orders GROUP BY customer_id
SELECT customer_id AS customer_id, SUM(total) AS total` defined, the file
has `CREATE TABLE public.order_totals`, its unique key, and this line:

```sql
insert into "trellis".restore_queue values (1, 'public.orders', 'public.order_totals', null, 'TRANSFORM order_totals FROM orders GROUP BY customer_id SELECT customer_id AS customer_id, SUM(total) AS total', 1, '0.2.0');
```

## The queue's shape is permanent

A checked-in dump outlives the release that wrote it. Every later release
reads the queue's columns as they are, and refuses a queue of any other shape.
Anything a later release needs to say about a definition goes into its floor,
so an older release never ignores a field it doesn't know (ADR-0024).

## `migrate` restores the queue

`migrate` runs Trellis's migrations, then restores each queued row in order
and drops the queue. Each row is restored in its own transaction, which
deletes the row, so a refusal leaves the rest queued for the next `migrate`.
It runs under `migrate`'s locks, so [ADR-0023](0023-version-handover.md)'s
refusal beside a live process of another version applies to it.

- A row the loading release can run is defined at its recorded grammar
  version, into the target the dump created, through the opt-in define into
  an existing empty target. The text is resolved first and must name the
  recorded source, target and `JOIN` table. A changed `target_schema`
  setting would resolve a bare name elsewhere, and that refuses rather than
  define a transform of other tables. Defining stamps the floor again.
- A row the loading release can't run is stored paused, without being
  parsed: its floor is above the release, its grammar version is outside the
  release's window, the release refuses its text, or it reads from such a
  row. It gets its catalog row, its source fences and its graph edges from
  the recorded identities, and nothing a parse would decide.
- Every definition loads unbuilt and unpaused, as a define leaves it. Pause
  intent is runtime state and is not carried.

The text is stored as recorded, not rewritten into the current grammar. A
rewrite could raise the definition's floor and stop the loaded database from
rolling back to a release the original text supports.

Rebuilding the internal state at load is why a dump stays loadable by every
release that meets each definition's floor, older or newer: the loading
release builds its own catalog, triggers and ledgers.

## The clients' hooks run the restore

Rails runs `migrate` after `db:schema:load`, `db:prepare` and
`maintain_test_schema!` load `structure.sql`. Ecto gets a `mix trellis.load`
task, aliased to `ecto.load`, that runs `ecto.load` and then `migrate`.
`mix ecto.dump` can't pass `pg_dump` extra arguments, so `mix trellis.dump`
runs `pg_dump` itself with `ecto.dump`'s arguments plus Trellis's. A file
loaded another way, `psql -f`, is followed by `migrate`. An application that
builds its own dump calls the same engine functions the hooks call.

Both hooks read the definitions before and after the `pg_dump`, which reads
its own snapshot, and redo the dump when the two reads differ.

## A dump in the older format still loads

A file written before this decision carries the Trellis schema and its
catalog rows, loads as plain SQL, and finds no queue to restore. Both load
paths stay until a major release.

## Consequences

- Loading an embedded dump is no longer plain SQL: every load path needs
  `migrate` after it. The Rails and Ecto hooks run it; any other path has to.
- The clients filter `pg_dump`'s text output. The filter is confined to this
  path and is tested against each supported `pg_dump` version.
- Two load formats, and two dump functions, until a major release.
- A row stored paused because its release couldn't run it has no ledger, so
  a 1-1 definition restored that way can't be resumed by a later release
  that meets its floor; it is dropped and defined again.
- A dumped target the opt-in define won't adopt refuses the load, naming the
  transform and the reason.
- Rejected alternatives: keeping the internals in the dump and deriving the
  rest at attach keeps the bytes and the layout coupling; a declarative
  definitions file with a reconcile step adds a second source of truth; an
  extension needs superuser.
