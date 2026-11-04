---
status: accepted
date: 2026-11-02
deciders: Michael Ries
---

# Compatibility Floors and Rollback

An application embeds Trellis through its Ruby gem or Elixir package, whose
version is the engine's. A release of the application bumps Trellis; a few
hours later the team finds a serious bug and rolls the application back. The
database stays as the newer release left it: its catalog state, and the
definitions it made, since the application defines transforms in its own
migrations and a code rollback runs no migration's `down`. Restoring a backup
undoes that, but Trellis's catalog lives in the application's database, so the
restore also discards every write the application made since.

Run on real binaries, with v0.1.0 pointed at a database a later build wrote:

- A definition using syntax the older grammar lacks stops its `trellis run` at
  startup. Every target goes stale while `status` reports `live`, and pausing
  that definition doesn't help, because a paused definition is still parsed.
- Text the older grammar reads differently is misread: a grammar without the
  bare `NULL` literal reads `COALESCE(amount, NULL)` as a read of a column named
  `"NULL"`, and pauses the definition with the wrong reason.
- Once the newer release has dropped the definitions only it can run, the
  older one runs the rest correctly, and rolling forward and resuming brings
  everything back.

Nothing records which release can read what. Refinery refuses a database
carrying any migration the binary lacks, but only in `migrate`; a newer
`grammar_version` refuses the whole attach
([ADR-0018](0018-grammar-versioning.md)); hidden tables carry no layout
version; and some enum-like stored values fall through to a default when a
binary doesn't know them.

This ADR settles which earlier release a database can be rolled back to, and
what that release does with the state a newer one left. There is no downgrade
command: the older release degrades per definition by itself.

## The rules

1. **Two floors, both Trellis release versions compared as semver.**
   - The **catalog floor** is the oldest release that can read the catalog's
     instance-wide state: its tables, the ring's format, instance-scoped
     values, and the shape of the capture objects on source tables (which
     triggers and functions exist, where, and what they record). Each
     migration and each new instance-wide format declares the oldest release
     that can read the catalog after it, and the catalog floor is the highest
     of them. One an older release can't read raises it. A change to the
     shape of capture objects always raises it: those objects belong to a
     source table, not to one definition, so every definition reading the
     table meets them, including the ones an older release can run.
   - The catalog floor is the one row of the catalog's `catalog_floor` table.
     The migration that creates it seeds it at 0.2.0. A migration that raises
     it does so in its own SQL, to the greater of the stored floor and its
     own, so the floor and the applied history never disagree. It replaces the
     marker's `instance_format_version`, which no release from 0.2.0 on reads
     or writes; it stays at 2, which every release before 0.2.0 refuses.
   - A build carries its release's migrations, so it meets every floor they
     declare, whatever its version string says: a pre-release, or a
     development build that still carries the previous release's version,
     reads the catalog its own migrations leave. On a catalog that has run a
     migration the build lacks, its own version decides: a later build of the
     same release leaves the same floor, so the floor can't say whether this
     one reads that migration.
   - A **definition floor** is the oldest release that can run one
     definition: its grammar features, its key space, its hidden-table layout
     and its per-definition state. Trellis stamps it whenever it writes the
     definition or recreates its hidden tables, from what the definition uses
     then.
2. **Attach.** A binary below the catalog floor refuses, with an error naming
   the floor and its own version. Every handle runs the check, a define-only
   one included, not only `migrate` ([ADR-0010](0010-embeddable-clients.md)):
   before it starts background work, which stands by below the floor and
   checks again every standby poll, and before each state-changing call,
   which refuses. Read-only calls keep working, and `status` names the
   floor. A binary at or above the floor
   tolerates applied migrations it doesn't carry, runs none of them, and runs
   none of its own again: it classifies the applied history before refinery
   runs, and refinery's blanket `abort_missing` is off. A name or checksum
   mismatch on a version both carry, or a gap below the binary's newest
   migration (one it lacks, or one of its own not applied below the newest
   applied), still refuses. The refusal below the floor has its own error
   code, `version_floor`.
3. **Per definition.** A definition whose floor is above the running version
   is paused with a `capture_failure` reason that names the definition's floor
   and the running version, and gives the remedy: roll forward to the floor
   or later, then `RESUME` it, or `DROP` it there. The binary never parses
   such a definition. No path that loads definitions errors on it, so it never
   blocks other definitions or the process. It reads nothing while paused, so
   it adds no columns to its tables' capture. A stored `grammar_version` newer than the
   binary reads pauses that one definition the same way instead of refusing
   the attach ([ADR-0018](0018-grammar-versioning.md)).
4. **Roll forward needs `RESUME`.** When a release that meets the floor runs
   again, the definition stays paused until an operator resumes it, as with
   every other pause ([ADR-0014](0014-pause-and-drop-a-transform.md)). The
   resume rebuilds it.
5. **`DROP` refuses a definition above the running version,** naming the
   definition's floor and the running version, and changes nothing. Only a
   release that meets the floor knows everything the definition owns, so only
   it can drop the definition completely; a partial drop could leave state
   behind that a later definition of the same name picks up. The remedy is to
   roll forward and drop it there. A definition at or below the running
   version drops as usual.
6. **Never default an unknown stored value.** An enum-like value a binary
   doesn't know pauses the definition it belongs to, or refuses the attach for
   instance-wide state. It never falls through to a default and never panics.
   Writing a new value raises the matching floor.
7. **No standing release promise.** The floors recorded in the database decide
   which releases can attach and what each can run. Each release's notes state
   whether it raised the catalog floor or introduced features with a new
   definition floor, and what that means for rolling back to the previous
   release.
8. **0.2.0 is the first release that can be rolled back to.** The check has to
   live in the older binary, and no release before 0.2.0 carries it. Rolling
   back to one means restoring a backup taken before the upgrade, and the 0.2.0
   release notes say so. A release before 0.2.0 refuses, in `migrate`, any
   database a later release has migrated; a handle of one that connects without
   migrating checks nothing.

## Rolling back

Before rolling back to an earlier release, the operator reads the floors from
`status()` and, under the newer release, drops each definition whose floor is
above the release they are rolling back to. The older release then attaches
with nothing to pause. An operator who skips this step loses nothing to
silence: the older release pauses those definitions, refuses to drop them, and
names the floor in both places.

Dropping a definition removes its target and its data, and rolling forward
again doesn't restore it: the application's migration that defined it has
already run, so it is defined again by hand, or by rolling that migration back
and forward. A definition the older application itself defined can have a
floor above it when the newer release rewrote it (for example by recreating
its hidden tables), and dropping it removes a table the older application
still reads. Pausing it would leave that table stale instead. So a release
raises an existing definition's floor only when it must.

A rollback is a cross-version deploy, so [ADR-0023](0023-version-handover.md)
applies to it: the older release stands by while a process of the newer one is
live, and attaches, pauses what it can't run and takes over once that process
has stopped.

## Precedents

- **Flyway** ignores applied migrations newer than the binary by default
  (`*:future`), the opposite of refinery's `abort_missing`.
- **Oban, GoodJob and Que,** job queues whose tables live in the
  application's database, run older versions on a newer schema by keeping
  schema changes additive within a line. GoodJob keeps the migrations of a
  minor release optional until the next major and detects at runtime which
  columns exist.
- **SQLite** lets an older library open a database a newer one wrote, as long
  as the database uses none of the newer features: a floor per feature in use,
  which is what a definition floor is.

## Rejected

* **Restore a backup as the only way back** (Postgres, Elasticsearch, Vault).
  Postgres has never supported running an older server on a cluster a newer
  one has modified; after a major upgrade the way back is an untouched old
  cluster or a restored backup. That costs the developers nothing and the
  operator everything: the restore also discards the application's own writes.
  It fits a database that is the product, not a catalog embedded in the
  application's database, and remains the answer only for releases before
  0.2.0.
* **An explicit downgrade command or reverse migrations** (etcd's
  `downgrade enable`, `ALTER EXTENSION UPDATE TO` with downgrade scripts,
  which TimescaleDB ships for one step back and notes may not work once the
  database has changed since the upgrade). Each release needs a reverse path,
  which gets lossy once newer data exists, and a team reverting a deploy
  mid-incident won't first run a step from the release it is abandoning.
* **A finalize step** (MongoDB's feature compatibility version, CockroachDB's
  finalize, Kafka's `metadata.version`), which raises a recorded floor only
  once the new version has burned in, so the previous version can still run
  until then. An embedded deploy has no place for it: the Trellis version
  moves with the application's own deploy. Floors raised automatically need no
  step.
* **A `grammar_version` bump for every additive change,** reusing ADR-0018's
  machinery instead of a definition floor. A bump ships only in a breaking
  release and brings a compatibility rule, and a grammar version covers only
  text, not a key space, a hidden-table layout or per-definition state.
* **An older binary rebuilding a layout it doesn't know** from the source. It
  conflicts with pausing with a reason, and isn't needed once the definition
  pauses cleanly.
* **A read-only rollback-check tool** (with a variant that prepares the
  database by dropping the newer-only definitions). `status()` shows the
  floors, and a definition above the running version pauses with its remedy.
* **`DROP` of a definition above the running version, from a record of its
  hidden objects.** The older release would remove what the record lists
  without understanding the layout. Each release that changes what a
  definition owns, or how its capture is shared with other definitions, would
  have to keep that record complete for every older release, and a gap would
  show up only as a drop that missed something. Dropping before the rollback,
  under the release that made the definition, needs no such promise.
* **A 0.1.x backport.** It would carry the per-definition pause, the floor
  check, tolerance of migrations it doesn't carry and the version handover, and
  every 0.1.x installation would have to take it before upgrading.
* **A standing window,** such as rollback to the latest patch of the previous
  minor. The floors already decide which releases can attach; the release notes
  report what each release raised.

## Consequences

* Every migration, grammar feature, key space, hidden-table layout and stored
  enum-like value declares its floor, and CI checks the declarations.
* Keeping the floor down costs compatibility code: an additive migration adds
  nullable or defaulted columns and tables an older release ignores. An older
  release at or above the floor keeps writing rows without them, so the
  release that adds a column reads a `NULL` there (or the default) as a row an
  older release wrote, and handles it.
* A definition resumed after a rollback is fully rebuilt.
* A definition the older release can't run is either dropped before the
  rollback, losing its target, or left paused through it, its target going
  stale, until the application rolls forward.
* The release that introduces a feature decides the floor of every definition
  that uses it, so a rolled-back application keeps every definition the older
  release can run, and only the others pause.
