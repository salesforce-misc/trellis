---
status: accepted
date: 2026-11-02
deciders: Michael Ries
---

# Compatibility Floors and Rollback

An application embeds Trellis through its Ruby gem or Elixir package, whose
version is the engine's. A release of the application bumps Trellis; a few
hours later the team finds a serious bug and rolls the application back.
Rolling back the code doesn't roll back the database: the catalog state the
newer release wrote stays, and so do the definitions it made, since the
application defines transforms in its own migrations and a code rollback runs
no migration's `down`. The older release then meets all of it.

Postgres's answer to a downgrade, restoring a backup, isn't available here.
Trellis's catalog lives in the application's database, so a restore that undoes
a Trellis upgrade also discards every write the application made since.

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

Underneath, nothing records which release can read what. Refinery refuses a
database carrying any migration the binary lacks, but only in `migrate`; a
newer `grammar_version` refuses the whole attach
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
     values. Each migration and each new instance-wide format declares the
     oldest release that can read the catalog after it, and the catalog floor
     is the highest of them. One an older release can't read raises it.
   - A **definition floor** is the oldest release that can run one
     definition: its grammar features, its key space, its hidden-table layout
     and its per-definition state. Trellis stamps it whenever it writes the
     definition or recreates its hidden tables, from what the definition uses
     then.
2. **Attach.** A binary below the catalog floor refuses, with an error naming
   the floor and its own version. Every handle runs the check when it
   connects, a define-only one included, not only `migrate`
   ([ADR-0010](0010-embeddable-clients.md)). A binary at or above the floor
   tolerates applied migrations it doesn't carry: refinery's blanket
   `abort_missing` no longer decides this. A checksum mismatch, or a gap below
   the binary's newest migration, still refuses.
3. **Per definition.** A definition whose floor is above the running version
   is paused with a `capture_failure` reason that names the definition's floor
   and the running version, and gives the remedies: roll forward to the floor
   or later and `RESUME`, or `DROP` and define it again with features the
   running version supports. The binary never parses such a definition. No
   path that loads definitions errors on it, so it never blocks other
   definitions or the process. A stored `grammar_version` newer than the
   binary reads pauses that one definition the same way instead of refusing
   the attach ([ADR-0018](0018-grammar-versioning.md)).
4. **Roll forward needs `RESUME`.** When a release that meets the floor runs
   again, the definition stays paused until an operator resumes it, as with
   every other pause ([ADR-0014](0014-pause-and-drop-a-transform.md)). The
   resume rebuilds it.
5. **`DROP` works from any release at or above the catalog floor,** including
   on a definition above the running version. Trellis records each
   definition's hidden objects, and the drop removes the recorded objects
   without understanding their layout.
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
   migrating checks nothing
   ([known correctness gaps](../known-correctness-gaps.md)).

A rollback is a cross-version deploy, so [ADR-0023](0023-version-handover.md)
applies to it: the older release stands by while a process of the newer one is
live, and attaches, pauses what it can't run and takes over once that process
has stopped.

## Precedents

- **Postgres is forward-only.** Its minor releases are "always compatible with
  earlier and later minor releases"
  ([upgrading](https://www.postgresql.org/docs/current/upgrading.html)), a
  promise about the storage format, not about running an older server on a
  cluster a newer one has modified, which the project has never supported.
  After a major upgrade the way back is an untouched old cluster or a restored
  backup, and either loses the writes made since. That fits a database that is
  the product; it doesn't fit a catalog embedded in the application's database.
- **Postgres extensions** are the closest analogue: versioned objects with a
  catalog inside the user's database. `ALTER EXTENSION UPDATE TO` follows any
  script path, downgrade scripts included, and TimescaleDB ships them for one
  step back (patch versions and consecutive minors), noting they may not work
  once the database has changed since the upgrade.
- **Flyway** ignores applied migrations newer than the binary by default
  (`*:future`), the opposite of refinery's `abort_missing`.
- **Oban, GoodJob and Que,** job queues whose tables live in the
  application's database, run older versions on a newer schema by keeping
  schema changes additive within a line. GoodJob, for one, keeps the
  migrations of a minor release optional until the next major and detects at
  runtime which columns exist.
- **SQLite** lets an older library open a database a newer one wrote, as long
  as the database uses none of the newer features: a floor per feature in use,
  which is what a definition floor is.
- **MongoDB's feature compatibility version and CockroachDB's finalize** record
  a floor too, but raise it in a separate step once the new version has burned
  in: an operator's command in MongoDB, an automatic step in CockroachDB that
  the operator can hold back. Until then the previous version can still run.
  Rejected below.

## Rejected

* **Restore a backup as the only way back** (Postgres, Elasticsearch, Vault).
  It costs the developers nothing and the operator everything: the restore also
  discards the application's own writes. It remains the answer only for
  releases before 0.2.0.
* **An explicit downgrade command or reverse migrations** (etcd's
  `downgrade enable`, extension downgrade scripts, Timescale's one-step
  scripts). Each release needs a reverse path, which gets lossy once newer data
  exists, and a team reverting a deploy mid-incident won't first run a step
  from the release it is abandoning.
* **An operator finalize step** (MongoDB, CockroachDB, Kafka's
  `metadata.version`). An embedded deploy has no place for it: the Trellis
  version moves with the application's own deploy. Floors raised automatically
  need no step.
* **A `grammar_version` bump for every additive change,** reusing ADR-0018's
  machinery instead of a definition floor. A bump ships only in a breaking
  release and brings a compatibility rule, and a grammar version covers only
  text, not a key space, a hidden-table layout or per-definition state.
* **An older binary rebuilding a layout it doesn't know** from the source. It
  conflicts with pausing with a reason, and isn't needed once the definition
  pauses cleanly.
* **A read-only rollback-check tool** (with a variant that prepares the
  database by dropping the newer-only definitions). `status()` shows the
  floors, and a definition above the running version pauses with its remedies.
* **A 0.1.x backport.** It would carry the per-definition pause, the floor
  check, tolerance of migrations it doesn't carry and the version handover, and
  every 0.1.x installation would have to take it before upgrading.
* **A standing window,** such as rollback to the latest patch of the previous
  minor. The floors already decide which releases can attach; the release notes
  report what each release raised.

## Consequences

* Every migration, grammar feature, key space, hidden-table layout and stored
  enum-like value declares its floor, and CI checks the declarations
  ([CONTRIBUTING](../../CONTRIBUTING.md#compatibility-floors)).
* Keeping the floor down costs compatibility code: an additive migration adds
  nullable or defaulted columns and tables an older release ignores.
* A definition resumed after a rollback is fully rebuilt.
* The release that introduces a feature decides the floor of every definition
  that uses it, so a rolled-back application keeps every definition the older
  release can run, and only the others pause.
* Rolling back to a release before 0.2.0 stays a restore from backup.
