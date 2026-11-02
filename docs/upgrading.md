# Upgrading and rolling back

How to move a Trellis instance to another release, and back. Trellis's state
(its catalog, the staging ring, the hidden tables beside each target) lives in
your database, so a release change is a change to that state as well as to the
binary. [Version handover](decisions/0023-version-handover.md) keeps two
releases from running side by side, and
[ADR-0024](decisions/0024-compatibility-floors-and-rollback.md) records the
design for rolling back.

## Upgrading

The same steps work for the `trellis` CLI and for an application that embeds
Trellis:

1. **Start the new release,** beside the old one or after it. While a process
   of the old release is live, every process of the new one stands by: it
   stages, drains and builds nothing, and checks again every 5 seconds. Its
   state-changing calls (`apply` with a define, `ALTER`, `PAUSE`, `RESUME` or
   `DROP`, `request_backfill`, `release_key`, `detect_capture_disruptions`,
   `self_check`) refuse with the `version_handover` error code. The old release
   runs on, and capture never stops: the triggers keep staging changes in your
   application's own transactions.
2. **Stop the old release.** The new one takes over at its next poll, or up to
   the 30-second reclaim TTL after a process killed without a clean shutdown.
3. **Run `migrate`** when the new release carries a migration. `trellis run`
   migrates by itself once the old release has stopped. An embedded fleet runs
   `migrate` (`bin/rails trellis:migrate`, or a release task that calls
   `Trellis.migrate!`) in a deploy step after the old processes are gone:
   `migrate` refuses with the `version_handover` code while one is live and a
   migration is pending, and the new release's background processes wait for
   it. With no migration pending, `migrate` beside the old release changes
   nothing and succeeds.

Meanwhile, `status()` carries `version_handover`, naming this binary's version
and each live process of the old release by worker id
([observability](observability.md#another-version-is-live)); `trellis status`
prints it as a `Standing by:` line above the definitions, and `trellis run`
prints `trellis standing by: …` while it waits. The definitions keep the
statuses the old release gives them.

After the takeover, a definition the old release accepted and the new one
refuses pauses, with a `capture_failure` naming the refusal and the remedy:
`DROP TRANSFORM`, then define it again in the current grammar. Each release's
notes list these cases. Every other definition carries on.

[Rolling deploys](embedding.md#rolling-deploys) covers the same handover for
an embedded fleet in detail, and the order of migrations and defines in
[Migrations and transactions](embedding.md#migrations-and-transactions).

## Rolling back

### To a release before 0.2.0

Rolling back to a release before 0.2.0 means restoring a backup of the whole
database taken before the upgrade. The restore also discards every write your
application made since the upgrade, not only Trellis's.

A release before 0.2.0 refuses, in `migrate`, a database 0.2.0 has migrated:
0.2.0 adds a migration it doesn't carry. `trellis run`, `trellis apply`,
`bin/rails trellis:migrate`, the `Trellis::Migration` and `Trellis.Migration`
helpers and a release task that calls `Trellis.migrate!` all migrate first, so
each one stops with that error. A handle of such a release that connects
without migrating, such as a worker the application starts at boot, checks
nothing and runs on the newer database
([known correctness gaps](known-correctness-gaps.md#a-release-before-020-started-on-a-database-a-later-release-migrated)).
Keep `migrate` in your deploy so that a rollback deploy stops at it, and
restore the backup before starting the older release.

## Embedded applications

An application defines its transforms in its own migrations
([Migrations and transactions](embedding.md#migrations-and-transactions)), and
its migration tooling records which ones have run. Rolling the application's
code back doesn't roll those migrations back: the definitions the newer release
made, and the catalog state it wrote, stay in the database, and the rolled-back
application's Trellis meets them.

Rolling a define migration back is not a way to undo an upgrade. A migration
written with `Trellis::Migration` or `Trellis.Migration` undoes its define in
`down` with `PAUSE TRANSFORM` and `DROP TRANSFORM`, so `bin/rails db:rollback`
or `mix ecto.rollback` of it drops the transform and its target table with
the target's data. Defining it again builds the target from empty.
