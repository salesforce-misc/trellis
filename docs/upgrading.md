# Upgrading and rolling back

How to move a Trellis instance to another release, and back. Trellis's state
(its catalog, the staging ring, the hidden tables beside each target) lives in
your database, so a release changes that state.
[Version handover](decisions/0023-version-handover.md) keeps two releases from
running side by side, and
[ADR-0024](decisions/0024-compatibility-floors-and-rollback.md) records the
rollback design.

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
   `migrate` (`bin/rails trellis:migrate`, or an Elixir release task that
   calls `Trellis.migrate!`) in a deploy step after the old processes are
   gone: while one is live and a migration is pending, `migrate` refuses with
   the `version_handover` code and the new release's background processes
   wait for it. With no migration pending, `migrate` beside the old release
   changes nothing: it succeeds, or refuses with the same code when it would
   have to pause a definition.

While the new release stands by, `status()` carries `version_handover`, naming
this binary's version and each live process of the old release by worker id
([observability](observability.md#another-version-is-live)); `trellis status`
prints it as a `Standing by:` line above the definitions, and `trellis run`
prints `trellis standing by: …`. The definitions keep the statuses the old
release gives them.

After the takeover, a definition the old release accepted and the new one
refuses pauses, with a `capture_failure` naming the refusal and the remedy:
`DROP TRANSFORM`, then define it again in the current grammar. Each release's
notes list these cases. Every other definition carries on.

For an embedded fleet, [Rolling deploys](embedding.md#rolling-deploys) covers
the handover in detail, and
[Migrations and transactions](embedding.md#migrations-and-transactions) the
order of migrations and defines.

## Rolling back

An application defines its transforms in its own migrations, and rolling its
code back doesn't roll those back: the definitions the newer release made, and
the catalog state it wrote, stay in the database, and the rolled-back
application's Trellis meets them.

Rolling a define migration back doesn't undo an upgrade either. The
`Trellis::Migration` and `Trellis.Migration` helpers can't reverse a statement
by themselves, so a migration written with them undoes its define in a `down`
that runs `PAUSE TRANSFORM` and `DROP TRANSFORM`: `bin/rails db:rollback` or
`mix ecto.rollback` of it drops the transform and its target table with the
target's data, and defining it again builds the target from empty.

### The catalog floor

The catalog floor is the oldest Trellis release that can read the instance's
catalog. 0.2.0 records it at 0.2.0, and a later release raises it only when it
changes the catalog in a way an earlier release can't read; its release notes
say so. Read it with:

```sql
select version from trellis.catalog_floor;  -- the instance's catalog schema
```

Every handle checks it, not only one that runs `migrate`: `migrate` before
it runs anything, and a handle that only connects (a worker the application
starts at boot, while a deploy step runs `migrate`) before it starts any
background work and before each state-changing call. A release below the
floor changes nothing:

- `migrate` and the state-changing calls (the same calls as in
  [Upgrading](#upgrading)) refuse with the `version_floor` error code, naming
  the floor and its own version:

  ```
  refusing to attach: the catalog floor is 0.3.0, the oldest Trellis version that can read this catalog, and this binary is version 0.2.0; run Trellis 0.3.0 or later
  ```

- Its background processes connect and stand by: they stage, drain and
  build nothing, register nowhere, and check again every 5 seconds, so an
  orchestrator doesn't restart them in a loop. `trellis run` stands by the
  same way, printing `trellis standing by: …`.
- `status()` and the listings keep working. `status()` carries
  `below_catalog_floor`, naming the floor and this binary's version
  ([observability](observability.md#below-the-catalog-floor)), and
  `trellis status` prints it as a `Standing by:` line.

Roll forward to the floor or later. Nothing changed, and the newer release
attaches as before.

A release at or above the floor attaches to a catalog a later release
migrated. It runs none of the later release's migrations and none of its own
again, and logs once which migrations it doesn't carry. It still refuses
(with the `internal` code) a catalog whose history doesn't fit its own
migrations: one it carries that ran with another name or checksum, or a gap
below its newest migration. A handle that connects without migrating
doesn't check the history: `migrate` does.

### Reading the floors

Each definition records its **floor**: the oldest Trellis release that can
run it, the latest release any construct, key space, option or hidden-table
layout it uses arrived in. `status()` reports it as `floor`
(`"major.minor.patch"`, in the Elixir and Ruby bindings too), and
`trellis status` prints it on each definition's line as `floor=`. Trellis
records it when a definition is defined, altered or rebuilt by a resume;
every definition that exists when floors are introduced has floor 0.2.0.

Before rolling back to an earlier release, read the floors under the newer
release, and drop each definition whose floor is above the release you roll
back to there. The release notes name the features each release introduces
with a new floor
([ADR-0024](decisions/0024-compatibility-floors-and-rollback.md#rolling-back)).

### To a release before 0.2.0

Restore a backup of the whole database taken before the upgrade. The restore
also discards every write your application made since the upgrade, not only
Trellis's.

Without it, the older release's `migrate` refuses the database: 0.2.0 marks
the catalog with an instance format version the older release doesn't know
(`refusing to attach: schema "trellis" was written by a newer Trellis
(instance format version 2)`). `trellis run`, `trellis apply`,
`bin/rails trellis:migrate`, the `Trellis::Migration` and `Trellis.Migration`
helpers and a release task that calls `Trellis.migrate!` all migrate first, so
each one stops with that error. A handle that connects without migrating, such
as a worker the application starts at boot, checks nothing and runs on the
newer database.
A rollback deploy's `bin/rails db:migrate` or `mix ecto.migrate` doesn't stop
it: every migration of the rolled-back application has already run, so no
helper runs. Keep an explicit Trellis `migrate` step in your deploy, as in
[Upgrading](#upgrading), so that a rollback deploy stops at it, and restore the
backup before starting the older release.
