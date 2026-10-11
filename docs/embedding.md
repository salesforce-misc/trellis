# Embedding Trellis

How to run Trellis in-process inside a host app — a Rails or Phoenix
application that wants its `TRANSFORM`/`RELATIONSHIP` declarations to live
alongside its own migrations, rather than deploying Trellis as a separate
service. The design commitments behind this shape are recorded in
[ADR-0010](decisions/0010-embeddable-clients.md); this doc is the operator/
embedder-facing walkthrough of what they mean in practice.

## The recommended shape

A Trellis-embedding fleet typically runs two kinds of process against the
same database:

* **Web and migration processes** — define transforms, read `status()`, run
  migrations. These connect at `drain_threads: 0`: no staging worker, no
  drain workers. Cheap to run in every web dyno/pod without each one
  competing to drain the same work.
* **One dedicated worker process** — runs the live pipeline: capture
  install + ring maintenance (`staging: true`) and one or more drain workers
  (`drain_threads: N`) that actually apply staged changes into target
  tables.

### Who runs what

Every process connects with the same two options, spelled the same way in
all three APIs: Rust's `TrellisOptions { staging, drain_threads, .. }`,
Elixir's `Trellis.connect(url: ..., staging: ..., drain_threads: ...)` and
Ruby's `Trellis.connect(url: ..., staging: ..., drain_threads: ...)`. Both
default to off (`false`, `0`), so a handle connected with the defaults runs
nothing in the background.

| Process | `staging` | `drain_threads` |
|---|---|---|
| Migration run (`rails db:migrate`, `mix ecto.migrate`, a release task) | `false` | `0` |
| Web process, console, background job | `false` | `0` |
| The dedicated Trellis worker, exactly one per fleet and instance | `true` | `N` (at least 1) |
| Extra drain capacity, if the worker's threads can't keep up | `false` | `N` |

Four rules sit behind that table, and they hold for each instance separately:

* **Exactly one process sets `staging: true` for each instance.** The staging
  worker holds a lock for as long as it runs, and a second `staging: true`
  connect to the same instance fails with a `conflict` error while the first
  is alive. In a rolling deploy, stop the old worker before the new one
  connects, or retry the new one's connect until the old one has gone.
* **At least one process runs drain threads for each instance.** Drain threads
  take their work from a queue in the database, so any number of processes can
  run them; the dedicated worker's own are usually enough.
* **`migrate` runs before the worker connects.** The staging worker reads
  Trellis's own tables as it starts, so a `staging: true` connect to a
  schema `migrate` hasn't created yet fails with a `not_found` error.
* **Nothing errors when a rule is broken.** The transforms just never go
  `live` ([the silent-stall hazard](#the-silent-stall-hazard-issue-144), and
  the health checks that catch it).

In a Phoenix app, each process's handle is `{Trellis, options}` in its
supervision tree, with the options read from `config/runtime.exs`, so a
node's role is its configuration: the worker node sets `staging` and
`drain_threads`, the web nodes leave the defaults. The supervisor connects
the handle as the application starts and shuts it down as it stops, and
the rest of the app calls it by name (`clients/elixir/README.md`). Ecto
doesn't start the application to migrate, so a migration run gets a handle
of its own from `Trellis.Migration`, always with the defaults (see
[Migrations and transactions](#migrations-and-transactions)).

In a Rails app, `Trellis::Railtie` connects each process's handle as the
app boots, from `config.trellis.connect`, so a process's role is its
configuration: the worker process sets `staging` and `drain_threads`, the
web processes leave the defaults. Rake tasks, `rails db:migrate` included,
get no boot handle, the way Ecto migrates without starting the app: a
migration gets a handle of its own from `Trellis::Migration`, always with
the defaults (see [Migrations and transactions](#migrations-and-transactions)
and `clients/ruby/README.md`).

**Several instances.** A handle belongs to one instance: a catalog schema in a
database ([instance identity](instance-identity.md)). A process that uses
two instances, in different databases or in two schemas of one, connects a
handle for each, and the table and rules above apply to each instance on its
own: the dedicated worker process sets `staging` and `drain_threads` on every
handle it holds, and a web process leaves them off on all of them. In Elixir
each handle is its own `{Trellis, name: ..., schema: ...}` child. Connect the
handles once, at boot, never per request. One `Trellis.Metrics.render_prometheus/0`
route covers all of them ([observability](observability.md#several-instances-in-one-process)).

A third option, `worker_threads`, is unrelated to either: it caps the worker
threads of each Tokio runtime a handle owns, which is the runtime a
`BlockingTrellis` or binding handle services its calls on and the runtime of
the background client a `staging` or `drain_threads` handle starts. The
bindings default it to 2; Rust's `TrellisOptions` leaves it at one thread per
core unless you set it. The cap is per runtime and per handle, and no budget is
shared between handles: a handle that runs a client holds up to `2 * worker_threads`
worker threads, and a process with H such handles holds up to `2 * worker_threads * H`.
Size it for the handles you open. It caps worker threads only, not every
thread a handle starts: each runtime also has a thread driving it and a
blocking pool Tokio grows on demand (for DNS lookups, for one).

Connections follow the same rule. A handle's calls draw on a pool of up to 20
connections, and a handle that runs a client has a second pool of up to 20.
The client also opens connections outside both pools: the staging worker holds
one for as long as it runs, and each drain thread holds a `LISTEN` connection,
plus up to two more while it drains (one that keeps its claims alive, and one
while it pages through an oversized share). A process with several handles
holds the sum, so add them up against the server's `max_connections`.

### What the staging worker needs from the database

The staging worker captures each source table's changes with statement
triggers that write into Trellis's staging ring in the application's own
transaction ([stage 1](staging-and-claiming/01-capture-by-triggers.md)).
So:

* **No `wal_level = logical` and no `REPLICATION` role attribute.** Nothing
  reads the WAL.
* **Ownership of every source table**, or membership in the role that owns
  it. The worker installs the triggers with `ENABLE ALWAYS`, which only the
  owner may run.
* **One role runs the migrations and owns the ring and the capture
  functions.** The functions are `SECURITY DEFINER`, so application roles
  writing a source table need no privilege on Trellis's schema. A worker whose
  login role isn't the one that ran the migrations must be a member of it, and
  that role must keep the privileges the capture functions use, which it has
  as owner unless someone revokes them: `USAGE` on the schema, `INSERT` on the
  ring segments (`seg_0` to `seg_3`), `USAGE` and `UPDATE` on
  `staging_change_id_seq`, `USAGE` and `SELECT` on `ring_slot_mirror`, and
  `SELECT` on each captured table (the functions re-read it). Revoking one is
  loud: writes to the captured table fail, naming the capture function, and
  `self_check` reports it.
* **Tables Trellis can read.** Row-level security that applies to the Trellis
  role, a logical-replication subscription into a source, a source without a
  primary key, and a partitioned table or a table in a partition or
  inheritance hierarchy: each is refused at define time, and pauses the
  transform if it comes about later. The full list
  is [transforms — Supported sources and targets](transforms.md#supported-sources-and-targets).
  The same goes for each target table, which the workers write as their login
  role: a target belongs to the role that defined it, so run the workers as
  that role or a member of it, and don't force row-level security on it.
  Every Trellis connection runs with `row_security = off`, so whichever role
  a worker logs in as, a read or write the policies would filter fails rather
  than skipping rows.
* **Any default isolation level.** Trellis's connections default to
  `read committed`, whatever the server's, database's or role's
  `default_transaction_isolation`. Only the connections Trellis opens itself
  are set: your application's sessions keep their own level, and capture
  works at `read committed`, `repeatable read` and `serializable`.
* **Leave the capture triggers alone.** Each source table carries five
  triggers named after the instance schema (`trellis_capture_insert` and so on
  for the default). Don't disable or drop them. The worker reinstalls a
  missing or disabled one on its next reconcile pass, and `self_check` reports
  a broken capture as a `capture` divergence before it compares any rows
  ([known correctness gaps](known-correctness-gaps.md)).
* **A long transaction can delay the triggers' install.** Installing or
  widening a table's triggers needs a brief table lock, which the worker
  retries every pass while a long transaction or an autovacuum holds the
  table. Nothing cancels the lock holder. Meanwhile the transform stays
  `waiting_to_backfill` (or `catching_up`, after an `ALTER TRANSFORM` that reads
  a new source column), and `status()` reports what it waits on
  (`capture_wait`), or `capture_failure` when the install fails for another
  reason, such as a source that lost its primary key. Every process's
  `status()` reports both, wherever the worker runs.

```rust
// A web process: define transforms, never drains anything.
let trellis = Trellis::connect(Config::resolve(None)?, TrellisOptions::default()).await?;
trellis.apply("TRANSFORM widget_totals FROM widgets SELECT price + tax AS total").await?;
```

Defining a transform is cheap wherever it runs. `apply` validates the
definition, creates the target table, records the transform as
`waiting_to_backfill`, and returns. The target name must be free: `apply`
refuses a transform whose target already exists as any table or view. It
doesn't read the source table's rows or install its capture triggers, so it
takes the same time against an empty table as against a billion-row one. The
dedicated worker does the rest in the background: it installs capture triggers
on the source, reads its existing rows, builds the target, catches it up with
whatever changed while it was building, and flips the transform to `live`
([data-flow — Capturing a table's existing rows](data-flow.md#capturing-a-tables-existing-rows)).
So a web process needs no ownership of the source tables; only the worker
does. It does need to create tables: each target table in the target
schema, and, for a to-one relationship, the relationship's projection in the
instance schema, where Trellis keeps its own state. A transform that reads
a parent column the projection doesn't carry yet adds it there, so the process
that applies it must own the projection too
([data-flow — What it asks of a deployment](data-flow.md#what-it-asks-of-a-deployment)).
The role that applies a transform owns the target table it creates, along
with the tables Trellis keeps beside it, and Trellis grants nothing on them.
The worker writes them, so it must log in as that role or as a member that
inherits it; a worker logging in as an unrelated role fails every write to the
target with a permission error.
Code that needs the target populated polls `status()` until the transform is
`live` ([Poll to `live`, don't wait](#poll-to-live-dont-wait)).

Dropping a transform (`PAUSE`, then `DROP`; `DROP` refuses one that isn't
paused) removes the definition and its target table with its data. The worker
uninstalls the source's capture triggers on its next reconcile pass once
nothing reads it. The worker doesn't need any transforms registered before it
starts; it picks up each one on the pass after `apply` registers it.

```rust
// The one dedicated worker process for this fleet.
let worker = Trellis::connect(
    Config::resolve(None)?,
    TrellisOptions { staging: true, drain_threads: 4, ..Default::default() },
)
.await?;
```

## Forking (issue #600)

Preforking servers (Puma, Unicorn, Passenger, Resque) fork after boot. The
engine is not safe to fork while it runs: `fork()` copies memory but only
the calling thread, so a lock one of the engine's threads held at that
instant (its enum type-name interner, its metrics registry, or one inside
`tracing`, `quanta` or Rust's stdio) is copied held, and nothing in the
child will ever release it. A child that then connects an engine of its own
can wait on that lock forever. The contract, as for most native libraries
with threads of their own (`librdkafka`, gRPC's core): **no engine may be
running in a process at the moment it forks** if the child is going to
connect. Shut every
handle down first (`Trellis::shutdown` joins every thread the engine
started) and connect in the child after the fork. A child that execs
straight away (`posix_spawn`, Ruby's `system`) is unaffected.

The crate doesn't try to make an engine safe to inherit mid-flight: it can't
reach the locks inside its dependencies. The host language's binding owns
the fork boundary instead. The Ruby binding enforces the contract: every
call on an inherited handle raises `Trellis::ForkedHandleError`, and so does
`Trellis.connect` (or `Trellis::Instance.connect`) in a child forked while its
parent had a handle running, rather than risk the hang. In a Rails app under a
preloading server, that means shutting the boot handles down in the parent and
connecting them again in each worker: Puma's
`before_fork { Trellis::Instance.shutdown_all }` and
`before_worker_boot { Trellis::Railtie.connect }` (`clients/ruby/README.md`
covers Unicorn, Passenger and Puma's `fork_worker`, which needs
`before_worker_fork`/`after_worker_fork` too). The BEAM never forks,
so the Elixir binding has nothing to enforce.

## The silent-stall hazard (issue #144)

This shape has one sharp edge: **if the dedicated worker process is never
deployed, or gets scaled to zero, nothing errors.** Every `apply()` call
still succeeds and every transform still gets registered. With no staging
worker, a new transform sits in `waiting_to_backfill` forever. With a staging
worker but no drain threads, it stops at `backfilling`. With drain threads but
no staging worker, a build already dispatched still finishes, but only the
staging worker runs the catch-up after it, so the transform stops at
`catching_up`. A `live` transform stops being maintained. Either way the read
paths quietly return nothing or stale data, with no exception, timeout, or log
line pointing at the cause.

This is the most likely misconfiguration for an embedded deployment: a deploy
config typo, an autoscaler with a bad minimum, a worker dyno nobody remembered
to add.

### Detecting it: `has_live_drain_workers` and `has_live_staging_worker`

`Trellis`/`BlockingTrellis` expose two cheap, single-query health checks, one
per kind of worker:

```rust
if !trellis.has_live_drain_workers().await? || !trellis.has_live_staging_worker().await? {
    // Every transform in this fleet is at risk of stalling — page someone,
    // don't just log it.
}
```

Each answers "is there a live worker of this kind anywhere in this fleet right
now", not "is *this* connection running one", so call it from any connection,
including a web process at `drain_threads: 0`, which is where an uptime
monitor or load balancer polls. A drain worker counts as live until its
heartbeat is older than the reclaim TTL (30 seconds by default), so a crashed
one drops out within that window. The staging worker counts as live while
some connection holds the instance's staging-worker lock, which Postgres frees
the moment a crashed worker's connection closes. It also reads `false` for the
tick or so the worker takes to reconnect after a failed step, so page on it
staying `false` across a few checks, not on one reading.

**These are liveness checks, not backlog checks.** `true` means the worker is
alive; it says nothing about whether it is keeping up. Use
`Trellis::status`/`Trellis::watermark_token` + `await_converged` to reason
about an individual transform's own progress.

### Wiring it into a host health check

In Elixir the two checks are `Trellis.has_live_drain_workers/1` and
`Trellis.has_live_staging_worker/1`, each returning `{:ok, boolean}` (or the
boolean itself from the bang variant). In Ruby they are the predicates
`Trellis.has_live_drain_workers?` and `Trellis.has_live_staging_worker?`, on
the process's default instance (a `Trellis::Instance` has the same methods
for each further instance), raising a `Trellis::Error` if the database can't
answer.

**Phoenix**, wired as a `Plug` health-check endpoint polled by the
platform's liveness probe:

```elixir
defmodule MyAppWeb.HealthController do
  use MyAppWeb, :controller

  def workers(conn, _params) do
    # This instance's handle, the kind ADR-0010 decision 3 describes, owned
    # by `{Trellis, name: MyApp.Trellis, ...}` in the supervision tree. With
    # several instances, check the handle of each.
    trellis = MyApp.Trellis

    cond do
      not Trellis.has_live_drain_workers!(trellis) ->
        send_resp(conn, 503, "no live drain workers in this fleet")

      not Trellis.has_live_staging_worker!(trellis) ->
        send_resp(conn, 503, "no live staging worker in this fleet")

      true ->
        send_resp(conn, 200, "ok")
    end
  end
end
```

**Rails**, as a scheduled check (e.g. a recurring Sidekiq job or a
`rails-healthcheck`-style route) rather than on every request — this check
is a fleet-wide question, not something that needs to be re-answered on
every web request:

```ruby
class TrellisWorkerHealthCheck
  def self.perform
    # The process's default instance is the `Trellis` module's (ADR-0010 decision 3).
    unless Trellis.has_live_drain_workers?
      Rails.logger.error("no live Trellis drain workers — every transform is stalled")
      # ... page, raise, whatever this app's alerting expects ...
    end
    unless Trellis.has_live_staging_worker?
      Rails.logger.error("no live Trellis staging worker — no change is captured")
      # ... same ...
    end
  end
end
```

Poll on a timer (not once at boot, since a worker process can be scaled to
zero well after a healthy start), and treat `false` as "every transform in
this fleet may be silently stuck," not as a transient blip to retry past.

### Halted definitions

The worker checks say the fleet is running; they can't say a definition has
stopped. A halting failure (a source key the drain can't use, a propagation
wave past the hop bound, an aggregate off the ledger, or Postgres refusing the
drain's role a read or write) pauses the definitions it reaches, and every
other definition keeps converging (#663,
[observability — A halting failure](observability.md#a-halting-failure)). A
halted definition reports `paused`, the same word as an operator pause, so
check `DefinitionSummary::halt` rather than the status: it is set only on a
halted definition, and carries the table the failure named, the error and when
it was found. For an aggregate off the ledger, that table is the aggregate's
target, not a source. One `definitions()` call answers for every definition:

```rust
let halted: Vec<_> = trellis
    .definitions()
    .await?
    .into_iter()
    .filter(|summary| summary.halt.is_some())
    .collect();
if !halted.is_empty() {
    // Each stays paused, and its target stale, until it is fixed and
    // resumed — page someone.
}
```

In Elixir, `halt` is a `%Trellis.CaptureFailure{kind: :halt}` or
`nil`; in Ruby, a `Trellis::CaptureFailure` with `kind` `:halt`, or `nil`:

```elixir
halted = Enum.filter(Trellis.definitions!(trellis), & &1.halt)
```

```ruby
halted = Trellis.definitions.select(&:halt)
```

Poll it on the same timer as the worker checks. Clear a halt by fixing its
cause and resuming each halted definition (`RESUME TRANSFORM`), in any order;
resuming while the cause persists halts it again.

### Drain holdups

Some failures stall a drain page without pausing anything or charging a key: a
refused read or write the catalog can't pin on a table (a column grant, a
function's `EXECUTE`, one of Trellis's own tables), records that fail only
together, or a failure that keeps reproducing until the page's retries run out.
Every drain pass retries the page, and the targets of the tables on it stop
short of it, so every watermark token taken since waits on it (#817,
[observability — Transform status lifecycle](observability.md#transform-status-lifecycle)).
The drain records each as a *drain holdup*, which `status(target)` reports as
`drain_failure` on every definition that isn't paused or quarantined and reads
a table on the page, directly or through a relationship: the segment, the
tables, the latest error and its SQLSTATE, when it started and last failed, and
how many passes have. The definition's status stays what it was (`live`, say),
so check the field, not the word. `self_check` reports every open holdup with
each audit (`drain_failures`), whichever target it audits. A holdup clears in
the transaction that commits its page, so fix the cause the error names; there
is nothing to resume.

`status` takes the bare target name, the part of `target_table` after the
schema:

```rust
for summary in trellis.definitions().await? {
    let bare = summary.target_table.split('.').nth(1).unwrap_or_default();
    if let Some(status) = trellis.status(bare).await?
        && let Some(held) = &status.drain_failure
    {
        // The drain has failed on segment `held.seg_seq` since `held.since`,
        // `held.attempts` times: page someone with `held.error`.
    }
}
```

In Elixir, `drain_failure` is a `%Trellis.DrainFailure{}` or `nil`, and
`self_check`'s report lists them in `drain_failures`; in Ruby, a
`Trellis::DrainFailure` or `nil`, and `SelfCheckReport#drain_failures`. Both
carry `seg_seq`, `tables`, `error`, `sqlstate` (`nil` for a failure that didn't
come from Postgres), `since` and `last_seen` (a `DateTime` in Elixir, a `Time`
in Ruby) and `attempts`:

```elixir
held_up =
  Enum.flat_map(Trellis.definitions!(trellis), fn summary ->
    bare = summary.target_table |> String.split(".") |> Enum.at(1)

    case Trellis.status!(trellis, bare) do
      %Trellis.Status{drain_failure: %Trellis.DrainFailure{} = held} -> [{bare, held}]
      _ -> []
    end
  end)
```

```ruby
held_up = Trellis.definitions.filter_map do |summary|
  bare = summary.target_table.split(".")[1]
  held = Trellis.status(bare)&.drain_failure
  [bare, held] if held
end
```

The CLI's `trellis status` prints each one on an indented line under the
definition it holds back.

### Unindexed join columns

`status(target)` reports, as `unindexed_joins`, each join column of a
relationship the definition reads that has no usable index: a relationship's
`from_col`, and a to-many relationship's `to_col`. An entry names the
relationship, the table (`schema.table`) and the column; the fix is an index on
that table and column, which Trellis doesn't create on your tables
([recommendations — Index relationship join columns](recommendations.md#index-relationship-join-columns)).
The list is empty for a definition whose join columns are all indexed, and it
never changes the definition's status, so a health check that wants it reads
the field:

```rust
for summary in trellis.definitions().await? {
    let bare = summary.target_table.split('.').nth(1).unwrap_or_default();
    if let Some(status) = trellis.status(bare).await? {
        for join in &status.unindexed_joins {
            // `join.fix()`: "create an index on public.orders (customer_id)"
        }
    }
}
```

`self_check` carries the same list on the reports it returns, but it refuses a
target that reads a relationship once it reaches the comparison
([observability](observability.md#transform-status-lifecycle)), so read the
warning from `status`. In `trellis-embed`, `PlainDefinitionStatus` and
`PlainSelfCheckReport` carry it as `unindexed_joins`, each entry with
`relationship`, `table`, `column` and the `fix` sentence. The CLI's
`trellis status` prints each entry as a warning line under its definition.

## Migrations and transactions

**Trellis never joins your migration's transaction.** `migrate`, `define` and
`apply` each run on the handle's own pooled connections, and each commits
before it returns. None of them takes a connection or a transaction from the
host, and neither binding's migration helper changes that: not Elixir's
`Trellis.Migration`, and not Ruby's `Trellis::Migration`. Rails and Ecto both
wrap each migration in a transaction by default, so a define made inside one
behaves in four ways you might not expect:

* **A rollback doesn't undo it.** If the migration fails after `define`
  returned, the host's changes roll back, but the definition and its target
  table stay. Running the migration again then fails on `define` with a
  `conflict` error, because the target table already exists. A define isn't
  idempotent, even for the identical statement, and no helper makes it so.
* **It can't see the migration's own uncommitted work.** A source table
  created earlier in the same transaction isn't visible to Trellis's
  connection, so `define` fails with `not_found`. A column added earlier
  isn't visible either, so a field that reads it fails with a `validation`
  error.
* **It can't go live until the migration commits.** A backfill waits out
  every transaction that had written anything when it was queued
  ([the `xmin` caveat](observability.md#backfill-status-and-the-xmin-caveat)),
  and that includes a migration that changed a table before its define. So
  a migration that polls for `live` inside its own transaction can wait on
  itself until its deadline.
* **Some statements wait on the migration's locks, then fail.** A define
  doesn't lock the source table, so a migration that altered it doesn't
  block one. But a define whose target name matches a table the migration
  created and hasn't committed waits for the migration to end, and so does a
  `DROP TRANSFORM` of a target the migration has read or written. The
  migration is waiting for Trellis meanwhile, and Postgres sees an idle
  transaction and a waiting one, not a deadlock. What ends the wait is the
  `lock_timeout` every Trellis connection is capped at (30 seconds): the
  call fails with a lock timeout and changes nothing.

So keep Trellis out of the host's transaction, in this order:

1. The host migrations that create or change source tables run and commit.
2. `migrate` creates or upgrades Trellis's own tables. It's idempotent, so
   running it on every deploy is fine.
3. The transforms are defined, once each, outside any host transaction.
4. The dedicated worker starts or restarts. It doesn't need the definitions
   to exist first; it picks each one up on its next pass.

Step 3 can be a deploy step of its own, run after migrations, or a migration
of its own with the host's transaction turned off (`@disable_ddl_transaction
true` in Ecto, `disable_ddl_transaction!` in Rails), so it's recorded like
any other migration. Keep it to its Trellis statements where you can:
without the transaction, a step that fails partway leaves whatever ran
before the failure in place, on both sides. The simplest order is to create
or alter the source table in one step and define the transform in the next,
but a transform defined against a table created earlier in the same
non-transactional step is fine too: the hazard is a rollback, not the order.

A deploy step runs on every deploy, and a define isn't idempotent, so a
deploy step checks `status` first: it returns nothing for a target no
transform writes. A migration is different: the host's migration tooling
(Ecto's and Rails's `schema_migrations`) records which migrations have run,
so a migration that defines a transform runs once per database and needs no
such guard. Either way, `down` undoes the define with `PAUSE TRANSFORM` and
then `DROP TRANSFORM` (`DROP` refuses a transform that isn't paused, and takes
the target table and its data with it). Both are no-ops when repeated, so a
replayed or rolled-back migration is safe in both directions
([ADR-0014](decisions/0014-pause-and-drop-a-transform.md)).

In Elixir, `Trellis.Migration` makes those statements read like Ecto's own:

```elixir
defmodule MyApp.Repo.Migrations.DefineOrderTotals do
  use Ecto.Migration
  use Trellis.Migration

  # Required: the helpers raise before applying anything if the migration
  # has a transaction open.
  @disable_ddl_transaction true

  def up do
    define "TRANSFORM order_totals FROM orders SELECT price + tax AS total"
  end

  def down do
    apply "PAUSE TRANSFORM order_totals"
    apply "DROP TRANSFORM order_totals"
  end
end
```

`define` and `apply` are queued like `create table`, so they run in order
with the migration's own commands. A module that uses `Trellis.Migration`
has to define `up/0` and `down/0`: Ecto can't reverse a Trellis statement on
its own, so `change/0` is a compile error.

Ecto doesn't start the application to migrate, so each helper connects a
handle of its own from the repo's `:trellis` configuration
(`config :my_app, MyApp.Repo, trellis: [url: database_url]`), with the
defaults, and runs `migrate` on it first. That covers step 2 whenever a
migration defines something. To upgrade Trellis's tables on a deploy that
doesn't, run `migrate` in the release's migration task too:

```elixir
defmodule MyApp.Release do
  @app :my_app

  def migrate do
    Application.load(@app)

    # The defaults: nothing runs in the background.
    trellis = Trellis.connect!(url: System.fetch_env!("DATABASE_URL"))
    :ok = Trellis.migrate!(trellis)
    :ok = Trellis.shutdown!(trellis)

    for repo <- Application.fetch_env!(@app, :ecto_repos) do
      {:ok, _, _} = Ecto.Migrator.with_repo(repo, &Ecto.Migrator.run(&1, :up, all: true))
    end
  end
end
```

In Rails, as a migration of its own, after the one that creates `orders`.
`Trellis::Migration` makes the statements read like the migration's own:

```ruby
class DefineOrderTotals < ActiveRecord::Migration[8.1]
  include Trellis::Migration

  # Required: the helpers raise before applying anything if the migration
  # has a transaction open.
  disable_ddl_transaction!

  # Assumes an initializer connected this process's handle with the
  # defaults (staging: false, drain_threads: 0), and that `Trellis.migrate`
  # has run.
  def up
    define "TRANSFORM order_totals FROM orders SELECT price + tax AS total"
  end

  def down
    apply "PAUSE TRANSFORM order_totals"
    apply "DROP TRANSFORM order_totals"
  end
end
```

As in Ecto, a migration that includes `Trellis::Migration` has to define `up`
and `down`, since ActiveRecord can't reverse a Trellis statement on its own:
one that defines `change` raises before anything runs.

`rails db:migrate` has no handle of its own (rake tasks skip the boot
connect), so each helper connects one from `config.trellis.connect`, with
the defaults, and runs `migrate` on it first. That covers step 2 whenever a
migration defines something. To upgrade Trellis's tables on a deploy that
doesn't, run `bin/rails trellis:migrate` in the deploy step too.

A migration that has to tolerate a transform defined some other way (by
hand, or by a deploy step of your own) can guard its define. The guard is
optional, and nothing adds it for you. `status` returns nothing for a
target no transform writes:

```elixir
def up do
  if status("order_totals") == nil do
    define "TRANSFORM order_totals FROM orders SELECT price + tax AS total"
  end
end
```

```ruby
def up
  define "TRANSFORM order_totals FROM orders SELECT price + tax AS total" if status("order_totals").nil?
end
```

The guard checks the target's name, not its definition. A changed statement
for a target that already exists is an `ALTER TRANSFORM`
([Changing a definition](transforms.md#changing-a-definition)), in a
migration of its own, not a second define.

A schema dump doesn't carry a definition. Rails' `db/schema.rb` and
`structure.sql`, and Ecto's `structure.sql`, record the target table and
mark the defining migration as run, but a definition is a row in Trellis's
own tables, which no schema dump holds. A database loaded from the dump
(`db:schema:load`, `db:prepare` on a new database, Rails' test database,
`mix ecto.load`) has the target tables but no transforms, and defining one
then fails with a `conflict` error because its table exists. Build a
database that needs its transforms by running the migrations.

## Every call returns within 30 seconds

Every call on a `Trellis` or `BlockingTrellis` handle has a 30-second deadline,
counted from when the call is submitted. Time queued for the handle and time
waiting for a pooled connection count against it. A call that runs out of
time returns the `timeout` error (Rust's `TrellisError::CallTimeout`), the
same code as `await_converged` running out of time: it's expected and
retryable, not a bug. Look at what the call waits on, usually a lock a long
application transaction holds, and call again.

The deadline is enforced on the server, not just abandoned by the caller. A
connection a call checks out gets a `statement_timeout` of the time left, and
so does every transaction it opens, so Postgres stops a statement stuck on a
lock and rolls its transaction back. A timed-out call changes nothing it
hadn't already committed. `migrate` commits one migration at a time and
gives each the time left, so one that runs out leaves the earlier ones
applied. Postgres times each statement on its own, so a statement's limit is
the time left when its transaction began (or, outside a transaction, when its
connection was checked out): one that starts late in a call can run on the
server past the deadline by up to that much, though the call itself returns on
time. A caller that goes away (Ctrl-C, a killed
thread) leaves its call running until the deadline, where the server stops
it, and the reply is dropped.

Heavy work isn't done inside a call. `apply` registers a definition and
returns, and the build runs in the background, so a caller that needs the
result polls `status` ([below](#poll-to-live-dont-wait)). One statement
still reads table rows inside the call: a to-one relationship's declaration
(or a transform reading through one) seeds the relationship's projection from
its to-side table, and on a large enough table it runs out of time and
registers nothing.

An `await_converged` timeout longer than the deadline is cut to what is left
of it. To wait longer, call again: each call is one bounded wait.
`self_check` is the exception for now: it runs outside the deadline, and its
`timeout` argument bounds each convergence wait it makes, not the whole call.

A `BlockingTrellis` runs its calls in parallel, so a call stuck on a lock
doesn't hold up the calls made after it. A caller's own calls stay in order,
because it waits for each reply. Calls from different threads have no order.
The pool bounds how many run at once. The Elixir binding's `Trellis.Owner`
still hands a handle's calls over one at a time, so an Elixir call can wait
behind the calls ahead of it before its own 30 seconds start.

The Ruby binding waits for each reply on the calling thread with the GVL
released, so `Timeout.timeout`, `Thread#kill` and Ctrl-C return control at
once and leave no thread behind; the abandoned call ends at its deadline.
`shutdown` cancels the calls still in flight instead of waiting for them, so
a call stuck on a lock doesn't hold it up: they raise
`Trellis::ValidationError`, and what each started on the server ends at the
call's own deadline.

## Poll to `live`, don't wait

`define` (and `apply` with a `TRANSFORM`) returns once the definition is
registered, with its target table created and empty, and its status
`waiting_to_backfill`. The build runs in the background on the dedicated
worker, and takes as long as the source's size calls for
([ADR-0008 decision 1](decisions/0008-public-api-design.md#1-synchronous-calls-at-the-ffi-boundary)).
Nothing waits for it for you: code that needs the target populated polls
`status` until it reads `live`.

`status` returns the transform's status word (Rust's `TransformStatus`,
Elixir atoms, Ruby symbols), or nothing if no transform writes that table:

| Status | What it means | What your poll does |
|---|---|---|
| `waiting_to_backfill` | Defined; the source's existing rows haven't been read yet. | Keep polling. |
| `backfilling` | The target is being built, or rebuilt after a repair. | Keep polling. |
| `catching_up` | Built and maintained, but may still be missing changes made while it was building. A plain aggregate (grouped by plain columns of a table rather than of another transform's target, with no relationship and no `MIN`/`MAX` of text) never reports it: its build goes from `backfilling` straight to `live`. | Keep polling. |
| `live` | The steady state. | Done. |
| `quarantined` | Too many source rows failed to apply, so the fuse froze it. | Stop and report it. |
| `paused` | Frozen by a `PAUSE TRANSFORM`, or by Trellis because it can't keep the target correct: capture broke, its build kept failing, or the drain hit a failure every key reproduces ([all the causes](observability.md#transform-status-lifecycle)). | Stop and report it. |

The lifecycle behind these words is in
[transforms — Status](transforms.md#status) and
[observability — Transform status lifecycle](observability.md#transform-status-lifecycle).
Things a poll needs to handle:

* **`waiting_to_backfill` has no deadline of its own.** A long-running
  transaction anywhere in the cluster holds most backfills back until it ends
  ([the `xmin` caveat](observability.md#backfill-status-and-the-xmin-caveat)),
  and Trellis reports nothing but the status meanwhile. Poll with a deadline
  of your own, from somewhere that can wait (a deploy check, a background job),
  not a web request.
* **A failing build doesn't fail the poll.** The error is on the status's
  `backfill_failure`; log it, it's usually the whole answer
  ([a backfill that keeps failing](observability.md#a-backfill-that-keeps-failing)).
  A row that fails is quarantined and the transform still goes `live`
  (`status` counts it in `held_keys`, `sample_quarantined` lists it, and
  `release_key` releases it once its cause is fixed); a build that keeps
  failing for a reason no row explains is paused.
* **A drain page that keeps failing doesn't change the status.** A failure
  charged to no key that pauses nothing, a refused read the catalog can't pin
  on a table say, leaves the transform `live` (or wherever it was) while its
  target stops short of the page. `drain_failure` names it until the page
  commits ([drain holdups](#drain-holdups)).
* **A schema change pauses, it never fails your writes.** Renaming or dropping a
  source column pauses every transform that reads it, with the table and column
  on `capture_failure`; the others on the table keep running.
* **`quarantined` can come before `live`.** The fuse can trip during the
  build, so a transform can reach `quarantined` without ever reporting `live`.
  `quarantined` and `sample_quarantined` show which rows failed and why. A
  single calculated column can be quarantined while the transform stays `live`;
  `status` doesn't show that, `quarantined` does.
* **`RESUME TRANSFORM <target>` is the way out of `paused` and `quarantined`.**
  Fix the cause first. Resume reconciles the target with the current source
  rather than replaying what was skipped while frozen, so its cost scales with
  the data, and the transform goes back through `waiting_to_backfill`
  ([ADR-0014](decisions/0014-pause-and-drop-a-transform.md)).

```rust
use std::time::Duration;
use trellis::TransformStatus;

let deadline = tokio::time::Instant::now() + Duration::from_secs(600);
loop {
    let Some(status) = trellis.status("order_totals").await? else {
        panic!("no transform writes order_totals");
    };
    match status.status {
        TransformStatus::Live => break,
        TransformStatus::Quarantined | TransformStatus::Paused => {
            panic!("order_totals is {:?}: it won't go live until it's resumed", status.status);
        }
        _ => {}
    }
    if let Some(failure) = &status.backfill_failure {
        eprintln!("backfill of {} is failing: {}", failure.source_table, failure.last_error);
    }
    if tokio::time::Instant::now() > deadline {
        panic!("order_totals is still {:?}", status.status);
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
}
```

```elixir
defmodule MyApp.TrellisReady do
  require Logger

  def await_live(trellis, target, timeout_ms \\ 600_000) do
    poll_live(trellis, target, System.monotonic_time(:millisecond) + timeout_ms)
  end

  defp poll_live(trellis, target, deadline) do
    case Trellis.status!(trellis, target) do
      nil ->
        {:error, :not_defined}

      %Trellis.Status{status: :live} ->
        :ok

      %Trellis.Status{status: frozen} when frozen in [:quarantined, :paused] ->
        {:error, frozen}

      %Trellis.Status{status: status, backfill_failure: failure} ->
        if failure do
          Logger.warning("backfill of #{failure.source_table} failing: #{failure.last_error}")
        end

        if System.monotonic_time(:millisecond) > deadline do
          {:error, {:still, status}}
        else
          Process.sleep(1_000)
          poll_live(trellis, target, deadline)
        end
    end
  end
end
```

```ruby
def await_live(target, timeout: 600)
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
  loop do
    status = Trellis.status(target) or raise "no transform writes #{target}"
    case status.status
    when :live then return
    when :quarantined, :paused
      raise "#{target} is #{status.status}: it won't go live until it's resumed"
    end
    if (failure = status.backfill_failure)
      Rails.logger.warn("backfill of #{failure.source_table} failing: #{failure.last_error}")
    end
    if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      raise "#{target} is still #{status.status}"
    end
    sleep 1
  end
end
```

### Reading your own writes

`live` is where the read-your-writes contract starts. Once a transform
reports `live`, write to its source, let the commit return, take a
`watermark_token`, and pass it to `await_converged`: when that returns, the
target reflects the write
([ADR-0002 — What `live` promises](decisions/0002-async-data-flow.md#what-live-promises)).
The same pair, taken right after the poll above reads `live`, waits out the
last of the build's own changes.

```rust
let token = trellis.watermark_token().await?;
trellis.await_converged(token, Duration::from_secs(30)).await?;
```

```elixir
token = Trellis.watermark_token!(trellis)
:ok = Trellis.await_converged!(trellis, token, 30_000)
```

```ruby
token = Trellis.watermark_token
Trellis.await_converged(token, timeout_ms: 30_000)
```

`await_converged` waits for captured changes only; it doesn't read status. A
transform that isn't `live` yet, `catching_up` included, can still be missing
rows after it returns, which is why the poll comes first. When the timeout
runs out first, it fails with a `timeout` error, not `internal`: the target is
behind, not broken, so retry or allow longer. The wait never lasts past the
call's [30-second deadline](#every-call-returns-within-30-seconds), whatever
timeout is passed; call again to wait longer.

A repair is visible to the poll too. `request_backfill` (and a capture
re-install) rebuilds each plain aggregate and plain 1-1 reader of the table
in its own transaction: they read `backfilling` when the call returns, and
`live` again once the rebuild is done. So polling for `live` after a repair,
then taking a token, covers the repair. Any other reader of the table reports
`catching_up` until its catch-up has run.

`self_check` follows the same rule: it compares only a `live` transform. For
any other it returns the outcome `not_live` (Rust's
`SelfCheckOutcome::NotLive(status)`, Elixir's and Ruby's `:not_live`) with
the status in the report's `status`, and compares and waits for nothing. Poll
for `live`, then check again. Like `not_caught_up`, the outcome is not a
verdict on correctness.

## What a transform can do

A transform reads one source table (which needs a primary key) or another
transform's target, is 1-1 or a `GROUP BY` aggregate, and computes its fields
from the source row, from other fields, and from related tables through a
declared `RELATIONSHIP`. What the grammar accepts, what `define` refuses, and
which column types play which role are in
[transforms](transforms.md) and the [type-support matrix](type-support.md).
Whatever the shape, what you can rely on is the status lifecycle and the `live`
plus `await_converged` contract above. `self_check` audits a target against a
recompute from its source ([known correctness gaps](known-correctness-gaps.md)
lists what it doesn't cover).
