# Trellis for Elixir

Embedded Trellis for Elixir apps: a [Rustler](https://github.com/rusterlium/rustler)
NIF over the `trellis` crate's `BlockingTrellis`, so a Phoenix app can define
and run streaming transforms without deploying a separate service. The design
is [ADR-0010](https://github.com/salesforce-misc/trellis/blob/main/docs/decisions/0010-embeddable-clients.md).

## Installation

The Hex package is `trellis_pg` (Hex's `trellis` is taken); the modules are
`Trellis.*`, and the OTP application is `:trellis_pg`.

```elixir
def deps do
  [{:trellis_pg, "0.1.0-alpha"}]
end
```

Installing needs no Rust: compiling the package downloads the NIF built for
the machine from the release's GitHub page and checks it against the SHA-256
the package ships. Builds exist for

- Linux, glibc 2.17 or newer: `x86_64` and `aarch64`
- Linux, musl (Alpine): `x86_64`
- macOS: Apple silicon and Intel

On any other target, or to build it yourself, build the NIF from source. It
needs a Rust toolchain (the version in the repository's `rust-toolchain.toml`)
and Rustler in the host's own deps:

```elixir
{:rustler, "~> 0.38.0", runtime: false}
```

```sh
TRELLIS_PG_BUILD=1 mix deps.compile trellis_pg --force
```

The choice is made when `trellis_pg` compiles, so `--force` it after changing
the variable. `config :rustler_precompiled, :force_build, trellis_pg: true`
does the same from config. Cargo fetches the `trellis` crate from this
repository at the released commit.

The NIF is built without the `trellis` crate's `otlp` feature, so neither
path pulls in an OpenTelemetry or gRPC stack. The engine's log lines reach
`Logger` through `Trellis.LogBridge`.

## Usage

The surface mirrors the Rust crate's `BlockingTrellis`. Every function has a bang variant.

- `connect/1`, `migrate/1`, `config/1`, `shutdown/1`: the handle's lifecycle.
- `apply/2`: any statement of Trellis's grammar (`TRANSFORM`,
  `PAUSE TRANSFORM`, `RESUME TRANSFORM`, `DROP TRANSFORM`,
  `ALTER TRANSFORM`), returning what it did as a tagged
  value (`{:transform_defined, target_table}`, `:paused`,
  `{:resumed, addresses}`, ...). `define/2` is an `apply/2` that refuses
  anything but a `TRANSFORM`.
- `status/2`, `definitions/1`: the catalog reads.
- `quarantined/1`, `quarantine_status/2`, `sample_quarantined/3`,
  `poisoned_since/2`: the quarantine reads. Resuming is a statement.
  `release_key/4` releases one key a transform holds, once its cause is
  fixed; `status/2` reports how many it holds (`held_keys`). `status/2`
  also reports a page the drain keeps failing on with nothing charged or
  paused (`drain_failure`, a `Trellis.DrainFailure`), and a finished `self_check/3`
  job's report lists every one open (`drain_failures`). `status/2` names the paused upstream columns
  (`paused_upstream_columns`, each `"target.column"`) that make a `:live`
  transform report `:catching_up`. `status/2` lists the source indexes a
  `GROUP BY` or `JOIN` target
  needs and lacks (`source_index_gaps`, each a `Trellis.SourceIndexGap` with
  `table`, `columns`, `role` (`:group_by`, `:join_from` or `:join_joined`: the part
  of the definition that needs it), `opted_out` and a `create index` statement as `fix`);
  `self_check/3` refuses such a target unless it was defined `WITHOUT SOURCE
  INDEX`. `status/2` lists the partitions of a partitioned source a transform
  is being corrected for while it reads `:backfilling` for them
  (`correcting`, each a `Trellis.CorrectingNode` with `node`, `since` and
  `keys_remaining`).
- `request_backfill/2`, `detect_capture_disruptions/1` (pauses now the readers
  of every table whose Trellis capture was changed outside Trellis, and
  returns the tables), `has_live_drain_workers/1`,
  `has_live_staging_worker/1`, `watermark_token/1`, `await_converged/3`.
- `self_check/3` and `self_check_job/2`: check a target against a fresh
  recompute from its source, as a background job. `self_check/3` starts it and
  returns a `Trellis.SelfCheckJob` (`id`, `target`, `mode`, `state`,
  `rows_compared`, `report`, `error`) at once; a drain worker runs it, and
  `self_check_job/2` reads it back by `id` until `state` is `:done`
  (`:queued` and `:running` are in progress; `:failed` and `:cancelled` carry
  an `error`), when `report` is a `Trellis.SelfCheckReport` (`outcome`,
  `divergences`, `rows_compared`, `truncated`, ...). A target whose job is
  still unfinished gets that job back; the next call replaces a finished
  one, and `self_check_job/2` returns `nil` for the replaced job or one whose
  transform was dropped. With no drain worker anywhere in the fleet a job
  stays `:queued`.
- `start_link/1`, as `{Trellis, options}` in a supervision tree: a process
  that owns the handle, so every function above also takes its name.
- `Trellis.Migration`: `define`, `apply` and `status` in an Ecto migration.
- `dump_definitions_sql/1`, `mix trellis.dump` and `Trellis.StructureDump`:
  the definitions as SQL appended to the structure file (see
  [Schema dumps](#schema-dumps)).
- `Trellis.Metrics.render_prometheus/0`: the process-wide metrics registry
  as Prometheus text, for a `/metrics` route the host already serves (the
  binding opens no port). It needs no handle. Every series carries the
  `trellis_instance` label (`<database>/<catalog schema>`, as in the log
  lines), so handles in one VM stay apart.
- `Trellis.LogBridge`: started with the `:trellis_pg` application, it forwards
  the engine's log lines to `Logger` (`domain: [:trellis]`, the Rust module
  as `:target` metadata). Every line names its instance in the text, as
  `trellis_instance=<database>/<catalog schema>`, so handles in one VM stay
  apart. Set `config :trellis_pg, log_level: :info` to choose the most
  verbose level forwarded (default: `Logger.level()`, read once at start),
  or `config :trellis_pg, log_bridge: false` to forward nothing. A
  `tracing` subscriber installed by another NIF never sees the engine's
  lines, since each native library links its own `tracing`; see
  `Trellis.LogBridge`. A line can be dropped if the bridge falls behind,
  and a warning says how many; logging never blocks or crashes the engine.

```elixir
# A deploy's migration step: the defaults run nothing in the background.
{:ok, migrator} = Trellis.connect(url: "postgres://localhost/app")
# TLS options go in the URL (docs/embedding.md, "TLS to Postgres"):
#   url: "postgres://db.example.com/app?sslmode=verify-full&sslrootcert=/etc/ssl/ca.pem"
:ok = Trellis.migrate(migrator)
:ok = Trellis.shutdown(migrator)

# The app, once migrated. The staging worker reads the catalog as it starts,
# so a `staging: true` handle can't connect before `migrate/1` has run.
{:ok, trellis} = Trellis.connect(url: "postgres://localhost/app", staging: true, drain_threads: 2)
{:ok, _definition} = Trellis.define(trellis, "TRANSFORM widget_prices FROM widgets SELECT price AS price")
{:ok, %Trellis.Status{status: :live}} = Trellis.status(trellis, "widget_prices")

# A column that paused on bad data: find the rows, fix them, resume it.
{:ok, [%Trellis.QuarantineEntry{target: "widget_prices.price"} | _]} = Trellis.quarantined(trellis)
{:ok, %Trellis.SamplePage{samples: rows, next_cursor: cursor}} =
  Trellis.sample_quarantined(trellis, "widget_prices.price", limit: 50)
{:ok, {:resumed, ["widget_prices.price"]}} =
  Trellis.apply(trellis, "RESUME TRANSFORM widget_prices.price")

:ok = Trellis.shutdown(trellis)
```

`connect/1`'s defaults (`staging: false, drain_threads: 0`) run nothing in
the background. For each instance (catalog schema), some connection in the
fleet must set `staging: true` and some connection must run drain threads, or
no definition ever reaches `:live`. Any number of connections may set
`staging: true`: one takes the producer lock and leads, the rest stand by and
take over when it goes away, so a rolling deploy needs no operator step and
the application boots even when the old worker is still running (see
[Rolling deploys](../../docs/embedding.md#rolling-deploys)).

### Resources

At the defaults, a handle holds up to 20 Postgres connections (its call pool,
opened as calls need them and closed after 60 seconds idle) and 3 threads. A
worker handle (`staging: true, drain_threads: N`) also owns a background pool
of up to `2N + 5` connections (`2N + 4` without staging, `1` with staging
alone), closed after 60 seconds idle, and two connections that stay open: the
staging session and one `LISTEN` shared by its drain threads. It holds 6
threads: drain threads are tasks on its runtime, not OS threads. Its memory is
bounded by row counts, not bytes: about `N` × 100,000 changed rows' images at
its peak. `:max_call_pool_size` lowers the 20 (the background pool is sized by
the handle's roles, not by an option), and `:worker_threads` sets the threads.
Every handle (each `{Trellis, name: ...}` child) costs this again. The full
accounting is in
[`docs/embedding.md`](../../docs/embedding.md#what-a-handle-uses).

### Conventions

- Non-bang functions return `{:ok, value}` (or `:ok`) or
  `{:error, %Trellis.Error{code: code, message: message}}`; bang variants
  return the value or raise that error. `code` is one of a closed set of
  atoms, with `:unknown` for a code newer than the binding.
- No atom is ever built from a string the database returned: every atom in a
  result comes from a closed set the NIF allocates when it loads.
- Times are `DateTime`s. They cross the NIF as epoch microseconds.
- A quarantine target is an address string, `"transform"` or
  `"transform.column"`, exactly as `quarantined/1` reports it.
- `sample_quarantined/3`'s cursor and `watermark_token/1`'s token are
  opaque: pass back what the previous call returned.
- A call on a handle holds no scheduler while it waits. The NIF submits the
  call on a normal scheduler and returns a reference at once; the engine
  sends the result to the calling process, which waits for it in a `receive`.
  The function is synchronous to its caller, a process killed during a call
  exits at once, and a call stuck on the database holds no scheduler or
  thread of its own.
  `connect/1` and `shutdown/1` run on dirty IO schedulers, as do the NIFs
  that touch no handle; `Trellis.Metrics.render_prometheus/0` runs on a
  dirty CPU scheduler.
- Every call returns within 30 seconds, counted from when it is submitted
  (a database that can't answer in time gives `{:error, %Trellis.Error{code:
  :timeout}}`; the server stops what the call started). Calls run in
  parallel, on one handle and across processes: a call stuck on a lock
  doesn't delay another process's `status/2`. A process's own calls stay in
  order, because it waits for each reply; calls from different processes
  have no order.
- `shutdown/1` doesn't wait for calls in flight: it cancels them, and each
  of their callers gets a `:validation` error.

## In a Phoenix app

Let the application's supervisor own the handle. `{Trellis, options}` starts
a process that connects with `connect/1`'s options as it starts, hands the
handle to the callers that make calls through its name, and shuts the handle
down when the supervisor stops it:

```elixir
# config/runtime.exs
database_url = System.fetch_env!("DATABASE_URL")

config :my_app, MyApp.Trellis,
  name: MyApp.Trellis,
  url: database_url,
  # Only the fleet's one worker node runs the background work.
  staging: System.get_env("TRELLIS_WORKER") == "true",
  drain_threads: if(System.get_env("TRELLIS_WORKER") == "true", do: 2, else: 0)

# lib/my_app/application.ex
children = [
  MyApp.Repo,
  {Trellis, Application.fetch_env!(:my_app, MyApp.Trellis)},
  MyAppWeb.Endpoint
]

# Anywhere in the app: the name stands in for the handle.
{:ok, status} = Trellis.status(MyApp.Trellis, "widget_prices")
```

Each call runs in the process that makes it, in parallel with every other,
so no process queues behind another's call. The caller has to be on the same
node: a handle works only on the node that connected it. `shutdown/1` refuses
the name; the supervisor stops it, which cancels the calls still in flight.

Define transforms in Ecto migrations with `Trellis.Migration`. Trellis never
joins a migration's transaction, so a migration that uses it must set
`@disable_ddl_transaction true` (the helpers raise before applying anything
if a transaction is open), and must define `up/0` and `down/0`:

```elixir
defmodule MyApp.Repo.Migrations.DefineWidgetPrices do
  use Ecto.Migration
  use Trellis.Migration

  @disable_ddl_transaction true

  def up do
    define "TRANSFORM widget_prices FROM widgets SELECT price AS price"
  end

  def down do
    apply "PAUSE TRANSFORM widget_prices"
    apply "DROP TRANSFORM widget_prices"
  end
end
```

Ecto doesn't start the application to migrate, so each helper connects a
handle of its own, from the repo's `:trellis` configuration
(`config :my_app, MyApp.Repo, trellis: [url: database_url]`), and runs
`migrate/1` on it first. A define isn't idempotent, and the helpers don't
make it so: Ecto's `schema_migrations` runs each migration once. See
`Trellis.Migration` for the details, and for an optional guard.

Add `import_deps: [:trellis_pg]` to the app's `.formatter.exs` to keep
`define` and `apply` free of parentheses, like Ecto's own commands.

## Fixtures and bulk loads in tests

A loader that disables triggers for the load (`ALTER TABLE ... DISABLE
TRIGGER ALL`, `pg_restore --disable-triggers`) disables Trellis's capture
triggers too, and `ENABLE TRIGGER ALL` leaves them in a mode Trellis treats as
a disruption. The rows it wrote never reached a target, so Trellis pauses
every definition that reads the table (`capture_failure` has `kind:
:disruption`) instead of letting a test read silently wrong data, and waits
for a `RESUME`. `Trellis.TestSupport` does that `RESUME` for a suite, then
waits for the rebuilt targets to catch up:

```elixir
setup do
  # ... load the fixtures with triggers disabled, then re-enabled ...
  {:ok, _resumed} = Trellis.TestSupport.resume_capture_disruptions(trellis)
  :ok
end
```

`resume_capture_disruptions/2` first pauses the readers of any disrupted
table itself (`Trellis.detect_capture_disruptions/1`), so it doesn't wait for
the staging worker's next pass; resumes only the definitions paused with kind
`:disruption`; waits until each is `:live` and every write committed so far
has reached its targets; and returns `{:ok, resumed}` (`[]` when nothing was
disrupted, which costs one catalog read per captured table). A transform an
operator paused, or one paused by a schema change or another failure, stays
paused. A `RESUME` the engine refuses (a table that is gone, a table define
would now refuse) leaves that definition paused; the rest are still resumed,
and the call then returns `{:error, {:resume_refused, refused}}`, a map of
target to reason. `resume_capture_disruptions!/2` returns the targets and
raises `Trellis.TestSupport.ResumeRefused` (or `Trellis.Error`). The
`:timeout_ms` option (default 30 000) bounds the wait. The handle needs its
staging worker and a drain worker (`staging: true, drain_threads: 1`),
because the resume rebuilds through them.

Trellis sees a write when it commits, so with Ecto's SQL sandbox, load the
fixtures that targets must reflect outside the sandbox transaction (in
`setup_all`, or with the sandbox in `:auto` mode) and call the helper once.

## Schema dumps

`mix ecto.dump` writes a schema-only `pg_dump`, which carries the Trellis
schema, the target tables and the capture triggers but no rows, so a
database `mix ecto.load` builds from it has targets and no transforms.
`mix trellis.dump` runs `mix ecto.dump` and appends the definitions the way
Ecto appends `schema_migrations`. Make it what `mix ecto.dump` runs, by an
alias in `mix.exs`:

```elixir
defp aliases do
  ["ecto.dump": "trellis.dump"]
end
```

The task passes its arguments (`-r`, `-d`, `--prefix`, `-q`) to `ecto.dump`
unchanged. It reads the definitions before and after the `pg_dump` and redoes
the dump, up to three times, when a define or drop committed in between, so
the file's DDL and definitions agree.

The repo needs its `:trellis` configuration (as for `Trellis.Migration`);
the definitions are appended for each repo that has one. If the dump names
its schemas (`--prefix`, or the repo's `:dump_prefixes`), the list has to
include the Trellis schema, and the task refuses one that doesn't.
`mix ecto.load` then runs the file and nothing else: the worker that starts
on the loaded database finds definitions that aren't built and builds them.
Only definitions are dumped; the ledger, backfill progress, quarantine and
claims are runtime state, and a paused or quarantined transform loads
`:waiting_to_backfill`.

`pg_dump` 17 writes `CREATE SCHEMA public;` when it dumps `public` by name,
which stops `mix ecto.load` into a database `mix ecto.create` made; the task
writes `CREATE SCHEMA IF NOT EXISTS public;` instead.

`Trellis.StructureDump.append/2` appends to a structure file someone else
dumped (reading the definitions once, so it can't see a define racing that
dump), and `Trellis.dump_definitions_sql/1` returns the SQL for a caller that
assembles its own dump.

## Layout

- `lib/`: the public `Trellis` module and its structs, the process that
  owns a supervised handle (`lib/trellis/owner.ex`), and `Trellis.Migration`, which
  is compiled only when the host depends on `ecto_sql`.
- `lib/trellis/test_support.ex`: `Trellis.TestSupport`, the test helper above.
- `native/trellis_nif/`: the NIF crate, a member of the repository's Cargo
  workspace. The release makes the packaged copy stand alone (see
  Releasing). Plain-data conversion and error codes come from
  `clients/embed` (`trellis-embed`), shared with the Ruby binding.

## Development

The Erlang and Elixir versions are pinned in `.tool-versions`. From this
directory:

```sh
cargo build -p testkit --bin trellis-testkit   # the suite's throwaway Postgres
mix deps.get
mix test
```

In this repository (`:dev` and `:test`) the NIF always builds from source,
never downloads. `mix test` compiles it through Cargo into the workspace's
`target/`, and the suite spawns `trellis-testkit` (from `PATH`, else the
workspace's debug build) for a Postgres cluster that is torn down when the
test run exits. It needs the Postgres server binaries on `PATH`, like the
Rust tests.

`test/trellis/parity_test.exs` runs `clients/parity/cases.json`, the fixture
the Ruby suite runs too, so the two bindings can't drift apart unnoticed
(see `clients/parity/README.md`).

## Releasing

`trellis_pg` is released with the rest of Trellis, from one `v<version>` tag
([docs/releasing.md](../../docs/releasing.md)). `@version` in `mix.exs` and
the NIF crate's version (`native/trellis_nif/Cargo.toml`) are the release's.
`.github/workflows/release.yml` builds the NIF for each target above and
attaches them to the tag's GitHub release, where `lib/trellis/native.ex`
downloads them from. The package is then published to Hex by hand with
`.github/scripts/publish-packages.sh`, which writes
`checksum-Elixir.Trellis.Native.exs` from the NIFs that release serves.

`.github/scripts/build-elixir-nif.sh <target> <out-dir>` builds one NIF the
way the workflow does. Linux targets need `cargo-zigbuild` and Zig
(`pip install cargo-zigbuild ziglang`).
