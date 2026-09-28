# Trellis for Elixir

Embedded Trellis for Elixir apps: a [Rustler](https://github.com/rusterlium/rustler)
NIF over the `trellis` crate's `BlockingTrellis`, so a Phoenix app can define
and run streaming transforms without deploying a separate service. The design
is [ADR-0010](../../docs/decisions/0010-embeddable-clients.md); the work is
epic #140.

The surface mirrors the Rust crate's `BlockingTrellis` (issues #146, #147,
#587 and #152). Every function has a bang variant.

- `connect/1`, `migrate/1`, `config/1`, `shutdown/1`: the handle's lifecycle.
- `apply/2`: any statement of Trellis's grammar (`TRANSFORM`,
  `RELATIONSHIP`, `PAUSE TRANSFORM`, `RESUME TRANSFORM`, `DROP TRANSFORM`,
  `DROP RELATIONSHIP`, `ALTER TRANSFORM`), returning what it did as a tagged
  value (`{:transform_defined, definition}`, `:paused`,
  `{:resumed, addresses}`, ...). `define/2` is an `apply/2` that refuses
  anything but a `TRANSFORM`.
- `status/2`, `definitions/1`, `relationships/1`: the catalog reads.
- `quarantined/1`, `quarantine_status/2`, `sample_quarantined/3`,
  `poisoned_since/2`: the quarantine reads. Resuming is a statement.
- `request_backfill/2`, `has_live_drain_workers/1`,
  `has_live_staging_worker/1`, `watermark_token/1`, `await_converged/3`.
- `self_check/3`: audit one page of a target against a fresh recompute from
  its source, reporting any divergence.
- `start_link/1`, as `{Trellis, options}` in a supervision tree: a process
  that owns the handle, so every function above also takes its name.
- `Trellis.Migration`: `define`, `apply` and `status` in an Ecto migration.
- `Trellis.Metrics.render_prometheus/0`: the process-wide metrics registry
  as Prometheus text, for a `/metrics` route the host already serves (the
  binding opens no port). It needs no handle.
- `Trellis.LogBridge`: started with the `:trellis` application, it forwards
  the engine's log lines to `Logger` (`domain: [:trellis]`, the Rust module
  as `:target` metadata). Set `config :trellis, log_level: :info` to choose
  the most verbose level forwarded (default: `Logger.level()`, read once at
  start), or `config :trellis, log_bridge: false` to forward nothing. A
  `tracing` subscriber installed by another NIF never sees the engine's
  lines, since each native library links its own `tracing`; see
  `Trellis.LogBridge`. A line can be dropped if the bridge falls behind,
  and a warning says how many; logging never blocks or crashes the engine.

```elixir
# A deploy's migration step: the defaults run nothing in the background.
{:ok, migrator} = Trellis.connect(url: "postgres://localhost/app")
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
the background. Exactly one connection in a fleet should set `staging: true`,
and some connection must run drain threads, or no definition ever reaches
`:live`.

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
- `sample_quarantined/3`'s and `self_check/3`'s cursors and
  `watermark_token/1`'s token are opaque: pass back what the previous call
  returned.
- Every call that takes a handle runs on a dirty IO scheduler.
  `Trellis.Metrics.render_prometheus/0` runs on a dirty CPU scheduler.

## In a Phoenix app

Let the application's supervisor own the handle. `{Trellis, options}` starts
a process that connects with `connect/1`'s options as it starts, runs every
call made through its name, and shuts the handle down when the supervisor
stops it:

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

The calls run in the owning process one at a time, as they would on the
handle, so only that process waits on a dirty IO scheduler however many
processes call it. `shutdown/1` refuses the name; the supervisor stops it.

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

Add `import_deps: [:trellis]` to the app's `.formatter.exs` to keep
`define` and `apply` free of parentheses, like Ecto's own commands.

## Layout

- `lib/`: the public `Trellis` module and its structs, the process that
  owns a supervised handle (`Trellis.Owner`), and `Trellis.Migration`, which
  is compiled only when the host depends on `ecto_sql`.
- `native/trellis_nif/`: the NIF crate, a member of the repository's Cargo
  workspace. Plain-data conversion and error codes come from
  `clients/embed` (`trellis-embed`), shared with the Ruby binding.

## Development

The Erlang and Elixir versions are pinned in `.tool-versions`. From this
directory:

```sh
cargo build -p testkit --bin trellis-testkit   # the suite's throwaway Postgres
mix deps.get
mix test
```

`mix test` compiles the NIF through Cargo into the workspace's `target/`, and
the suite spawns `trellis-testkit` (from `PATH`, else the workspace's debug
build) for a Postgres cluster that is torn down when the test run exits. It
needs the Postgres server binaries on `PATH`, like the Rust tests.

`test/trellis/parity_test.exs` runs `clients/parity/cases.json`, the fixture
the Ruby suite runs too, so the two bindings can't drift apart unnoticed
(see `clients/parity/README.md`).
