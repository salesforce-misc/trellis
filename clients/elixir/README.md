# Trellis for Elixir

Embedded Trellis for Elixir apps: a [Rustler](https://github.com/rusterlium/rustler)
NIF over the `trellis` crate's `BlockingTrellis`, so a Phoenix app can define
and run streaming transforms without deploying a separate service. The design
is [ADR-0010](../../docs/decisions/0010-embeddable-clients.md); the work is
epic #140.

The surface mirrors the Rust crate's `BlockingTrellis` (issues #146 and
#147). Every function has a bang variant.

- `connect/1`, `migrate/1`, `shutdown/1`: the handle's lifecycle.
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
- `sample_quarantined/3`'s cursor and `watermark_token/1`'s token are opaque:
  pass back what the previous call returned.
- Every call runs on a dirty IO scheduler.

## Layout

- `lib/`: the public `Trellis` module and its structs.
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
