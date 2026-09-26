# Trellis for Elixir

Embedded Trellis for Elixir apps: a [Rustler](https://github.com/rusterlium/rustler)
NIF over the `trellis` crate's `BlockingTrellis`, so a Phoenix app can define
and run streaming transforms without deploying a separate service. The design
is [ADR-0010](../../docs/decisions/0010-embeddable-clients.md); the work is
epic #140.

This is the vertical slice (issue #146): `connect/1`, `migrate/1`,
`define/2`, `status/2` and `shutdown/1`, each with a bang variant.

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
:ok = Trellis.shutdown(trellis)
```

`connect/1`'s defaults (`staging: false, drain_threads: 0`) run nothing in
the background. Exactly one connection in a fleet should set `staging: true`,
and some connection must run drain threads, or no definition ever reaches
`:live`.

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
