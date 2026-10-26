# Trellis

> **0.1.0-alpha, pre-release:** the API and stored definitions may change.

A declarative API for creating data transformations in PostgreSQL.
Correctness and performance are the key design goals.
All state managed in Postgres for simplified architecture and efficient, provably correct transforms.
The known exceptions to that guarantee are listed in [known correctness gaps](docs/known-correctness-gaps.md).

## Why incrementally maintained tables?

Applications often compute the same derived facts over and over — the same
aggregate, join, or enrichment recomputed on every request. That recomputation
is **spikey read load**: the cost lands at query time, scales with how often
you read, and grows as the underlying data grows, so the queries that matter
most are the ones that get slow under pressure.

Trellis inverts the trade-off. Instead of recomputing a derived table on every
read, it maintains that table **incrementally**: when a source row changes,
only the affected target rows are updated. This replaces spikey read load with
a **steady write load** — work proportional to how fast the source changes, not
to how often you query — and leaves behind a plain table that is cheap and
predictable to read.

The trade-off is worth it when a fact is read far more often than its inputs
change, or when read latency matters more than write throughput. It is the same
bargain a database index makes — pay a little on write to make reads fast.

Connections to Postgres use TLS when the database URL asks for it (`sslmode`, `sslrootcert`, `sslcert`, `sslkey`) and try it first when it does not; see [TLS to Postgres](docs/embedding.md#tls-to-postgres).

## Install

Pick the form that matches how you run it:

* **The `trellis` command-line tool** for defining transforms and running the pipeline from a shell: download `trellis-v<version>-<target>.tar.gz` for your platform from the [GitHub release](https://github.com/salesforce-misc/trellis/releases) and unpack it, or build it with `cargo install --path cli` from a checkout.
* **Rust**: the engine crate is published as `trellis-pg` (its library is `trellis`), so `trellis-pg = "<version>"` in `Cargo.toml` and `use trellis::...` in code.
* **Elixir**: the Hex package `trellis_pg`; see [clients/elixir](clients/elixir/README.md).
* **Ruby**: the gem `trellis-pg`; see [clients/ruby](clients/ruby/README.md).

[docs/embedding.md](docs/embedding.md) is the guide to running Trellis inside an application, and [docs/releasing.md](docs/releasing.md) lists what each release ships.

## A first transform

Trellis runs against a PostgreSQL database and a role that can create objects in it (see [Roles and permissions](docs/recommendations.md#roles-and-permissions)). Given a source table and, for an aggregate, an index on its `GROUP BY` columns:

```sql
CREATE TABLE orders (id bigint PRIMARY KEY, customer_id bigint NOT NULL, amount numeric(12,2) NOT NULL);
CREATE INDEX ON orders (customer_id);
```

define a transform and run the pipeline:

```sh
export TRELLIS_DATABASE_URL=postgres://user@localhost/shop

trellis apply "TRANSFORM customer_totals FROM orders GROUP BY customer_id
  SELECT customer_id AS customer_id, COUNT(*) AS order_count, SUM(amount) AS total"

trellis run      # until interrupted: captures changes and keeps the target current
trellis status   # in another shell: each definition's status, warnings and quarantined rows
```

`customer_totals` is now an ordinary table. It holds one row per customer, backfilled from the rows already in `orders`, and stays exactly equal to `SELECT customer_id, COUNT(*), SUM(amount) FROM orders GROUP BY customer_id` as `orders` changes. [docs/transforms.md](docs/transforms.md) is the reference for the grammar (1-1, aggregate and `JOIN` transforms) and for which sources and targets are supported, and [docs/known-correctness-gaps.md](docs/known-correctness-gaps.md) lists the ways a target can go stale or wrong without Trellis repairing it.

## Structure

This repo is home to a few key components:

* `trellis` is the rust crate that users will download and install into their application code
* `cli` (package `trellis-cli`) builds the `trellis` operator-facing binary, which wraps the `trellis` library crate for defining transforms, running the live pipeline, and checking status from a shell — run `cargo run -p trellis-cli -- --help` (or `trellis --help` once installed) to see what it can do
* `docs` outlines the key ideas and decisions this project has taken; [docs/recommendations.md](docs/recommendations.md) is the operator's guide to running Trellis well (WAL and checkpoint tuning, ring sizing, roles and permissions), and [docs/operations.md](docs/operations.md) says what to do when Trellis stops a definition and how each repair behaves, and [docs/releasing.md](docs/releasing.md) says how a release is cut and published
* `clients` holds the Elixir and Ruby bindings for running Trellis inside a host app; [docs/embedding.md](docs/embedding.md) is the guide to embedding it
* `benchmark` is a harness for measuring throughput, latency and other key-metrics
* `generative` is a generative test suite that is used to validate correctness under various scenarios and loads
* `testkit` is a reusable, test-only harness that spins up a throwaway PostgreSQL instance and hands out isolated, migrated databases per test scenario. It is never shipped in a user's binary — other crates pull it in as a dev-dependency and exercise it through `cargo test`. Its `trellis-testkit` binary provisions the same kind of cluster for test suites written in other languages; see `cargo run -p testkit --bin trellis-testkit -- --help`.

## Up & Running

This project uses [asdf](https://asdf-vm.com/) to define the default versions used for local/development work.
From the root of this project run `asdf install postgres` to make sure you have the appropriate version.

> For MacOS you often need to set brew install `icu4c` and then set `export PKG_CONFIG_PATH="$(brew --prefix icu4c)/lib/pkgconfig:$PKG_CONFIG_PATH"` before running the asdf install command
