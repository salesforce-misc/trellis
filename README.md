# Trellis

A declarative API for creating data transformations in PostgreSQL.
Correctness and performance are the key design goals.
All state managed in Postgres for simplified architecture and efficient, provably correct transforms.

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

## Structure

This repo is home to a few key components:

* `trellis` is the rust crate that users will download and install into their application code
* `cli` (package `trellis-cli`) builds the `trellis` operator-facing binary, which wraps the `trellis` library crate for defining transforms/relationships, running the live pipeline, and checking status from a shell — run `cargo run -p trellis-cli -- --help` (or `trellis --help` once installed) to see what it can do
* `docs` outlines the key ideas and decisions this project has taken
* `benchmark` is a harness for measuring throughput, latency and other key-metrics
* `generative` is a generative test suite that is used to validate correctness under various scenarios and loads
* `testkit` is a reusable, test-only harness that spins up a throwaway, logical-replication-enabled PostgreSQL instance and hands out isolated, migrated databases per test scenario. It is never shipped in a user's binary — other crates pull it in as a dev-dependency and exercise it through `cargo test`. Its `trellis-testkit` binary provisions the same kind of cluster for test suites written in other languages; see `cargo run -p testkit --bin trellis-testkit -- --help`.

## Up & Running

This project uses [asdf](https://asdf-vm.com/) to define the default versions used for local/development work.
From the root of this project run `asdf install postgres` to make sure you have the appropriate version.

> For MacOS you often need to set brew install `icu4c` and then set `export PKG_CONFIG_PATH="$(brew --prefix icu4c)/lib/pkgconfig:$PKG_CONFIG_PATH"` before running the asdf install command
