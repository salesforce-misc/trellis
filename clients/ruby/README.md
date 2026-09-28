# Trellis for Ruby

Embedded Trellis for Ruby apps: a [Magnus](https://github.com/matsadler/magnus)
native extension over the `trellis` crate's `BlockingTrellis`, so a Rails app
can define and run streaming transforms without deploying a separate service.
The design is [ADR-0010](../../docs/decisions/0010-embeddable-clients.md); the
work is epic #140.

The surface mirrors the Rust crate's `BlockingTrellis` (issues #151 and
#152), and the Elixir binding's (`clients/elixir`) wherever Ruby and Elixir
don't call for a difference. Every method is on the `Trellis` module, which
holds the process's one handle.

- `Trellis.connect(url:, ...)`, `Trellis.migrate`, `Trellis.config`,
  `Trellis.shutdown`, `Trellis.connected?`: the handle's lifecycle.
- `Trellis.apply(statement)`: any statement of Trellis's grammar
  (`TRANSFORM`, `RELATIONSHIP`, `PAUSE TRANSFORM`, `RESUME TRANSFORM`,
  `DROP TRANSFORM`, `DROP RELATIONSHIP`, `ALTER TRANSFORM`), returning what
  it did as a `Trellis::Applied` whose `kind` names the outcome
  (`:transform_defined`, `:paused`, `:resumed`, ...).
  `Trellis.define(statement)` is an `apply` that refuses anything but a
  `TRANSFORM`, and returns its `Trellis::Definition`.
- `Trellis.status(target_table)`, `Trellis.definitions`,
  `Trellis.relationships`: the catalog reads.
- `Trellis.quarantined`, `Trellis.quarantine_status(target)`,
  `Trellis.sample_quarantined(target, limit:, after:)`,
  `Trellis.poisoned_since(time)`: the quarantine reads. Resuming is a
  statement: `Trellis.apply("RESUME TRANSFORM order_totals.total")`.
- `Trellis.request_backfill(source_table)`,
  `Trellis.has_live_drain_workers?`, `Trellis.has_live_staging_worker?`,
  `Trellis.watermark_token`, `Trellis.await_converged(token, timeout_ms:)`.
- `Trellis.self_check(target_table, limit:, timeout_ms:, after:, mode:)`:
  audit one page of a target against a fresh recompute from its source,
  reporting any divergence.

```ruby
require "trellis"

# A deploy's migration step: the defaults run nothing in the background.
Trellis.connect(url: "host=localhost dbname=app")
Trellis.migrate
Trellis.shutdown

# The app, once migrated. The staging worker reads the catalog as it starts,
# so a `staging: true` handle can't connect before `migrate` has run.
Trellis.connect(url: "host=localhost dbname=app", staging: true, drain_threads: 2)
Trellis.define("TRANSFORM widget_prices FROM widgets SELECT price AS price")
# => #<data Trellis::Definition id=1, target_table="public.widget_prices", ...,
#    status=:waiting_to_backfill, source_columns={"id"=>"integer", "price"=>"integer"}>
Trellis.status("widget_prices")
# => #<data Trellis::Status status=:live, backfill_failure=nil>, once backfilled

# A column that keeps failing is paused, not fatal: find it, see why, fix
# the data, resume it.
Trellis.quarantined
# => [#<data Trellis::QuarantineEntry target="widget_prices.price", state=:paused, ...>]
page = Trellis.sample_quarantined("widget_prices.price", limit: 50)
page.samples # => [#<data Trellis::PoisonSample src_table="public.widgets", key="7", ...>]
Trellis.apply("RESUME TRANSFORM widget_prices.price").columns
# => ["widget_prices.price"]
Trellis.shutdown
```

`connect`'s options, all explicit (nothing is read from the environment):

| Option | Default | |
|---|---|---|
| `url:` | required | a libpq connection string or URL |
| `schema:` | `"trellis"` | the schema Trellis keeps its own tables in |
| `target_schema:` | `"public"` | where a bare target table name is created |
| `staging:` | `false` | run the staging worker (change capture) here |
| `drain_threads:` | `0` | threads applying staged changes to the targets |
| `worker_threads:` | `2` | the Rust runtime's threads, invisible to Ruby's own sizing |

The defaults run nothing in the background. Exactly one process in a fleet
should set `staging: true`, and some process must run drain threads, or no
definition ever reaches `:live`.

### Conventions

- **One handle per process, held by the `Trellis` module** (ADR-0010
  decision 3). `connect` raises if the process is already connected;
  `shutdown` it first. An `at_exit` hook shuts it down if the app didn't.
- **Every call releases the GVL** while it waits on the database, so other
  threads (Puma's request threads, say) keep running, and `Thread#kill`,
  `Thread#raise` and Ctrl-C interrupt a thread waiting in one. An interrupt
  abandons the wait, not the work: a `define` that was interrupted may still
  register its transform (`Trellis.status` tells you whether it did), and
  the handle serves no other call until the abandoned one has finished.
- **A handle doesn't survive `fork`.** Connect after forking: Puma's
  `on_worker_boot`, Passenger's `starting_worker_process`. A forked child's
  calls on a handle it inherited raise `Trellis::ForkedHandleError` rather
  than hang. `Trellis.shutdown` in a child that hasn't connected does
  nothing. If the parent connects too (to migrate, say), shut it down before
  forking. A child forked while the parent's handle was running (connected,
  connecting on another thread, or not yet fully shut down) may have
  inherited a lock one of that handle's threads held, which nothing in the
  child can release, so its `connect` raises `Trellis::ForkedHandleError`
  too, rather than risk hanging on it (issue #600). Forks whose children
  never connect, and `system`/`spawn`, are unaffected.

  ```ruby
  # config/puma.rb
  before_fork do
    Trellis.shutdown
  end

  on_worker_boot do
    Trellis.connect(url: ENV.fetch("TRELLIS_URL"), drain_threads: 1)
  end
  ```
- **Errors** are raised as a subclass of `Trellis::Error` per error code
  (`ParseError`, `ValidationError`, `ConnectivityError`, `ConflictError`,
  `NotFoundError`, `InternalError`, `TimeoutError`), each with `#code` (a
  symbol) and `#message`. A code newer than the binding arrives as
  `UnknownError`, naming the code in its message. `ForkedHandleError` is a
  `ValidationError`. Raising is Ruby's idiom, so where the Elixir binding
  has `{:ok, value}` / `{:error, error}` and bang variants, every Ruby
  method returns its value or raises.
- **Words are symbols** (statuses, quarantine states, cardinalities,
  `Applied` kinds, `self_check` outcomes and divergence kinds), each from a
  closed set: none is ever made from a string the database returned.
- **Times are `Time`s** in UTC, at microsecond precision. They cross the
  extension as epoch microseconds.
- **A quarantine target is an address string**, `"transform"` or
  `"transform.column"`, exactly as `Trellis.quarantined` reports it.
- **Cursors and tokens are opaque strings**: `sample_quarantined`'s
  `next_cursor`, `self_check`'s `next_after` and `watermark_token`'s token.
  Pass back what the previous call returned.
- **Durations are milliseconds**, as `timeout_ms:`, matching the Elixir
  binding.
- **Results are `Data` values** (`Trellis::Definition`,
  `Trellis::QuarantineEntry`, ...), so they compare by value and
  pattern-match: `case Trellis.apply(stmt) in { kind: :resumed, columns: }`.

## Layout

- `lib/`: the `Trellis` module, its error classes and its `Data` values.
- `ext/trellis_ruby/`: the extension crate, a member of the repository's
  Cargo workspace. Plain-data conversion and error codes come from
  `clients/embed` (`trellis-embed`), shared with the Elixir binding.

The crate's code sits behind its `ruby` feature: rb-sys needs a Ruby install
and libclang to build, and the Rust-only CI job and `verify` have neither, so
without the feature (`cargo build --workspace`) it compiles to an empty
library. The root manifest leaves it out of `default-members` too.

There's no gemspec yet: the published gem name is one of ADR-0010's open
questions, so packaging comes later.

## Development

Ruby is pinned in `.tool-versions`. rb-sys generates its bindings with
libclang, so building needs it (`libclang-dev` on Debian/Ubuntu,
`clang-devel` on Fedora). From this directory:

```sh
cargo build -p testkit --bin trellis-testkit   # the suite's throwaway Postgres
bundle install
bundle exec rake test
```

`rake compile` (which `rake test` runs first) builds the extension through
Cargo into the workspace's `target/`, telling rb-sys which Ruby to build
against, and copies it to `lib/trellis/`. The suite spawns
`trellis-testkit` (from `PATH`, else the workspace's debug build) for a
Postgres cluster that is torn down when the run exits. It needs the Postgres
server binaries on `PATH`, like the Rust tests.

The tests use Minitest: it ships with Ruby, and its plain `assert_*` style
reads like the Elixir binding's ExUnit suite, which this one mirrors.
`test/parity_test.rb` runs `clients/parity/cases.json`, the fixture the
Elixir suite runs too, so the two bindings can't drift apart unnoticed (see
`clients/parity/README.md`).
