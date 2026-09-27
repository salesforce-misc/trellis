# Trellis for Ruby

Embedded Trellis for Ruby apps: a [Magnus](https://github.com/matsadler/magnus)
native extension over the `trellis` crate's `BlockingTrellis`, so a Rails app
can define and run streaming transforms without deploying a separate service.
The design is [ADR-0010](../../docs/decisions/0010-embeddable-clients.md); the
work is epic #140.

This is the vertical slice (issue #151): enough to take a transform from
nothing to `:live` from `irb`. The rest of the `BlockingTrellis` surface
follows the Elixir binding's (`clients/elixir`).

- `Trellis.connect(url:, ...)`, `Trellis.migrate`, `Trellis.shutdown`,
  `Trellis.connected?`: the process's handle.
- `Trellis.define(statement)`: registers a `TRANSFORM` statement and returns
  a `Trellis::Definition`. Any other statement form (`DROP`, `PAUSE`, ...) is
  refused before anything is applied.
- `Trellis.status(target_table)`: the `Trellis::Status` of the definition
  writing a table, or `nil`.

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
  than hang, and `connect` in the child replaces it. `Trellis.shutdown` in a
  child that hasn't connected does nothing. If the parent connects too (to
  migrate, say), shut it down before forking: a child forked while the
  parent's handle is busy can inherit a lock one of its threads held.

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
  `ValidationError`.
- **Statuses are symbols** (`:waiting_to_backfill`, `:live`, ...) from a
  closed set: none is ever made from a string the database returned.
  Times are `Time`s in UTC.

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
