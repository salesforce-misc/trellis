# Trellis for Ruby

Embedded Trellis for Ruby apps: a [Magnus](https://github.com/matsadler/magnus)
native extension over the `trellis` crate's `BlockingTrellis`, so a Rails app
can define and run streaming transforms without deploying a separate service.
The design is [ADR-0010](../../docs/decisions/0010-embeddable-clients.md); the
work is epic #140.

The surface mirrors the Rust crate's `BlockingTrellis` (issues #151 and
#152), and the Elixir binding's (`clients/elixir`) wherever Ruby and Elixir
don't call for a difference. Every method is on `Trellis::Instance`, one
catalog schema's handle, and a process holds as many as it has schemas. The
`Trellis` module is the shortcut for the common case: it holds the process's
default instance and delegates every call to it.

## Installing

The gem is `trellis-pg` (the name `trellis` is taken on RubyGems), required
as `trellis/pg`. The module is still `Trellis`.

```ruby
# Gemfile
gem "trellis-pg"
```

`Bundler.require`, which a Rails app's `config/application.rb` runs,
requires it by itself. Anywhere else, `require "trellis/pg"`.

Platform gems carry a prebuilt extension for Ruby 3.3, 3.4 and 4.0 on:

| Platform | Gem platform |
|---|---|
| Linux, glibc 2.29 or later, x86_64 and arm64 | `x86_64-linux`, `aarch64-linux` |
| Linux, musl (Alpine), x86_64 and arm64 | `x86_64-linux-musl`, `aarch64-linux-musl` |
| macOS, Apple silicon and Intel | `arm64-darwin`, `x86_64-darwin` |

Anywhere else, RubyGems installs the source gem, which builds the extension
as it installs. That needs a Rust toolchain (releases are built with Rust
1.98), libclang (rb-sys generates its bindings with it: `libclang-dev` on
Debian and Ubuntu, `clang-devel` on Fedora) and network access, since Cargo
fetches the `trellis` crate from this repository at the released commit, and
its dependencies from crates.io. To build from source on a listed platform
too, on a glibc older than 2.29 say, ask for the source gem:
`gem install trellis-pg --platform ruby`, or
`gem "trellis-pg", force_ruby_platform: true` in a Gemfile.

Neither kind of gem includes the `trellis` crate's optional `otlp` feature
(OTLP trace export), so neither pulls in an OpenTelemetry or gRPC stack.
Nothing in the binding uses it.

## Usage

- `Trellis.connect(url:, ...)`, `Trellis.migrate`, `Trellis.config`,
  `Trellis.shutdown`, `Trellis.connected?`: the default instance's
  lifecycle. `Trellis::Instance.connect(url:, ...)` returns another
  instance, which has every call below as a method (see
  [Several instances](#several-instances)).
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
  `Trellis.release_key(transform, source_table, key)` releases one key a
  transform holds, once its cause is fixed; `status` reports how many it
  holds (`held_keys`). `status` also reports a page the drain keeps failing
  on with nothing charged or paused (`drain_failure`, a
  `Trellis::DrainFailure`), and a finished `self_check` job's report lists
  every one open (`drain_failures`). `status` lists the join columns of the
  relationships a definition reads that have no usable index
  (`unindexed_joins`, each a `Trellis::UnindexedJoin` with `relationship`,
  `table`, `column` and the `fix` sentence), a warning that never changes its
  status.
- `Trellis.request_backfill(source_table)`,
  `Trellis.has_live_drain_workers?`, `Trellis.has_live_staging_worker?`,
  `Trellis.watermark_token`, `Trellis.await_converged(token, timeout_ms:)`.
- `Trellis.self_check(target_table, timeout_ms:, mode:)` and
  `Trellis.self_check_job(id)`: check a target against a fresh recompute from
  its source, as a background job. `self_check` starts it and returns a
  `Trellis::SelfCheckJob` (`id`, `target`, `mode`, `state`, `rows_compared`,
  `report`, `error`) at once; a drain worker runs it, and `self_check_job`
  reads it back by `id` until `state` is `:done` (`:queued` and `:running` are
  in progress; `:failed` and `:cancelled` carry an `error`; `finished?` says
  which), when `report` is a `Trellis::SelfCheckReport` (`outcome`,
  `divergences`, `rows_compared`, `truncated`, ...). A target whose job is
  still unfinished gets that job back; the next call replaces a finished one,
  and `self_check_job` returns `nil` for the replaced job or one whose
  transform was dropped. With no drain worker anywhere in the fleet a job
  stays `:queued`.

```ruby
require "trellis/pg"

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
# => #<data Trellis::Status status=:live, backfill_failure=nil, capture_wait=nil,
#    capture_failure=nil>, once backfilled

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
| `schema:` | `"trellis"` | the schema Trellis keeps its own tables in; not `"public"`, not `target_schema:`, and not another instance's schema or target schema |
| `target_schema:` | `"public"` | where a bare target table name is created |
| `staging:` | `false` | run the staging worker (installs change capture, starts backfills) here |
| `drain_threads:` | `0` | threads applying staged changes to the targets |
| `worker_threads:` | `2` | the threads of each Rust runtime a handle owns (its calls', and its background client's), invisible to Ruby's own sizing |

The defaults run nothing in the background. Exactly one process in a fleet
should set `staging: true`, and some process must run drain threads, or no
definition ever reaches `:live`.

### Several instances

A process can hold several Trellis instances, for different catalog schemas
in one database or for different databases. `Trellis::Instance.connect`
takes `Trellis.connect`'s options and returns an instance with the same
methods:

```ruby
reports = Trellis::Instance.connect(url: "host=localhost dbname=app", schema: "reports")
reports.migrate
reports.define("TRANSFORM monthly_totals FROM orders SELECT total AS total")
reports.status("monthly_totals").status # => :live, once it has backfilled
reports.shutdown                        # every other instance keeps running
```

Each instance has its own connections, threads and `worker_threads` cap;
nothing is shared between them. `Trellis.connect` opens the default
instance, `Trellis.default_instance` returns it, and a second
`Trellis.connect` raises while it is connected. `Trellis::Instance.connect`
has no such limit. `Trellis::Instance.connected` lists the instances the
process holds, and `Trellis::Instance.shutdown_all` shuts them all down, the
default one included.

### Conventions

- **One default instance, held by the `Trellis` module** (ADR-0010
  decision 3). `Trellis.connect` raises if it is already connected;
  `Trellis.shutdown` it first. An `at_exit` hook shuts down every instance
  the process holds if the app didn't.
- **Every call releases the GVL** while it waits on the database, so other
  threads (Puma's request threads, say) keep running, and `Thread#kill`,
  `Thread#raise`, `Timeout.timeout` and Ctrl-C interrupt a thread waiting in
  one. The thread waits for the reply itself, so an interrupt leaves no thread
  behind. It abandons the wait, not the work: a `define` that was interrupted
  may still register its transform (`Trellis.status` tells you whether it
  did). Every call ends within 30 seconds on its own, and
  the server stops what it started at that deadline, so an abandoned call
  holds a pooled connection for at most that long. A call that runs out of
  time raises `Trellis::TimeoutError`. A call stuck on a lock doesn't hold up the others:
  calls from different threads run in parallel, in no particular order.
  `shutdown` doesn't wait for a call stuck on a lock either: it cancels the
  calls in flight, and they raise `Trellis::ValidationError`.
- **A handle doesn't survive `fork`.** Connect after forking: Puma's
  `before_worker_boot` (`on_worker_boot` before Puma 7), Passenger's
  `starting_worker_process`. A forked child's
  calls on a handle it inherited raise `Trellis::ForkedHandleError` rather
  than hang. `Trellis.shutdown` in a child that hasn't connected does
  nothing. If the parent connects too (to migrate, say), shut it down before
  forking. A child forked while the parent's handle was running (connected,
  connecting on another thread, or not yet fully shut down) may have
  inherited a lock one of that handle's threads held, which nothing in the
  child can release, so its `connect` raises `Trellis::ForkedHandleError`
  too, rather than risk hanging on it (issue #600). Forks whose children
  never connect, and `system`/`spawn`, are unaffected. [Forking
  servers](#forking-servers) has the hooks for Puma (with `preload_app!`
  and `fork_worker`), Unicorn and Passenger.

  ```ruby
  # config/puma.rb, in a Rails app (see "In a Rails app" below)
  before_fork { Trellis::Instance.shutdown_all }
  before_worker_boot { Trellis::Railtie.connect }
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
  `Applied` kinds, `self_check` modes, states and outcomes, and divergence kinds), each from a
  closed set: none is ever made from a string the database returned.
- **Times are `Time`s** in UTC, at microsecond precision. They cross the
  extension as epoch microseconds.
- **A quarantine target is an address string**, `"transform"` or
  `"transform.column"`, exactly as `Trellis.quarantined` reports it.
- **Cursors and tokens are opaque strings**: `sample_quarantined`'s
  `next_cursor` and `watermark_token`'s token. Pass back what the previous call
  returned.
- **Durations are milliseconds**, as `timeout_ms:`, matching the Elixir
  binding.
- **Results are `Data` values** (`Trellis::Definition`,
  `Trellis::QuarantineEntry`, ...), so they compare by value and
  pattern-match: `case Trellis.apply(stmt) in { kind: :resumed, columns: }`.

## In a Rails app

`require "trellis/pg"` after Rails (which `Bundler.require` in
`config/application.rb` does) loads `Trellis::Railtie`. Without Rails, the
gem loads no Rails code and requires no gem at all; `activerecord` and
`railties` are only ever the app's own dependencies.

### Configuration and boot

The Rails integration reads `Trellis.connect`'s options from one place,
`config.trellis.connect`:

```ruby
# config/application.rb (or config/environments/*.rb)
config.trellis.connect = {
  url: ENV.fetch("DATABASE_URL"),
  # The one dedicated worker process sets TRELLIS_WORKER=1.
  staging: ENV["TRELLIS_WORKER"] == "1",
  drain_threads: ENV["TRELLIS_WORKER"] == "1" ? 2 : 0
}
```

With it set, the app's default instance connects as the app boots (an
`after_initialize` hook), and the `at_exit` hook shuts it down. Left `nil`,
the default, the Railtie connects no default instance.

An app with more instances names them in `config.trellis.instances`, each
with `Trellis::Instance.connect`'s options. They connect with the default
instance (which is optional when they are set), and
`Trellis::Railtie.named_instance(:reports)` returns one:

```ruby
config.trellis.instances = {
  reports: { url: ENV.fetch("REPORTS_DATABASE_URL"), schema: "reports" }
}
```

If any instance fails to connect, the ones that did are shut down again,
and `Trellis::Railtie.connect` raises while one of them is already
connected, as `Trellis.connect` does.
With neither set, the Railtie does nothing. Two exceptions:

- **An app booted by a rake task gets no boot handle.** `rails db:create`
  may run before the database exists, and `rails db:migrate` must not start
  a worker's background work, so the process stays disconnected, as Ecto
  migrates without starting the app. The migration helpers connect a handle
  of their own. A rake task of your own that calls Trellis calls
  `Trellis::Railtie.connect` first. It's the running task that counts:
  `rails test` boots the app outside any task and connects, but tests run
  by a rake task (`bin/rails db:test:prepare test`, or plain `rake`) don't,
  so a test helper that needs the handles can call
  `Trellis::Railtie.connect if Trellis::Instance.connected.empty?`.
- **`config.trellis.connect_on_boot = false`** skips the boot connect, for a
  server that forks without a hook to shut the handle down first (a
  preloading Passenger, say). Connect in each child with
  `Trellis::Railtie.connect`.

### Forking servers

A handle doesn't survive `fork` (issue #600). A server that loads the app
before it forks has to shut the boot handle down in the parent and connect
one in each child. Puma preloads by default whenever it runs more than one
worker:

```ruby
# config/puma.rb
before_fork { Trellis::Instance.shutdown_all } # the parent's boot handles
before_worker_boot { Trellis::Railtie.connect } # on_worker_boot before Puma 7

# Only with fork_worker, where worker 0 forks the others (at boot, on a
# respawn and on a refork): shut its handle down around each fork. The
# master, already disconnected by before_fork, stays that way.
trellis_was_connected = false
before_worker_fork { trellis_was_connected = !Trellis::Instance.connected.empty?; Trellis::Instance.shutdown_all }
after_worker_fork { Trellis::Railtie.connect if trellis_was_connected }
```

(`before_refork`/`after_refork` aren't enough for `fork_worker`: worker 0
forks the first workers at boot, and respawns a dead one, without running
them.) Without `preload_app!`, each worker boots the app itself and the boot
connect is all it needs: leave these hooks out, since `Trellis` isn't loaded
yet when they run.

Unicorn's `before_fork` and `after_fork`, and Passenger's
`starting_worker_process` (with `connect_on_boot = false`), work the same
way. If the hooks are missing, the child's calls raise
`Trellis::ForkedHandleError` rather than hang. (Puma logs a hook's
exception as a warning and boots the worker anyway, so a failed connect
shows up as that warning and then as errors from the worker's calls.)

### Migrations

`Trellis::Migration` makes Trellis statements read like the migration's
own:

```ruby
class DefineOrderTotals < ActiveRecord::Migration[8.1]
  include Trellis::Migration

  # Required: the helpers raise before applying anything if the migration
  # has a transaction open.
  disable_ddl_transaction!

  def up
    define "TRANSFORM order_totals FROM orders SELECT price + tax AS total"
  end

  def down
    apply "PAUSE TRANSFORM order_totals"
    apply "DROP TRANSFORM order_totals"
  end
end
```

- `define`, `apply` and `status` are `Trellis.define`, `Trellis.apply` and
  `Trellis.status`, run as the migration reaches them, so a transform can
  read a table created earlier in the same migration.
- **No transaction.** Trellis never joins the migration's transaction: each
  call commits on Trellis's own connections. `define` and `apply` raise
  `ActiveRecord::MigrationError` before applying anything if the
  migration's connection has a transaction open, so a migration missing
  `disable_ddl_transaction!` rolls back with nothing applied on either
  side.
- **`up` and `down`, not `change`.** ActiveRecord can't reverse a Trellis
  statement: undoing a define is a `PAUSE TRANSFORM` and a `DROP
  TRANSFORM` of a target only the statement names. A migration that
  includes `Trellis::Migration` and defines a public `change` raises
  before anything runs.
- **Which handle.** The helpers run on the default instance. If the process
  is connected, they use its handle. Otherwise (`rails db:migrate` is never connected), each call
  connects one from `config.trellis.connect`, always with `staging: false`
  and `drain_threads: 0`, runs `Trellis.migrate`, makes its call and shuts
  the handle down.
- **Once per database.** A define isn't idempotent, and the helpers add no
  guard: `schema_migrations` already records which migrations have run.
  A migration that has to tolerate a transform defined some other way can
  guard its define itself, if you choose to:

  ```ruby
  def up
    define "TRANSFORM order_totals FROM orders SELECT price + tax AS total" if status("order_totals").nil?
  end
  ```

  The guard checks the target's name, not its definition. A changed
  statement for a target that exists is an `ALTER TRANSFORM`, in a
  migration of its own.
- **Schema dumps don't carry Trellis's definitions.** `db/schema.rb` (or
  `structure.sql`) records a transform's target table as a plain table and
  its migration as run, but not the definition, which is a row in Trellis's
  own tables. A database built from the dump rather than the migrations
  (`db:schema:load`, `db:prepare` or `db:setup` on a new database, or the
  test database that `maintain_test_schema!` loads) has the target tables
  and no transforms: nothing ever writes them, and a define of one fails
  with `ConflictError`, since its table exists. Build a database that needs
  its transforms by running the migrations (`bin/rails db:create
  db:migrate`).

`rails trellis:migrate` creates or upgrades Trellis's own tables the same
way. The helpers run it whenever a migration defines something; run it in
the deploy step as well (`bin/rails trellis:migrate db:migrate`), so a
deploy that upgrades the gem without a new define still upgrades the
tables before the worker restarts.

`Trellis::Railtie.with_handle { ... }` is what `trellis:migrate` and the
helpers use, for a script or rake task of your own that needs Trellis the
way a migration does. It yields the instance, and `with_handle(:reports)`
runs on an instance of `config.trellis.instances`, which is how a migration
reaches one (`Trellis::Railtie.with_handle(:reports) { |reports|
reports.define "TRANSFORM ..." }`). `trellis:migrate` migrates the default
instance and every instance of `config.trellis.instances`. Neither it nor
`trellis:migrate` needs ActiveRecord.
Outside Rails, `require "trellis/migration"` and `Trellis.connect` before
migrating.

## Layout

- `lib/`: the `Trellis` module and `Trellis::Instance`, its error classes and its `Data` values,
  and the Rails integration (`trellis/railtie.rb`, `trellis/migration.rb`),
  loaded only in an app that has Rails.
- `ext/trellis_ruby/`: the extension crate, a member of the repository's
  Cargo workspace, and the `extconf.rb` that builds it with rb-sys.
  Plain-data conversion and error codes come from `clients/embed`
  (`trellis-embed`), shared with the Elixir binding.
- `trellis-pg.gemspec`: the source gem. The `Rakefile` derives the platform
  gems from it.

The crate's code sits behind its `ruby` feature: rb-sys needs a Ruby install
and libclang to build, and the Rust-only CI job and `verify` have neither, so
without the feature (`cargo build --workspace`) it compiles to an empty
library. The root manifest leaves it out of `default-members` too.

## Development

Ruby is pinned in `.tool-versions`. rb-sys generates its bindings with
libclang, so building needs it (`libclang-dev` on Debian/Ubuntu,
`clang-devel` on Fedora). From this directory:

```sh
cargo build -p testkit --bin trellis-testkit   # the suite's throwaway Postgres
bundle install
bundle exec rake test
```

`rake compile:dev` (which `rake test` runs first) builds the extension the
way installing the source gem does, through `ext/trellis_ruby/extconf.rb`,
but with Cargo's dev profile and into the workspace's `target/`, and copies
it to `lib/trellis/`. `rake compile` builds the release profile. The suite spawns
`trellis-testkit` (from `PATH`, else the workspace's debug build) for a
Postgres cluster that is torn down when the run exits. It needs the Postgres
server binaries on `PATH`, like the Rust tests.

The tests use Minitest: it ships with Ruby, and its plain `assert_*` style
reads like the Elixir binding's ExUnit suite, which this one mirrors.
`test/parity_test.rb` runs `clients/parity/cases.json`, the fixture the
Elixir suite runs too, so the two bindings can't drift apart unnoticed (see
`clients/parity/README.md`).

## Releasing

`.github/workflows/ruby-release.yml` builds and publishes the gems. Pushing
a `ruby-v<version>` tag, where `<version>` is `Trellis::VERSION`
(`lib/trellis/version.rb`), builds the six platform gems and the source gem,
installs and loads each one (every Linux gem on each Ruby it supports), and
pushes them all to RubyGems, platform gems first. Running the workflow by
hand does everything but the push, leaving the gems as workflow artifacts.

It publishes as a RubyGems [trusted publisher](https://guides.rubygems.org/trusted-publishing/),
so there's no API key to keep: on rubygems.org, `trellis-pg` (or, before its
first release, a pending trusted publisher for it) names this repository and
the workflow file `ruby-release.yml`.

A platform gem builds in rb-sys's cross-compiling container for its
platform, through `rb-sys-dock`, and takes one build of the extension per
Ruby minor version. `.github/scripts/build-ruby-gem.sh <platform> <out-dir>`
does that, locally too (it needs Docker or Podman; see the script). Adding a
Ruby version means adding it to that script's `RUBY_VERSIONS`, the
workflow's check matrix, the table above and, for a new oldest version,
`required_ruby_version` in the gemspec.

The source gem's extension crate has to build outside this repository, so
`.github/scripts/ruby-prepare-source-gem.sh` rewrites its path dependencies
as git dependencies on the released commit and gives it a lockfile, before
`gem build`. A `gem build` of an unprepared checkout makes a gem that only
installs from inside the checkout.
