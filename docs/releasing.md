# Releasing

A Trellis release is one version for everything it ships, cut from one tag,
`v<version>`, on a commit of `main`:

| What | Where | How it gets there |
|---|---|---|
| The `trellis` CLI | The tag's GitHub release | `.github/workflows/release.yml` |
| The Elixir binding's precompiled NIFs | The tag's GitHub release, where `trellis_pg` downloads them | `.github/workflows/release.yml` |
| The Ruby binding's platform gems and source gem | The tag's GitHub release | `.github/workflows/release.yml` |
| The engine crate, `trellis-pg` | crates.io | `.github/scripts/publish-packages.sh`, by hand |
| The Elixir binding, `trellis_pg` | Hex | `.github/scripts/publish-packages.sh`, by hand |
| The Ruby binding, `trellis-pg` | RubyGems | `.github/scripts/publish-packages.sh`, by hand |

## The version

Every version below is the release's:

- `trellis/Cargo.toml`, `cli/Cargo.toml`, `clients/embed/Cargo.toml`,
  `clients/elixir/native/trellis_nif/Cargo.toml` and
  `clients/ruby/ext/trellis_ruby/Cargo.toml`, and their entries in
  `Cargo.lock`
- `@version` in `clients/elixir/mix.exs`
- `Trellis::VERSION` in `clients/ruby/lib/trellis/version.rb`, and its entry
  in `clients/ruby/Gemfile.lock`

The release builds run with `--locked` and a frozen bundle, so a lockfile
left behind fails them. `.github/scripts/bump-version.sh <version>` sets all
of them and checks the result with `.github/scripts/check-release-versions.sh`;
it updates no dependency. Commit what it changes as an ordinary pull request,
and tag the merged commit on `main`.

A prerelease (`0.1.0-alpha`) is released the same way, as a GitHub
prerelease. RubyGems writes its `-` as `.pre.`, so its gems are
`0.1.0.pre.alpha`.

## Cutting a release

1. Write the release notes into a **draft** GitHub release for the tag, on
   the commit to release:
   `gh release create v<version> --draft --target <commit> --title "Trellis <version>" --notes-file <notes>`.
   The notes open with a summary of what shipped and the validation the
   release had beyond CI (runs, hours and source rows written by the
   benchmarks, generative sweeps, soak and chaos runs and repro loops, from
   the maintainers' validation logbook).
2. Run the `.github/scripts/bump-version.sh <version>` command and commit the changes
3. Push the annotated tag `v<version>` on that commit to
   `salesforce-misc/trellis`.

The tag starts `release.yml`. It checks the versions, then builds:

- the CLI for x86_64 and aarch64 Linux (glibc 2.17 and later, and static
  musl builds for Alpine and scratch images) and for Apple silicon and Intel
  macOS, as `trellis-v<version>-<target>.tar.gz`;
- the NIF for every target `Trellis.Native` lists;
- the six platform gems and the source gem, installing and loading each;

and checks the Hex package builds from source. Each CLI build also checks
its binary carries none of the engine's test-only code
(`.github/scripts/check-release-binary.sh`, which CI runs on every change
too). It then attaches everything,
with a `SHA256SUMS` file, to the draft and publishes it. With no draft for
the tag, it creates the release itself, with short notes.

The release job refuses to replace the assets of a version already on
crates.io, Hex or RubyGems: the published packages were made from them (Hex's
checksum file names the exact NIFs). Before any package is published, a
failed release can be re-run.

Running the workflow by hand (Actions, "Release", Run workflow) is a dry run:
it builds and checks everything and keeps it as workflow artifacts, touching
no release.

## Publishing the packages

Once the GitHub release is published, publish the packages from the tag:

```sh
.github/scripts/publish-packages.sh <version> --dry-run
.github/scripts/publish-packages.sh <version>
```

It runs on Linux or macOS, from any checkout of the repository: only the
script comes from the checkout. It clones the tag into a scratch directory, checks the versions and every
asset against `SHA256SUMS`, then publishes, asking before each:

- **crates.io**: the engine crate as `trellis-pg` (`trellis` is taken
  there). Its library is still `trellis`, so code reads `use trellis::...`
  with `trellis-pg = "<version>"` in `Cargo.toml`. The rename is made in the
  published copy only.
- **Hex**: `trellis_pg`, built the way the workflow checks it, plus
  `checksum-Elixir.Trellis.Native.exs` written from the NIFs the release
  serves. Its NIF crate depends on `trellis` by git, at the tag's commit, for
  a host that builds the NIF from source.
- **RubyGems**: the release's gems, byte for byte, platform gems first, so no
  host finds only the source gem and compiles it for want of its platform's.

A package already on its registry is skipped, so a run that stopped halfway
is simply re-run. `--only crates,hex,gems` picks a subset. The script's header
lists the tools and credentials each registry needs (`cargo login`,
`mix hex.user auth`, `gem signin`).

## The upgrade battery

A release that has a published predecessor is gated on a battery run by hand
in the release cycle: the predecessor's released build defines and builds
databases of every definition shape, and the release candidate takes them
over. It is not a CI test and does not run on every pull request. Between
releases the design rules keep upgrades compatible: the append-only migration
list, the stored-definition rules
([grammar versions](transforms.md#grammar-versions),
[ADR-0018](decisions/0018-grammar-versioning.md)) and the grammar's mirror of
Postgres ([ADR-0022](decisions/0022-grammar-mirrors-postgres.md)). An
incompatibility is found at release time, not in the pull request that
introduced it, and each finding is filed as its own issue, labelled
`Next Release` when it blocks the release.

The starting point is the previous release (for `0.2.0`, `v0.1.0`). A
pre-release is not a supported starting point.

### Running it

```sh
.github/scripts/upgrade-battery.sh --candidate <commit-or-directory> [--old-tag v0.1.0] [step ...]
```

`upgrade-battery.sh --help` lists the options and the steps. The script

1. takes the predecessor's published CLI from its GitHub release and checks it
   against `SHA256SUMS` (`--build-old` builds the tag instead, which is also
   the fallback when the download fails), and builds the candidate's CLI;
2. builds `upgrade-battery/check`, a small probe over the `trellis` library
   for the two calls the CLI lacks (`self_check` and `dump_definitions_sql`),
   once against the tag and once against the candidate;
3. starts throwaway clusters of its own on `127.0.0.1` (it touches no other
   database), builds the databases with the old CLI, with writers running and
   ring segments the old build has not drained, stops the old build and starts
   the candidate on the same databases, with the writers still running; and
4. runs each step below, printing `PASS`, `FAIL`, `INFO`, `MANUAL` or `SKIP`
   per check, then a summary. It exits 1 on any `FAIL`.

`INFO` is an observation to quote in the release notes, `MANUAL` a check the
script does not run, and `SKIP` a step not selected or one whose tooling did
not build. A summary with a `SKIP` or `MANUAL` is not a pass of that step.

Record the run in the validation logbook (kind `acceptance`, see
[CLAUDE.md](../CLAUDE.md#validation-logbook)) and give its id in the release
notes. The script prints an estimate of the source rows it wrote; pass it to
`logbook finish --rows`. Logs, the clusters' data and the base backup stay
under `target/upgrade-battery/` (`--work`); `--keep` leaves the clusters
running for a manual step.

### The steps

| Step | What it checks | How |
|---|---|---|
| `migrations` | A database the old build created opens under the candidate: no pending migration, no checksum mismatch, the history rows unchanged. Control: with one stored checksum altered the candidate refuses. A candidate with a new migration changes the upgrade path and rules out downgrade: the script reports the difference as `INFO`, and the release notes say so. | Script |
| `definitions` | One of each shape the release supports, built under the old build with writers running and undrained ring segments in flight: a plain 1-1, a chained 1-1, a 1-1 with a paused column, a group-basis aggregate, an aggregate over a `JOIN` (inner and `LEFT`), an aggregate and a `JOIN` over a hash-partitioned source, a 1-1 with a held key, and a paused transform. Under the candidate each resumes, converges and has a clean `self_check`; the held key and the pauses are unchanged; nothing else pauses. A plain recompute of two targets in SQL cross-checks `self_check`. | Script |
| `grammar` | A definition the old build accepted and the candidate refuses pauses with a reason that names the remedy, not an error loop and not silent staleness, and the remedy works: a bare reserved word as a column name, and an expression nested over 100 levels (as a chain and as parentheses). | Script, plus the manual cases below |
| `inflight` | Upgrading while a definition is mid-build, mid-rebuild (a `RESUME`), waiting for capture, paused, or holding a key: each continues to `live`, or stays as it was. A held key stands for quarantine: the script does not trip the whole-definition fuse, so no definition is `quarantined` at the upgrade. | Script |
| `structure` | A `structure.sql` made by the old build (a `pg_dump --schema-only` of the Trellis schema and `public`, plus the old build's `dump_definitions_sql`) loads into an empty database and every definition builds. | Script |
| `pgrestore` | A `pg_dump` and `pg_restore` of the old database opens under the candidate, pauses every definition with the documented reason ([operations — runbooks](operations.md#runbooks)), and `RESUME` brings each back to `live` with a clean `self_check`. | Script |
| `basebackup` | A `pg_basebackup` taken while the old build and its writers ran starts as a cluster, and the candidate converges it with no pause and no `RESUME`. | Script |
| `clients` | The Ruby gem and the Elixir package built from the candidate open a database the predecessor's clients used, with the same options. | Manual |
| `mixed` | The candidate attached while an old instance is still attached to the database refuses, or pauses the definitions with a reason. | Script |
| `downgrade` | What the old build does when pointed at a database the candidate has written. | Script, observed |

### The manual steps, and what the script expects

**Clients (step 6).** Open one database the old clients used from both
versions, with the same options the old clients were given (the
connection-budget options, `max_call_pool_size` and
`call_pool_wait_timeout_ms`, came with `0.1.0`).
`upgrade-battery.sh --candidate <candidate> --keep clients` leaves a cluster
holding a database the old CLI built and ran and the candidate has not opened
(`postgres://postgres@127.0.0.1:54920/clients`, at the default `--port`);
then, from the candidate's checkout:

```sh
export DSN=postgres://postgres@127.0.0.1:54920/clients
tmp=$(mktemp -d)
cat >"$tmp/open.rb" <<'RUBY'
require "trellis/pg"
Trellis.connect(url: ENV.fetch("DSN"), staging: true, drain_threads: 2,
                max_call_pool_size: 5, call_pool_wait_timeout_ms: 30_000)
Trellis.migrate
Trellis.definitions.each { |d| puts "#{d.target_table} #{d.status}" }
RUBY
# Ruby: the predecessor's published gem, then the candidate's, from source.
gh release download v0.1.0 --repo salesforce-misc/trellis -p 'trellis-pg-0.1.0-x86_64-linux.gem' --dir "$tmp"
(cd clients/ruby && gem install --local --no-document --ignore-dependencies --install-dir "$tmp/gems-old" "$tmp"/trellis-pg-0.1.0-x86_64-linux.gem)
(cd clients/ruby && GEM_HOME=$tmp/gems-old GEM_PATH=$tmp/gems-old ruby "$tmp/open.rb")  # old
(cd clients/ruby && bundle install && bundle exec rake compile)
(cd clients/ruby && bundle exec ruby -Ilib "$tmp/open.rb")                           # candidate
# Elixir: the candidate's package, building its NIF from source.
(cd clients/elixir && TRELLIS_PG_BUILD=1 mix run --no-start -e '
  {:ok, t} = Trellis.connect(url: System.fetch_env!("DSN"), staging: true, drain_threads: 2,
                             max_call_pool_size: 5, call_pool_wait_timeout_ms: 30_000)
  {:ok, defs} = Trellis.definitions(t)
  for d <- defs, do: IO.puts("#{d.target_table} #{d.status}")')
```

The three runs print the same definitions and statuses, and `migrate` raises
no error. A client that cannot open the database, or a gem or package that
does not build, is a finding.

**Refusals the script does not build (step 3).** The script constructs the
refusals it can build with the old CLI. Check the rest by reading the
candidate's list of refusals against what the old build accepted (the
"Supported sources and targets" table in [transforms](transforms.md), the
changes since the predecessor's tag), and for each one the old build accepts,
define it under the old CLI, upgrade, and confirm the definition pauses with a
reason that names the remedy and the remedy works. Cases that the old build
refuses itself are not upgrade cases: a `JOIN` on a repeated column pair, an
index over more than 32 key columns, and (not tried by the script) a table
over 1600 columns. Add each new case to the script when you find one.

**Mixed versions (step 7).** No mixed-version combination is supported: stop
every instance, upgrade, start. The expectation the battery checks is that the
candidate refuses to attach beside a running old instance, or pauses the
definitions with a reason. A candidate that attaches without either fails the
step, and the finding is filed.

**Downgrade (step 8).** Downgrade is not supported. After the candidate has
attached to a database, the way back is a restore from a backup taken before
the upgrade. The script records what the old build does when pointed at a
database the candidate has written, for the release notes; it does not make
downgrade a supported path.

### Release notes checklist

For each release, the notes carry:

- **Definitions the new version refuses.** Every definition the previous
  release accepted and this one refuses pauses on upgrade, with its reason in
  `capture_failure`; the notes list each case, with the remedy (`DROP
  TRANSFORM`, then define it again in the current grammar). For `0.2.0`: a bare
  reserved word as a column name (`SELECT order AS o`; write `table.order`),
  and an expression nested over 100 levels (split it into calculated fields).
  Add every case from the manual refusal check.
- **The upgrade procedure:** stop every instance, upgrade, start; no mixed
  versions; downgrade by restoring a backup; whether the release adds a
  migration.
- **For databases that came from a pre-release,** which the battery does not
  start from:
  - a database loaded from a `0.1.0-gamma` `structure.sql` has aggregate
    `JOIN` lock tables with no row, so the locks a page or a build takes lock
    nothing, until the transform is paused and resumed; a `structure.sql`
    dumped by `0.1.0` or later carries the row;
  - the rebuild or first build of a 1-1 target keyed by an explicitly collated
    text column pauses after five attempts on `0.1.0-gamma`; run `RESUME` after
    upgrading.
- **The validation the release had:** the battery's logbook id and its result,
  with every `FAIL`, `SKIP` and `MANUAL` it left and how each was settled.
