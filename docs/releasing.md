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

Every version below is the release's, and `.github/scripts/check-release-versions.sh <version>`
checks they agree:

- `trellis/Cargo.toml`, `cli/Cargo.toml`, `clients/embed/Cargo.toml`,
  `clients/elixir/native/trellis_nif/Cargo.toml` and
  `clients/ruby/ext/trellis_ruby/Cargo.toml`
- `@version` in `clients/elixir/mix.exs`
- `Trellis::VERSION` in `clients/ruby/lib/trellis/version.rb`

Setting a new version is an ordinary pull request. A prerelease
(`0.1.0-alpha`) is released the same way, as a GitHub prerelease; RubyGems
writes its `-` as `.pre.`, so its gems are `0.1.0.pre.alpha`.

## Cutting a release

1. Write the release notes into a **draft** GitHub release for the tag, on
   the commit to release:
   `gh release create v<version> --draft --target <commit> --title "Trellis <version>" --notes-file <notes>`.
   The notes open with a summary of what shipped and the validation the
   release had beyond CI (runs, hours and source rows written by the
   benchmarks, generative sweeps, soak and chaos runs and repro loops, from
   the maintainers' validation logbook).
2. Push the annotated tag `v<version>` on that commit to
   `salesforce-misc/trellis`.

The tag starts `release.yml`. It checks the versions, then builds:

- the CLI for x86_64 and aarch64 Linux (glibc 2.17 and later, and static
  musl builds for Alpine and scratch images) and for Apple silicon and Intel
  macOS, as `trellis-v<version>-<target>.tar.gz`;
- the NIF for every target `Trellis.Native` lists;
- the six platform gems and the source gem, installing and loading each;

and checks the Hex package builds from source. It then attaches everything,
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
