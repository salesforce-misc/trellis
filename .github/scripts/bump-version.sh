#!/usr/bin/env bash
# Sets the release version everywhere a release reads it (docs/releasing.md):
# the five crates, mix.exs, Trellis::VERSION, and the lockfiles that record
# them, Cargo.lock and clients/ruby/Gemfile.lock. Then checks the result with
# check-release-versions.sh. Commit what it changes as one pull request.
#
# usage: bump-version.sh <version>      e.g. 0.1.0-alpha or 0.1.0
#
# Needs cargo and Ruby's bundler (clients/ruby/.tool-versions). Updates no
# dependency: Cargo.lock changes only the workspace's own entries, and
# `bundle lock` keeps every locked gem.
set -euo pipefail

version="${1:?usage: bump-version.sh <version>}"
version="${version#v}"
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] ||
  { echo "bump-version: $version isn't a version (want e.g. 0.1.0 or 0.1.0-alpha)" >&2; exit 64; }
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"

# perl rather than `sed -i`, whose syntax differs between GNU and BSD. Each
# replaces only the first match: a manifest's own [package] version.
for file in cli/Cargo.toml clients/embed/Cargo.toml clients/elixir/native/trellis_nif/Cargo.toml \
  clients/ruby/ext/trellis_ruby/Cargo.toml trellis/Cargo.toml; do
  V="$version" perl -0pi -e 's/^version = "[^"]*"$/version = "$ENV{V}"/m' "$file"
done
V="$version" perl -0pi -e 's/^  \@version "[^"]*"$/  \@version "$ENV{V}"/m' clients/elixir/mix.exs
V="$version" perl -0pi -e 's/VERSION = "[^"]*"/VERSION = "$ENV{V}"/' clients/ruby/lib/trellis/version.rb

# Rewrites only the workspace members' entries.
cargo update --workspace --quiet
# Rewrites the gem's own entry from the gemspec.
(cd clients/ruby && bundle lock >/dev/null)

.github/scripts/check-release-versions.sh "$version"
git status --short
