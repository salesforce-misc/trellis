#!/usr/bin/env bash
# Checks that every version a release ships equals <version>: the `trellis`
# crate (published to crates.io as `trellis-pg`), the CLI, the shared
# embedding crate, the Elixir NIF crate and mix.exs, and the Ruby extension
# crate and Trellis::VERSION. A `v<version>` tag releases all of them at once
# (docs/releasing.md), so they move together. So must the lockfiles that
# record them, Cargo.lock and clients/ruby/Gemfile.lock: the release builds
# run with --locked and a frozen bundle, and fail on a stale one.
# bump-version.sh sets them all.
#
# usage: check-release-versions.sh <version> [<repo-root>]
#
# Prints each version it reads, and exits 1 naming every one that differs.
# Portable to macOS's bash 3.2 (publish-packages.sh runs it there).
set -euo pipefail

usage="usage: check-release-versions.sh <version> [<repo-root>]"
want="${1:?$usage}"
want="${want#v}"
root="${2:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
# RubyGems writes a prerelease's `-` as `.pre.` (0.1.0-alpha is 0.1.0.pre.alpha).
gem_want="$(echo "$want" | sed 's/-/.pre./g')"

# locked_version <crate>: the version Cargo.lock records for that package.
locked_version() {
  awk -v name="$1" '
    $0 == "name = \"" name "\"" { found = 1; next }
    found && /^version = / { gsub(/"/, "", $3); print $3; exit }
  ' "$root/Cargo.lock"
}

bad=0
# check <label> <found> <want>
check() {
  if [ "$2" = "$3" ]; then
    printf '  ok   %-48s %s\n' "$1" "$2"
  else
    printf '  BAD  %-48s %s (want %s)\n' "$1" "${2:-<none>}" "$3"
    bad=1
  fi
}

for file in \
  cli/Cargo.toml \
  clients/embed/Cargo.toml \
  clients/elixir/native/trellis_nif/Cargo.toml \
  clients/ruby/ext/trellis_ruby/Cargo.toml \
  trellis/Cargo.toml; do
  crate="$(awk -F'"' '/^name = / { print $2; exit }' "$root/$file")"
  check "$file" "$(awk -F'"' '/^version = / { print $2; exit }' "$root/$file")" "$want"
  check "Cargo.lock ($crate)" "$(locked_version "$crate")" "$want"
done
check clients/elixir/mix.exs \
  "$(awk -F'"' '/^  @version / { print $2; exit }' "$root/clients/elixir/mix.exs")" "$want"
check clients/ruby/lib/trellis/version.rb \
  "$(awk -F'"' '/VERSION = / { print $2; exit }' "$root/clients/ruby/lib/trellis/version.rb")" "$want"
check "clients/ruby/Gemfile.lock (trellis-pg)" \
  "$(sed -n 's/^    trellis-pg (\(.*\))$/\1/p' "$root/clients/ruby/Gemfile.lock" | head -1)" "$gem_want"

if [ "$bad" -ne 0 ]; then
  echo "check-release-versions: run .github/scripts/bump-version.sh $want, and merge that, before tagging v$want" >&2
  exit 1
fi
