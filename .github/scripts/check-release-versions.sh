#!/usr/bin/env bash
# Checks that every version a release ships equals <version>: the `trellis`
# crate (published to crates.io as `trellis-pg`), the CLI, the shared
# embedding crate, the Elixir NIF crate and mix.exs, and the Ruby extension
# crate and Trellis::VERSION. The install text in the four READMEs follows from
# the version too (release-pins.sh). A `v<version>` tag releases all of them at
# once (docs/releasing.md), so they move together. So must the lockfiles that
# record them, Cargo.lock and clients/ruby/Gemfile.lock: the release builds
# run with --locked and a frozen bundle, and fail on a stale one. And so must
# the release the tree develops, `DEVELOPING` in trellis/src/defs/floor.rs:
# <version>'s release (0.2.0 for 0.2.0 or 0.2.0-alpha), so a released binary
# never claims a later release's floors (ADR-0024). bump-version.sh sets them
# all.
#
# usage: check-release-versions.sh <version> [<repo-root>]
#
# With CHECK_DEVELOPING=0 it leaves DEVELOPING out: release.yml run by hand
# checks a tree that isn't a release commit, where DEVELOPING may be the next
# release's (docs/releasing.md).
#
# Prints each version it reads, and exits 1 naming every one that differs.
# Portable to macOS's bash 3.2 (publish-packages.sh runs it there).
set -euo pipefail

usage="usage: check-release-versions.sh <version> [<repo-root>]"
want="${1:?$usage}"
want="${want#v}"
root="${2:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
# shellcheck source=release-pins.sh
. "$(dirname "${BASH_SOURCE[0]}")/release-pins.sh"
derive_pins "$want"
gem_want="$pin_gem_version"

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

# The READMEs' install text. Their first match is the one the bump rewrites.
for file in README.md trellis/README.md; do
  check "$file banner" \
    "$(sed -n 's/^> \*\*Version \([^:*]*\):\*\*.*/\1/p' "$root/$file" | head -1)" "$pin_banner"
  check "$file trellis-pg" \
    "$(sed -n 's/.*trellis-pg = "\([^"]*\)".*/\1/p' "$root/$file" | head -1)" "$pin_cargo"
done
check "clients/ruby/README.md gem" \
  "$(sed -n 's/^gem "trellis-pg", "\([^"]*\)".*/\1/p' "$root/clients/ruby/README.md" | head -1)" "$pin_gem"
check "clients/elixir/README.md dep" \
  "$(sed -n 's/.*{:trellis_pg, "\([^"]*\)"}.*/\1/p' "$root/clients/elixir/README.md" | head -1)" "$pin_hex"

if [ "${CHECK_DEVELOPING:-1}" != 0 ]; then
  check "trellis/src/defs/floor.rs DEVELOPING" \
    "$(sed -n 's/^pub const DEVELOPING: Release = Release::new(\([0-9]*\), \([0-9]*\), \([0-9]*\));$/\1.\2.\3/p' \
      "$root/trellis/src/defs/floor.rs" | head -1)" "$pin_release"
fi

if [ "$bad" -ne 0 ]; then
  echo "check-release-versions: run .github/scripts/bump-version.sh $want, and merge that, before tagging v$want" >&2
  exit 1
fi
