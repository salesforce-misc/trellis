#!/usr/bin/env bash
# Checks that every version a release ships equals <version>: the `trellis`
# crate (published to crates.io as `trellis-pg`), the CLI, the shared
# embedding crate, the Elixir NIF crate and mix.exs, and the Ruby extension
# crate and Trellis::VERSION. A `v<version>` tag releases all of them at once
# (docs/releasing.md), so they move together.
#
# usage: check-release-versions.sh <version> [<repo-root>]
#
# Prints each file it reads, and exits 1 naming every file that differs.
# Portable to macOS's bash 3.2 (publish-packages.sh runs it there).
set -euo pipefail

usage="usage: check-release-versions.sh <version> [<repo-root>]"
want="${1:?$usage}"
want="${want#v}"
root="${2:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"

# version_of <file>: the version that file declares.
version_of() {
  case "$1" in
    *Cargo.toml) awk -F'"' '/^version = / { print $2; exit }' "$root/$1" ;;
    *mix.exs) awk -F'"' '/^  @version / { print $2; exit }' "$root/$1" ;;
    *version.rb) awk -F'"' '/VERSION = / { print $2; exit }' "$root/$1" ;;
  esac
}

bad=0
for file in \
  cli/Cargo.toml \
  clients/elixir/mix.exs \
  clients/elixir/native/trellis_nif/Cargo.toml \
  clients/embed/Cargo.toml \
  clients/ruby/ext/trellis_ruby/Cargo.toml \
  clients/ruby/lib/trellis/version.rb \
  trellis/Cargo.toml; do
  found="$(version_of "$file")"
  if [ "$found" = "$want" ]; then
    printf '  ok   %-48s %s\n' "$file" "$found"
  else
    printf '  BAD  %-48s %s (want %s)\n' "$file" "${found:-<none>}" "$want"
    bad=1
  fi
done
if [ "$bad" -ne 0 ]; then
  echo "check-release-versions: set every version above to $want before tagging v$want" >&2
  exit 1
fi
