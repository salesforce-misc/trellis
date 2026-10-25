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
set -euo pipefail

usage="usage: check-release-versions.sh <version> [<repo-root>]"
want="${1:?$usage}"
want="${want#v}"
root="${2:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"

crate_version() { awk -F'"' '/^version = / { print $2; exit }' "$root/$1"; }

declare -A found=(
  [trellis/Cargo.toml]="$(crate_version trellis/Cargo.toml)"
  [cli/Cargo.toml]="$(crate_version cli/Cargo.toml)"
  [clients/embed/Cargo.toml]="$(crate_version clients/embed/Cargo.toml)"
  [clients/elixir/native/trellis_nif/Cargo.toml]="$(crate_version clients/elixir/native/trellis_nif/Cargo.toml)"
  [clients/ruby/ext/trellis_ruby/Cargo.toml]="$(crate_version clients/ruby/ext/trellis_ruby/Cargo.toml)"
  [clients/elixir/mix.exs]="$(awk -F'"' '/^  @version / { print $2; exit }' "$root/clients/elixir/mix.exs")"
  [clients/ruby/lib/trellis/version.rb]="$(awk -F'"' '/VERSION = / { print $2; exit }' "$root/clients/ruby/lib/trellis/version.rb")"
)

bad=0
for file in $(printf '%s\n' "${!found[@]}" | sort); do
  if [ "${found[$file]}" = "$want" ]; then
    printf '  ok   %-48s %s\n' "$file" "${found[$file]}"
  else
    printf '  BAD  %-48s %s (want %s)\n' "$file" "${found[$file]:-<none>}" "$want"
    bad=1
  fi
done
if [ "$bad" -ne 0 ]; then
  echo "check-release-versions: set every version above to $want before tagging v$want" >&2
  exit 1
fi
