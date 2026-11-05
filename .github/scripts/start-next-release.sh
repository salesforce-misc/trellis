#!/usr/bin/env bash
# Starts the next release on `main` once a final release is tagged: sets the
# release the tree develops, `DEVELOPING` in trellis/src/defs/floor.rs, to
# <next>, and nothing else (docs/releasing.md, "Cutting a release", step 4).
# A build of `main` then runs as <next> and stamps <next>'s floors, which the
# tagged release refuses to run (ADR-0024). The manifests keep the tagged
# version until <next> is cut. Commit what it changes as one pull request.
#
# usage: start-next-release.sh <next>      e.g. 0.3.0 after v0.2.0
#
# Not for a prerelease tag (main still develops that release) nor a patch tag
# cut from a release branch (main already develops a later release).
# check-floors.py fails main until this runs after a final tag. Running it
# again, or with the release already set, changes nothing. Portable to
# macOS's bash 3.2.
set -euo pipefail

next="${1:?usage: start-next-release.sh <next>}"
next="${next#v}"
[[ "$next" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] ||
  { echo "start-next-release: $next isn't a release X.Y.Z (no prerelease)" >&2; exit 64; }
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
floor_rs="$root/trellis/src/defs/floor.rs"

current="$(sed -n 's/^pub const DEVELOPING: Release = Release::new(\([0-9]*\), \([0-9]*\), \([0-9]*\));$/\1.\2.\3/p' \
  "$floor_rs" | head -1)"
[ -n "$current" ] || { echo "start-next-release: $floor_rs declares no DEVELOPING" >&2; exit 1; }

# key <X.Y.Z>: a string that sorts as the release does.
key() {
  local IFS=.
  # shellcheck disable=SC2086
  set -- $1
  printf '%010d%010d%010d' "$1" "$2" "$3"
}
if [ "$current" = "$next" ]; then
  echo "start-next-release: DEVELOPING is already $next"
  exit 0
fi
if [[ "$(key "$next")" < "$(key "$current")" ]]; then
  echo "start-next-release: DEVELOPING is $current; $next would go back" >&2
  exit 1
fi

N="$next" perl -0pi -e '($a, $b, $c) = split /\./, $ENV{N};
  s/^pub const DEVELOPING: Release = Release::new\(\d+, \d+, \d+\);$/pub const DEVELOPING: Release = Release::new($a, $b, $c);/m' \
  "$floor_rs"
echo "start-next-release: DEVELOPING $current -> $next"
git -C "$root" status --short
