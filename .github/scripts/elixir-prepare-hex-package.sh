#!/usr/bin/env bash
# Readies the Elixir binding (clients/elixir, or <package-dir>) for
# `mix hex.publish`. Run by .github/workflows/elixir-release.yml; it edits the
# tree in place, so don't commit the result.
#
# usage: elixir-prepare-hex-package.sh <git-url> <rev> [<package-dir>]
#
# 1. The NIF crate must build on its own, outside this repository: a host
#    that builds the NIF from source (TRELLIS_PG_BUILD=1) compiles
#    deps/trellis_pg/native/trellis_nif with no workspace around it.
#    - Its path dependencies on `trellis` and `trellis-embed` become git
#      dependencies on <git-url> at <rev>, the commit being released.
#    - The edition it inherits from the workspace is written out, and an
#      empty `[workspace]` table makes the crate its own workspace root, so a
#      host project that is itself a Cargo workspace doesn't claim it.
#    - Its Cargo.lock starts as a copy of the workspace's and is then trimmed
#      to the crate's own graph, so a source build resolves the dependency
#      versions the precompiled NIFs were built with.
# 2. The repository's license is copied in, since the package ships it.
set -euo pipefail

usage="usage: elixir-prepare-hex-package.sh <git-url> <rev> [<package-dir>]"
git_url="${1:?$usage}"
rev="${2:?$usage}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
package_dir="${3:-$repo_root/clients/elixir}"
crate_dir="$package_dir/native/trellis_nif"
manifest="$crate_dir/Cargo.toml"

edition="$(awk -F'"' '
  /^\[/ { in_package = ($0 == "[workspace.package]") }
  in_package && /^edition = / { print $2; exit }
' "$repo_root/Cargo.toml")"
if [ -z "$edition" ]; then
  echo "elixir-prepare-hex-package: no edition in $repo_root/Cargo.toml's [workspace.package]" >&2
  exit 1
fi

git_dep="{ git = \"$git_url\", rev = \"$rev\" }"
sed -i.orig \
  -e "s|^edition\.workspace = true\$|edition = \"$edition\"|" \
  -e "s|^trellis = { path = \"[^\"]*\" }\$|trellis = $git_dep|" \
  -e "s|^trellis-embed = { path = \"[^\"]*\" }\$|trellis-embed = $git_dep|" \
  "$manifest"
rm "$manifest.orig"
printf '\n# Its own workspace root, not part of any host project'"'"'s.\n[workspace]\n' >>"$manifest"

# Anything the rewrite missed still points into this repository.
if grep -nE 'path = |\.workspace = true' "$manifest"; then
  echo "elixir-prepare-hex-package: $manifest still depends on this repository's layout (above)" >&2
  exit 1
fi

cp "$repo_root/Cargo.lock" "$crate_dir/Cargo.lock"
# Resolving rewrites the lockfile to this crate's graph, keeping every locked
# version it can (it fetches <rev> to do so).
(cd "$crate_dir" && cargo metadata --format-version 1 >/dev/null)

cp "$repo_root/LICENSE.txt" "$package_dir/LICENSE.txt"

cat "$manifest"
