#!/usr/bin/env bash
# Readies the Ruby binding (clients/ruby, or <gem-dir>) for building the
# trellis-pg source gem. Run by .github/workflows/ruby-release.yml; it edits
# the tree in place, so don't commit the result.
#
# usage: ruby-prepare-source-gem.sh <git-url> <rev> [<gem-dir>]
#
# 1. The extension crate must build on its own, outside this repository:
#    installing the source gem compiles ext/trellis_ruby with no workspace
#    around it.
#    - Its path dependencies on `trellis` and `trellis-embed` become git
#      dependencies on <git-url> at <rev>, the commit being released.
#    - The edition it inherits from the workspace is written out, and an
#      empty `[workspace]` table makes the crate its own workspace root, so a
#      Cargo workspace the gem happens to be installed under doesn't claim it.
#    - Its Cargo.lock starts as a copy of the workspace's and is then trimmed
#      to the crate's own graph, so a source install resolves the dependency
#      versions the platform gems were built with.
# 2. The repository's license is copied in, since the gem ships it.
#
# Locally, a file:// URL to a clone works as <git-url>, for a <rev> that
# clone has.
set -euo pipefail

usage="usage: ruby-prepare-source-gem.sh <git-url> <rev> [<gem-dir>]"
git_url="${1:?$usage}"
rev="${2:?$usage}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
gem_dir="${3:-$repo_root/clients/ruby}"
crate_dir="$gem_dir/ext/trellis_ruby"
manifest="$crate_dir/Cargo.toml"

edition="$(awk -F'"' '
  /^\[/ { in_package = ($0 == "[workspace.package]") }
  in_package && /^edition = / { print $2; exit }
' "$repo_root/Cargo.toml")"
if [ -z "$edition" ]; then
  echo "ruby-prepare-source-gem: no edition in $repo_root/Cargo.toml's [workspace.package]" >&2
  exit 1
fi

git_dep="{ git = \"$git_url\", rev = \"$rev\" }"
sed -i.orig \
  -e "s|^edition\.workspace = true\$|edition = \"$edition\"|" \
  -e "s|^trellis = { path = \"[^\"]*\" }\$|trellis = $git_dep|" \
  -e "s|^trellis-embed = { path = \"[^\"]*\" }\$|trellis-embed = $git_dep|" \
  "$manifest"
rm "$manifest.orig"
printf '\n# Its own workspace root, not part of any Cargo workspace it is installed under.\n[workspace]\n' >>"$manifest"

# Anything the rewrite missed still points into this repository.
if grep -nE 'path = |\.workspace = true' "$manifest"; then
  echo "ruby-prepare-source-gem: $manifest still depends on this repository's layout (above)" >&2
  exit 1
fi

cp "$repo_root/Cargo.lock" "$crate_dir/Cargo.lock"
# Resolving rewrites the lockfile to this crate's graph, keeping every locked
# version it can (it fetches <rev> to do so).
(cd "$crate_dir" && cargo metadata --format-version 1 >/dev/null)

cp "$repo_root/LICENSE.txt" "$gem_dir/LICENSE.txt"

cat "$manifest"
