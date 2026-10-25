#!/usr/bin/env bash
# Publishes a released Trellis version's packages, by hand, from its tag:
#
#   crates.io  trellis-pg   the engine crate (`trellis/`, renamed below)
#   Hex        trellis_pg   the Elixir binding (`clients/elixir`)
#   RubyGems   trellis-pg   the Ruby binding's 6 platform gems and source gem
#
# Run it after .github/workflows/release.yml has published the `v<version>`
# GitHub release (docs/releasing.md). It clones that tag into a scratch
# directory (your checkout is never touched), checks every version equals
# <version> and every release asset matches the release's SHA256SUMS, then
# publishes each package in turn. A package whose version is already on its
# registry is skipped, so a run that stopped halfway can simply be re-run.
# Each publish asks first, unless --yes.
#
# usage: publish-packages.sh <version> [--dry-run] [--yes] [--only crates,hex,gems] [--keep]
#
#   --dry-run  build and check everything, publish nothing: `cargo publish
#              --dry-run`, `mix hex.build` (Hex's own dry run needs a login),
#              and the list of gems it would push. Needs no credentials.
#   --yes      don't ask before each publish
#   --only     a comma-separated subset of crates, hex and gems
#   --keep     keep the scratch directory (it's kept anyway on failure)
#
# Needs git, curl, jq, sha256sum, a Rust toolchain (rustup installs the
# pinned one), Erlang/Elixir (clients/elixir/.tool-versions; asdf or mise
# pick it up from the clone) and Ruby's `gem`. And credentials, unless
# --dry-run:
#
#   crates.io  `cargo login` (or CARGO_REGISTRY_TOKEN), a token with
#              publish-new/publish-update for trellis-pg
#   Hex        `mix hex.user auth` (or HEX_API_KEY)
#   RubyGems   `gem signin` (or GEM_HOST_API_KEY); `gem push` asks for an
#              OTP code when the account has MFA
#
# What each step publishes:
#
# - crates.io: `trellis` is taken there, so the engine crate publishes as
#   `trellis-pg`, with its library still named `trellis` (`use trellis::...`).
#   The rename is made in the clone only, on the package and on every
#   workspace dependency on it, so that `cargo publish` resolves; the tracked
#   manifests keep `trellis` so `cargo test -p trellis` and friends stay as
#   they are.
# - Hex: the package as release.yml's `hex-package` job checks it
#   (elixir-prepare-hex-package.sh, a source build of the NIF), plus
#   checksum-Elixir.Trellis.Native.exs, written from the NIFs the GitHub
#   release serves: rustler_precompiled on a host checks its download
#   against it.
# - RubyGems: the gems attached to the GitHub release, byte for byte (they
#   were installed and loaded by release.yml), platform gems first, so no
#   host ever finds only the source gem and compiles it for want of its
#   platform's.
set -euo pipefail

# TRELLIS_RELEASE_REPO points a --dry-run at a fork's release, to try the
# script out; the Hex step still downloads the NIFs from salesforce-misc's.
repo="${TRELLIS_RELEASE_REPO:-salesforce-misc/trellis}"
[ "$repo" = salesforce-misc/trellis ] || [[ " $* " == *" --dry-run "* ]] ||
  { echo "publish-packages: TRELLIS_RELEASE_REPO is only for a --dry-run" >&2; exit 64; }
repo_url="https://github.com/$repo"
ua="trellis-publish-packages ($repo_url)"

usage() { sed -n '/^# usage:/,/^#   --keep/p' "$0" | sed 's/^# \{0,1\}//' >&2; exit 64; }
die() { echo "publish-packages: $*" >&2; exit 1; }
step() { printf '\n==> %s\n' "$*"; }

version="" dry_run=0 yes=0 keep=0 only="crates,hex,gems"
while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) dry_run=1; shift ;;
    --yes) yes=1; shift ;;
    --keep) keep=1; shift ;;
    --only) only="${2:?--only needs a list}"; shift 2 ;;
    -h|--help) usage ;;
    -*) echo "publish-packages: unknown flag $1" >&2; usage ;;
    *) [ -z "$version" ] || usage; version="${1#v}"; shift ;;
  esac
done
[ -n "$version" ] || usage
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] || die "$version isn't a version (want e.g. 0.1.0)"
tag="v$version"
# RubyGems writes a prerelease's `-` as `.pre.`: 0.1.0-alpha is the gem 0.1.0.pre.alpha.
gem_version="${version//-/.pre.}"
wants() { [[ ",$only," == *",$1,"* ]]; }
for part in ${only//,/ }; do
  case "$part" in crates|hex|gems) ;; *) die "--only takes crates, hex and gems, not $part" ;; esac
done

confirm() {
  [ "$dry_run" -eq 0 ] || return 0
  [ "$yes" -eq 0 ] || return 0
  local reply
  read -r -p "$1 [y/N] " reply </dev/tty
  [[ "$reply" =~ ^[Yy] ]]
}

# 200: <name> <version> is on its registry; 404: it isn't. Anything else stops.
published() {
  local status
  status="$(curl -sS -o /dev/null -w '%{http_code}' -A "$ua" "$2")"
  case "$status" in
    200) return 0 ;;
    404) return 1 ;;
    *) die "couldn't ask whether $1 $version is published (HTTP $status from $2)" ;;
  esac
}
crate_published() { published "crates.io trellis-pg" "https://crates.io/api/v1/crates/trellis-pg/$version"; }
hex_published() { published "Hex trellis_pg" "https://hex.pm/api/packages/trellis_pg/releases/$version"; }

step "Checking tools and credentials"
need=(git curl jq sha256sum)
wants crates && need+=(cargo)
wants hex && need+=(cargo mix)
wants gems && need+=(gem)
for tool in "${need[@]}"; do
  command -v "$tool" >/dev/null || die "$tool isn't on PATH"
done
if [ "$dry_run" -eq 0 ]; then
  if wants crates && [ -z "${CARGO_REGISTRY_TOKEN:-}" ] &&
     ! grep -qs 'token' "${CARGO_HOME:-$HOME/.cargo}/credentials.toml" "${CARGO_HOME:-$HOME/.cargo}/credentials"; then
    die "no crates.io token: run \`cargo login\` or set CARGO_REGISTRY_TOKEN"
  fi
  if wants hex && [ -z "${HEX_API_KEY:-}" ] && ! mix hex.user whoami >/dev/null 2>&1; then
    die "not signed in to Hex: run \`mix hex.user auth\` or set HEX_API_KEY"
  fi
  if wants gems && [ -z "${GEM_HOST_API_KEY:-}" ] && ! grep -qs 'rubygems_api_key' "$HOME/.gem/credentials" "$HOME/.local/share/gem/credentials"; then
    die "not signed in to RubyGems: run \`gem signin\` or set GEM_HOST_API_KEY"
  fi
fi
echo "ok"

step "Checking the $tag GitHub release"
# The public API's by-tag lookup sees only a published release, which is
# what rustler_precompiled downloads from.
release="$(curl -sS --fail-with-body -A "$ua" "https://api.github.com/repos/$repo/releases/tags/$tag")" ||
  die "$repo has no published $tag release; push the tag and let release.yml finish (docs/releasing.md)"
jq -r '.assets[] | "  \(.name)  \(.size) bytes"' <<<"$release"

work="$(mktemp -d "${TMPDIR:-/tmp}/trellis-publish-$version.XXXX")"
cleanup() {
  local status=$?
  if [ "$status" -ne 0 ] || [ "$keep" -eq 1 ]; then
    echo "publish-packages: the scratch directory is $work" >&2
  else
    rm -rf "$work"
  fi
}
trap cleanup EXIT

mkdir "$work/assets"
jq -r '.assets[] | "\(.name)\t\(.browser_download_url)"' <<<"$release" |
  while IFS=$'\t' read -r name url; do
    curl -sSLf -A "$ua" -o "$work/assets/$name" "$url"
  done
[ -f "$work/assets/SHA256SUMS" ] || die "the $tag release has no SHA256SUMS"
(cd "$work/assets" && sha256sum --quiet -c SHA256SUMS) || die "an asset doesn't match the release's SHA256SUMS"
echo "every asset matches SHA256SUMS"

step "Cloning $tag"
git clone --quiet --depth 1 --branch "$tag" "$repo_url.git" "$work/trellis" 2>/dev/null ||
  die "couldn't clone $repo_url at $tag"
src="$work/trellis"
sha="$(git -C "$src" rev-parse HEAD)"
echo "$tag is $sha"
"$src/.github/scripts/check-release-versions.sh" "$version" "$src"

if wants crates; then
  step "crates.io: trellis-pg $version"
  if crate_published; then
    echo "already on crates.io; skipping"
  else
    grep -q '^\[lib\]' "$src/trellis/Cargo.toml" && die "trellis/Cargo.toml has a [lib] table now; update the rename below"
    sed -i 's|^name = "trellis"$|name = "trellis-pg"|' "$src/trellis/Cargo.toml"
    sed -i '0,/^\[dependencies\]$/s||[lib]\nname = "trellis"\n\n[dependencies]|' "$src/trellis/Cargo.toml"
    find "$src" -name Cargo.toml -not -path '*/target/*' -print0 |
      xargs -0 sed -i 's|^trellis = { path = |trellis = { package = "trellis-pg", path = |'
    if grep -rn --include=Cargo.toml '^trellis = ' "$src" | grep -v 'package = "trellis-pg"'; then
      die "a dependency on trellis above wasn't renamed"
    fi
    if [ "$dry_run" -eq 1 ]; then
      (cd "$src" && cargo publish -p trellis-pg --allow-dirty --dry-run)
    elif confirm "Publish trellis-pg $version to crates.io?"; then
      (cd "$src" && cargo publish -p trellis-pg --allow-dirty)
    else
      echo "skipped"
    fi
    # The Hex step builds from this tree too, and expects it as tagged.
    git -C "$src" checkout --quiet -- .
  fi
fi

if wants hex; then
  step "Hex: trellis_pg $version"
  if hex_published; then
    echo "already on Hex; skipping"
  else
    "$src/.github/scripts/elixir-prepare-hex-package.sh" "$repo_url" "$sha" "$src/clients/elixir" >/dev/null
    (
      cd "$src/clients/elixir"
      mix local.hex --if-missing --force
      mix local.rebar --if-missing --force
      mix deps.get
      # A source build of the NIF, from the crate the package ships (a few
      # minutes). It also records the module's download metadata, which the
      # checksum task reads.
      mix compile --warnings-as-errors
      # Without --ignore-unavailable: a target the release doesn't serve
      # fails here instead of shipping a package that can't install there.
      # 5 is the length of Trellis.Native's `targets`.
      mix rustler_precompiled.download Trellis.Native --all --print
      targets="$(grep -c '"sha256:' checksum-Elixir.Trellis.Native.exs)"
      [ "$targets" -eq 5 ] || { echo "checksum-Elixir.Trellis.Native.exs lists $targets NIFs, not 5" >&2; exit 1; }
      if [ "$dry_run" -eq 1 ]; then
        mix hex.build
      elif [ "$yes" -eq 1 ]; then
        mix hex.publish --yes
      else
        # Hex lists the package's files and asks itself.
        mix hex.publish </dev/tty
      fi
    )
  fi
fi

if wants gems; then
  step "RubyGems: trellis-pg $gem_version"
  mapfile -t platform_gems < <(find "$work/assets" -name "trellis-pg-$gem_version-*.gem" | sort)
  source_gem="$work/assets/trellis-pg-$gem_version.gem"
  [ "${#platform_gems[@]}" -eq 6 ] || die "the release has ${#platform_gems[@]} platform gems, not 6"
  [ -f "$source_gem" ] || die "the release has no source gem trellis-pg-$gem_version.gem"
  # The platforms of this version RubyGems already has (a re-run after a
  # partial push), "ruby" being the source gem's.
  status="$(curl -sS -o "$work/versions.json" -w '%{http_code}' -A "$ua" https://rubygems.org/api/v1/versions/trellis-pg.json)"
  case "$status" in
    200) pushed="$(jq -r --arg v "$gem_version" '.[] | select(.number == $v) | .platform' "$work/versions.json")" ;;
    404) pushed="" ;;
    *) die "couldn't ask RubyGems which trellis-pg gems are published (HTTP $status)" ;;
  esac
  platform_of() {
    local name="${1##*/}"
    name="${name%.gem}"
    name="${name#"trellis-pg-$gem_version"}"
    echo "${name#-}" | sed 's/^$/ruby/'
  }
  todo=()
  for gem in "${platform_gems[@]}" "$source_gem"; do
    if grep -qx -- "$(platform_of "$gem")" <<<"$pushed"; then
      echo "  ${gem##*/} is already on RubyGems"
    else
      todo+=("$gem")
    fi
  done
  if [ "${#todo[@]}" -eq 0 ]; then
    echo "all 7 are on RubyGems; skipping"
  elif [ "$dry_run" -eq 1 ]; then
    echo "dry run: would push, in this order:"
    printf '  %s\n' "${todo[@]##*/}"
  elif confirm "Push ${#todo[@]} gems to RubyGems, platform gems first?"; then
    for gem in "${todo[@]}"; do
      gem push "$gem" </dev/tty
    done
  else
    echo "skipped"
  fi
fi

step "Done"
[ "$dry_run" -eq 0 ] || echo "(dry run: nothing was published)"
wants crates && echo "  https://crates.io/crates/trellis-pg/$version"
wants hex && echo "  https://hex.pm/packages/trellis_pg/$version"
wants gems && echo "  https://rubygems.org/gems/trellis-pg/versions/$gem_version"
