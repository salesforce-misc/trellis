#!/usr/bin/env bash
# Builds one trellis-pg platform gem (clients/ruby) with rb-sys-dock: in
# rb-sys's cross-compiling container for <platform>, one extension per Ruby
# minor version in RUBY_VERSIONS, packed into a single gem that loads the one
# matching the running Ruby. Used by .github/workflows/ruby-release.yml, once
# per platform; runs the same way locally.
#
# usage: build-ruby-gem.sh <platform> <out-dir>
#
#   <platform>      a RubyGems platform rb-sys-dock supports: x86_64-linux,
#                   x86_64-linux-musl, aarch64-linux, aarch64-linux-musl,
#                   x86_64-darwin or arm64-darwin
#   RUBY_VERSIONS   the Ruby minor versions to build for, comma-separated
#                   (default: 3.3,3.4,4.0, matching trellis-pg.gemspec's
#                   required_ruby_version)
#
# Needs Docker (or Podman; rb-sys-dock looks for either, or takes $DOCKER),
# and clients/ruby's bundle installed (`bundle install`; the test group isn't
# needed), whose rb_sys provides rb-sys-dock and picks its image version.
# With rootless Podman, the container's user can't write the mounted
# checkout unless its user namespace keeps your ID (`podman run
# --userns=keep-id`), and Podman needs the image named in full:
# RCD_IMAGE=docker.io/rbsys/<platform>:<rb_sys version>.
#
# Written as a script rather than using oxidize-rb/actions/cross-gem because
# the org's Actions policy allows only a curated set of third-party actions
# (see .github/actions/rust-toolchain/action.yml); that action runs
# rb-sys-dock too.
set -euo pipefail

platform="${1:?usage: build-ruby-gem.sh <platform> <out-dir>}"
out_dir="${2:?usage: build-ruby-gem.sh <platform> <out-dir>}"
ruby_versions="${RUBY_VERSIONS:-3.3,3.4,4.0}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
gem_dir="$repo_root/clients/ruby"
version="$(awk -F'"' '/VERSION = / { print $2; exit }' "$gem_dir/lib/trellis/version.rb")"

# Clears every build output in clients/ruby, before building (rake-compiler
# would package an extension it finds staged for this platform rather than
# build it again) and after (lib/trellis/ would otherwise keep this
# platform's extensions; lib/trellis/pg.rb loads one only when no `rake
# compile` build is there, but a checkout shouldn't keep them anyway).
clean() {
  rm -rf "$gem_dir/tmp" "$gem_dir/pkg" "$gem_dir"/lib/trellis/*.so \
    "$gem_dir"/lib/trellis/*.bundle "$gem_dir"/lib/trellis/[0-9]*.[0-9]*/
}
clean
trap clean EXIT

# In the container, from clients/ruby, with the whole repository mounted (it
# mounts the working directory, hence running from the root), so the
# extension crate builds as the workspace member it is, against the
# workspace's Cargo.lock.
#
# - The repository's rust-toolchain.toml pins the Rust version, which rustup
#   installs on first use; the target's standard library comes separately.
# - The gem ships without the `trellis` crate's `otlp` feature and its
#   OpenTelemetry and gRPC dependency tree (issue #154). Nothing turns it on
#   today; fail loudly if a dependency change ever does.
# - The bundle's test group has pg, which needs libpq to build.
#
# rb-sys-dock runs this with `bash -c '...'` after collapsing every run of
# whitespace to one space, so every statement ends in `;` and none holds a
# single quote.
build="$(
  cat <<'EOF'
set -euo pipefail;
rustup target add "$RUST_TARGET";
deps="$(cargo tree --locked -p trellis_ruby --features ruby -e normal --prefix none --target "$RUST_TARGET")";
if grep -Eq "^(opentelemetry|tonic)" <<<"$deps"; then
  echo "build-ruby-gem: trellis_ruby pulls in OpenTelemetry; the gem must build without the trellis crate otlp feature" >&2;
  exit 1;
fi;
export BUNDLE_WITHOUT=test;
bundle install;
bundle exec rake "native:$RUBY_TARGET" gem
EOF
)"

cd "$repo_root"
BUNDLE_GEMFILE="$gem_dir/Gemfile" bundle exec rb-sys-dock \
  --platform "$platform" --directory "$gem_dir" --ruby-versions "$ruby_versions" \
  -- "$build"

gem="$gem_dir/pkg/trellis-pg-$version-$platform.gem"
if [ ! -f "$gem" ]; then
  echo "build-ruby-gem: no $gem; pkg/ holds:" >&2
  ls -l "$gem_dir/pkg" >&2
  exit 1
fi
mkdir -p "$out_dir"
cp "$gem" "$out_dir/"
echo "$out_dir/$(basename "$gem")"
