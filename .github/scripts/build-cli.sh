#!/usr/bin/env bash
# Builds the `trellis` CLI (cli/) in release mode for one target and packs it
# as `trellis-v<version>-<target>.tar.gz`, holding a `trellis-v<version>-<target>/`
# directory with the binary, LICENSE.txt and README.md. Used by
# .github/workflows/release.yml, once per target; runs the same way locally.
#
# usage: build-cli.sh <target> <out-dir>
#
# Linux targets build with cargo-zigbuild, as build-elixir-nif.sh's do: the
# gnu binaries link against the old glibc named below, so they run on older
# distributions than the runner's, and the musl ones are fully static (for
# Alpine and scratch images). It needs `cargo-zigbuild` and `zig` on PATH (or
# zig through `python3 -m ziglang`). Apple targets build with plain Cargo on
# macOS, which links either architecture.
set -euo pipefail

target="${1:?usage: build-cli.sh <target> <out-dir>}"
out_dir="${2:?usage: build-cli.sh <target> <out-dir>}"

# The oldest glibc the Linux gnu binaries run on: Rust's own floor.
glibc="2.17"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
version="$(awk -F'"' '/^version = / { print $2; exit }' "$repo_root/cli/Cargo.toml")"

# The released CLI ships without the engine's `otlp` feature and its gRPC
# tree, and without its test-only `test-util` surface; see
# trellis/Cargo.toml. Fail loudly if a dependency change ever turns either on.
deps="$(cd "$repo_root" && cargo tree --locked -p trellis-cli --target "$target" -e normal,features --prefix none)"
if grep -Eq '^(opentelemetry|tonic)|trellis feature "(otlp|test-util)"' <<<"$deps"; then
  echo "build-cli: the CLI's dependency tree turns on trellis's \`otlp\` or \`test-util\` feature" >&2
  exit 1
fi

case "$target" in
  *-unknown-linux-gnu) build=(cargo zigbuild --target "$target.$glibc") ;;
  *-unknown-linux-musl) build=(cargo zigbuild --target "$target") ;;
  *-apple-darwin) build=(cargo build --target "$target") ;;
  *)
    echo "build-cli: no build recipe for $target" >&2
    exit 1
    ;;
esac

(cd "$repo_root" && "${build[@]}" -p trellis-cli --release --locked)

target_dir="$(cd "$repo_root" && cargo metadata --format-version 1 --no-deps | jq -r .target_directory)"
built="$target_dir/$target/release/trellis"
if [[ "$target" == *-unknown-linux-musl ]] && file "$built" | grep -q 'dynamically linked'; then
  echo "build-cli: the $target binary isn't static" >&2
  exit 1
fi

# And that none of that surface reached the binary itself.
"$repo_root/.github/scripts/check-release-binary.sh" "$built"

name="trellis-v$version-$target"
mkdir -p "$out_dir"
out_dir="$(cd "$out_dir" && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
mkdir "$work/$name"
cp "$built" "$repo_root/LICENSE.txt" "$repo_root/README.md" "$work/$name/"
tar -C "$work" -czf "$out_dir/$name.tar.gz" "$name"
echo "$out_dir/$name.tar.gz"
