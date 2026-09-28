#!/usr/bin/env bash
# Builds the Elixir binding's NIF (clients/elixir/native/trellis_nif) in
# release mode for one target and packs it the way rustler_precompiled
# downloads it: `lib<crate>-v<version>-nif-<nif version>-<target>.so.tar.gz`,
# holding the one library under that name without the `.tar.gz`. Used by
# .github/workflows/elixir-release.yml, once per target; runs the same way
# locally.
#
# usage: build-elixir-nif.sh <target> <out-dir>
#
# Linux targets build with cargo-zigbuild, which links against the glibc
# version named below rather than the build machine's, so the library loads
# on older distributions than the runner's; the workflow cross-builds all
# three Linux targets from one x86_64 runner this way. It needs
# `cargo-zigbuild` and `zig` on PATH (or zig through `python3 -m ziglang`).
# Apple targets build with plain Cargo on macOS, which links either
# architecture.
#
# Written as a script rather than using philss/rustler-precompiled-action
# because the org's Actions policy allows only a curated set of third-party
# actions (see .github/actions/rust-toolchain/action.yml).
set -euo pipefail

target="${1:?usage: build-elixir-nif.sh <target> <out-dir>}"
out_dir="${2:?usage: build-elixir-nif.sh <target> <out-dir>}"

# rustler's default NIF version (its `nif_version_2_15` feature). A NIF built
# for 2.15 loads on every later NIF version, i.e. OTP 22 and up; Trellis.Native
# lists only this one.
nif_version="2.15"
# The oldest glibc the Linux gnu libraries run on: Rust's own floor.
glibc="2.17"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
crate_dir="$repo_root/clients/elixir/native/trellis_nif"
crate="trellis_nif"

crate_version="$(awk -F'"' '/^version = / { print $2; exit }' "$crate_dir/Cargo.toml")"
mix_version="$(awk -F'"' '/^  @version / { print $2; exit }' "$repo_root/clients/elixir/mix.exs")"
if [ -z "$crate_version" ] || [ "$crate_version" != "$mix_version" ]; then
  echo "build-elixir-nif: $crate's version ($crate_version) must equal mix.exs's @version ($mix_version)" >&2
  exit 1
fi

# The precompiled NIF ships without the crate's `otlp` feature and its gRPC
# dependency tree (issue #150). Nothing turns it on today; fail loudly if a
# dependency change ever does.
deps="$(cd "$crate_dir" && cargo tree --locked --target "$target" -e normal --prefix none)"
if grep -Eq '^(opentelemetry|tonic)' <<<"$deps"; then
  echo "build-elixir-nif: $crate's dependency tree pulls in OpenTelemetry; the precompiled NIF must build without trellis's \`otlp\` feature" >&2
  exit 1
fi

case "$target" in
  *-unknown-linux-gnu) build=(cargo zigbuild --target "$target.$glibc") ;;
  *-unknown-linux-musl) build=(cargo zigbuild --target "$target") ;;
  *-apple-darwin) build=(cargo build --target "$target") ;;
  *)
    echo "build-elixir-nif: no build recipe for $target" >&2
    exit 1
    ;;
esac

# From the crate's directory, so Cargo reads its .cargo/config.toml (musl's
# `-crt-static`), exactly as a source build through `mix compile` does.
(cd "$crate_dir" && "${build[@]}" --release --locked)

target_dir="$(cd "$crate_dir" && cargo metadata --format-version 1 --no-deps | jq -r .target_directory)"
case "$target" in
  *-apple-darwin) built="$target_dir/$target/release/lib$crate.dylib" ;;
  *) built="$target_dir/$target/release/lib$crate.so" ;;
esac

lib_name="lib$crate-v$crate_version-nif-$nif_version-$target.so"
mkdir -p "$out_dir"
out_dir="$(cd "$out_dir" && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
cp "$built" "$work/$lib_name"
tar -C "$work" -czf "$out_dir/$lib_name.tar.gz" "$lib_name"
echo "$out_dir/$lib_name.tar.gz"
