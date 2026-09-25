#!/usr/bin/env bash
# Shrinks target/debug to what's worth caching between CI runs: compiled
# third-party dependencies. The workspace's own crates and test binaries are
# rebuilt on every run anyway (their sources are what changed), and keeping
# them would blow through GitHub's 10GB-per-repo cache limit: a cold
# clippy-plus-test build is ~5.6GB with CI's line-tables debug info (~13GB
# with full debug info), almost all of it the ~115 statically linked test
# executables. Pruned, it's ~800MB.
set -euo pipefail

target="${CARGO_TARGET_DIR:-target}/debug"
[ -d "$target" ] || exit 0

# Every workspace target's crate name (`-` becomes `_` in artifact names),
# and every workspace package name (fingerprint and build-script dirs).
metadata="$(cargo metadata --format-version 1 --no-deps)"
crates="$(jq -r '.packages[].targets[].name | gsub("-"; "_")' <<<"$metadata" | sort -u)"
packages="$(jq -r '.packages[].name' <<<"$metadata" | sort -u)"

for crate in $crates; do
  rm -rf "$target/deps/$crate"-* "$target/deps/lib$crate"-* "$target/$crate" "$target/$crate"-*
done
for package in $packages; do
  rm -rf "$target/.fingerprint/$package"-* "$target/build/$package"-*
done
rm -rf "$target/incremental" "$target/examples" "$target/.cargo-lock"
