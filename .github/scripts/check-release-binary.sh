#!/usr/bin/env bash
# Checks that the `trellis` CLI binary carries none of the engine's test-only
# surface: the pause and plant hooks and the `dev` re-exports compile in only
# with the engine's `test-util` feature, and nothing that ships may switch it
# on (the feature's comment in trellis/Cargo.toml says how it could leak:
# Cargo unifies features, so one normal dependency naming it is enough). Prints
# every offending string and exits non-zero.
#
# usage: check-release-binary.sh [--profile release|dev] [BINARY | -- CARGO_ARGS...]
#
#   With no BINARY, builds the CLI (`cargo build -p trellis-cli --locked`, in
#   the release profile unless --profile dev) and checks that. release.yml
#   checks every shipped binary through build-cli.sh; ci.yml checks a dev
#   build, which holds the same strings and reuses the cached dependencies.
#   Anything after `--` is passed on to cargo build.
set -euo pipefail

forbidden=(TRELLIS_TEST_PAUSE TRELLIS_TEST_PLANT render_aggregate_select_sql render_one_to_one_select_sql)

profile=release
binary=""
cargo_args=()
while [ $# -gt 0 ]; do
  case "$1" in
    --profile) profile="${2:?--profile needs release or dev}"; shift 2 ;;
    --) shift; cargo_args=("$@"); break ;;
    -h|--help) sed -n '2,/^set -e/p' "$0" | sed '$d; s/^# \{0,1\}//' >&2; exit 64 ;;
    -*) echo "check-release-binary: unknown flag $1" >&2; exit 64 ;;
    *) binary="$1"; shift ;;
  esac
done

if [ -z "$binary" ]; then
  repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
  case "$profile" in
    release) cargo_profile=(--release); dir=release ;;
    dev) cargo_profile=(); dir=debug ;;
    *) echo "check-release-binary: --profile is release or dev" >&2; exit 64 ;;
  esac
  (cd "$repo_root" && cargo build -p trellis-cli --locked ${cargo_profile[@]+"${cargo_profile[@]}"} ${cargo_args[@]+"${cargo_args[@]}"})
  target_dir="$(cd "$repo_root" && cargo metadata --format-version 1 --no-deps | jq -r .target_directory)"
  binary="$target_dir/$dir/trellis"
fi
# An empty or unreadable file would pass vacuously.
[ -f "$binary" ] && [ -s "$binary" ] && [ -r "$binary" ] ||
  { echo "check-release-binary: no readable, non-empty binary at $binary" >&2; exit 1; }

# grep -a reads the binary as text, so this needs no `strings`. Its status is
# 0 for a match, 1 for none, and anything else is an error, not a pass.
leaked=0
for s in "${forbidden[@]}"; do
  status=0
  grep -aq -- "$s" "$binary" || status=$?
  case "$status" in
    0) echo "check-release-binary: $binary contains \`$s\`" >&2; leaked=1 ;;
    1) ;;
    *) echo "check-release-binary: grep failed (status $status) reading $binary" >&2; exit 1 ;;
  esac
done
if [ "$leaked" -ne 0 ]; then
  echo "check-release-binary: the test-only surface is compiled in; see the \`test-util\` feature in trellis/Cargo.toml" >&2
  exit 1
fi
echo "check-release-binary: $binary has none of: ${forbidden[*]}"
