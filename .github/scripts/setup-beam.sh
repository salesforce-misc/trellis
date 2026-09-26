#!/usr/bin/env bash
# Installs the Erlang/OTP and Elixir versions a .tool-versions file pins, from
# Hex's prebuilt builds (builds.hex.pm, the same builds erlef/setup-beam
# installs), each checked against the sha256 Hex publishes for it.
#
# Vendored as a script rather than using erlef/setup-beam because the org's
# Actions policy allows only a curated set of third-party actions (see
# .github/actions/rust-toolchain/action.yml).
#
# usage: setup-beam.sh <.tool-versions> <install-dir>
#
# Puts OTP's and Elixir's bin directories on $GITHUB_PATH when run in Actions,
# and prints them either way. The OS build flavor defaults to this machine's
# (`ubuntu-24.04` on ubuntu-latest); SETUP_BEAM_OS overrides it for a local
# dry run on another distro.
set -euo pipefail

tool_versions="${1:?usage: setup-beam.sh <.tool-versions> <install-dir>}"
install_dir="${2:?usage: setup-beam.sh <.tool-versions> <install-dir>}"

otp="$(awk '$1 == "erlang" { print $2 }' "$tool_versions")"
elixir="$(awk '$1 == "elixir" { print $2 }' "$tool_versions")"
if [ -z "$otp" ] || [ -z "$elixir" ]; then
  echo "setup-beam: $tool_versions must pin both erlang and elixir" >&2
  exit 1
fi

if [ -z "${SETUP_BEAM_OS:-}" ]; then
  # shellcheck disable=SC1091
  . /etc/os-release
  SETUP_BEAM_OS="${ID}-${VERSION_ID}"
fi

mkdir -p "$install_dir"
install_dir="$(cd "$install_dir" && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# fetch <builds.txt url> <artifact url> <build name> <output file>
fetch() {
  local index="$1" url="$2" name="$3" out="$4" expected
  expected="$(curl -sSfL "$index" | awk -v name="$name" '$1 == name { print $4 }')"
  if [ -z "$expected" ]; then
    echo "setup-beam: $name is not listed in $index" >&2
    exit 1
  fi
  curl -sSfL -o "$out" "$url"
  echo "$expected  $out" | sha256sum --check --quiet
}

otp_builds="https://builds.hex.pm/builds/otp/$SETUP_BEAM_OS"
fetch "$otp_builds/builds.txt" "$otp_builds/OTP-$otp.tar.gz" "OTP-$otp" "$work/otp.tar.gz"
# The tarball is a relocatable OTP tree; `Install` fixes it up in place.
mkdir -p "$install_dir/otp"
tar -xzf "$work/otp.tar.gz" -C "$install_dir/otp" --strip-components=1
"$install_dir/otp/Install" -minimal "$install_dir/otp" >/dev/null

elixir_builds="https://builds.hex.pm/builds/elixir"
fetch "$elixir_builds/builds.txt" "$elixir_builds/v$elixir.zip" "v$elixir" "$work/elixir.zip"
unzip -q "$work/elixir.zip" -d "$install_dir/elixir"

for bin in "$install_dir/otp/bin" "$install_dir/elixir/bin"; do
  echo "$bin"
  if [ -n "${GITHUB_PATH:-}" ]; then
    echo "$bin" >>"$GITHUB_PATH"
  fi
done
