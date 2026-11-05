# Sourced by bump-version.sh and check-release-versions.sh (not run directly).
# Portable to macOS's bash 3.2.
#
# derive_pins <version>: the install text the READMEs carry for a release
# (docs/releasing.md). A stable X.Y.Z is named by its series, so a patch
# release leaves the text alone: Cargo's "X.Y", Bundler and Mix's "~> X.Y.0".
# A prerelease names itself exactly, since Cargo, Bundler and Mix never pick
# one from a range; RubyGems writes its `-` as `.pre.` (0.2.0-alpha is
# 0.2.0.pre.alpha). Sets:
#   pin_release       the release it belongs to, without a prerelease tag
#                     (0.2.0 for 0.2.0-alpha): defs::floor::DEVELOPING's
#   pin_gem_version   what Trellis::VERSION and Gemfile.lock hold
#   pin_banner        the README banner's label: "Version <pin_banner>:"
#   pin_cargo         the Rust dependency requirement
#   pin_gem           the Gemfile requirement
#   pin_hex           the mix.exs requirement
derive_pins() {
  pin_release="${1%%-*}"
  pin_gem_version="$(echo "$1" | sed 's/-/.pre./g')"
  case "$1" in
    *-*)
      pin_banner="$1, pre-release"
      pin_cargo="$1"
      pin_gem="$pin_gem_version"
      pin_hex="$1"
      ;;
    *)
      pin_banner="${1%.*}"
      pin_cargo="${1%.*}"
      pin_gem="~> ${1%.*}.0"
      pin_hex="~> ${1%.*}.0"
      ;;
  esac
}
