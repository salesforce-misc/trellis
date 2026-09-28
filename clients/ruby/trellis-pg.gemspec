# frozen_string_literal: true

require_relative "lib/trellis/version"

# The `trellis-pg` gem (issue #154): `trellis` is taken on RubyGems. It is
# required as "trellis/pg", which Bundler.require finds by itself for a gem
# with a hyphenated name; the module stays `Trellis`.
#
# This spec is the source gem. The Rakefile's RbSys::ExtensionTask derives
# each platform gem from it, dropping the Rust sources, extconf.rb and the
# rb_sys dependency in favor of prebuilt extensions
# (.github/workflows/ruby-release.yml).
#
# The source gem's extension crate has to build with no Cargo workspace
# around it: .github/scripts/ruby-prepare-source-gem.sh rewrites its
# manifest, writes its Cargo.lock and copies in the license before a release
# builds it. `gem build` in a plain checkout makes a gem that only installs
# from inside this repository.
Gem::Specification.new do |spec|
  spec.name = "trellis-pg"
  spec.version = Trellis::VERSION
  spec.authors = ["Salesforce"]
  spec.summary = "Embedded Trellis: streaming SQL transforms inside your Postgres, run from a Ruby app"
  spec.description = <<~DESC
    A native extension over the trellis crate's BlockingTrellis, so a Rails
    app can define and run Trellis's streaming transforms in its own process,
    against its own Postgres database, without deploying a separate service.
  DESC
  spec.homepage = "https://github.com/salesforce-misc/trellis/tree/main/clients/ruby"
  spec.license = "Apache-2.0"
  # The oldest Ruby still maintained upstream, and the oldest the platform
  # gems carry an extension for (build-ruby-gem.sh's RUBY_VERSIONS).
  spec.required_ruby_version = ">= 3.3"

  spec.metadata = {
    "source_code_uri" => "https://github.com/salesforce-misc/trellis/tree/main/clients/ruby",
    "bug_tracker_uri" => "https://github.com/salesforce-misc/trellis/issues",
    "rubygems_mfa_required" => "true",
    # RbSys::ExtensionTask checks it against the crate it builds.
    "cargo_crate_name" => "trellis_ruby"
  }

  spec.files = Dir.chdir(__dir__) do
    Dir[
      "lib/**/*.rb",
      "ext/trellis_ruby/{Cargo.toml,Cargo.lock,extconf.rb}",
      "ext/trellis_ruby/src/**/*.rs",
      "README.md",
      "LICENSE.txt"
    ].sort
  end
  spec.require_paths = ["lib"]
  spec.extensions = ["ext/trellis_ruby/extconf.rb"]

  # Only to build the source gem's extension; platform gems drop it.
  spec.add_dependency "rb_sys", "~> 0.9.130"
end
