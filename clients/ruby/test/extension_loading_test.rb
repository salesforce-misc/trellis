# frozen_string_literal: true

require "fileutils"
require "open3"
require "rbconfig"
require "tmpdir"
require "test_helper"

# Which native extension `require "trellis/pg"` loads (lib/trellis/pg.rb):
# the one `rake compile` or a source install puts in lib/trellis/, else a
# platform gem's build for this Ruby's minor version in
# lib/trellis/<major.minor>/. Each case copies the gem's Ruby files into a
# scratch lib/ and stands a Ruby file in for each extension (require_relative
# finds trellis_ruby.rb ahead of trellis_ruby.so), so it shows which one got
# picked without loading any native code.
class ExtensionLoadingTest < Minitest::Test
  LIB = File.expand_path("../lib", __dir__)
  DLEXT = RbConfig::CONFIG.fetch("DLEXT")
  MINOR = RUBY_VERSION[/\A\d+\.\d+/]

  # A per-version directory left in a checkout by building a platform gem
  # (git ignores it) must not shadow the extension `rake test` just built.
  def test_a_lib_trellis_build_wins_over_a_leftover_per_version_one
    assert_equal "lib/trellis", loaded_extension(flat: true, per_version: true)
  end

  def test_a_lib_trellis_build_loads
    assert_equal "lib/trellis", loaded_extension(flat: true, per_version: false)
  end

  # A platform gem's layout: only the per-version builds.
  def test_a_platform_gem_loads_its_build_for_this_ruby
    assert_equal "lib/trellis/#{MINOR}", loaded_extension(flat: false, per_version: true)
  end

  private

  def loaded_extension(flat:, per_version:)
    Dir.mktmpdir("trellis-extension-loading") do |dir|
      trellis = File.join(dir, "trellis")
      FileUtils.mkdir_p(trellis)
      FileUtils.cp(Dir[File.join(LIB, "trellis", "*.rb")], trellis)
      stand_in(trellis, "lib/trellis") if flat
      stand_in(File.join(trellis, MINOR), "lib/trellis/#{MINOR}") if per_version

      out, err, status = Open3.capture3(
        RbConfig.ruby, "--disable-gems", "-I", dir, "-e",
        'require "trellis/pg"; print $trellis_extension_stand_in'
      )
      assert status.success?, "require \"trellis/pg\" failed: #{err}"
      out
    end
  end

  # An extension file for pg.rb's existence check, and the Ruby file
  # require_relative loads in its place, which says where it is.
  def stand_in(dir, name)
    FileUtils.mkdir_p(dir)
    File.write(File.join(dir, "trellis_ruby.#{DLEXT}"), "")
    File.write(File.join(dir, "trellis_ruby.rb"), "$trellis_extension_stand_in = #{name.dump}\n")
  end
end
