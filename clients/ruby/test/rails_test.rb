# frozen_string_literal: true

require "json"
require "tempfile"
require "rbconfig"
require "test_helper"

# The Rails integration's boot half (issue #153): Trellis::Railtie, in a
# minimal app booted in a process of its own per scenario
# (support/rails_boot_app.rb), and the promise that without Rails the gem
# requires nothing at all.
class RailsTest < Minitest::Test
  include TrellisTestCase

  LIB = File.expand_path("../lib", __dir__)
  BOOT_APP = File.expand_path("support/rails_boot_app.rb", __dir__)

  # With RubyGems off, only the standard library's own files can load, so
  # this proves `require "trellis/pg"` pulls in no gem, Rails least of all, and
  # that the handle works without one.
  def test_the_gem_loads_and_runs_without_rails
    script = <<~RUBY
      require "trellis/pg"
      abort "Rails got loaded" if defined?(::Rails) || defined?(::ActiveRecord)
      abort "RubyGems got loaded" if defined?(::Gem)
      abort "the Railtie loaded without Rails" if Trellis.const_defined?(:Railtie, false)
      Trellis.connect(url: ARGV.fetch(0))
      abort "status of an unknown target wasn't nil" unless Trellis.status("no_such_target").nil?
      Trellis.shutdown
      begin
        require "trellis/migration"
        abort "trellis/migration loaded without ActiveRecord"
      rescue LoadError => e
        puts e.message
      end
    RUBY
    out = run_ruby("the Rails-free script", { "RUBYOPT" => nil, "BUNDLE_GEMFILE" => nil },
                   "--disable-gems", "-I", LIB, "-e", script, TestCluster.dsn)
    assert_match "active_record", out
  end

  def test_boot_connects_the_handle_from_config_trellis_connect
    seen = boot("boot")
    assert_equal true, seen.fetch("connected")
    assert_equal "boot_targets", seen.fetch("target_schema")
    assert_equal true, seen.fetch("migration_after_active_record")
  end

  def test_boot_does_nothing_when_config_trellis_connect_is_unset
    assert_equal({ "connected" => false }, boot("unset"))
  end

  def test_connect_on_boot_false_leaves_the_connect_to_the_app
    assert_equal({ "connected" => false, "connected_after_connect" => true },
                 boot("connect_on_boot_false"))
  end

  # An app booted by a rake task gets no boot handle, as Ecto migrates
  # without starting the app; trellis:migrate connects one of its own and
  # shuts it down.
  def test_a_rake_task_gets_no_boot_handle_and_trellis_migrate_runs_on_its_own
    assert_equal({ "connected" => false, "connected_after_migrate" => false }, boot("rake"))
  end

  # Trellis.migrate needs no ActiveRecord, so neither does the task: it
  # mustn't load the migration helpers, which do (issue #645).
  def test_trellis_migrate_runs_in_an_app_without_active_record
    assert_equal({ "connected" => false, "connected_after_migrate" => false, "active_record" => false },
                 boot("rake_without_active_record"))
  end

  # It's the running task that counts, not the loaded tasks: `rails test`
  # loads them to run test:prepare, then boots the app outside any task.
  def test_an_app_booted_after_a_rake_run_connects
    assert_equal({ "connected" => true }, boot("tasks_loaded_before_boot"))
  end

  # The documented Puma wiring: before_fork shuts the boot handle down, and
  # a worker connects its own.
  def test_the_documented_fork_hooks_give_each_worker_a_handle
    seen = boot("puma_hooks")
    assert_equal({ "connected" => true, "status" => nil }, seen.fetch("child"))
    assert_equal false, seen.fetch("parent_connected")
  end

  # config.trellis.instances: extra named instances, connected with the
  # default one and reachable by name.
  def test_boot_connects_the_named_instances_with_the_default_one
    seen = boot("named_instances")
    assert_equal true, seen.fetch("connected")
    assert_equal "boot_reports", seen.fetch("named_schema")
    assert_equal "Trellis::Instance", seen.fetch("named_class")
    assert_match "no connected instance named :nope", seen.fetch("unknown_instance")
    assert_equal [false, false], seen.fetch("connected_after_shutdown_all")
  end

  def test_named_instances_connect_without_a_default_connect
    seen = boot("named_instances_only")
    assert_equal false, seen.fetch("connected")
    assert_equal true, seen.fetch("named_connected")
    # Like a second Trellis.connect, a second Railtie.connect raises while a
    # named instance is connected, rather than open another handle beside it.
    assert_equal "the instance named :reports is already connected: call Trellis::Instance.shutdown_all first",
                 seen.fetch("second_connect")
    assert_equal true, seen.fetch("same_instance")
    assert_equal 1, seen.fetch("instances_held")
  end

  def test_trellis_migrate_migrates_each_named_instance_on_a_handle_of_its_own
    seen = boot("named_instances_rake")
    assert_equal false, seen.fetch("connected_after_migrate")
    assert_equal ["reports"], seen.fetch("named_after_migrate")
    assert_equal "boot_reports", seen.fetch("with_handle")
    assert_equal "config.trellis.instances has no :nope", seen.fetch("with_handle_unknown")
  end

  def test_a_named_instance_that_cannot_connect_leaves_nothing_connected
    seen = boot("failed_connect_rolls_back")
    assert_match "worker_threads must be a positive Integer", seen.fetch("error")
    assert_equal false, seen.fetch("connected")
    assert_equal false, seen.fetch("first_connected")
  end

  def test_the_fork_hooks_give_each_worker_every_instance
    seen = boot("puma_hooks_named")
    assert_equal({ "connected" => true, "named" => nil }, seen.fetch("child"))
    assert_equal [false, false], seen.fetch("parent_connected")
  end

  private

  def boot(scenario)
    out = run_ruby("the #{scenario} scenario", { "TRELLIS_TEST_DSN" => TestCluster.dsn },
                   BOOT_APP, scenario)
    JSON.parse(out.lines.last)
  end

  # Runs a Ruby process to completion, bounded (#297), and returns its
  # stdout, failing with its stderr if it doesn't exit 0.
  def run_ruby(what, env, *args)
    Tempfile.create("out") do |out|
      Tempfile.create("err") do |err|
        pid = spawn(env, RbConfig.ruby, *args, out: out.path, err: err.path, pgroup: true)
        status = wait_for_child(pid, seconds: 120, group: true)
        assert status.success?, "#{what} failed: #{File.read(err.path)}"
        File.read(out.path)
      end
    end
  end
end
