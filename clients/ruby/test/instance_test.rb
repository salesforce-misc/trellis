# frozen_string_literal: true

require "rbconfig"
require "test_helper"

# Trellis::Instance (issue #878, epic #806): one process holding several
# handles, each on a catalog schema of its own, in one database.
class InstanceTest < Minitest::Test
  include TrellisTestCase

  # An instance of the shared cluster's database on `schema`, migrated first
  # (a staging worker can't start on an unmigrated schema).
  def connect(schema, **options)
    plain = Trellis::Instance.connect(url: TestCluster.dsn, schema:)
    plain.migrate
    plain.shutdown
    Trellis::Instance.connect(url: TestCluster.dsn, schema:, **options)
  end

  def test_connect_returns_an_instance_and_a_second_one_can_follow
    one = connect("inst_one")
    two = connect("inst_two")

    assert_instance_of Trellis::Instance, one
    assert one.connected?
    assert two.connected?
    refute_same one, two
    assert_equal "inst_one", one.config.schema
    assert_equal "inst_two", two.config.schema
  end

  def test_new_is_private_because_connect_is_the_constructor
    assert_raises(NoMethodError) { Trellis::Instance.new(nil, "inst_one") }
  end

  def test_an_instance_validates_its_options_like_the_module_does
    assert_raises(Trellis::ValidationError) { Trellis::Instance.connect(url: 1) }
    assert_raises(Trellis::ValidationError) { Trellis::Instance.connect(url: TestCluster.dsn, staging: 1) }
    assert_raises(Trellis::ValidationError) { Trellis::Instance.connect(url: TestCluster.dsn, drain_threads: -1) }
    assert_raises(Trellis::ValidationError) { Trellis::Instance.connect(url: TestCluster.dsn, worker_threads: 0) }
  end

  # Each instance keeps its definitions in its own schema.
  def test_two_instances_keep_their_own_definitions
    pg = TestCluster.pg
    pg.exec("create table if not exists inst_widgets (id integer primary key, price integer)")
    one = connect("inst_defs_a")
    two = connect("inst_defs_b")

    one.define("TRANSFORM inst_one_prices FROM inst_widgets SELECT price AS price")

    assert_equal ["public.inst_one_prices"], one.definitions.map(&:target_table)
    assert_empty two.definitions
    assert_nil two.status("inst_one_prices")
    assert one.status("inst_one_prices")
  ensure
    pg&.exec("drop table if exists inst_widgets cascade")
    pg&.close
  end

  def test_shutting_one_instance_down_leaves_the_others_working
    one = connect("inst_one")
    two = connect("inst_two")

    assert_nil one.shutdown
    refute one.connected?
    assert two.connected?
    assert_nil two.status("no_such_target")
    assert_nil one.shutdown, "shutdown is idempotent"

    error = assert_raises(Trellis::ValidationError) { one.status("no_such_target") }
    assert_match "not connected", error.message
    assert_match "inst_one", error.message
  end

  # Each instance runs its own staging worker and drain threads: one going
  # away stops its own work and none of the other's.
  def test_a_running_instance_keeps_converging_while_another_shuts_down
    pg = TestCluster.pg
    pg.exec("create table if not exists inst_gadgets (id integer primary key, price integer)")
    pg.exec("create table if not exists inst_gizmos (id integer primary key, price integer)")
    one = connect("inst_conv_a", staging: true, drain_threads: 1)
    two = connect("inst_conv_b", staging: true, drain_threads: 1)
    one.define("TRANSFORM inst_gadget_prices FROM inst_gadgets SELECT price AS price")
    two.define("TRANSFORM inst_gizmo_prices FROM inst_gizmos SELECT price AS price")
    [[one, "inst_gadget_prices"], [two, "inst_gizmo_prices"]].each do |instance, target|
      eventually_value("#{target} to go live", ->(status) { status&.status == :live }) { instance.status(target) }
    end

    one.shutdown
    pg.exec("insert into inst_gizmos (id, price) values (1, 9)")
    token = two.watermark_token
    two.await_converged(token, timeout_ms: 30_000)
    assert_equal [[1, 9]], pg.exec("select id, price from inst_gizmo_prices").values.map { |row| row.map(&:to_i) }
  ensure
    one&.shutdown
    two&.shutdown
    if pg
      %w[inst_gadget_prices inst_gizmo_prices inst_gadgets inst_gizmos].each { |t| pg.exec("drop table if exists #{t} cascade") }
      pg.close
    end
  end

  def test_the_default_instance_and_named_instances_are_independent
    Trellis.connect(url: TestCluster.dsn)
    other = connect("inst_one")

    assert Trellis.connected?
    assert_same Trellis.default_instance, Trellis.default_instance
    refute_same other, Trellis.default_instance

    error = assert_raises(Trellis::ValidationError) { Trellis.connect(url: TestCluster.dsn) }
    assert_match "already connected", error.message

    Trellis.shutdown
    refute Trellis.connected?
    assert other.connected?, "shutting the default instance down left the other one running"
    assert_raises(Trellis::ValidationError) { Trellis.default_instance }
  end

  def test_shutdown_all_shuts_down_every_instance_including_the_default
    Trellis.connect(url: TestCluster.dsn)
    one = connect("inst_one")
    two = connect("inst_two")

    assert_equal [Trellis.default_instance, one, two].to_set, Trellis::Instance.connected.to_set
    assert_nil Trellis::Instance.shutdown_all

    assert_empty Trellis::Instance.connected
    refute Trellis.connected?
    refute one.connected?
    refute two.connected?
    assert_nil Trellis::Instance.shutdown_all, "with nothing connected it does nothing"
  end

  def test_an_instance_prints_its_schema_and_never_its_connection_string
    password = "s3cret-hunter2"
    instance = Trellis::Instance.connect(url: "#{TestCluster.dsn} password=#{password}", schema: "inst_one")

    [instance.inspect, instance.to_s].each do |printed|
      assert_includes printed, "inst_one"
      assert_includes printed, "connected"
      refute_includes printed, password
    end
    instance.shutdown
    assert_includes instance.inspect, "shut down"
  end

  # The at_exit backstop shuts down every instance, not only the default.
  # The script's own hook is registered before the first connect, so it runs
  # after Trellis's.
  def test_at_exit_shuts_down_every_registered_instance
    script = <<~RUBY
      require "trellis/pg"
      dsn = ARGV.fetch(0)
      instances = []
      at_exit { puts "after the exit hook: " + instances.map(&:connected?).inspect }
      Trellis.connect(url: dsn)
      instances << Trellis.default_instance
      instances << Trellis::Instance.connect(url: dsn, schema: "inst_one")
      instances << Trellis::Instance.connect(url: dsn, schema: "inst_two")
      puts "before exit: " + instances.map(&:connected?).inspect
    RUBY
    out = run_script(script)
    assert_equal ["before exit: [true, true, true]", "after the exit hook: [false, false, false]"],
                 out.lines(chomp: true)
  end

  def test_an_instance_shut_down_before_exit_is_not_shut_down_twice
    script = <<~RUBY
      require "trellis/pg"
      one = Trellis::Instance.connect(url: ARGV.fetch(0), schema: "inst_one")
      two = Trellis::Instance.connect(url: ARGV.fetch(0), schema: "inst_two")
      one.shutdown
      puts "two still connected: \#{two.connected?}"
    RUBY
    assert_equal "two still connected: true\n", run_script(script)
  end

  # An instance a forked child inherits is the parent's: the child's calls
  # on it raise, its shutdown_all leaves it alone, and the parent's
  # instances carry on.
  def test_a_forked_child_leaves_every_inherited_instance_alone
    one = connect("inst_one")
    two = connect("inst_two")
    out_r, out_w = IO.pipe

    child = fork do
      out_r.close
      [one, two].each do |instance|
        instance.status("no_such_target")
        out_w.puts "call: returned"
      rescue Trellis::ForkedHandleError
        out_w.puts "call: ForkedHandleError"
      end
      Trellis::Instance.shutdown_all
      out_w.puts "connected: #{[one, two].map(&:connected?).inspect}"
    ensure
      out_w.close
      exit!(0)
    end
    out_w.close

    status = wait_for_child(child, seconds: 30)
    output = out_r.read
    assert status.success?, output
    assert_equal ["call: ForkedHandleError", "call: ForkedHandleError", "connected: [false, false]"],
                 output.lines(chomp: true)
    assert one.connected?
    assert_nil two.status("no_such_target")
  ensure
    out_r&.close
  end

  private

  # Runs a Ruby script in a process of its own against the shared cluster,
  # and returns its output, failing if it fails or hangs.
  def run_script(script)
    lib = File.expand_path("../lib", __dir__)
    out_r, out_w = IO.pipe
    pid = Process.spawn(RbConfig.ruby, "-I", lib, "-e", script, TestCluster.dsn,
                        out: out_w, err: out_w, pgroup: true)
    out_w.close
    status = wait_for_child(pid, seconds: 60, group: true)
    output = out_r.read
    assert status.success?, output
    output
  ensure
    out_r&.close
  end
end
