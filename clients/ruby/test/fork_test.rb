# frozen_string_literal: true

require "rbconfig"
require "socket"
require "test_helper"

# ADR-0010 decision 3: a handle does not survive fork. Rust threads don't
# cross it, so a child's calls on the handle it inherited would wait forever
# for a reply; instead every one raises Trellis::ForkedHandleError at once.
class ForkTest < Minitest::Test
  include TrellisTestCase

  # Every call on the handle but shutdown, each with arguments it would
  # accept. The last test below keeps it complete.
  SURFACE = {
    "status" => -> { Trellis.status("no_such_target") },
    "migrate" => -> { Trellis.migrate },
    "define" => -> { Trellis.define("TRANSFORM t FROM no_such_source SELECT a AS a") },
    "apply" => -> { Trellis.apply("PAUSE TRANSFORM t") },
    "config" => -> { Trellis.config },
    "definitions" => -> { Trellis.definitions },
    "relationships" => -> { Trellis.relationships },
    "request_backfill" => -> { Trellis.request_backfill("t") },
    "release_key" => -> { Trellis.release_key("t", "s", "1") },
    "poisoned_since" => -> { Trellis.poisoned_since(Time.now) },
    "quarantined" => -> { Trellis.quarantined },
    "quarantine_status" => -> { Trellis.quarantine_status("t.c") },
    "sample_quarantined" => -> { Trellis.sample_quarantined("t.c") },
    "has_live_drain_workers?" => -> { Trellis.has_live_drain_workers? },
    "has_live_staging_worker?" => -> { Trellis.has_live_staging_worker? },
    "watermark_token" => -> { Trellis.watermark_token },
    "await_converged" => -> { Trellis.await_converged("0/0", timeout_ms: 1_000) },
    "self_check" => -> { Trellis.self_check("t", limit: 1, timeout_ms: 1_000) }
  }.freeze

  def test_a_forked_child_s_calls_raise_rather_than_hang
    Trellis.connect(url: TestCluster.dsn)
    parent = Process.pid
    out_r, out_w = IO.pipe

    child = fork do
      out_r.close
      {
        **SURFACE,
        # Not connected in this process, so shutdown does nothing, and leaves
        # the inherited handle in place for the calls after it to refuse.
        "shutdown" => -> { Trellis.shutdown },
        "status after shutdown" => -> { Trellis.status("no_such_target") }
      }.each do |name, call|
        call.call
        out_w.puts "#{name}: returned"
      rescue StandardError => e
        out_w.puts "#{name}: #{e.class}: #{e.message}"
      end
      out_w.puts "connected?: #{Trellis.connected?}"
    ensure
      out_w.close
      # Never the parent's at_exit hooks (minitest's would rerun the suite).
      exit!(0)
    end
    out_w.close

    status = wait_for_child(child, seconds: 30)
    output = out_r.read
    assert status.success?, output

    lines = output.lines(chomp: true)
    [*SURFACE.keys, "status after shutdown"].each do |name|
      assert_includes lines,
                      "#{name}: Trellis::ForkedHandleError: this Trellis handle was connected by " \
                      "process #{parent}, and this is process #{child}: a handle does not survive " \
                      "fork, so call Trellis.connect in this process (after forking: Puma's " \
                      "before_worker_boot, Unicorn's after_fork, Passenger's " \
                      "starting_worker_process; \"Forking servers\" in clients/ruby/README.md " \
                      "covers preload_app! and fork_worker)",
                      output
    end
    assert_includes lines, "shutdown: returned", output
    assert_includes lines, "connected?: false"

    # The parent's handle is untouched.
    assert Trellis.connected?
    assert_nil Trellis.status("no_such_target")
  ensure
    out_r&.close
  end

  # The whole life of a forking server's workers, in a script of its own so
  # every process exits normally, running its at_exit hooks and freeing its
  # handles. A child forked while the parent's handle runs can't use it or
  # connect its own (issue #600); a worker forked after the parent shut down
  # connects its own, uses it, and exits; the parent connects again.
  def test_a_child_connects_only_if_its_parent_shut_down_before_forking
    script = File.expand_path("support/fork_worker.rb", __dir__)
    lib = File.expand_path("../lib", __dir__)
    out_r, out_w = IO.pipe
    pid = Process.spawn(RbConfig.ruby, "-I", lib, script, TestCluster.dsn,
                        out: out_w, err: out_w, pgroup: true)
    out_w.close

    status = wait_for_child(pid, seconds: 60, group: true)
    output = out_r.read
    assert status.success?, output
    assert_equal <<~OUT, output
      child: Trellis::ForkedHandleError
      child: connect Trellis::ForkedHandleError, forked while running: true
      child: connected? false
      child exit: 0
      worker: own handle status nil
      worker: connected? true
      worker exit: 0
      parent: status nil
    OUT
  ensure
    out_r&.close
  end

  # Issue #600: an engine still connecting on another thread has threads,
  # and may hold a process-wide lock, before `Trellis` holds any handle. A
  # child forked then can't connect; one forked once that connect has
  # finished (here, failed) can.
  #
  # The connect is held mid-way by a server that accepts its connection and
  # never answers, so the fork is certain to land while its threads run.
  def test_a_child_forked_while_another_thread_connects_can_t_connect
    server = TCPServer.new("127.0.0.1", 0)
    url = "host=127.0.0.1 port=#{server.addr[1]} dbname=trellis user=trellis"
    connecting = Thread.new do
      Thread.current.report_on_exception = false
      Trellis.connect(url: url, staging: true)
    end
    assert server.wait_readable(30), "the connect never reached the server"
    peer = server.accept
    refute Trellis.connected?

    parent = Process.pid
    child, output = fork_to_connect
    assert_equal "Trellis::ForkedHandleError: this process (#{child}) was forked from process " \
                 "#{parent} while that process had a Trellis engine running, so it may have " \
                 "inherited a lock one of the engine's threads held, which nothing in this " \
                 "process can release: it can't connect. Call Trellis.shutdown before forking " \
                 "and Trellis.connect after: Puma's before_fork and before_worker_boot, and " \
                 "with fork_worker, before_worker_fork and after_worker_fork too " \
                 "(\"Forking servers\" in clients/ruby/README.md)\n", output

    # The server hangs up, so the connect fails, and its threads are gone.
    peer.close
    assert_raises(Trellis::ConnectivityError) do
      connecting.join(30) or flunk "the connect didn't fail once the server hung up"
    end
    _, output = fork_to_connect
    assert_equal "connected\n", output
  ensure
    peer&.close
    server&.close
  end

  # Every public method but connect and connected? goes through the handle,
  # so a method added to the module without a line in SURFACE (or, for
  # shutdown, its own case above) fails here rather than going unchecked.
  def test_the_surface_names_every_public_method_that_uses_the_handle
    uses_handle = Trellis.singleton_methods.map(&:to_s) - %w[connect connected?]
    assert_equal uses_handle.sort, [*SURFACE.keys, "shutdown"].sort
  end

  private

  # Forks a child that connects (running nothing in the background) and
  # reports how that went. Returns the child's pid and its report.
  def fork_to_connect
    out_r, out_w = IO.pipe
    child = fork do
      out_r.close
      begin
        Trellis.connect(url: TestCluster.dsn)
        out_w.puts "connected"
        Trellis.shutdown
      rescue StandardError => e
        out_w.puts "#{e.class}: #{e.message}"
      end
    ensure
      out_w.close
      exit!(0)
    end
    out_w.close
    assert wait_for_child(child, seconds: 30).success?
    [child, out_r.read]
  ensure
    out_r&.close
  end
end
