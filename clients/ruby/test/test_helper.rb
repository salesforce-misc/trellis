# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "trellis/pg"
require_relative "support/test_cluster"
require_relative "support/eventually"

# Registered before minitest/autorun's own at_exit hook, so it runs after the
# suite has: at_exit hooks run last-registered first.
TestCluster.start
at_exit { TestCluster.stop }

require "minitest/autorun"

module TrellisTestCase
  include Eventually

  # Every test leaves this process disconnected, whatever it did.
  def teardown
    Trellis::Instance.shutdown_all
    super
  end

  # A bounded wait on a child process: its exit status, or a failure (after
  # killing it) if it doesn't exit within `seconds`. Never hangs. With
  # `group: true` the child leads its own process group (spawned with
  # `pgroup: true`), and a hang kills the whole group, grandchildren too.
  def wait_for_child(pid, seconds:, group: false)
    deadline = monotonic + seconds
    loop do
      _, status = Process.waitpid2(pid, Process::WNOHANG)
      return status if status

      if monotonic > deadline
        Process.kill(:KILL, group ? -pid : pid)
        Process.waitpid(pid)
        flunk "process #{pid} was still running after #{seconds}s: it hung"
      end
      sleep 0.02
    end
  end

  # Forks a process that takes an ACCESS EXCLUSIVE lock on `table`, so any
  # Trellis call that reads it blocks until the lock goes. Returns once the
  # lock is held, with the holder's pid and a `release` IO: the lock is held
  # for `seconds` if given, else until `release` is closed (or 60s pass).
  # Close `release` and wait_for_child(pid) when done.
  #
  # A process rather than a thread, so the lock is released on time whether
  # or not the blocked call lets this process's other threads run.
  def hold_lock(table, seconds: nil)
    ready_r, ready_w = IO.pipe
    release_r, release_w = IO.pipe
    pid = fork do
      ready_r.close
      release_w.close
      pg = PG.connect(TestCluster.dsn)
      pg.exec("BEGIN")
      pg.exec("LOCK TABLE #{table} IN ACCESS EXCLUSIVE MODE")
      ready_w.puts "locked"
      ready_w.close
      seconds ? sleep(seconds) : release_r.wait_readable(60)
      pg.exec("COMMIT")
    ensure
      # Never the parent's at_exit hooks (minitest's would rerun the suite).
      exit!(0)
    end
    ready_w.close
    release_r.close
    unless ready_r.wait_readable(30) && ready_r.gets == "locked\n"
      release_w.close
      wait_for_child(pid, seconds: 30)
      flunk "the lock holder never took its lock on #{table}"
    end
    [pid, release_w]
  ensure
    ready_r&.close
  end

  def monotonic
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end
end
