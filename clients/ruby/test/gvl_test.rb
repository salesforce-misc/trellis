# frozen_string_literal: true

require "test_helper"
require "timeout"

# ADR-0010 decision 3: every call releases the GVL, and has an unblocking
# function so an interrupt reaches a thread blocked in one. A call waits on
# the calling thread itself, so an interrupt leaves no thread behind (issue
# #599).
#
# A call is made slow by locking the catalog table `status` reads from
# another process, so it blocks inside the engine for as long as the lock
# is held.
class GvlTest < Minitest::Test
  include TrellisTestCase

  CATALOG = "trellis.transform_definitions"

  # Every call goes through the same GVL-releasing wait in the extension;
  # this checks it for a call from the slice and one from the full surface.
  def test_a_blocking_call_lets_other_threads_run
    Trellis.connect(url: TestCluster.dsn)
    {
      "status" => -> { assert_nil Trellis.status("no_such_target") },
      "definitions" => -> { assert_kind_of Array, Trellis.definitions }
    }.each do |name, call|
      holder, release = hold_lock(CATALOG, seconds: 1.0)
      begin
        assert_lets_other_threads_run(name, call)
      ensure
        release.close
        wait_for_child(holder, seconds: 30)
      end
    end
  end

  def test_thread_kill_and_thread_raise_interrupt_a_blocked_call
    Trellis.connect(url: TestCluster.dsn)
    holder, release = hold_lock(CATALOG)

    killed = Thread.new { Trellis.status("no_such_target") }
    assert_blocked killed
    killed.kill
    assert killed.join(5), "Thread#kill didn't end a thread blocked in a Trellis call"

    stop = Class.new(StandardError)
    raised = Thread.new do
      Thread.current.report_on_exception = false
      Trellis.status("no_such_target")
    end
    assert_blocked raised
    raised.raise(stop, "stop waiting")
    assert_raises(stop) do
      raised.join(5) or flunk "Thread#raise didn't reach a thread blocked in a Trellis call"
    end

    release.close
    wait_for_child(holder, seconds: 30)
    holder = nil
    # The handle still works: the abandoned calls ended on their own.
    assert_nil Trellis.status("no_such_target")
  ensure
    release&.close
    wait_for_child(holder, seconds: 30) if holder
  end

  # Ctrl-C: SIGINT raises Interrupt in the main thread even while it waits
  # on a call. Run in a child process of its own, which connects its own
  # handle (after the fork, as a forking server's workers do), so the signal
  # can't land anywhere else.
  def test_sigint_interrupts_a_blocked_call_in_the_main_thread
    holder, release = hold_lock(CATALOG)
    out_r, out_w = IO.pipe
    child = fork do
      out_r.close
      Trellis.connect(url: TestCluster.dsn)
      out_w.puts "calling"
      out_w.flush
      begin
        Trellis.status("no_such_target")
        out_w.puts "returned"
      rescue Interrupt
        out_w.puts "interrupted"
      end
    ensure
      out_w.close
      exit!(0)
    end
    out_w.close

    assert out_r.wait_readable(30), "the child never connected"
    assert_equal "calling\n", out_r.gets
    sleep 0.3 # into the call, which blocks on the lock
    Process.kill(:INT, child)

    assert wait_for_child(child, seconds: 10).success?
    assert_equal "interrupted\n", out_r.read
  ensure
    out_r&.close
    release&.close
    wait_for_child(holder, seconds: 30) if holder
  end

  # Issue #599: an interrupted call used to leave a helper thread behind until
  # the engine answered, so repeated interrupt-and-retry against a stuck
  # database piled threads up without bound. The wait is on the calling thread
  # now, so interrupts add none. Each interrupted call leaves its engine-side
  # work running (and holding a pooled connection) until its deadline, which
  # can make the engine's own runtime start a few threads, so the bound is
  # well below the number of interrupts rather than zero.
  def test_interrupting_calls_leaves_no_thread_behind
    Trellis.connect(url: TestCluster.dsn)
    Trellis.status("no_such_target") # the engine's threads are up
    holder, release = hold_lock(CATALOG)
    before = os_threads

    interrupts = 24
    interrupts.times do |i|
      case i % 3
      when 0
        assert_raises(Timeout::Error) { Timeout.timeout(0.05) { Trellis.status("no_such_target") } }
      when 1
        thread = Thread.new { Trellis.status("no_such_target") }
        sleep 0.1 # into the call, which blocks on the lock
        thread.kill
        assert thread.join(5), "Thread#kill didn't end a thread blocked in a Trellis call"
      else
        thread = Thread.new do
          Thread.current.report_on_exception = false
          Trellis.status("no_such_target")
        end
        sleep 0.1 # into the call, which blocks on the lock
        thread.raise(Class.new(StandardError), "stop waiting")
        assert_raises(StandardError) { thread.join(5) or flunk "Thread#raise didn't end the call" }
      end
    end

    grown = os_threads - before
    assert_operator grown, :<, 8,
                    "#{interrupts} interrupted calls left #{grown} more OS threads behind (a thread per call would leave #{interrupts})"
  ensure
    release&.close
    wait_for_child(holder, seconds: 30) if holder
  end

  # Shutdown doesn't wait for a call stuck on a lock, abandoned or not: it
  # cancels what is in flight, and what the call started on the server ends at
  # the call's deadline.
  def test_shutdown_returns_promptly_with_an_abandoned_call_stuck_on_a_lock
    Trellis.connect(url: TestCluster.dsn)
    holder, release = hold_lock(CATALOG)

    assert_raises(Timeout::Error) { Timeout.timeout(0.3) { Trellis.status("no_such_target") } }
    started = monotonic
    assert_nil Trellis.shutdown
    elapsed = monotonic - started

    assert_operator elapsed, :<, 10, "shutdown waited #{elapsed.round(1)}s for an abandoned call stuck on a lock"
    refute Trellis.connected?
  ensure
    release&.close
    wait_for_child(holder, seconds: 30) if holder
  end

  def test_shutdown_returns_promptly_while_another_thread_is_stuck_in_a_call
    Trellis.connect(url: TestCluster.dsn)
    holder, release = hold_lock(CATALOG)

    stuck = Thread.new do
      Thread.current.report_on_exception = false
      Trellis.status("no_such_target")
    end
    sleep 0.3 # into the call, which blocks on the lock
    started = monotonic
    assert_nil Trellis.shutdown
    elapsed = monotonic - started

    assert_operator elapsed, :<, 10, "shutdown waited #{elapsed.round(1)}s for a call stuck on a lock"
    assert_raises(Trellis::Error, "the stuck call fails once the handle is shut down") do
      stuck.join(10) or flunk "the stuck call never ended after the shutdown"
    end
  ensure
    release&.close
    wait_for_child(holder, seconds: 30) if holder
  end

  # A call stuck on a lock ends at the engine's 30-second deadline with a
  # TimeoutError, on the calling thread, without a thread of its own. Not
  # shortened: the extension has no knob for the deadline, so this one waits it
  # out.
  def test_a_call_stuck_on_a_lock_raises_timeout_error_at_the_deadline
    Trellis.connect(url: TestCluster.dsn)
    Trellis.status("no_such_target")
    holder, release = hold_lock(CATALOG)
    before = os_threads

    started = monotonic
    error = assert_raises(Trellis::TimeoutError) { Trellis.status("no_such_target") }
    elapsed = monotonic - started

    assert_operator elapsed, :>=, 29, "the call ended after #{elapsed.round(1)}s, before its deadline: #{error.message}"
    assert_operator elapsed, :<, 45, "the call took #{elapsed.round(1)}s: #{error.message}"
    assert_operator os_threads, :<=, before + 2
    release.close
    wait_for_child(holder, seconds: 30)
    holder = nil
    assert_nil Trellis.status("no_such_target"), "the handle works after a timed-out call"
  ensure
    release&.close
    wait_for_child(holder, seconds: 30) if holder
  end

  private

  # This process's OS threads (Linux).
  def os_threads
    Dir.children("/proc/self/task").size
  end

  def assert_lets_other_threads_run(name, call)
    ticks = 0
    ticker = Thread.new do
      loop do
        ticks += 1
        sleep 0.01
      end
    end
    started = monotonic
    call.call
    elapsed = monotonic - started
    ticker.kill.join

    assert_operator elapsed, :>=, 0.5, "#{name} wasn't slow: it didn't wait for the lock"
    # A call holding the GVL starves the ticker for its whole duration: it
    # ticks 0 times (checked by making the extension wait with the GVL held).
    # ~100 ticks fit in a second when the call releases it; 3 leaves a slow
    # CI runner plenty of room while still telling the two apart.
    assert_operator ticks, :>=, 3,
                    "the ticker ran only #{ticks} times in #{elapsed.round(2)}s: #{name} held the GVL"
  end

  def assert_blocked(thread)
    sleep 0.3
    assert thread.alive?, "the call returned rather than blocking on the lock"
  end
end
