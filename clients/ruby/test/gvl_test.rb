# frozen_string_literal: true

require "test_helper"

# ADR-0010 decision 3: every call releases the GVL, and has an unblocking
# function so an interrupt reaches a thread blocked in one.
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
    # The handle still works: the abandoned calls finished in the background.
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

  private

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
