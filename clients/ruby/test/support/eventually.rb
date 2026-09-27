# frozen_string_literal: true

# Polling for the tests that wait on the engine's background workers. Every
# wait is bounded, and a timeout names what it waited for and the last value
# it saw (#297).
module Eventually
  CONVERGE_SECONDS = 30

  # Calls the block until it returns something truthy and returns that, or
  # fails the test naming what it waited for and the last value it saw.
  def eventually(what, seconds: CONVERGE_SECONDS, &)
    eventually_value(what, ->(seen) { seen }, seconds:, &)
  end

  # Calls the block until `done` accepts the value it returns, and returns
  # that value; or fails the test naming what it waited for and the last
  # value it saw. For a wait whose "not yet" value is worth reporting:
  #
  #   eventually_value("orders to go live", ->(s) { s&.status == :live }) do
  #     Trellis.status("orders")
  #   end
  def eventually_value(what, done, seconds: CONVERGE_SECONDS)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + seconds
    loop do
      seen = yield
      return seen if done.call(seen)
      if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
        flunk "waited #{seconds}s for #{what}; last saw #{seen.inspect}"
      end

      sleep 0.05
    end
  end

  # Waits for target_table's definition to reach `wanted` and returns its
  # Status.
  def await_status(target_table, wanted = :live)
    eventually_value("#{target_table} to reach #{wanted.inspect}",
                     ->(status) { status&.status == wanted }) do
      Trellis.status(target_table)
    end
  end
end
