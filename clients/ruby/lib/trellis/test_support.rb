# frozen_string_literal: true

require_relative "error"
require_relative "instance"

module Trellis
  # Helpers for a test suite that loads fixtures or bulk data into tables
  # Trellis captures. Not loaded by `require "trellis/pg"`:
  #
  #   require "trellis/test_support"
  #
  # A fixture loader that disables triggers for the load (Rails'
  # `disable_referential_integrity`, `pg_restore --disable-triggers`, a
  # hand-written `ALTER TABLE ... DISABLE TRIGGER ALL`) also disables
  # Trellis's capture triggers, so the rows it writes never reach a target,
  # and `ENABLE TRIGGER ALL` leaves them in a mode the staging worker treats
  # as a disruption of capture. Trellis then pauses every definition that
  # reads the table (a CaptureFailure of kind :disruption on its status)
  # rather than let a test read targets that are silently wrong. A test
  # suite's answer is a RESUME after the load, which rebuilds the targets
  # from the tables as they are now:
  #
  #   Trellis::TestSupport.resume_capture_disruptions
  #
  # Include the module to call it as a method of the test or its class:
  #
  #   # test/test_helper.rb of a Rails app
  #   class ActiveSupport::TestCase
  #     include Trellis::TestSupport
  #     fixtures :all
  #     # Rails loads the fixtures in its own setup, before this block runs.
  #     setup { resume_capture_disruptions }
  #   end
  #
  # The call is cheap when nothing was disrupted (one catalog read per
  # captured table), so running it before every test is fine.
  module TestSupport
    # Raised when a RESUME is refused or a resumed definition pauses again.
    # `refused` maps each such target (as "schema.table") to the reason: the
    # engine's error for a refused RESUME, or the new CaptureFailure's
    # sentence. Those definitions stay paused. Every other disrupted
    # definition was resumed, and waited for, before this is raised.
    class ResumeRefused < ValidationError
      attr_reader :refused

      def initialize(refused)
        @refused = refused.freeze
        super("could not resume #{refused.size} definition(s) paused by a capture disruption " \
              "(they stay paused): " + refused.map { |target, why| "#{target}: #{why}" }.join("; "))
      end
    end

    POLL_SECONDS = 0.05
    # How a CaptureFailure's error starts while a resume is still under way.
    RESUMING = "resuming:"

    module_function

    # Resumes every definition of `instance` (the default instance if none
    # is given) that is paused because Trellis's capture of a table it reads
    # was changed outside Trellis, waits until each is :live again, then
    # waits until every write committed so far has reached its targets
    # (await_converged), so a test that follows reads exact targets. Returns
    # the resumed targets, as "schema.table" strings (empty when nothing was
    # disrupted).
    #
    # It runs a disruption check of its own first (Instance#detect_capture_disruptions),
    # so a disruption the staging worker has not seen yet is paused and
    # resumed here rather than a pass later.
    #
    # It resumes only the CaptureFailure kind :disruption. A definition an
    # operator paused (PAUSE TRANSFORM), one paused by a schema change, a
    # drain halt or a failing build stays paused. A definition paused because
    # an upstream one was resumes with its upstream.
    #
    # A RESUME the engine refuses (for example, the table a definition read
    # is missing, or a table define would now refuse) leaves that definition
    # paused. The others are still resumed and waited for; then
    # ResumeRefused, listing every refusal, is raised. A definition that
    # does not reach :live within `timeout_ms` raises Trellis::TimeoutError.
    #
    # The instance needs its staging worker and a drain worker running
    # somewhere (the usual test setup connects with `staging: true,
    # drain_threads: 1`): resuming rebuilds through them.
    def resume_capture_disruptions(instance = Trellis.default_instance, timeout_ms: 30_000)
      deadline = monotonic + (timeout_ms / 1000.0)
      instance.detect_capture_disruptions

      refused = {}
      resumed = []
      disrupted(instance).each do |target|
        instance.apply("RESUME TRANSFORM #{target}")
        resumed << target
      rescue ValidationError, NotFoundError, ConflictError => e
        refused[target] = e.message
      end

      resumed.each { |target| await_live(instance, target, deadline, refused) }
      converge(instance, deadline)
      raise ResumeRefused, refused unless refused.empty?

      resumed - refused.keys
    end

    # The targets of `instance`'s definitions that are paused with a
    # CaptureFailure of kind :disruption, oldest first.
    def disrupted(instance)
      instance.definitions.select { |definition| definition.status == :paused }.filter_map do |definition|
        status = instance.status(definition.target_table)
        definition.target_table if status&.capture_failure&.kind == :disruption
      end
    end

    # Waits for `target` to be :live. A resumed definition that pauses
    # again (its rebuild failed) is recorded in `refused` instead of waited
    # for.
    def await_live(instance, target, deadline, refused)
      loop do
        status = instance.status(target)
        return if status&.status == :live

        if status&.status == :paused
          failure = status.capture_failure
          # Paused again by something else: its rebuild failed. A failure
          # whose error starts "resuming:" is the resume still under way (the
          # staging worker re-types Trellis's columns first, or waits for an
          # upstream definition), and is waited for. (Paused again by a
          # disruption is waited out: the next resume is the caller's, and
          # the timeout reports it.)
          if failure && failure.kind != :disruption && !failure.error.start_with?(RESUMING)
            refused[target] = failure.error
            return
          end
        end
        raise TimeoutError, "#{target} was not :live in time after its resume; last saw #{status.inspect}" if monotonic > deadline

        sleep POLL_SECONDS
      end
    end

    # Waits for everything committed so far to reach its targets, in as many
    # waits as the 30-second call limit needs.
    def converge(instance, deadline)
      token = instance.watermark_token
      loop do
        remaining_ms = ((deadline - monotonic) * 1000).to_i
        raise TimeoutError, "targets did not converge in time" if remaining_ms <= 0

        begin
          return instance.await_converged(token, timeout_ms: [remaining_ms, 30_000].min)
        rescue TimeoutError
          raise if monotonic >= deadline
        end
      end
    end

    def monotonic
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
end
