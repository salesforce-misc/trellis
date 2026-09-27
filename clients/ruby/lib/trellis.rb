# frozen_string_literal: true

require_relative "trellis/error"
require_relative "trellis/values"
# The native extension (ext/trellis_ruby), which `rake compile` builds and
# copies here.
require_relative "trellis/trellis_ruby"

# Embedded Trellis for Ruby apps: a native extension over the `trellis`
# crate's BlockingTrellis, so a Rails app can define and run streaming
# transforms without deploying a separate service
# (docs/decisions/0010-embeddable-clients.md).
#
# One handle per process, held by this module: Trellis.connect opens it,
# every other method uses it, and Trellis.shutdown closes it (an at_exit hook
# is the backstop).
#
#   # A deploy's migration step: the defaults run nothing in the background.
#   Trellis.connect(url: "host=localhost dbname=app")
#   Trellis.migrate
#   Trellis.shutdown
#
#   # The app, once migrated.
#   Trellis.connect(url: "host=localhost dbname=app", staging: true, drain_threads: 2)
#   Trellis.define("TRANSFORM widget_prices FROM widgets SELECT price AS price")
#   Trellis.status("widget_prices").status # => :live, once it has backfilled
#
# Every call blocks on the database with the GVL released, so other threads
# run meanwhile, and Thread#kill, Thread#raise and Ctrl-C interrupt it. An
# interrupt abandons the wait, not the work: the call itself still finishes
# in the background.
#
# A handle doesn't survive `fork`. Shut down before forking (Puma's
# before_fork) and connect after (on_worker_boot, Passenger's
# starting_worker_process). A forked child's calls on a handle it inherited
# raise Trellis::ForkedHandleError, except Trellis.shutdown, which leaves the
# parent's handle alone and does nothing. So does Trellis.connect in a child
# forked while its parent's handle was running (issue #600).
module Trellis
  @handle = nil
  @lock = Mutex.new
  @at_exit_installed = false

  class << self
    # Connects this process's handle. Raises ValidationError if this process
    # is already connected: shut that handle down first. Raises
    # ForkedHandleError in a process forked while its parent had a handle
    # running (connected, connecting, or not yet fully shut down): it may
    # have inherited a lock one of that handle's threads held (issue #600).
    #
    # - url: where the database is, as a libpq connection string
    #   ("host=... dbname=...") or URL. Nothing is read from the environment.
    # - schema: the schema Trellis keeps its own tables in.
    # - target_schema: the schema a bare target table name is created in.
    # - staging: whether this process runs the staging worker (change
    #   capture). Exactly one process in a fleet should.
    # - drain_threads: how many threads apply staged changes to the targets.
    #   Some process must run at least one, or no definition reaches :live.
    # - worker_threads: the Rust runtime's worker threads, invisible to
    #   Ruby's own thread sizing, so kept small and explicit.
    #
    # The defaults (staging: false, drain_threads: 0) run nothing in the
    # background, which is what a migration step or a console wants. The
    # staging worker reads Trellis's tables as it starts, so a staging: true
    # connect to an unmigrated schema fails: migrate first.
    def connect(url:, schema: "trellis", target_schema: "public", staging: false,
                drain_threads: 0, worker_threads: 2)
      string_option!(:url, url)
      string_option!(:schema, schema)
      string_option!(:target_schema, target_schema)
      unless [true, false].include?(staging)
        raise ValidationError, "staging must be true or false, got: #{staging.inspect}"
      end
      unless drain_threads.is_a?(Integer) && drain_threads >= 0
        raise ValidationError,
              "drain_threads must be a non-negative Integer, got: #{drain_threads.inspect}"
      end
      unless worker_threads.is_a?(Integer) && worker_threads >= 1
        raise ValidationError,
              "worker_threads must be a positive Integer, got: #{worker_threads.inspect}"
      end

      @lock.synchronize do
        if connected?
          raise ValidationError,
                "this process is already connected: call Trellis.shutdown first " \
                "(one handle per process)"
        end
        @handle = Native.connect(url, schema, target_schema, staging, drain_threads,
                                 worker_threads)
        install_at_exit
      end
      nil
    end

    # Whether this process holds a live handle. False before connect, after
    # shutdown, and in a forked child that hasn't connected its own.
    def connected?
      handle = @handle
      !handle.nil? && handle.owner_pid == Process.pid
    end

    # Creates or upgrades Trellis's own tables in the configured schema. Safe
    # to run on every deploy.
    def migrate
      handle.migrate
      nil
    end

    # Registers a transform from a `TRANSFORM` statement and returns its
    # Definition. Its backfill runs in the background: poll #status for :live.
    #
    # Only a `TRANSFORM` statement: any other form (DROP, PAUSE, ...) raises
    # ValidationError without being applied.
    def define(statement)
      raise ValidationError, "the statement must be a String" unless statement.is_a?(String)

      Definition.new(**handle.define(statement))
    end

    # The Status of the definition writing target_table (a bare table name,
    # or "schema.table"), or nil when none does.
    def status(target_table)
      raise ValidationError, "the target table must be a String" unless target_table.is_a?(String)

      hash = handle.status(target_table)
      hash && Status.from_native(hash)
    end

    # Stops this process's handle: its background work, its connections and
    # its runtime thread, and waits for them. Does nothing if not connected,
    # which includes a forked child holding only the handle it inherited: that
    # one is the parent's, so it's left in place, untouched, and the child's
    # other calls on it still raise ForkedHandleError. A child's cleanup code
    # (Puma's on_worker_shutdown, say) can call this whether or not the child
    # connected.
    def shutdown
      @lock.synchronize do
        return nil unless connected?

        handle = @handle
        begin
          handle.shutdown
        ensure
          # Also when the shutdown raised or was interrupted: its call still
          # takes the instance out of the handle, so the handle is spent.
          @handle = nil
        end
      end
      nil
    end

    private

    def handle
      @handle or raise ValidationError, "Trellis is not connected: call Trellis.connect first"
    end

    def string_option!(name, value)
      return if value.is_a?(String)

      raise ValidationError, "#{name} must be a String, got: #{value.inspect}"
    end

    def install_at_exit
      return if @at_exit_installed

      @at_exit_installed = true
      at_exit { shutdown_at_exit }
    end

    # The backstop for a process that exits without calling shutdown. A
    # forked child that inherited the handle leaves it alone (see #shutdown).
    def shutdown_at_exit
      shutdown
    rescue Error => e
      warn "trellis: shutting down at exit failed: #{e.class}: #{e.message}"
    end
  end
end
