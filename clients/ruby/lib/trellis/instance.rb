# frozen_string_literal: true

require_relative "error"
require_relative "values"

module Trellis
  # The largest `limit:` and `timeout_ms:` the engine takes (an i64 and a
  # u64 millisecond count); anything larger is refused as a ValidationError
  # rather than failing to convert in the extension.
  MAX_LIMIT = (2**63) - 1
  MAX_TIMEOUT_MS = (2**64) - 1
  SELF_CHECK_MODES = %i[standard strict].freeze

  # The argument checks every call makes before it reaches the extension.
  module Checks # :nodoc:
    private

    def string_option!(name, value)
      return if value.is_a?(String)

      raise ValidationError, "#{name} must be a String, got: #{value.inspect}"
    end

    def string!(what, value)
      raise ValidationError, "#{what} must be a String, got: #{value.inspect}" unless value.is_a?(String)
    end

    def limit!(limit)
      return if limit.is_a?(Integer) && limit.between?(1, MAX_LIMIT)

      raise ValidationError, "limit must be a positive Integer, got: #{limit.inspect}"
    end

    def timeout_ms!(timeout_ms)
      return if timeout_ms.is_a?(Integer) && timeout_ms.between?(0, MAX_TIMEOUT_MS)

      raise ValidationError, "timeout_ms must be a non-negative Integer, got: #{timeout_ms.inspect}"
    end

    def cursor!(cursor, what)
      return if cursor.nil? || cursor.is_a?(String)

      raise ValidationError, "after must be #{what}, got: #{cursor.inspect}"
    end
  end

  # One Trellis instance: a catalog schema in a database, and the native
  # handle that runs it from this process. A process holds as many as it has
  # catalog schemas (one per database, or several in one database), each with
  # its own connections and threads and no shared budget
  # (docs/decisions/0010-embeddable-clients.md, decision 3).
  #
  #   analytics = Trellis::Instance.connect(url: "host=localhost dbname=app", schema: "analytics")
  #   billing = Trellis::Instance.connect(url: "host=localhost dbname=app", schema: "billing")
  #   analytics.define("TRANSFORM widget_prices FROM widgets SELECT price AS price")
  #   billing.shutdown # analytics keeps running
  #
  # Every call the `Trellis` module makes is a method here: the module
  # delegates to its default instance (see Trellis.connect). The surface
  # mirrors the Rust crate's BlockingTrellis:
  #
  # - Lifecycle: connect, connected?, migrate, config, shutdown.
  # - Every statement form: apply runs one statement of Trellis's grammar
  #   (TRANSFORM, RELATIONSHIP, PAUSE TRANSFORM, RESUME TRANSFORM, DROP
  #   TRANSFORM, DROP RELATIONSHIP, ALTER TRANSFORM) and reports what it did
  #   as an Applied. define is the one guarded convenience: an apply that
  #   refuses anything but a TRANSFORM statement before applying it.
  # - Reads: status, definitions, relationships.
  # - Quarantine: quarantined, quarantine_status, sample_quarantined,
  #   poisoned_since, release_key. Pausing and resuming are statements:
  #   instance.apply("RESUME TRANSFORM order_totals.total").
  # - Operations: request_backfill, has_live_drain_workers?,
  #   has_live_staging_worker?, the read-your-writes pair watermark_token and
  #   await_converged, and the audit self_check, a background job that
  #   self_check_job reads back.
  #
  # Every call blocks on the database with the GVL released, so other threads
  # run meanwhile, and Thread#kill, Thread#raise, Timeout.timeout and Ctrl-C
  # interrupt it, leaving no thread behind. An interrupt abandons the wait, not
  # the work: the call itself ends within its 30-second deadline, where the
  # server stops it. Every failure raises a Trellis::Error subclass, a call that
  # runs out of time a Trellis::TimeoutError.
  #
  # Conventions: times are Times in UTC, at microsecond precision; a
  # quarantine target is an address string, a transform's bare target table
  # name ("order_totals") or "transform.column" ("order_totals.total"); the
  # cursor sample_quarantined returns, and watermark_token's token, are opaque
  # strings to pass back unchanged; and every symbol in a result comes from a
  # closed set, never from a string the database returned.
  #
  # An instance doesn't survive `fork`. Shut every instance down before
  # forking (Instance.shutdown_all; Puma's before_fork) and connect after
  # (Puma's before_worker_boot, Passenger's starting_worker_process). A
  # forked child's calls on an instance it inherited raise
  # Trellis::ForkedHandleError, and its shutdown leaves the parent's handle
  # alone and does nothing. So does Instance.connect in a child forked while
  # any instance of its parent was running (issue #600).
  class Instance
    extend Checks
    include Checks

    @instances = []
    @registry = Mutex.new
    @at_exit_installed = false

    class << self
      # Connects a new instance and returns it. Raises ForkedHandleError in
      # a process forked while its parent had an instance running
      # (connected, connecting, or not yet fully shut down): it may have
      # inherited a lock one of that instance's threads held (issue #600).
      #
      # - url: where the database is, as a libpq connection string
      #   ("host=... dbname=...") or URL. Nothing is read from the environment.
      # - schema: the schema Trellis keeps this instance's own tables in. Not
      #   "public", not target_schema, and not another instance's schema or
      #   target_schema; migrate refuses those.
      # - target_schema: the schema a bare target table name is created in.
      # - staging: whether this process runs the instance's staging worker,
      #   which installs change capture on the source tables and starts each
      #   new transform's backfill. Exactly one process in a fleet should,
      #   per instance.
      # - drain_threads: how many threads apply staged changes to the targets.
      #   Some process must run at least one per instance, or no definition
      #   reaches :live.
      # - worker_threads: the worker threads of each Rust runtime this
      #   instance owns (its calls', and its background client's), invisible
      #   to Ruby's own thread sizing, so kept small and explicit. Each
      #   instance has its own; nothing is shared between instances.
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

        handle = Native.connect(url, schema, target_schema, staging, drain_threads, worker_threads)
        instance = new(handle, schema)
        register(instance)
        instance
      end

      # The instances this process holds, the default one included: every
      # one it connected and hasn't shut down, minus any a forked child only
      # inherited.
      def connected
        snapshot.select(&:connected?)
      end

      # Shuts down every instance this process holds, the `Trellis` module's
      # default instance included, and waits for each. Does nothing for an
      # instance a forked child only inherited. Every instance is attempted;
      # if some fail, the first error is raised once the rest are done and
      # the others are warned about. What a forking server calls before it
      # forks (Puma's before_fork), and the at_exit hook's backstop.
      def shutdown_all
        errors = snapshot.filter_map do |instance|
          instance.shutdown
          nil
        rescue Error => e
          e
        end
        return nil if errors.empty?

        errors.drop(1).each { |e| warn "trellis: shutting down an instance failed: #{e.class}: #{e.message}" }
        raise errors.first
      end

      private

      # Removes a shut-down instance from the registry.
      def deregister(instance)
        @registry.synchronize { @instances.delete(instance) }
      end

      def register(instance)
        @registry.synchronize do
          @instances << instance
          install_at_exit
        end
      end

      def snapshot
        @registry.synchronize { @instances.dup }
      end

      def install_at_exit
        return if @at_exit_installed

        @at_exit_installed = true
        at_exit { shutdown_all_at_exit }
      end

      # The backstop for a process that exits without shutting its instances
      # down. A forked child that inherited them leaves them alone (see
      # #shutdown).
      def shutdown_all_at_exit
        shutdown_all
      rescue Error => e
        warn "trellis: shutting down at exit failed: #{e.class}: #{e.message}"
      end
    end

    def initialize(handle, schema)
      @handle = handle
      @schema = schema
      @lock = Mutex.new
    end
    private_class_method :new

    # Whether this instance holds a live handle. False after shutdown, and
    # in a forked child that hasn't connected its own.
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

    # The Config this instance connected with.
    def config
      Config.new(**handle.config)
    end

    # Registers a transform from a `TRANSFORM` statement, creates its target
    # table, and returns its Definition, at :waiting_to_backfill. Its
    # backfill runs in the background: poll #status for :live.
    #
    # Only a `TRANSFORM` statement: any other form (DROP, PAUSE, ...) raises
    # ValidationError without being applied, and a statement that doesn't
    # parse raises ParseError. #apply takes every form.
    #
    # Trellis runs this on its own connections, not in the caller's
    # transaction: if an enclosing migration rolls back, the definition stays.
    def define(statement)
      string!("the statement", statement)
      Definition.new(**handle.define(statement))
    end

    # Runs one statement of Trellis's grammar, whatever its form, and returns
    # an Applied saying what it did.
    #
    #   trellis.apply("TRANSFORM order_totals FROM orders SELECT price + tax AS total")
    #   # => #<data Trellis::Applied kind=:transform_defined, definition=#<data Trellis::Definition ...>, ...>
    #   trellis.apply("RESUME TRANSFORM order_totals.total").columns
    #   # => ["order_totals.total"]
    #
    # A statement that doesn't parse raises ParseError, and nothing is
    # applied. Like #define, this runs on Trellis's own connections, not in
    # the caller's transaction.
    #
    # An Applied of kind :unknown is a success like any other: the statement
    # took effect, and only its outcome is newer than this binding can
    # describe. Don't retry it; read the result back with #status,
    # #definitions or #relationships if you need it.
    def apply(statement)
      string!("the statement", statement)
      Applied.from_native(handle.apply(statement))
    end

    # The Status of the definition writing target_table (a bare table name,
    # or "schema.table"), or nil when none does.
    def status(target_table)
      string!("the target table", target_table)
      hash = handle.status(target_table)
      hash && Status.from_native(hash)
    end

    # Every registered transform definition, oldest first, as
    # DefinitionSummary values.
    def definitions
      handle.definitions.map { |hash| DefinitionSummary.from_native(hash) }
    end

    # Every registered relationship, oldest first, as RelationshipSummary
    # values.
    def relationships
      handle.relationships.map { |hash| RelationshipSummary.from_native(hash) }
    end

    # Asks the staging worker to re-read source_table (a bare table name,
    # resolved like one in a statement) for every definition that reads it,
    # and returns once that re-read is queued.
    #
    # Each :live reader reports :backfilling (a plain aggregate or a plain
    # 1-1 transform, rebuilt by the call itself) or :catching_up (any other)
    # until the re-read has re-derived every row the table still has and
    # deleted any target row it no longer backs. A newly defined transform
    # doesn't need this: its backfill is queued for it. Only a table Trellis
    # already captures can be re-read; any other raises NotFoundError.
    def request_backfill(source_table)
      string!("the source table", source_table)
      handle.request_backfill(source_table)
      nil
    end

    # Releases one key `transform` holds in quarantine, once its cause is
    # fixed. source_table ("public.orders" or "orders") and key are as
    # sample_quarantined and poisoned_since report them.
    #
    # It stages a recompute of the key, which every transform reading the
    # table applies from the key's current row, and discards the changes held
    # for `transform` meanwhile. Another transform holding the same key keeps
    # holding it. If the cause is still there, the key is poisoned again.
    # Resuming the transform releases every key it holds.
    #
    # An unknown transform raises NotFoundError, and so does a key it doesn't
    # hold, which changes nothing. The release first waits for the drain
    # pages in flight on the table to commit; a wait past the lock timeout
    # (30 seconds) raises TimeoutError and changes nothing: call it again.
    #
    #   trellis.release_key("order_totals", "public.orders", "42")
    def release_key(transform, source_table, key)
      string!("the transform", transform)
      string!("the source table", source_table)
      string!("the key", key)
      handle.release_key(transform, source_table, key)
      nil
    end

    # Every source row the apply path poisoned (gave up on) after `since` (a
    # Time), oldest first, as PoisonEntry values. Poll it with the last
    # entry's poisoned_at so a whole-table failure doesn't sit unnoticed.
    def poisoned_since(since)
      raise ValidationError, "since must be a Time, got: #{since.inspect}" unless since.is_a?(::Time)

      micros = EpochMicros.from_time(since) or
        raise ValidationError, "since is out of range: #{since.inspect}"
      handle.poisoned_since(micros).map { |hash| PoisonEntry.from_native(hash) }
    end

    # Every quarantined transform and paused column, across every transform,
    # as QuarantineEntry values. Cheap enough for a dashboard or health
    # check to poll.
    def quarantined
      handle.quarantined.map { |hash| QuarantineEntry.from_native(hash) }
    end

    # The QuarantineEntry of one target: a transform ("order_totals") or one
    # of its columns ("order_totals.total"). A column that isn't paused is
    # :live.
    def quarantine_status(target)
      string!("the target", target)
      QuarantineEntry.from_native(handle.quarantine_status(target))
    end

    # One SamplePage of the rows quarantined under target, to diagnose a
    # quarantine's cause: for a column ("order_totals.total"), the rows that
    # failed evaluating it; for a whole transform ("order_totals"), the keys
    # poisoned from its source table.
    #
    # - limit: the most rows to return.
    # - after: the previous page's next_cursor; nil for the first page.
    #
    #   page = trellis.sample_quarantined("order_totals.total", limit: 50)
    #   page = trellis.sample_quarantined("order_totals.total", limit: 50, after: page.next_cursor)
    def sample_quarantined(target, limit: 100, after: nil)
      string!("the target", target)
      limit!(limit)
      cursor!(after, "a cursor from a previous page")
      SamplePage.from_native(handle.sample_quarantined(target, after, limit))
    end

    # Whether at least one drain worker is alive anywhere in the fleet. With
    # none, nothing reaches a target table: poll this from a health check.
    def has_live_drain_workers?
      handle.has_live_drain_workers
    end

    # Whether the staging worker (which installs change capture and starts
    # backfills) is alive anywhere in the fleet. The other half of the health check.
    def has_live_staging_worker?
      handle.has_live_staging_worker
    end

    # A read-your-writes token (an opaque String) covering every write
    # committed before this call. Take it after a source-table write commits,
    # then pass it to #await_converged to wait for that write to reach its
    # targets.
    def watermark_token
      handle.watermark_token
    end

    # Waits until every change committed at or before `token` has reached its
    # target tables, or timeout_ms passes (a TimeoutError; retry it). Returns
    # nil.
    #
    # It waits for captured changes only: a transform that isn't :live yet
    # may still be missing rows when this returns (see #status).
    #
    # Like every call, it returns within 30 seconds: a longer timeout_ms is
    # cut to that, and a caller that wants to wait longer calls again. Calls
    # from other threads don't wait behind it.
    def await_converged(token, timeout_ms:)
      string!("the token", token)
      timeout_ms!(timeout_ms)
      handle.await_converged(token, timeout_ms)
      nil
    end

    # Starts a background check of target_table against a fresh recompute of
    # its definition from the source tables, and returns a SelfCheckJob at
    # once. The comparison is a drain worker's, a page of keys at a time
    # (with no drain worker anywhere in the fleet the job stays :queued, as
    # a define does); poll self_check_job with the job's id until it is
    # finished, and its report is the verdict.
    #
    # - timeout_ms: how long a page of the job waits for the target to catch
    #   up, per wait. A :standard check waits up to twice per page, a
    #   :strict one once.
    # - mode: :standard re-checks anything that differs after a fresh wait,
    #   so a change still in flight isn't reported; it is safe while the
    #   source is being written. :strict skips the re-check, and is only
    #   sound once writes to the audited tables have stopped.
    #
    # A target whose job is still :queued or :running gets that job back,
    # whatever mode and timeout_ms this call passed; a finished job stays
    # until the next self_check of its target replaces it. Only a
    # one-row-per-source-key transform that reads no relationship can be
    # audited: an aggregate or relationship-reading target raises
    # ValidationError, and an unknown one NotFoundError. A column that is
    # paused is left out of the comparison.
    #
    #   job = trellis.self_check("order_totals", timeout_ms: 30_000)
    #   sleep 1 until (job = trellis.self_check_job(job.id)).finished?
    #   job.report.outcome # => :converged
    #
    # The call reads nothing of the target, so it returns well inside the
    # 30-second call limit, holding no connection afterwards.
    def self_check(target_table, timeout_ms:, mode: :standard)
      string!("the target table", target_table)
      timeout_ms!(timeout_ms)
      unless SELF_CHECK_MODES.include?(mode)
        raise ValidationError, "mode must be :standard or :strict, got: #{mode.inspect}"
      end

      SelfCheckJob.from_native(handle.self_check(target_table, mode.to_s, timeout_ms))
    end

    # The SelfCheckJob that self_check returned with this id, as it stands
    # now, or nil when there is none: a newer self_check of its target
    # replaced it, or its transform was dropped.
    def self_check_job(id)
      unless id.is_a?(Integer) && id.between?(-(2**63), MAX_LIMIT)
        raise ValidationError, "the job id must be an Integer, got: #{id.inspect}"
      end

      hash = handle.self_check_job(id)
      hash && SelfCheckJob.from_native(hash)
    end

    # Stops this instance: its background work, its connections and its
    # runtime thread, and waits for them. The other instances of the process
    # are untouched. Does nothing if not connected, which includes a forked
    # child holding only the instance it inherited: that one is the parent's,
    # so it's left in place, untouched, and the child's other calls on it
    # still raise ForkedHandleError. A child's cleanup code (Puma's
    # on_worker_shutdown, say) can call this whether or not the child
    # connected. A shut-down instance is spent: connect a new one.
    def shutdown
      @lock.synchronize do
        return nil unless connected?

        handle = @handle
        begin
          handle.shutdown
        ensure
          # Also when the shutdown raised or was interrupted: its call still
          # takes the engine out of the handle, so the handle is spent.
          @handle = nil
          self.class.send(:deregister, self)
        end
      end
      nil
    end

    # Shows the schema, never the connection string.
    def inspect
      "#<#{self.class.name} schema=#{@schema.inspect} #{@handle.nil? ? 'shut down' : 'connected'}>"
    end
    alias to_s inspect

    private

    def handle
      @handle or raise ValidationError,
                       "this Trellis instance (schema #{@schema.inspect}) is not connected: " \
                       "it has been shut down"
    end
  end
end
