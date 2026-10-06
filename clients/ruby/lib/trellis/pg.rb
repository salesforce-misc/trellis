# frozen_string_literal: true

require_relative "error"
require_relative "values"
require_relative "version"
require "rbconfig"

# The native extension (ext/trellis_ruby). Installing the source gem, or
# `rake compile`, builds the one this Ruby needs into lib/trellis/. A platform
# gem instead carries one build per Ruby minor version, each in
# lib/trellis/<major.minor>/, and none in lib/trellis/ itself.
#
# A lib/trellis/ build wins when both are there: in a checkout, a
# per-version directory is only ever left over from building a platform gem
# (it's ignored by git, so nothing shows it), and `rake test` must load the
# extension it just compiled, not that one.
dlext = RbConfig::CONFIG.fetch("DLEXT")
per_version = "#{RUBY_VERSION[/\A\d+\.\d+/]}/trellis_ruby"
if !File.exist?(File.join(__dir__, "trellis_ruby.#{dlext}")) &&
   File.exist?(File.join(__dir__, "#{per_version}.#{dlext}"))
  require_relative per_version
else
  require_relative "trellis_ruby"
end

# Embedded Trellis for Ruby apps: a native extension over the `trellis`
# crate's BlockingTrellis, so a Rails app can define and run streaming
# transforms without deploying a separate service
# (docs/decisions/0010-embeddable-clients.md).
#
# One handle per process, held by this module: Trellis.connect opens it,
# every other method uses it, and Trellis.shutdown closes it (an at_exit hook
# is the backstop).
#
# The surface mirrors the Rust crate's BlockingTrellis:
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
#   Trellis.apply("RESUME TRANSFORM order_totals.total").
# - Operations: request_backfill, has_live_drain_workers?,
#   has_live_staging_worker?, the read-your-writes pair watermark_token and
#   await_converged, and the audit self_check.
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
# in the background. Every failure raises a Trellis::Error subclass.
#
# Conventions: times are Times in UTC, at microsecond precision; a
# quarantine target is an address string, a transform's bare target table
# name ("order_totals") or "transform.column" ("order_totals.total"); the
# cursors sample_quarantined and self_check return, and watermark_token's
# token, are opaque strings to pass back unchanged; and every symbol in a
# result comes from a closed set, never from a string the database returned.
#
# A handle doesn't survive `fork`. Shut down before forking (Puma's
# before_fork) and connect after (Puma's before_worker_boot, Passenger's
# starting_worker_process); in a Rails app, Trellis::Railtie.connect does
# the connecting. A forked child's calls on a handle it inherited
# raise Trellis::ForkedHandleError, except Trellis.shutdown, which leaves the
# parent's handle alone and does nothing. So does Trellis.connect in a child
# forked while its parent's handle was running (issue #600).
module Trellis
  # The largest `limit:` and `timeout_ms:` the engine takes (an i64 and a
  # u64 millisecond count); anything larger is refused as a ValidationError
  # rather than failing to convert in the extension.
  MAX_LIMIT = (2**63) - 1
  MAX_TIMEOUT_MS = (2**64) - 1
  SELF_CHECK_MODES = %i[standard strict].freeze

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
    # - staging: whether this process runs the staging worker, which installs
    #   change capture on the source tables and starts each new transform's
    #   backfill. Exactly one process in a fleet should.
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

    # The Config this process's handle connected with.
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
    #   Trellis.apply("TRANSFORM order_totals FROM orders SELECT price + tax AS total")
    #   # => #<data Trellis::Applied kind=:transform_defined, definition=#<data Trellis::Definition ...>, ...>
    #   Trellis.apply("RESUME TRANSFORM order_totals.total").columns
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
    # Each :live reader reports :catching_up until the re-read has re-derived
    # every row the table still has and deleted any target row it no longer
    # backs. A newly defined transform doesn't need this: its backfill is
    # queued for it. Only a table Trellis already captures can be re-read;
    # any other raises NotFoundError.
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
    # hold, which changes nothing.
    #
    #   Trellis.release_key("order_totals", "public.orders", "42")
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
    #   page = Trellis.sample_quarantined("order_totals.total", limit: 50)
    #   page = Trellis.sample_quarantined("order_totals.total", limit: 50, after: page.next_cursor)
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
    # The handle runs one call at a time, so every other call on it, from
    # any thread, waits behind this one for up to timeout_ms. Size it
    # accordingly.
    def await_converged(token, timeout_ms:)
      string!("the token", token)
      timeout_ms!(timeout_ms)
      handle.await_converged(token, timeout_ms)
      nil
    end

    # Audits one page of target_table against a fresh recompute of its
    # definition from the source tables, and returns a SelfCheckReport of
    # what differs.
    #
    # - limit: the most keys to audit in this call.
    # - timeout_ms: how long to wait for the target to catch up, per wait. A
    #   :standard check waits up to twice, a :strict one once.
    # - after: the previous report's next_after; nil for the first page.
    # - mode: :standard re-checks anything that differs after a fresh wait,
    #   so a change still in flight isn't reported; it is safe while the
    #   source is being written. :strict skips the re-check, and is only
    #   sound once writes to the audited tables have stopped.
    #
    # Only a one-row-per-source-key transform can be audited: an aggregate
    # target raises ValidationError, and an unknown one NotFoundError. A
    # column that is paused is left out of the comparison. To sweep a whole
    # target, chain calls through next_after:
    #
    #   report = Trellis.self_check("order_totals", limit: 1_000, timeout_ms: 30_000)
    #   report = Trellis.self_check("order_totals", limit: 1_000, timeout_ms: 30_000,
    #                               after: report.next_after)
    #
    # The handle runs one call at a time, and this one can hold it for up to
    # timeout_ms per wait plus the comparison of up to `limit` keys. To sweep
    # a large target alongside live traffic, run the audit in a process of
    # its own, connected with the defaults so it runs no background work.
    def self_check(target_table, limit:, timeout_ms:, after: nil, mode: :standard)
      string!("the target table", target_table)
      limit!(limit)
      timeout_ms!(timeout_ms)
      cursor!(after, "a cursor from a previous report")
      unless SELF_CHECK_MODES.include?(mode)
        raise ValidationError, "mode must be :standard or :strict, got: #{mode.inspect}"
      end

      SelfCheckReport.from_native(
        handle.self_check(target_table, after, limit, mode.to_s, timeout_ms)
      )
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

# The Rails integration, only when Rails is loaded first (Bundler.require in
# config/application.rb). Without Rails, nothing here requires a gem.
# Trellis::Migration (lib/trellis/migration.rb) needs ActiveRecord: the
# Railtie loads it with ActiveRecord, and an app without Rails requires it.
require_relative "railtie" if defined?(::Rails::Railtie)
