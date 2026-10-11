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

require_relative "instance"

# Embedded Trellis for Ruby apps: a native extension over the `trellis`
# crate's BlockingTrellis, so a Rails app can define and run streaming
# transforms without deploying a separate service
# (docs/decisions/0010-embeddable-clients.md).
#
# A Trellis::Instance is one catalog schema's handle, and a process holds as
# many as it has schemas (Trellis::Instance.connect). This module is the
# common case's shortcut: Trellis.connect opens the process's default
# instance, every other method here is that instance's, and Trellis.shutdown
# closes it. An at_exit hook shuts down every instance the process still
# holds, the default included.
#
# The surface mirrors the Rust crate's BlockingTrellis, and is documented on
# Trellis::Instance:
#
# - Lifecycle: connect, connected?, migrate, config, shutdown.
# - Every statement form: apply, and define for a TRANSFORM statement alone.
# - Reads: status, definitions, relationships.
# - Quarantine: quarantined, quarantine_status, sample_quarantined,
#   poisoned_since, release_key.
# - Operations: request_backfill, has_live_drain_workers?,
#   has_live_staging_worker?, watermark_token, await_converged, self_check,
#   self_check_job.
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
#   # A second instance, in a schema of its own.
#   reports = Trellis::Instance.connect(url: "host=localhost dbname=app", schema: "reports")
#   reports.status("monthly_totals")
#   reports.shutdown # the default instance keeps running
#
# A handle doesn't survive `fork`. Shut down before forking
# (Trellis::Instance.shutdown_all; Puma's before_fork) and connect after
# (Puma's before_worker_boot, Passenger's starting_worker_process); in a
# Rails app, Trellis::Railtie.connect does the connecting. A forked child's
# calls on a handle it inherited raise Trellis::ForkedHandleError, except
# Trellis.shutdown, which leaves the parent's handle alone and does nothing.
# So does Trellis.connect in a child forked while its parent's handle was
# running (issue #600).
module Trellis
  @default = nil
  @lock = Mutex.new

  class << self
    # Connects this process's default instance, with the options
    # Trellis::Instance.connect takes. Raises ValidationError if the default
    # instance is already connected: shut it down first. A second instance
    # is Trellis::Instance.connect's. Raises ForkedHandleError in a process
    # forked while its parent had an instance running.
    def connect(**options)
      @lock.synchronize do
        if connected?
          raise ValidationError,
                "this process's default instance is already connected: call Trellis.shutdown " \
                "first (Trellis::Instance.connect opens another instance)"
        end
        @default = Instance.connect(**options)
      end
      nil
    end

    # Whether this process's default instance is connected. False before
    # connect, after shutdown, and in a forked child that hasn't connected
    # its own.
    def connected?
      default = @default
      !default.nil? && default.connected?
    end

    # The default instance Trellis.connect opened, for code that takes an
    # Instance. Raises ValidationError if there is none.
    def default_instance
      @default or raise ValidationError, "Trellis is not connected: call Trellis.connect first"
    end

    # Stops the default instance (see Trellis::Instance#shutdown). Does
    # nothing if it is not connected, which includes a forked child holding
    # only the one it inherited.
    def shutdown
      @lock.synchronize do
        return nil unless connected?

        begin
          @default.shutdown
        ensure
          @default = nil
        end
      end
      nil
    end

    # Trellis::Instance's calls, on the default instance.
    Instance.public_instance_methods(false).each do |name|
      next if %i[connected? shutdown inspect to_s].include?(name)

      define_method(name) { |*args, **options, &block| default_instance.public_send(name, *args, **options, &block) }
    end
  end
end

# The Rails integration, only when Rails is loaded first (Bundler.require in
# config/application.rb). Without Rails, nothing here requires a gem.
# Trellis::Migration (lib/trellis/migration.rb) needs ActiveRecord: the
# Railtie loads it with ActiveRecord, and an app without Rails requires it.
require_relative "railtie" if defined?(::Rails::Railtie)
