# frozen_string_literal: true

require "rails/railtie"

module Trellis
  # Connects this process's handle as a Rails app boots, from the app's
  # `config.trellis`. `require "trellis/pg"` loads it when Rails is loaded
  # first, which is what Bundler.require in config/application.rb does.
  #
  #   # config/application.rb, or config/environments/*.rb
  #   config.trellis.connect = {
  #     url: ENV.fetch("DATABASE_URL"),
  #     staging: ENV["TRELLIS_WORKER"] == "1",
  #     drain_threads: ENV["TRELLIS_WORKER"] == "1" ? 2 : 0
  #   }
  #
  # config.trellis.connect takes Trellis.connect's options, and is the one
  # place the Rails integration reads them from: the boot connect, the
  # migration helpers (Trellis::Migration) and `rails trellis:migrate` all
  # use it. Leave it nil (the default) and the Railtie does nothing.
  #
  # When it's set, the app's handle connects in an after_initialize hook,
  # except in these two cases:
  #
  # - An app booted by a rake task (`rails db:migrate`, `rails db:create`,
  #   ...): like Ecto, which migrates without starting the app, those get no
  #   boot handle. The database may not exist yet, and a migration run must
  #   not start the worker's background work. The migration helpers connect
  #   a handle of their own; a task of your own that calls Trellis calls
  #   Trellis::Railtie.connect. It's the task that counts, not the command:
  #   `rails test` loads the tasks to run test:prepare, then boots the app
  #   outside any task, so it gets its handle (but `bin/rails db:test:prepare
  #   test` boots it inside the first task, so it doesn't).
  # - config.trellis.connect_on_boot = false, for a server that forks with
  #   no hook to shut the handle down first (see below).
  #
  # A handle doesn't survive fork (issue #600). A preforking server that
  # loads the app before forking (Puma's preload_app!, on by default with
  # more than one worker) must shut the boot handle down before it forks,
  # and connect one in each worker:
  #
  #   # config/puma.rb
  #   before_fork { Trellis.shutdown }
  #   before_worker_boot { Trellis::Railtie.connect } # on_worker_boot before Puma 7
  #
  # With Puma's fork_worker, worker 0 forks the others, so it needs
  # before_worker_fork and after_worker_fork hooks as well: see "Forking
  # servers" in the README.
  class Railtie < ::Rails::Railtie
    config.trellis = ActiveSupport::OrderedOptions.new
    config.trellis.connect = nil
    config.trellis.connect_on_boot = true

    rake_tasks do
      namespace :trellis do
        desc "Create or upgrade Trellis's own tables, with config.trellis.connect's url"
        task migrate: :environment do
          Trellis::Railtie.with_handle { nil }
        end
      end
    end

    initializer "trellis.migration" do
      ActiveSupport.on_load(:active_record) { require "trellis/migration" }
    end

    config.after_initialize { Trellis::Railtie.instance.send(:boot) }

    class << self
      # Connects this process's handle with config.trellis.connect's options:
      # in a forked worker (Puma's before_worker_boot, Unicorn's after_fork,
      # Passenger's starting_worker_process), or a rake task of your own.
      # Raises ValidationError if config.trellis.connect isn't set, and
      # whatever Trellis.connect raises.
      def connect
        Trellis.connect(**required_connect_options)
      end

      # Runs the block with this process's handle, running Trellis.migrate
      # first, and returns the block's value. If the process isn't connected
      # (a rake task gets no boot handle), connects one for the length of
      # the block from config.trellis.connect, with staging: false and
      # drain_threads: 0 so it starts no background work, and shuts it down
      # after. What `rails trellis:migrate` and Trellis::Migration's helpers
      # run on, for a script or rake task of your own that needs Trellis the
      # way a migration does. Needs no ActiveRecord.
      #
      # Raises what the block raises. If the shutdown fails too, that's a
      # warning, so it doesn't hide the block's error; after a block that
      # returned, the shutdown's error is raised. Raises ValidationError if
      # the process isn't connected and config.trellis.connect isn't set.
      def with_handle
        if Trellis.connected?
          Trellis.migrate
          return yield
        end

        Trellis.connect(**required_connect_options.merge(staging: false, drain_threads: 0))
        primary = nil
        begin
          Trellis.migrate
          yield
        rescue Exception => e # any exception, Interrupt too: it's re-raised
          primary = e
          raise
        ensure
          shutdown_after(primary)
        end
      end

      # config.trellis.connect, as Trellis.connect's keyword options, or nil
      # when it isn't set (or there's no Rails app).
      def connect_options
        app = ::Rails.respond_to?(:application) && ::Rails.application or return nil
        options = app.config.trellis.connect or return nil
        unless options.respond_to?(:to_h)
          raise ValidationError,
                "config.trellis.connect must be a Hash of Trellis.connect's options, got: #{options.inspect}"
        end

        options.to_h.transform_keys(&:to_sym)
      end

      private

      def required_connect_options
        connect_options or
          raise ValidationError, "config.trellis.connect is not set: give it Trellis.connect's options"
      end

      # Shuts with_handle's own handle down. With a primary error on its way
      # out, a failed shutdown is only a warning: raising would replace it.
      def shutdown_after(primary)
        Trellis.shutdown
      rescue StandardError => e
        raise unless primary

        warn "trellis: shutting down after #{primary.class} failed: #{e.class}: #{e.message}"
      end
    end

    private

    def boot
      return if rake_running?

      settings = ::Rails.application.config.trellis
      return unless settings.connect && settings.connect_on_boot

      self.class.connect
    end

    # Whether the app is booting inside a rake run (`rails db:migrate`,
    # `rake db:create`), not just in a process that loaded the tasks:
    # `rails test` loads them to run test:prepare, and only then, outside
    # any task, boots the app.
    def rake_running?
      defined?(::Rake.application) && ::Rake.application.top_level_tasks.any?
    end
  end
end
