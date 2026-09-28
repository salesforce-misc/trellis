# frozen_string_literal: true

require "rails/railtie"

module Trellis
  # Connects this process's handle as a Rails app boots, from the app's
  # `config.trellis`. `require "trellis"` loads it when Rails is loaded
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
  # - A rake task (`rails db:migrate`, `rails db:create`, ...): like Ecto,
  #   which migrates without starting the app, those get no boot handle. The
  #   database may not exist yet, and a migration run must not start the
  #   worker's background work. The migration helpers connect a handle of
  #   their own; a task of your own that calls Trellis calls
  #   Trellis::Railtie.connect.
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
  class Railtie < ::Rails::Railtie
    config.trellis = ActiveSupport::OrderedOptions.new
    config.trellis.connect = nil
    config.trellis.connect_on_boot = true

    # Runs in a rake process only, before any task loads the app: the
    # railtie instance is `self` here.
    rake_tasks do
      @rake_task = true

      namespace :trellis do
        desc "Create or upgrade Trellis's own tables, with config.trellis.connect's url"
        task migrate: :environment do
          require "trellis/migration"
          Trellis::Migration.with_handle { nil }
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
        options = connect_options or
          raise ValidationError, "config.trellis.connect is not set: give it Trellis.connect's options"
        Trellis.connect(**options)
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
    end

    private

    def boot
      return if @rake_task

      settings = ::Rails.application.config.trellis
      return unless settings.connect && settings.connect_on_boot

      self.class.connect
    end
  end
end
