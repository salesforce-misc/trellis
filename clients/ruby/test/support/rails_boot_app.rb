# frozen_string_literal: true

# A minimal Rails app, booted in a process of its own by rails_test.rb, one
# scenario per run: ARGV[0] names it, and TRELLIS_TEST_DSN is the cluster.
# Prints one JSON line of what it saw.
$LOAD_PATH.unshift File.expand_path("../../lib", __dir__)
require "json"
# ActiveRecord's own connection, never opened: with no config/database.yml,
# loading ActiveRecord::Base needs one configured.
ENV["DATABASE_URL"] = "postgresql://localhost/unused"
require "logger"
require "rails"
require "active_record/railtie"
# After Rails, as Bundler.require in config/application.rb loads it.
require "trellis"

class BootApp < Rails::Application
  config.root = __dir__
  config.eager_load = false
  config.logger = Logger.new(nil)
  config.secret_key_base = "test"
  config.trellis.connect = { "url" => ENV.fetch("TRELLIS_TEST_DSN"), "target_schema" => "boot_targets" }
end

seen = {}
case ARGV.fetch(0)
when "boot"
  BootApp.initialize!
  seen[:connected] = Trellis.connected?
  seen[:target_schema] = Trellis.config.target_schema
  # The Railtie requires the migration helpers as ActiveRecord loads.
  ActiveRecord::Base
  seen[:migration_after_active_record] = defined?(Trellis::Migration) ? true : false
when "unset"
  BootApp.config.trellis.connect = nil
  BootApp.initialize!
  seen[:connected] = Trellis.connected?
when "connect_on_boot_false"
  BootApp.config.trellis.connect_on_boot = false
  BootApp.initialize!
  seen[:connected] = Trellis.connected?
  Trellis::Railtie.connect
  seen[:connected_after_connect] = Trellis.connected?
when "rake"
  # What `rails db:migrate` does: load the tasks, then the environment
  # (the `environment` task requires config/environment.rb, which
  # initializes the app; this app has no such file).
  BootApp.load_tasks
  BootApp.initialize!
  seen[:connected] = Trellis.connected?
  Rake::Task["trellis:migrate"].invoke
  seen[:connected_after_migrate] = Trellis.connected?
when "puma_hooks"
  # A preloading server: the app boots in the parent, which shuts its handle
  # down before forking (before_fork), and each worker connects its own
  # (before_worker_boot).
  BootApp.initialize!
  Trellis.shutdown
  reader, writer = IO.pipe
  pid = fork do
    reader.close
    Trellis::Railtie.connect
    writer.puts JSON.generate(connected: Trellis.connected?, status: Trellis.status("no_such_target"))
    Trellis.shutdown
    exit!(0)
  rescue StandardError => e
    writer.puts JSON.generate(error: e.class.name)
    exit!(1)
  end
  writer.close
  seen[:child] = reader.wait_readable(60) ? JSON.parse(reader.gets || "null") : "no report within 60s"
  Process.kill(:KILL, pid) if seen[:child].is_a?(String)
  Process.waitpid(pid)
  seen[:parent_connected] = Trellis.connected?
else
  abort "unknown scenario: #{ARGV[0]}"
end
puts JSON.generate(seen)
Trellis.shutdown
