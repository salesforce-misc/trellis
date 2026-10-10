# frozen_string_literal: true

# A minimal Rails app, booted in a process of its own by rails_test.rb, one
# scenario per run: ARGV[0] names it, and TRELLIS_TEST_DSN is the cluster.
# Prints one JSON line of what it saw.
$LOAD_PATH.unshift File.expand_path("../../lib", __dir__)
require "json"
require "tmpdir"

# An app without ActiveRecord: the bundle has the gem, so this shadows it
# with a file whose require fails the way it would if the app's Gemfile
# didn't list it.
WITHOUT_ACTIVE_RECORD = ARGV.fetch(0) == "rake_without_active_record"
if WITHOUT_ACTIVE_RECORD
  shadow = Dir.mktmpdir("no-active-record")
  File.write(File.join(shadow, "active_record.rb"),
             'raise LoadError, "cannot load such file -- active_record"')
  $LOAD_PATH.unshift shadow
end

# ActiveRecord's own connection, never opened: with no config/database.yml,
# loading ActiveRecord::Base needs one configured.
ENV["DATABASE_URL"] = "postgresql://localhost/unused"
require "logger"
require "rails"
require "active_record/railtie" unless WITHOUT_ACTIVE_RECORD
# After Rails, as Bundler.require in config/application.rb loads it.
require "trellis/pg"

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
when "rake", "rake_without_active_record"
  # What `rails db:migrate` does: a rake run loads the tasks and runs one
  # that needs the app, whose `environment` prerequisite initializes it (by
  # requiring config/environment.rb, which this app hasn't got).
  require "rake"
  Rake.with_application do |rake|
    rake.init("rails", ["probe"])
    BootApp.load_tasks
    Rake::Task["environment"].enhance { BootApp.initialize! }
    Rake::Task.define_task(probe: :environment) do
      seen[:connected] = Trellis.connected?
      Rake::Task["trellis:migrate"].invoke
      seen[:connected_after_migrate] = Trellis.connected?
      # The scenario's premise: nothing loaded ActiveRecord.
      seen[:active_record] = defined?(::ActiveRecord) ? true : false if WITHOUT_ACTIVE_RECORD
    end
    rake.top_level
  end
when "tasks_loaded_before_boot"
  # What `rails test` does: it loads the tasks to run test:prepare, and
  # only then, outside any task, boots the app.
  require "rake"
  Rake.with_application do |rake|
    rake.init("rails", ["test:prepare"])
    BootApp.load_tasks
    Rake::Task.define_task("test:prepare")
    rake.top_level
  end
  BootApp.initialize!
  seen[:connected] = Trellis.connected?
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
when "named_instances"
  # config.trellis.instances connect with the default instance at boot.
  BootApp.config.trellis.instances = { reports: { url: ENV.fetch("TRELLIS_TEST_DSN"), schema: "boot_reports" } }
  BootApp.initialize!
  seen[:connected] = Trellis.connected?
  seen[:named_schema] = Trellis::Railtie.named_instance(:reports).config.schema
  seen[:named_class] = Trellis::Railtie.named_instance("reports").class.name
  begin
    Trellis::Railtie.named_instance(:nope)
  rescue Trellis::ValidationError => e
    seen[:unknown_instance] = e.message
  end
  reports = Trellis::Railtie.named_instance(:reports)
  Trellis::Instance.shutdown_all
  seen[:connected_after_shutdown_all] = [Trellis.connected?, reports.connected?]
when "named_instances_only"
  BootApp.config.trellis.connect = nil
  BootApp.config.trellis.instances = { reports: { url: ENV.fetch("TRELLIS_TEST_DSN"), schema: "boot_reports" } }
  BootApp.initialize!
  seen[:connected] = Trellis.connected?
  seen[:named_connected] = Trellis::Railtie.named_instance(:reports).connected?
when "named_instances_rake"
  # `trellis:migrate` migrates every configured instance on handles of its
  # own, and leaves none connected.
  BootApp.config.trellis.instances = { reports: { url: ENV.fetch("TRELLIS_TEST_DSN"), schema: "boot_reports" } }
  require "rake"
  Rake.with_application do |rake|
    rake.init("rails", ["probe"])
    BootApp.load_tasks
    Rake::Task["environment"].enhance { BootApp.initialize! }
    Rake::Task.define_task(probe: :environment) do
      Rake::Task["trellis:migrate"].invoke
      seen[:connected_after_migrate] = Trellis.connected?
      seen[:named_after_migrate] = Trellis::Railtie.instance_options.keys
      seen[:with_handle] = Trellis::Railtie.with_handle(:reports) { |trellis| trellis.config.schema }
      seen[:with_handle_unknown] = begin
        Trellis::Railtie.with_handle(:nope) { nil }
      rescue Trellis::ValidationError => e
        e.message
      end
    end
    rake.top_level
  end
when "failed_connect_rolls_back"
  # A named instance that can't connect doesn't leave the default one (or an
  # earlier named one) connected.
  BootApp.config.trellis.connect_on_boot = false
  BootApp.config.trellis.instances = {
    first: { url: ENV.fetch("TRELLIS_TEST_DSN"), schema: "boot_reports" },
    broken: { url: ENV.fetch("TRELLIS_TEST_DSN"), schema: "boot_reports", worker_threads: 0 }
  }
  BootApp.initialize!
  begin
    Trellis::Railtie.connect
  rescue Trellis::ValidationError => e
    seen[:error] = e.message
  end
  seen[:connected] = Trellis.connected?
  begin
    Trellis::Railtie.named_instance(:first)
    seen[:first_connected] = true
  rescue Trellis::ValidationError
    seen[:first_connected] = false
  end
when "puma_hooks_named"
  # The same hooks with a named instance: shutdown_all in the parent, and
  # each worker connects every instance of its own.
  BootApp.config.trellis.instances = { reports: { url: ENV.fetch("TRELLIS_TEST_DSN"), schema: "boot_reports" } }
  BootApp.initialize!
  reports = Trellis::Railtie.named_instance(:reports)
  reports.migrate
  Trellis::Instance.shutdown_all
  reader, writer = IO.pipe
  pid = fork do
    reader.close
    Trellis::Railtie.connect
    writer.puts JSON.generate(connected: Trellis.connected?,
                              named: Trellis::Railtie.named_instance(:reports).status("no_such_target"))
    Trellis::Instance.shutdown_all
    exit!(0)
  rescue StandardError => e
    writer.puts JSON.generate(error: e.class.name)
    exit!(1)
  end
  writer.close
  seen[:child] = reader.wait_readable(60) ? JSON.parse(reader.gets || "null") : "no report within 60s"
  Process.kill(:KILL, pid) if seen[:child].is_a?(String)
  Process.waitpid(pid)
  seen[:parent_connected] = [Trellis.connected?, reports.connected?]
else
  abort "unknown scenario: #{ARGV[0]}"
end
puts JSON.generate(seen)
Trellis::Instance.shutdown_all
