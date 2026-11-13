# frozen_string_literal: true

require "fileutils"
require "minitest/mock"
require "tmpdir"
require "test_helper"
require_relative "support/rails_app"
require "trellis/schema_dump"

# config.trellis.off_in: the environments Trellis is off in, so a test suite
# runs without it. The predicate and the Railtie's entry points need no
# database; the schema load, the dump and the migration helpers run Rails'
# own tasks in-process over databases of the test cluster, the way
# schema_dump_test.rb does. (`parallelize` is simulated by the call Rails
# makes in each forked worker.)
class OffInTest < Minitest::Test
  include TrellisTestCase

  Tasks = ActiveRecord::Tasks::DatabaseTasks

  @serial = 0
  def self.next_id = (@serial += 1)

  def setup
    super
    @dir = Dir.mktmpdir("trellis-off-in")
    @admin = TestCluster.pg
    @id = self.class.next_id
    @databases = []
    @saved = { db_dir: Tasks.db_dir, schema_format: ActiveRecord.schema_format, env: Tasks.env,
               configurations: ActiveRecord::Base.configurations }
    Tasks.db_dir = @dir
    ActiveRecord.schema_format = :sql
    # Rails' load records itself in the app's own database.
    use_app_database
    # The suite's app has Trellis on everywhere (support/rails_app.rb); these
    # tests run the Railtie's default.
    TrellisTestApp.config.trellis.off_in = DEFAULT_OFF_IN
  end

  def teardown
    TrellisTestApp.config.trellis.connect = nil
    TrellisTestApp.config.trellis.off_in = []
    Tasks.db_dir = @saved.fetch(:db_dir)
    Tasks.env = @saved.fetch(:env)
    ActiveRecord.schema_format = @saved.fetch(:schema_format)
    ActiveRecord::Base.configurations = @saved.fetch(:configurations)
    @databases.each { |name| @admin.exec("drop database if exists #{name} with (force)") }
    @admin.close
    FileUtils.rm_rf(@dir)
    super
  end

  def test_trellis_is_off_in_the_test_environment_by_default
    assert_equal %w[test], DEFAULT_OFF_IN
    assert Trellis::Railtie.off_in?("test")
    refute Trellis::Railtie.enabled?("test")
    assert Trellis::Railtie.enabled?("development")
    assert Trellis::Railtie.enabled?("production")
  end

  # An app that runs Trellis in its tests leaves `test` out of off_in.
  def test_an_empty_off_in_turns_trellis_on_in_the_test_environment
    TrellisTestApp.config.trellis.off_in = []
    refute Trellis::Railtie.off_in?("test")
    in_environment("test") { assert Trellis.enabled? }
  end

  def test_off_in_names_environments_as_strings_or_symbols
    TrellisTestApp.config.trellis.off_in = [:test, "staging"]
    assert Trellis::Railtie.off_in?("test")
    assert Trellis::Railtie.off_in?(:staging)
    refute Trellis::Railtie.off_in?("development")
    assert Trellis::Railtie.enabled?("development")
    refute Trellis::Railtie.enabled?("test")
  end

  def test_enabled_follows_this_processs_rails_environment
    in_environment("development") { assert Trellis.enabled? }
    in_environment("test") { refute Trellis.enabled? }
  end

  def test_connect_and_migrate_all_connect_nothing_in_an_off_environment
    TrellisTestApp.config.trellis.connect = { url: TestCluster.dsn }
    in_environment("test") do
      _, warned = capture_io { assert_nil Trellis::Railtie.connect }
      assert_includes warned, "Trellis is off in the test environment (config.trellis.off_in)"
      assert_includes warned, "connects nothing"
      refute Trellis.connected?

      _, warned = capture_io { assert_nil Trellis::Railtie.migrate_all }
      assert_includes warned, "migrates nothing"
      refute Trellis.connected?
    end
    # The same app and config, in an environment it is on in.
    in_environment("development") do
      Trellis::Railtie.connect
      assert Trellis.connected?
    end
  end

  # Nothing connects, so config.trellis.connect's metrics_bind binds nothing.
  def test_metrics_bind_serves_nothing_in_an_off_environment
    free = TCPServer.new("127.0.0.1", 0)
    port = free.addr[1]
    free.close
    TrellisTestApp.config.trellis.connect = { url: TestCluster.dsn, metrics_bind: "127.0.0.1:#{port}" }
    in_environment("test") do
      capture_io { assert_nil Trellis::Railtie.connect }
      assert_raises(Errno::ECONNREFUSED) { TCPSocket.new("127.0.0.1", port).close }
    end
    in_environment("development") do
      Trellis::Railtie.connect
      assert_equal "127.0.0.1:#{port}", Trellis.metrics_address
    end
  end

  # Run in the off environment itself (`RAILS_ENV=test bin/rails
  # db:migrate`), the helpers refuse; run from an environment Trellis is on
  # in (`db:prepare` in development migrates the test database too), they
  # skip, and the migration completes. The database is the migrating
  # connection's.
  def test_the_migration_helpers_refuse_in_an_off_environment_and_skip_from_an_on_one
    database = database("migr")
    ActiveRecord::Base.establish_connection(db_config(database))
    TrellisTestApp.config.trellis.connect = { url: dsn(database) }
    klass = Class.new(ActiveRecord::Migration[8.1]) do
      include Trellis::Migration

      disable_ddl_transaction!

      def up
        create_table :of_migrated, if_not_exists: true
        define "TRANSFORM of_migrated_copy FROM of_migrated SELECT id AS id"
      end
    end
    ActiveRecord::Migration.verbose = false

    Tasks.env = "test"
    error = assert_raises(ActiveRecord::MigrationError) { klass.new.migrate(:up) }
    assert_includes error.message, "Trellis is off in the test environment"
    assert_includes error.message, "loading the schema"
    assert_includes error.message, "of_migrated_copy"
    assert_raises(ActiveRecord::MigrationError) { klass.new.status("of_migrated_copy") }

    Tasks.env = "development"
    klass.new.migrate(:up)
    assert_nil klass.new.status("of_migrated_copy")
    # The migration's own create_table ran; the database never got Trellis.
    assert_nil to_regclass(database, "of_migrated_copy")
    assert_nil to_regclass(database, "trellis.transform_definitions")
  end

  # The load of an off environment's database creates the targets as the
  # plain tables structure.sql carries, restores nothing, and leaves Trellis's
  # schema out: Rails' fixture check, which resolves every foreign key of
  # every schema, then passes. With Trellis on, it fails on Trellis's tables.
  def test_a_schema_load_into_an_off_database_leaves_plain_tables
    source = source_database
    structure = dump(source)
    assert_includes structure, "CREATE TABLE public.of_totals_#{@id}"
    assert_includes structure, "-- Trellis definitions"

    on = database("on")
    TrellisTestApp.config.trellis.connect = { url: dsn(on) }
    TrellisTestApp.config.trellis.off_in = [] # Trellis on in the test environment
    Tasks.load_schema(db_config(on), :sql, File.join(@dir, "structure.sql"))
    assert_includes tables(on, "trellis"), "transform_definitions"
    error = assert_raises(ActiveRecord::StatementInvalid) { check_foreign_keys(on) }
    assert_match(/does not exist/, error.message)
    # The other way out, with Trellis on: Trellis's schema on the search path.
    check_foreign_keys(on, schema_search_path: "public,trellis")

    off = database("off")
    TrellisTestApp.config.trellis.connect = { url: dsn(off) }
    TrellisTestApp.config.trellis.off_in = DEFAULT_OFF_IN
    Tasks.load_schema(db_config(off), :sql, File.join(@dir, "structure.sql"))
    assert_includes tables(off, "public"), "of_totals_#{@id}"
    assert_nil to_regclass(off, "trellis.transform_definitions")
    assert_equal "0", scalar(off, "select count(*) from pg_trigger where tgname like 'trellis_capture_%'")
    # What Rails' fixture load does, with the fixture check on.
    check_foreign_keys(off)
    # Nothing was restored: the definition is still queued, and Trellis was
    # never migrated into the database.
    assert_equal "1", scalar(off, "select count(*) from trellis.restore_queue")
  end

  # `db:prepare` in development loads the test database with development's
  # config.trellis.connect, and `db:test:prepare` loads it from development
  # too: the test database's environment is what decides, not the running one.
  def test_a_load_of_an_off_environments_database_from_another_environment_restores_nothing
    dump(source_database)
    test = database("envtest")
    TrellisTestApp.config.trellis.connect = { url: dsn(database("envdev")) }
    Tasks.env = "development"
    Tasks.load_schema(db_config(test), :sql, File.join(@dir, "structure.sql"))
    assert_nil to_regclass(test, "trellis.transform_definitions")
    assert_includes tables(test, "public"), "of_totals_#{@id}"
  end

  # `parallelize(workers: n)` makes a database per worker (`<name>_<i>`) and
  # loads the schema into it in each forked worker. config.trellis.connect
  # names the one database, so with Trellis on the load is refused.
  def test_a_parallel_worker_database_loads_when_trellis_is_off
    dump(source_database)
    name = "of_app_#{@id}_#{Process.pid}"
    @databases << "#{name}_0"
    info = TestCluster.info
    base = { "adapter" => "postgresql", "host" => info.fetch("host"), "port" => info.fetch("port"),
             "username" => info.fetch("user"), "schema_search_path" => "public", "database" => name }
    ActiveRecord::Base.configurations = { "development" => { "primary" => base },
                                          "test" => { "primary" => base } }
    TrellisTestApp.config.trellis.connect = { url: dsn(name) }
    Tasks.env = "test" # `rails test`

    TrellisTestApp.config.trellis.off_in = [] # Trellis on in the test environment
    error = assert_raises(Trellis::ValidationError) do
      ActiveRecord::TestDatabases.create_and_load_schema(0, env_name: "test")
    end
    assert_includes error.message, "config.trellis.dump_database"

    TrellisTestApp.config.trellis.off_in = DEFAULT_OFF_IN
    ActiveRecord::TestDatabases.create_and_load_schema(0, env_name: "test")
    assert_includes tables("#{name}_0", "public"), "of_totals_#{@id}"
    assert_nil to_regclass("#{name}_0", "trellis.transform_definitions")
  end

  # The dump file is the environment Trellis is on in: a `db:prepare` there
  # also dumps the test database, which has no definitions to put in it.
  def test_a_dump_of_an_off_database_from_an_on_environment_is_skipped
    source = source_database
    TrellisTestApp.config.trellis.connect = { url: dsn(source) }
    Tasks.env = "development"
    path = File.join(@dir, "structure.sql")
    File.write(path, "-- the development dump\n")
    Tasks.dump_schema(db_config(source))
    assert_equal "-- the development dump\n", File.read(path)

    # From the off environment itself, it is Rails' own dump.
    Tasks.env = "test"
    Tasks.dump_schema(db_config(source))
    structure = File.read(path)
    assert_includes structure, "CREATE TABLE public.of_totals_#{@id}"
    refute_includes structure, "Trellis definitions"
  end

  private

  def in_environment(name, &)
    Rails.stub(:env, ActiveSupport::StringInquirer.new(name), &)
  end

  # A database with a foreign key and a 1-1 transform over it, defined by Trellis.
  def source_database
    @source ||= begin
      name = database("src")
      pg = PG.connect(dsn(name))
      pg.exec("create table of_customers_#{@id} (id bigint primary key)")
      pg.exec("create table of_orders_#{@id} (id bigint primary key, customer_id bigint " \
              "references of_customers_#{@id} (id), price bigint)")
      pg.close
      TrellisTestApp.config.trellis.connect = { url: dsn(name) }
      Trellis::Railtie.with_handle do |trellis|
        trellis.define("TRANSFORM of_totals_#{@id} FROM of_orders_#{@id} SELECT price AS price")
      end
      TrellisTestApp.config.trellis.connect = nil
      name
    end
  end

  # Dumps `database` as structure.sql, with Trellis on (these databases are
  # named "test"), and returns the file.
  def dump(database)
    TrellisTestApp.config.trellis.connect = { url: dsn(database) }
    TrellisTestApp.config.trellis.off_in = []
    Tasks.dump_schema(db_config(database))
    File.read(File.join(@dir, "structure.sql"))
  ensure
    TrellisTestApp.config.trellis.connect = nil
    TrellisTestApp.config.trellis.off_in = DEFAULT_OFF_IN
  end

  def check_foreign_keys(database, **options)
    ActiveRecord::Base.establish_connection(db_config(database, **options))
    ActiveRecord::Base.lease_connection.check_all_foreign_keys_valid!
  ensure
    use_app_database
  end

  def use_app_database
    ActiveRecord::Base.establish_connection(db_config(TestCluster.info.fetch("dbname")))
  end

  def database(role)
    name = "of_#{role}_#{@id}_#{Process.pid}"
    @admin.exec("create database #{name}")
    @databases << name
    name
  end

  def dsn(database)
    TestCluster.dsn.sub(/dbname=\S+/, "dbname=#{database}")
  end

  def db_config(database, **options)
    info = TestCluster.info
    hash = { adapter: "postgresql", database:, host: info.fetch("host"), port: info.fetch("port"),
             username: info.fetch("user"), schema_search_path: "public", **options }
    ActiveRecord::DatabaseConfigurations::HashConfig.new("test", "primary", hash)
  end

  def scalar(database, sql)
    pg = PG.connect(dsn(database))
    pg.exec(sql).getvalue(0, 0)
  ensure
    pg&.close
  end

  def to_regclass(database, name)
    pg = PG.connect(dsn(database))
    pg.exec_params("select to_regclass($1)", [name]).getvalue(0, 0)
  ensure
    pg&.close
  end

  def tables(database, schema)
    pg = PG.connect(dsn(database))
    pg.exec_params("select tablename from pg_tables where schemaname = $1", [schema]).map { |row| row.fetch("tablename") }
  ensure
    pg&.close
  end
end
