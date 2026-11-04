# frozen_string_literal: true

require "fileutils"
require "minitest/mock"
require "tmpdir"
require "test_helper"
require_relative "support/rails_app"
require "trellis/schema_dump"

# Trellis::SchemaDump: `structure.sql` carries Trellis's
# definitions. The round trip runs Rails' own dump and load, in-process, over
# two databases of the test cluster: define in one, dump with
# DatabaseTasks.dump_schema, load the file into a fresh one with
# DatabaseTasks.load_schema (what db:schema:load runs), which restores the
# definitions, and ask the loaded database what it defines. Nothing here
# waits for a worker.
class SchemaDumpTest < Minitest::Test
  include TrellisTestCase

  Tasks = ActiveRecord::Tasks::DatabaseTasks

  @serial = 0
  def self.next_id = (@serial += 1)

  def setup
    super
    @dir = Dir.mktmpdir("trellis-structure")
    @admin = TestCluster.pg
    @id = self.class.next_id
    @databases = []
    @saved = { db_dir: Tasks.db_dir, dump_schemas: ActiveRecord.dump_schemas, schema_format: ActiveRecord.schema_format }
    Tasks.db_dir = @dir
    # Rails' dump runs on a temporary pool and restores the app's own after.
    ActiveRecord::Base.establish_connection(db_config(TestCluster.info.fetch("dbname")))
  end

  def teardown
    TrellisTestApp.config.trellis.connect = nil
    TrellisTestApp.config.trellis.instances = {}
    TrellisTestApp.config.trellis.dump_database = "primary"
    Tasks.db_dir = @saved.fetch(:db_dir)
    ActiveRecord.dump_schemas = @saved.fetch(:dump_schemas)
    ActiveRecord.schema_format = @saved.fetch(:schema_format)
    @databases.each { |name| @admin.exec("drop database if exists #{name} with (force)") }
    @admin.close
    FileUtils.rm_rf(@dir)
    super
  end

  def test_a_dump_loaded_into_a_fresh_database_restores_what_the_source_defined
    source = database("src")
    define_in(source)

    # No format argument, as db:prepare's dump calls it: the format is the
    # app's config.active_record.schema_format.
    ActiveRecord.schema_format = :sql
    Tasks.dump_schema(db_config(source))
    structure = File.read(File.join(@dir, "structure.sql"))
    assert_includes structure, "CREATE TABLE public.sd_totals_#{@id}"
    assert_includes structure, "CREATE TABLE public.sd_orders_#{@id}"
    assert_includes structure, "-- Trellis definitions"
    assert_includes structure, "insert into \"trellis\".restore_queue values (1, 'public.sd_orders_#{@id}'"
    # Trellis's internals are left out: its schema, the capture triggers and
    # the 1-1 target's ledger.
    refute_includes structure, "CREATE SCHEMA trellis"
    refute_includes structure, "trellis.transform_definitions"
    refute_includes structure, "CREATE TRIGGER"
    refute_includes structure, "__ledger"
    refute_match(/^CREATE SCHEMA public;/, structure)

    target = database("dst")
    # The database `db:create` makes has a `public` already: the dump has to
    # load into it (pg_dump 17 writes CREATE SCHEMA public). Rails' load
    # restores the definitions.
    TrellisTestApp.config.trellis.connect = { url: dsn(target) }
    Tasks.load_schema(db_config(target), :sql, File.join(@dir, "structure.sql"))

    assert_loaded(target)
  end

  # A structure.sql written before the restore queue carries Trellis's
  # schema and dump_definitions_sql's rows: it loads as it did.
  def test_a_dump_in_the_older_format_still_loads
    source = database("old")
    define_in(source)
    file = File.join(@dir, "old.sql")
    info = TestCluster.info
    assert system("pg_dump", "--schema-only", "--no-acl", "--no-owner", "--schema=trellis", "--schema=public",
                  "--host=#{info.fetch("host")}", "--port=#{info.fetch("port")}",
                  "--username=#{info.fetch("user")}", "--file=#{file}", source)
    appendix = Trellis::Railtie.with_handle(&:dump_definitions_sql)
    old = File.read(file).gsub(/^CREATE SCHEMA public;$/, "CREATE SCHEMA IF NOT EXISTS public;")
    File.write(file, "#{old}\n#{appendix}\n")

    target = database("old_dst")
    TrellisTestApp.config.trellis.connect = { url: dsn(target) }
    Tasks.load_schema(db_config(target), :sql, file)

    assert_loaded(target)
  end

  # `db:prepare` in development loads the test database too, with
  # development's config.trellis.connect: the restore runs in the test
  # database, not development's.
  def test_a_load_of_another_environment_s_database_restores_there
    saved = { env: Tasks.env, configurations: ActiveRecord::Base.configurations }
    development = database("env_dev")
    test = database("env_test")
    define_in(development)
    ActiveRecord.schema_format = :sql
    Tasks.dump_schema(db_config(development))

    info = TestCluster.info
    base = { "adapter" => "postgresql", "host" => info.fetch("host"), "port" => info.fetch("port"),
             "username" => info.fetch("user"), "schema_search_path" => "public" }
    ActiveRecord::Base.configurations = {
      "development" => { "primary" => base.merge("database" => development) },
      "test" => { "primary" => base.merge("database" => test) }
    }
    Tasks.env = "development"
    test_config = ActiveRecord::Base.configurations.configs_for(env_name: "test", name: "primary")
    Tasks.load_schema(test_config, :sql, File.join(@dir, "structure.sql"))

    Tasks.env = saved.fetch(:env)
    assert_loaded(test)
  ensure
    Tasks.env = saved.fetch(:env)
    ActiveRecord::Base.configurations = saved.fetch(:configurations)
  end

  def test_a_dump_of_another_database_is_left_as_rails_wrote_it
    source = database("other")
    define_in(source)
    TrellisTestApp.config.trellis.dump_database = "somewhere_else"

    Tasks.dump_schema(db_config(source), :sql)
    refute_includes File.read(File.join(@dir, "structure.sql")), "Trellis definitions"
  end

  def test_a_connect_at_another_database_is_refused_and_nothing_is_written
    source = database("mine")
    other = database("theirs")
    define_in(source)
    TrellisTestApp.config.trellis.connect = { url: dsn(other) }
    error = assert_raises(Trellis::ValidationError) { Tasks.dump_schema(db_config(source), :sql) }
    assert_includes error.message, "config.trellis.connect points at #{other}"
    assert_includes error.message, "is #{source}"
    assert_match(/dump_database/, error.message)
    refute File.exist?(File.join(@dir, "structure.sql"))
  end

  def test_a_connect_at_another_host_or_port_is_refused
    source = database("hostport")
    config = db_config(source)
    info = TestCluster.info
    TrellisTestApp.config.trellis.connect = { url: "host=elsewhere.invalid port=#{info.fetch("port")} dbname=#{source}" }
    assert_raises(Trellis::ValidationError) { SchemaDump.verify_same_database!(config) }
    TrellisTestApp.config.trellis.connect = { url: "host=#{info.fetch("host")} port=1 dbname=#{source}" }
    assert_raises(Trellis::ValidationError) { SchemaDump.verify_same_database!(config) }
    TrellisTestApp.config.trellis.connect = { url: dsn(source) }
    SchemaDump.verify_same_database!(config)
  end

  def test_an_instance_at_another_database_is_refused
    source = database("inst")
    TrellisTestApp.config.trellis.connect = { url: dsn(source) }
    TrellisTestApp.config.trellis.instances = { reports: { url: dsn(database("inst_other")), schema: "reports" } }
    error = assert_raises(Trellis::ValidationError) { SchemaDump.verify_same_database!(db_config(source)) }
    assert_includes error.message, "config.trellis.instances[:reports]"
  end

  def test_url_forms_are_compared_by_host_port_and_database
    hash = { adapter: "postgresql", url: "postgres://u:p@db.example:6543/app" }
    config = ActiveRecord::DatabaseConfigurations::HashConfig.new("test", "primary", hash)
    TrellisTestApp.config.trellis.connect = { url: "postgresql://other@db.example:6543/app?sslmode=disable" }
    SchemaDump.verify_same_database!(config)
    TrellisTestApp.config.trellis.connect = { url: "host=db.example port=6543 dbname=app" }
    SchemaDump.verify_same_database!(config)
    TrellisTestApp.config.trellis.connect = { url: "postgres://db.example:6543/app2" }
    assert_raises(Trellis::ValidationError) { SchemaDump.verify_same_database!(config) }
    TrellisTestApp.config.trellis.connect = { url: "postgres://db.example/app" }
    assert_raises(Trellis::ValidationError) { SchemaDump.verify_same_database!(config) }
  end

  # Rails' generated database.yml names no host: libpq connects to its local
  # socket. Trellis needs a host, and any local one matches.
  def test_a_rails_config_with_no_host_matches_a_local_trellis_host
    config = ActiveRecord::DatabaseConfigurations::HashConfig.new("test", "primary",
                                                                   { adapter: "postgresql", database: "app" })
    with_env("PGHOST" => nil, "PGPORT" => nil) do
      ["host=localhost dbname=app", "postgres://127.0.0.1/app", "host=/var/run/postgresql dbname=app"].each do |url|
        TrellisTestApp.config.trellis.connect = { url: }
        SchemaDump.verify_same_database!(config)
      end
      TrellisTestApp.config.trellis.connect = { url: "host=db.example dbname=app" }
      assert_raises(Trellis::ValidationError) { SchemaDump.verify_same_database!(config) }
    end
  end

  # Trellis reads nothing from the environment: a url with no port is 5432,
  # one with no dbname is the user's database, whatever PGPORT says.
  def test_a_trellis_url_is_resolved_without_the_environment
    config = ActiveRecord::DatabaseConfigurations::HashConfig.new("test", "primary",
                                                                   { adapter: "postgresql", host: "h", database: "app" })
    with_env("PGPORT" => "5433") do
      TrellisTestApp.config.trellis.connect = { url: "host=h dbname=app" }
      assert_raises(Trellis::ValidationError) { SchemaDump.verify_same_database!(config) }
    end
    with_env("PGPORT" => nil) do
      TrellisTestApp.config.trellis.connect = { url: "host=h user=app" }
      SchemaDump.verify_same_database!(config)
    end
  end

  def test_a_url_that_does_not_parse_is_refused_without_quoting_it
    config = db_config(TestCluster.info.fetch("dbname"))
    ["postgres://u:hunter2@h/app?pool=5", "host=h password=hunter2 xyzzy"].each do |url|
      TrellisTestApp.config.trellis.connect = { url: }
      error = assert_raises(Trellis::ValidationError) { SchemaDump.verify_same_database!(config) }
      assert_includes error.message, "config.trellis.connect"
      refute_includes error.message, "hunter2"
    end
  end

  # `db:prepare` in development migrates and dumps the test database too,
  # with development's config.trellis.connect: that dump is checked against
  # development's config of the same name.
  def test_another_environments_dump_is_checked_against_the_current_one
    saved = { env: Tasks.env, configurations: ActiveRecord::Base.configurations }
    info = TestCluster.info
    base = { "adapter" => "postgresql", "host" => info.fetch("host"), "port" => info.fetch("port") }
    ActiveRecord::Base.configurations = {
      "development" => { "primary" => base.merge("database" => "app_development") },
      "test" => { "primary" => base.merge("database" => "app_test") }
    }
    Tasks.env = "development"
    test_config = ActiveRecord::Base.configurations.configs_for(env_name: "test", name: "primary")
    where = "host=#{info.fetch("host")} port=#{info.fetch("port")}"
    TrellisTestApp.config.trellis.connect = { url: "#{where} dbname=app_development" }
    SchemaDump.verify_same_database!(test_config)
    TrellisTestApp.config.trellis.connect = { url: "#{where} dbname=app_test" }
    assert_raises(Trellis::ValidationError) { SchemaDump.verify_same_database!(test_config) }
  ensure
    Tasks.env = saved.fetch(:env)
    ActiveRecord::Base.configurations = saved.fetch(:configurations)
  end

  def test_a_mismatch_against_another_database_of_the_app_is_not_checked
    source = database("multi")
    TrellisTestApp.config.trellis.connect = { url: dsn(database("multi_other")) }
    TrellisTestApp.config.trellis.dump_database = "somewhere_else"
    Tasks.dump_schema(db_config(source), :sql)
    assert File.exist?(File.join(@dir, "structure.sql"))
  end

  def test_a_dump_redone_when_the_definitions_changed_under_it
    source = database("race")
    define_in(source)
    reads = %w[old new new new].map { |appendix| [{ pg_dump_args: [], appendix: }] }
    dumps = 0
    config = db_config(source)
    SchemaDump.stub(:parts, -> { reads.shift }) do
      pg_dump = lambda do |_config, file|
        dumps += 1
        File.write(file, "-- dump #{dumps}\n")
      end
      Tasks.stub(:structure_dump, pg_dump) do
        Tasks.dump_schema(config, :sql)
      end
    end
    assert_equal 2, dumps
    assert_equal "-- dump 2\n", File.read(File.join(@dir, "structure.sql"))[/\A.*\n/]
    assert_includes File.read(File.join(@dir, "structure.sql")), "new"
  end

  def test_a_dump_that_never_settles_raises
    source = database("busy")
    define_in(source)
    serial = 0
    config = db_config(source)
    reads = -> { [{ pg_dump_args: [], appendix: "read #{serial += 1}" }] }
    Trellis::SchemaDump.stub(:parts, reads) do
      Tasks.stub(:structure_dump, ->(_config, file) { File.write(file, "-- dump\n") }) do
        assert_raises(Trellis::ConflictError) { Tasks.dump_schema(config, :sql) }
      end
    end
  end

  def test_a_ruby_format_dump_raises_rather_than_write_a_schema_without_definitions
    source = database("ruby")
    define_in(source)
    error = assert_raises(Trellis::ValidationError) { Tasks.dump_schema(db_config(source), :ruby) }
    assert_match(/schema_format = :sql/, error.message)
    refute File.exist?(File.join(@dir, "schema.rb"))
  end

  def test_the_target_schemas_and_pg_dump_args_join_the_dump_s_flags
    TrellisTestApp.config.trellis.connect = { url: "unused" }
    TrellisTestApp.config.trellis.instances = {
      reports: { url: "unused", schema: "reports", target_schema: "report_targets" },
      # A name pg_dump would fold to lower case and read "." in as a pattern.
      audit: { url: "unused", schema: "audit", target_schema: "Audit.T" }
    }
    config = db_config(database("flags"))
    hash = { schema_search_path: "public" }
    parts = [
      { pg_dump_args: ['--exclude-schema="trellis"',
                                                  '--exclude-table="public"."t__ledger"'], appendix: "" },
      { pg_dump_args: ['--exclude-schema="reports"'], appendix: "" }
    ]
    assert_equal ["--schema=public", "--schema=report_targets", '--schema="Audit.T"', '--exclude-schema="trellis"',
                  '--exclude-table="public"."t__ledger"', '--exclude-schema="reports"'],
                 Trellis::SchemaDump.schema_flags(config, hash, parts)
    # No --schema at all means Rails dumps every schema: only the
    # exclusions are added.
    exclusions = parts.flat_map { |part| part.fetch(:pg_dump_args) }
    assert_equal exclusions, Trellis::SchemaDump.schema_flags(config, {}, parts)
    ActiveRecord.dump_schemas = :all
    assert_equal exclusions, Trellis::SchemaDump.schema_flags(config, hash, parts)
  end

  def test_another_environment_s_url_takes_the_libpq_keys_rails_accepts
    configured = "host=devhost hostaddr=10.0.0.1 port=5432 dbname=app_dev user=dev password=x"
    hash = { adapter: "postgresql", host: "testhost", dbname: "app_test", user: "bob" }
    config = ActiveRecord::DatabaseConfigurations::HashConfig.new("test", "primary", hash)
    url = Trellis::SchemaDump.send(:environment_url, config, configured, "test")
    keywords = PG::Connection.conninfo_parse(url).to_h { |entry| [entry[:keyword], entry[:val]] }
    assert_equal({ "host" => "testhost", "hostaddr" => nil, "dbname" => "app_test", "user" => "bob",
                   "password" => "x" },
                 keywords.slice("host", "hostaddr", "dbname", "user", "password").merge(
                   "hostaddr" => keywords["hostaddr"]
                 ))
  end

  def test_another_environment_s_url_carries_its_ssl_keys_and_a_blank_password_keeps_the_urls
    configured = "host=devhost port=5432 dbname=app_dev user=dev password=secret sslmode=disable connect_timeout=3"
    hash = { adapter: "postgresql", host: "testhost", dbname: "app_test", sslmode: "verify-full",
             sslrootcert: "/etc/ssl/ca.pem", password: "", connect_timeout: "" }
    keywords = environment_keywords(hash, configured)
    assert_equal "verify-full", keywords["sslmode"]
    assert_equal "/etc/ssl/ca.pem", keywords["sslrootcert"]
    # Blank values name nothing: the url's stay.
    assert_equal "secret", keywords["password"]
    assert_equal "3", keywords["connect_timeout"]

    # A password the config does name replaces the url's, and so does a
    # keyword the url doesn't have.
    keywords = environment_keywords(hash.merge(password: "other"), "host=devhost user=dev")
    assert_equal "other", keywords["password"]
    assert_equal "verify-full", keywords["sslmode"]
  end

  def test_a_dump_that_fails_leaves_no_file_to_load
    source = database("failed")
    define_in(source)
    path = File.join(@dir, "structure.sql")
    config = db_config(source)

    # Never settles.
    serial = 0
    reads = -> { [{ pg_dump_args: [], appendix: "read #{serial += 1}" }] }
    SchemaDump.stub(:parts, reads) do
      Tasks.stub(:structure_dump, ->(_config, file) { File.write(file, "-- raw\n") }) do
        assert_raises(Trellis::ConflictError) { Tasks.dump_schema(config, :sql) }
      end
    end
    refute File.exist?(path)

    # The pg_dump itself fails after writing part of the file.
    failing = lambda do |_config, file|
      File.write(file, "-- partial\n")
      raise "pg_dump failed"
    end
    Tasks.stub(:structure_dump, failing) do
      assert_raises(RuntimeError) { Tasks.dump_schema(config, :sql) }
    end
    refute File.exist?(path)

    # A failure before the first pg_dump leaves the file already there alone.
    File.write(path, "-- previous\n")
    SchemaDump.stub(:parts, -> { raise Trellis::ConnectivityError, "down" }) do
      assert_raises(Trellis::ConnectivityError) { Tasks.dump_schema(config, :sql) }
    end
    assert_equal "-- previous\n", File.read(path)

    # So does a pg_dump that fails before it opens the file (it can't
    # connect, or it is older than the server).
    Tasks.stub(:structure_dump, ->(_config, _file) { raise "pg_dump failed" }) do
      assert_raises(RuntimeError) { Tasks.dump_schema(config, :sql) }
    end
    assert_equal "-- previous\n", File.read(path)

    # But not one that wrote to it.
    Tasks.stub(:structure_dump, failing) do
      assert_raises(RuntimeError) { Tasks.dump_schema(config, :sql) }
    end
    refute File.exist?(path)
  end

  private

  SchemaDump = Trellis::SchemaDump

  def environment_keywords(hash, configured)
    config = ActiveRecord::DatabaseConfigurations::HashConfig.new("test", "primary", hash)
    url = SchemaDump.send(:environment_url, config, configured, "test")
    PG::Connection.conninfo_parse(url).to_h { |entry| [entry[:keyword], entry[:val]] }
  end

  def database(role)
    name = "sd_#{role}_#{@id}_#{Process.pid}"
    @admin.exec("create database #{name}")
    @databases << name
    name
  end

  def with_env(vars)
    saved = vars.keys.to_h { |key| [key, ENV.fetch(key, nil)] }
    vars.each { |key, value| ENV[key] = value }
    yield
  ensure
    saved&.each { |key, value| ENV[key] = value }
  end

  def dsn(database)
    TestCluster.dsn.sub(/dbname=\S+/, "dbname=#{database}")
  end

  def db_config(database)
    info = TestCluster.info
    hash = { adapter: "postgresql", database:, host: info.fetch("host"), port: info.fetch("port"),
             username: info.fetch("user"), schema_search_path: "public" }
    ActiveRecord::DatabaseConfigurations::HashConfig.new("test", "primary", hash)
  end

  # The database restores what define_in defined: both definitions, unbuilt,
  # the 1-1 target's ledger rebuilt by the restore, the targets empty.
  def assert_loaded(database)
    TrellisTestApp.config.trellis.connect = { url: dsn(database) }
    Trellis::Railtie.with_handle do |loaded|
      assert_equal ["public.sd_totals_#{@id}", "public.sd_grand_#{@id}"].sort,
                   loaded.definitions.map(&:target_table).sort
      assert_equal %i[waiting_to_backfill], loaded.definitions.map(&:status).uniq
    end
    pg = PG.connect(dsn(database))
    begin
      assert_equal "0", pg.exec("select count(*) from public.sd_totals_#{@id}__ledger").first.fetch("count")
      assert_equal "0", pg.exec("select count(*) from public.sd_totals_#{@id}").first.fetch("count")
      assert_equal "f", pg.exec("select to_regclass('trellis.restore_queue') is not null as q").first.fetch("q")
    ensure
      pg.close
    end
  end

  # Migrates Trellis into the database, creates a source table and a 1-1 and an
  # aggregate transform over it, as a deploy's migrations would.
  def define_in(database)
    TrellisTestApp.config.trellis.connect = { url: dsn(database) }
    pg = PG.connect(dsn(database))
    pg.exec("create table public.sd_orders_#{@id} (id bigint primary key, price bigint)")
    pg.close
    Trellis::Railtie.with_handle do |trellis|
      trellis.define("TRANSFORM sd_totals_#{@id} FROM sd_orders_#{@id} SELECT price AS price")
      trellis.define("TRANSFORM sd_grand_#{@id} FROM sd_orders_#{@id} GROUP BY price WITHOUT SOURCE INDEX SELECT COUNT(*) AS n")
    end
  end
end
