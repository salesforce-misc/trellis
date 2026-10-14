# frozen_string_literal: true

require "fileutils"
require "minitest/mock"
require "tmpdir"
require "test_helper"
require_relative "support/rails_app"
require "trellis/schema_dump"

# Trellis::SchemaDump (issue #644): `structure.sql` carries Trellis's
# definitions. The round trip runs Rails' own dump and load, in-process, over
# two databases of the test cluster: define in one, dump with
# DatabaseTasks.dump_schema, load the file into a fresh one with
# DatabaseTasks.structure_load (what db:schema:load runs), and ask the loaded
# database what it defines. Nothing here waits for a worker.
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

  def test_a_dump_loaded_into_a_fresh_database_defines_what_the_source_defined
    source = database("src")
    define_in(source)

    # No format argument, as db:prepare's dump calls it: the format is the
    # app's config.active_record.schema_format.
    ActiveRecord.schema_format = :sql
    Tasks.dump_schema(db_config(source))
    structure = File.read(File.join(@dir, "structure.sql"))
    assert_includes structure, "CREATE SCHEMA trellis"
    assert_includes structure, "trellis.transform_definitions"
    assert_includes structure, "CREATE TABLE public.sd_totals_#{@id}"
    assert_includes structure, "-- Trellis definitions"
    refute_match(/^CREATE SCHEMA public;/, structure)
    assert_includes structure, "sd_totals_#{@id}"

    target = database("dst")
    # The database `db:create` makes has a `public` already: the dump has to
    # load into it (pg_dump 17 writes CREATE SCHEMA public).
    Tasks.structure_load(db_config(target), File.join(@dir, "structure.sql"))

    TrellisTestApp.config.trellis.connect = { url: dsn(target) }
    Trellis::Railtie.with_handle do |loaded|
      assert_equal ["public.sd_totals_#{@id}", "public.sd_grand_#{@id}"].sort,
                   loaded.definitions.map(&:target_table).sort
      assert_equal %i[waiting_to_backfill], loaded.definitions.map(&:status).uniq
      assert_equal :waiting_to_backfill, loaded.status("sd_grand_#{@id}").status
    end
    pg = PG.connect(dsn(target))
    begin
      assert_equal "0", pg.exec("select count(*) from public.sd_totals_#{@id}__ledger").first.fetch("count")
      assert_equal "0", pg.exec("select count(*) from public.sd_totals_#{@id}").first.fetch("count")
    ensure
      pg.close
    end
  end

  def test_a_dump_of_another_database_is_left_as_rails_wrote_it
    source = database("other")
    define_in(source)
    TrellisTestApp.config.trellis.dump_database = "somewhere_else"

    Tasks.dump_schema(db_config(source), :sql)
    refute_includes File.read(File.join(@dir, "structure.sql")), "Trellis definitions"
  end

  def test_a_dump_redone_when_the_definitions_changed_under_it
    source = database("race")
    define_in(source)
    reads = %w[old new new new]
    dumps = 0
    config = db_config(source)
    SchemaDump.stub(:appendix, -> { reads.shift }) do
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
    Trellis::SchemaDump.stub(:appendix, -> { "read #{serial += 1}" }) do
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

  def test_the_trellis_and_target_schemas_join_the_dump_s_schema_flags
    TrellisTestApp.config.trellis.connect = { url: "unused" }
    TrellisTestApp.config.trellis.instances = {
      reports: { url: "unused", schema: "reports", target_schema: "report_targets" }
    }
    config = db_config(database("flags"))
    hash = { schema_search_path: "public" }
    assert_equal %w[--schema=trellis --schema=public --schema=reports --schema=report_targets],
                 Trellis::SchemaDump.schema_flags(config, hash)
    # No --schema at all means Rails dumps every schema: nothing to add.
    assert_empty Trellis::SchemaDump.schema_flags(config, {})
    ActiveRecord.dump_schemas = :all
    assert_empty Trellis::SchemaDump.schema_flags(config, hash)
  end

  private

  SchemaDump = Trellis::SchemaDump

  def database(role)
    name = "sd_#{role}_#{@id}_#{Process.pid}"
    @admin.exec("create database #{name}")
    @databases << name
    name
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

  # Migrates Trellis into the database, creates a source table and a 1-1 and an
  # aggregate transform over it, as a deploy's migrations would.
  def define_in(database)
    TrellisTestApp.config.trellis.connect = { url: dsn(database) }
    pg = PG.connect(dsn(database))
    pg.exec("create table public.sd_orders_#{@id} (id bigint primary key, price bigint)")
    pg.close
    Trellis::Railtie.with_handle do |trellis|
      trellis.define("TRANSFORM sd_totals_#{@id} FROM sd_orders_#{@id} SELECT price AS price")
      trellis.define("TRANSFORM sd_grand_#{@id} FROM sd_orders_#{@id} GROUP BY price SELECT COUNT(*) AS n")
    end
  end
end
