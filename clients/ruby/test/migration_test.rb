# frozen_string_literal: true

require "fileutils"
require "tmpdir"
require "test_helper"
require_relative "support/rails_app"

# Trellis::Migration (issue #153), run the way `rails db:migrate` runs it:
# migration files in a directory, through ActiveRecord's MigrationContext,
# with ActiveRecord connected to the test cluster. Mirrors the Elixir
# suite's migration_test.exs.
class MigrationTest < Minitest::Test
  include TrellisTestCase

  CATALOG = "trellis.transform_definitions"

  # A serial per test, for table, class and version names that don't
  # collide with an earlier test's.
  @serial = 0
  def self.next_id = (@serial += 1)

  def setup
    super
    @dir = Dir.mktmpdir("trellis-migrations")
    @versions = []
    @pg = TestCluster.pg
    @id = self.class.next_id
    use_cluster(TestCluster.info)
  end

  def teardown
    ActiveRecord::Base.connection_pool.disconnect!
    TrellisTestApp.config.trellis.connect = nil
    FileUtils.rm_rf(@dir)
    @pg&.close
    super
  end

  # The docs' example: a table created and a transform defined in one
  # migration, on a handle the helper connects for each call and shuts down,
  # and the down that undoes both.
  def test_up_defines_on_a_handle_of_its_own_and_down_pauses_and_drops
    source = "mig_widgets_#{@id}"
    target = "mig_widget_prices_#{@id}"
    migration("DefineWidgetPrices", <<~RUBY)
      disable_ddl_transaction!

      def up
        create_table :#{source} do |t|
          t.integer :price
        end
        define "TRANSFORM #{target} FROM #{source} SELECT price AS price"
      end

      def down
        apply "PAUSE TRANSFORM #{target}"
        apply "DROP TRANSFORM #{target}"
        drop_table :#{source}
      end
    RUBY

    migrate_up
    refute Trellis.connected?, "the migration left its handle connected"
    assert table?(target), "the define didn't create #{target}"
    assert_equal 1, defined_count(target)

    migrate_down
    refute table?(target), "the drop didn't drop #{target}"
    refute table?(source)
    assert_equal 0, defined_count(target)
  end

  # A process that's already connected (a console, a test) keeps its
  # handle: the helpers use it rather than connecting another.
  def test_the_helpers_use_this_processs_handle_when_it_is_connected
    source = committed_source
    target = "mig_connected_#{@id}"
    migration("DefineOnTheAppHandle", <<~RUBY)
      disable_ddl_transaction!

      def up
        define "TRANSFORM #{target} FROM #{source} SELECT price AS price"
      end
    RUBY

    Trellis.connect(url: TestCluster.dsn)
    migrate_up
    assert Trellis.connected?, "the migration shut this process's handle down"
    assert_equal :waiting_to_backfill, Trellis.status(target).status
  end

  # The hazard the helper exists for. The define reads a table committed
  # beforehand, so nothing but the transaction check stops it.
  def test_a_transactional_migration_raises_with_nothing_applied_on_either_side
    source = committed_source
    target = "mig_in_transaction_#{@id}"
    created = "mig_created_in_transaction_#{@id}"
    migration("DefineInATransaction", <<~RUBY)
      def up
        create_table :#{created}
        define "TRANSFORM #{target} FROM #{source} SELECT price AS price"
      end
    RUBY

    error = assert_migration_fails(ActiveRecord::MigrationError) { migrate_up }
    assert_match "disable_ddl_transaction!", error.message
    refute table?(created), "the host's change was applied"
    refute table?(target), "the define was applied"
    assert_equal 0, defined_count(target)
    assert_empty migrated_versions
  end

  # disable_ddl_transaction! but a `transaction` block of its own: the
  # define checks as it runs.
  def test_a_define_inside_a_transaction_block_raises_before_applying
    source = committed_source
    target = "mig_in_block_#{@id}"
    migration("DefineInATransactionBlock", <<~RUBY)
      disable_ddl_transaction!

      def up
        transaction do
          define "TRANSFORM #{target} FROM #{source} SELECT price AS price"
        end
      end
    RUBY

    error = assert_migration_fails(ActiveRecord::MigrationError) { migrate_up }
    assert_match "wasn't applied", error.message
    refute table?(target), "the define was applied"
    assert_equal 0, defined_count(target)
  end

  def test_a_public_change_is_refused_before_anything_runs
    source = committed_source
    target = "mig_change_#{@id}"
    created = "mig_created_by_change_#{@id}"
    migration("DefineInChange", <<~RUBY)
      disable_ddl_transaction!

      def change
        create_table :#{created}
        define "TRANSFORM #{target} FROM #{source} SELECT price AS price"
      end
    RUBY

    error = assert_migration_fails(ActiveRecord::MigrationError) { migrate_up }
    assert_match "must define up and down", error.message
    refute table?(created), "the host's change was applied"
    assert_equal 0, defined_count(target)
  end

  # ActiveRecord only runs a public change, so a private helper of that name
  # in an up/down migration is left alone.
  def test_a_private_change_helper_is_allowed
    source = committed_source
    target = "mig_private_change_#{@id}"
    migration("DefineWithAPrivateChange", <<~RUBY)
      disable_ddl_transaction!

      def up
        define "TRANSFORM #{target} FROM #{source} SELECT price AS price"
      end

      private

      def change = nil
    RUBY

    migrate_up
    assert_equal 1, defined_count(target)
  end

  def test_a_define_in_a_revert_block_is_irreversible
    source = committed_source
    target = "mig_revert_#{@id}"
    migration("DefineInRevert", <<~RUBY)
      disable_ddl_transaction!

      def up
        revert do
          define "TRANSFORM #{target} FROM #{source} SELECT price AS price"
        end
      end
    RUBY

    error = assert_migration_fails(ActiveRecord::IrreversibleMigration) { migrate_up }
    assert_match "cannot reverse a Trellis statement", error.message
    assert_equal 0, defined_count(target)
  end

  # The optional guard the docs show: nothing adds it for you, but a
  # migration can skip a define whose target exists.
  def test_the_optional_status_guard_skips_a_target_that_exists
    source = committed_source
    target = "mig_guarded_#{@id}"
    statement = "TRANSFORM #{target} FROM #{source} SELECT price AS price"
    Trellis.connect(url: TestCluster.dsn)
    Trellis.define(statement)
    Trellis.shutdown
    migration("DefineIfMissing", <<~RUBY)
      disable_ddl_transaction!

      def up
        define "#{statement}" if status("#{target}").nil?
      end
    RUBY

    migrate_up
    assert_equal 1, defined_count(target)
    assert_equal 1, migrated_versions.length
  end

  # No hidden guard: without one, defining an existing target fails the
  # migration, and schema_migrations doesn't record it.
  def test_a_failing_define_fails_the_migration_with_the_trellis_error
    source = committed_source
    target = "mig_twice_#{@id}"
    statement = "TRANSFORM #{target} FROM #{source} SELECT price AS price"
    Trellis.connect(url: TestCluster.dsn)
    Trellis.define(statement)
    Trellis.shutdown
    migration("DefineTwice", <<~RUBY)
      disable_ddl_transaction!

      def up
        define "#{statement}"
      end
    RUBY

    assert_migration_fails(Trellis::ConflictError) { migrate_up }
    assert_empty migrated_versions
    refute Trellis.connected?, "the failed call left its handle connected"
  end

  def test_a_missing_config_trellis_connect_raises_naming_it
    TrellisTestApp.config.trellis.connect = nil
    migration("DefineWithoutConfig", <<~RUBY)
      disable_ddl_transaction!

      def up
        define "TRANSFORM mig_unconfigured_#{@id} FROM nowhere SELECT a AS a"
      end
    RUBY

    error = assert_migration_fails(ActiveRecord::MigrationError) { migrate_up }
    assert_match "config.trellis.connect", error.message
  end

  # The configuration's background work is ignored: a migration's handle
  # never runs the staging worker, whose connect would fail on a database
  # Trellis hasn't migrated, and the helper runs `migrate` first, so a
  # database Trellis has never seen works.
  def test_a_fresh_database_is_migrated_first_and_background_options_are_ignored
    TestCluster.private_cluster do |cluster|
      use_cluster(cluster, staging: true, drain_threads: 4)
      pg = TestCluster.pg(cluster)
      pg.exec("create table mig_fresh (id integer primary key, price integer)")
      migration("DefineOnAFreshDatabase", <<~RUBY)
        disable_ddl_transaction!

        def up
          define "TRANSFORM mig_fresh_prices FROM mig_fresh SELECT price AS price"
        end
      RUBY

      migrate_up
      assert_equal 1, pg.exec("select count(*) from #{CATALOG}").getvalue(0, 0).to_i
    ensure
      ActiveRecord::Base.connection_pool.disconnect!
      pg&.close
    end
  end

  private

  # Points ActiveRecord and config.trellis.connect at a cluster.
  def use_cluster(info, **trellis_options)
    ActiveRecord::Base.establish_connection(
      adapter: "postgresql", host: info.fetch("host"), port: info.fetch("port"),
      username: info.fetch("user"), database: info.fetch("dbname")
    )
    TrellisTestApp.config.trellis.connect = { url: info.fetch("dsn"), **trellis_options }
  end

  # Writes a migration file whose class body is `body`. Class names get the
  # test's serial, since a loaded migration's class stays defined.
  def migration(name, body)
    version = format("2026092800%04d%02d", @id, @versions.length)
    @versions << version
    klass = "#{name}#{@id}"
    path = File.join(@dir, "#{version}_#{klass.gsub(/(?<!^)([A-Z])/, '_\1').downcase}.rb")
    File.write(path, <<~RUBY)
      class #{klass} < ActiveRecord::Migration[8.1]
        include Trellis::Migration

      #{body.gsub(/^/, '  ')}
      end
    RUBY
  end

  def context
    ActiveRecord::MigrationContext.new(@dir)
  end

  def migrate_up
    context.up
  end

  def migrate_down
    context.down(0)
  end

  def migrated_versions
    context.get_all_versions & @versions.map(&:to_i)
  end

  # A source table committed before any migration runs.
  def committed_source
    name = "mig_source_#{@id}"
    @pg.exec("create table #{name} (id integer primary key, price integer)")
    name
  end

  # ActiveRecord's migrator re-raises a migration's error as a StandardError
  # naming it; the error the migration raised is its cause.
  def assert_migration_fails(klass)
    wrapper = assert_raises(StandardError) { yield }
    assert_kind_of klass, wrapper.cause, "the migration failed with: #{wrapper.message}"
    wrapper.cause
  end

  def table?(name)
    !@pg.exec_params("select to_regclass($1)", [name]).getvalue(0, 0).nil?
  end

  def defined_count(target)
    @pg.exec_params("select count(*) from #{CATALOG} where target_table = $1",
                    ["public.#{target}"]).getvalue(0, 0).to_i
  end
end
