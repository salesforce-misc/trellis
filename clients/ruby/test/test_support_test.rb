# frozen_string_literal: true

require "test_helper"
require "trellis/test_support"
require_relative "support/rails_app"

# Trellis::TestSupport (issue #1181): a fixture load that disables the
# triggers, the way ActiveRecord's disable_referential_integrity does for
# Rails fixtures, pauses the readers of the table (a capture failure of kind
# :disruption), and the helper resumes them and waits for exact targets.
# Mirrors the Elixir suite's test_support_test.exs.
#
# Each test has a cluster of its own: the shared one holds definitions other
# tests dropped the sources of, and the helper rightly pauses-and-refuses
# those.
class TestSupportTest < Minitest::Test
  include TrellisTestCase

  def teardown
    ActiveRecord::Base.connection_pool.disconnect!
    super
  end

  def test_a_fixture_load_is_resumed_and_the_targets_equal_the_oracle
    with_instance do |pg|
      pg.exec("create table loads (id integer primary key, g integer not null, n integer not null)")
      pg.exec("insert into loads select i, i % 3, i from generate_series(1, 10) i")
      Trellis.apply("TRANSFORM load_copy FROM loads SELECT n AS n")
      Trellis.apply("TRANSFORM load_sums FROM loads GROUP BY g WITHOUT SOURCE INDEX SELECT g AS g, SUM(n) AS total")
      Trellis.apply("TRANSFORM load_operator FROM loads SELECT n AS n")
      await_status("load_copy")
      await_status("load_sums")
      await_status("load_operator")
      Trellis.apply("PAUSE TRANSFORM load_operator")

      # What Rails' fixtures do: the load runs with every trigger disabled
      # and ends with ENABLE TRIGGER ALL.
      ActiveRecord::Base.connection.disable_referential_integrity do
        ActiveRecord::Base.connection.execute("insert into loads select i, i % 3, i from generate_series(11, 40) i")
      end

      # Nothing has told Trellis yet; the helper's own check pauses them.
      resumed = Trellis::TestSupport.resume_capture_disruptions
      assert_equal %w[public.load_copy public.load_sums], resumed.sort

      assert_equal (1..40).map { |i| [i.to_s, i.to_s] }.sort,
                   pg.exec("select id::text, n::text from loads").values.sort
      assert_equal (1..40).map(&:to_s).sort, pg.exec("select n::text from load_copy").values.flatten.sort
      assert_equal pg.exec("select g::text, sum(n)::text from loads group by g order by g").values,
                   pg.exec("select g::text, total::text from load_sums order by g").values

      # The operator's pause is the operator's: still paused. The disruption
      # seen while it was paused is recorded as kind :capture, not
      # :disruption, so the helper leaves it to the operator.
      operator = Trellis.status("load_operator")
      assert_equal :paused, operator.status
      assert_equal :capture, operator.capture_failure.kind

      # And nothing is left to do.
      assert_equal [], Trellis::TestSupport.resume_capture_disruptions
    end
  end

  def test_a_disruption_is_a_capture_failure_of_kind_disruption
    with_instance do |pg|
      pg.exec("create table kinds (id integer primary key, n integer not null)")
      Trellis.apply("TRANSFORM kind_copy FROM kinds SELECT n AS n")
      await_status("kind_copy")

      pg.exec("alter table kinds disable trigger all")
      pg.exec("alter table kinds enable trigger all")
      assert_equal ["public.kinds"], Trellis.detect_capture_disruptions

      status = Trellis.status("kind_copy")
      assert_equal :paused, status.status
      assert_equal :disruption, status.capture_failure.kind
      assert_match "changed outside Trellis", status.capture_failure.error
      Trellis::TestSupport.resume_capture_disruptions
      assert_equal :live, Trellis.status("kind_copy").status
    end
  end

  def test_a_refused_resume_leaves_the_definition_paused_and_is_raised
    with_instance do |pg|
      pg.exec("create table gone (id integer primary key, n integer not null)")
      pg.exec("create table kept (id integer primary key, n integer not null)")
      Trellis.apply("TRANSFORM gone_copy FROM gone SELECT n AS n")
      Trellis.apply("TRANSFORM kept_copy FROM kept SELECT n AS n")
      await_status("gone_copy")
      await_status("kept_copy")

      pg.exec("alter table gone rename to gone_away")
      pg.exec("alter table kept disable trigger all")
      pg.exec("alter table kept enable trigger all")
      pg.exec("insert into kept values (1, 7)")

      error = assert_raises(Trellis::TestSupport::ResumeRefused) do
        Trellis::TestSupport.resume_capture_disruptions
      end
      assert_equal ["public.gone_copy"], error.refused.keys
      assert_match "gone", error.message

      # The refused one stays paused for its disruption; the other was
      # resumed and waited for, and holds its row.
      refused = Trellis.status("gone_copy")
      assert_equal :paused, refused.status
      assert_equal :disruption, refused.capture_failure.kind
      assert_equal :live, Trellis.status("kept_copy").status
      assert_equal [["7"]], pg.exec("select n::text from kept_copy").values
    end
  end

  # A table recreated with a wider column (a test schema reloaded after a
  # migration): the resume re-types Trellis's copy first, reporting a
  # "resuming:" capture failure meanwhile, which is a resume in progress,
  # not a refusal.
  def test_a_resume_that_re_types_first_is_waited_for
    with_instance do |pg|
      pg.exec("create table widened (id integer primary key, n integer not null)")
      pg.exec("insert into widened values (1, 1)")
      Trellis.apply("TRANSFORM widened_copy FROM widened SELECT n AS n")
      await_status("widened_copy")

      pg.exec("drop table widened")
      pg.exec("create table widened (id integer primary key, n bigint not null)")
      pg.exec("insert into widened values (1, 5000000000)")

      assert_equal ["public.widened_copy"], Trellis::TestSupport.resume_capture_disruptions
      assert_equal :live, Trellis.status("widened_copy").status
      assert_equal [["5000000000"]], pg.exec("select n::text from widened_copy").values
    end
  end

  def test_it_is_callable_as_a_method_of_a_test_that_includes_it
    klass = Class.new do
      include Trellis::TestSupport

      def run_it = resume_capture_disruptions
    end
    assert_includes klass.private_instance_methods, :resume_capture_disruptions
    with_instance { assert_equal [], klass.new.run_it }
  end

  private

  # A private cluster with a connected instance running both workers, and
  # ActiveRecord connected to it.
  def with_instance
    TestCluster.private_cluster do |info|
      Trellis.connect(url: info.fetch("dsn"))
      Trellis.migrate
      Trellis.shutdown
      Trellis.connect(url: info.fetch("dsn"), staging: true, drain_threads: 1)
      ActiveRecord::Base.establish_connection(
        adapter: "postgresql", host: info.fetch("host"), port: info.fetch("port"),
        username: info.fetch("user"), database: info.fetch("dbname")
      )
      pg = TestCluster.pg(info)
      begin
        yield pg
      ensure
        pg.close
        ActiveRecord::Base.connection_pool.disconnect!
        Trellis.shutdown
      end
    end
  end
end
