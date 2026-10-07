# frozen_string_literal: true

require "pp"
require "test_helper"

class TrellisTest < Minitest::Test
  include TrellisTestCase

  # ADR-0010's definition of done for the slice: define a 1-1 transform,
  # watch it reach :live, insert a source row, see it in the target.
  #
  # Something has to run the staging worker and drain threads or the
  # definition never leaves :waiting_to_backfill. Here that's the test's own
  # handle: this process is the whole fleet. A Trellis schema has one
  # staging worker at a time, so tests that run it take turns:
  # Minitest runs them one at a time, and teardown shuts each one down.
  def test_a_defined_transform_goes_live_and_keeps_its_target_converged
    pg = TestCluster.pg
    pg.exec("create table widgets (id integer primary key, price integer)")
    pg.exec("insert into widgets (id, price) values (1, 5)")

    Trellis.connect(url: TestCluster.dsn, staging: true, drain_threads: 1)

    definition = Trellis.define("TRANSFORM widget_prices FROM widgets SELECT price AS price")
    assert_instance_of Trellis::Definition, definition
    assert_equal "public.widget_prices", definition.target_table
    assert_equal "public.widgets", definition.source_table
    assert_equal :waiting_to_backfill, definition.status
    assert_equal({ "id" => "integer", "price" => "integer" }, definition.source_columns)

    status = await_status("widget_prices")
    assert_equal Trellis::Status.new(status: :live, backfill_failure: nil, capture_wait: nil,
                                     capture_failure: nil, held_keys: nil, drain_failure: nil),
                 status

    # The backfill carried the existing row across.
    assert_equal [[1, 5]], rows(pg)

    pg.exec("insert into widgets (id, price) values (2, 7)")
    eventually_value("the new source row to reach widget_prices",
                     ->(rows) { rows == [[1, 5], [2, 7]] }) { rows(pg) }

    assert_nil Trellis.shutdown
    refute Trellis.connected?
  ensure
    pg&.close
  end

  # define is `apply` underneath, which carries out every statement form, so
  # it must refuse the others before applying them, not after. The handle
  # runs nothing in the background, so the definition stays at
  # :waiting_to_backfill unless one of these statements reaches the engine.
  def test_define_refuses_any_other_statement_form_without_applying_it
    pg = TestCluster.pg
    pg.exec("create table gadgets (id integer primary key, price integer)")
    Trellis.connect(url: TestCluster.dsn)

    assert_equal :waiting_to_backfill,
                 Trellis.define("TRANSFORM gadget_prices FROM gadgets SELECT price AS price").status

    ["PAUSE TRANSFORM gadget_prices",
     "  drop transform gadget_prices",
     "RELATIONSHIP owner FROM gadgets.id TO gadgets.id",
     "DROP RELATIONSHIP gadgets.owner"].each do |statement|
      error = assert_raises(Trellis::ValidationError) { Trellis.define(statement) }
      assert_equal :validation, error.code
      assert_match "nothing was applied", error.message
      assert_equal :waiting_to_backfill, Trellis.status("gadget_prices").status,
                   "#{statement.inspect} took effect"
    end
  ensure
    pg&.close
  end

  def test_a_statement_that_does_not_parse_is_a_parse_error
    Trellis.connect(url: TestCluster.dsn)

    error = assert_raises(Trellis::ParseError) { Trellis.define("TRANSFORM oops") }
    assert_equal :parse, error.code
    # A malformed statement of another form is the same parse error, not a
    # validation refusal naming a form it never managed to be.
    assert_raises(Trellis::ParseError) { Trellis.define("DROP gadget_prices") }
  end

  def test_config_reads_back_the_options_the_handle_connected_with_but_never_the_url
    # The test cluster trusts every local connection, so the password is
    # accepted and ignored: it's here to be looked for in the output.
    password = "s3cret-hunter2"
    Trellis.connect(url: "#{TestCluster.dsn} password=#{password}", target_schema: "reporting")
    config = Trellis.config
    assert_instance_of Trellis::Config, config
    assert_equal "trellis", config.schema
    assert_equal "reporting", config.target_schema
    assert_operator config.pool_max_size, :>=, 1
    assert_operator config.pool_wait_timeout_ms, :>=, 1

    # A config is the kind of value that gets logged whole.
    refute_includes config.to_h.keys, :url
    [config.inspect, config.to_s, config.to_h.to_s, config.pretty_inspect].each do |printed|
      refute_includes printed, password
    end
  end

  def test_a_table_no_transform_writes_has_no_status
    Trellis.connect(url: TestCluster.dsn)
    assert_nil Trellis.status("no_such_target")
  end

  def test_an_engine_error_is_raised_as_the_subclass_for_its_code
    Trellis.connect(url: TestCluster.dsn)
    error = assert_raises(Trellis::NotFoundError) do
      Trellis.define("TRANSFORM ghost_prices FROM no_such_source SELECT price AS price")
    end
    assert_equal :not_found, error.code
  end

  def test_shutdown_disconnects_and_is_idempotent
    Trellis.connect(url: TestCluster.dsn)
    assert Trellis.connected?
    assert_nil Trellis.shutdown
    refute Trellis.connected?
    assert_nil Trellis.shutdown

    error = assert_raises(Trellis::ValidationError) { Trellis.status("widget_prices") }
    assert_match "not connected", error.message

    # And the process can connect again.
    Trellis.connect(url: TestCluster.dsn)
    assert_nil Trellis.status("no_such_target")
  end

  # ADR-0010 decision 3: one handle per process.
  def test_a_second_connect_in_the_same_process_is_refused
    Trellis.connect(url: TestCluster.dsn)
    error = assert_raises(Trellis::ValidationError) { Trellis.connect(url: TestCluster.dsn) }
    assert_match "already connected", error.message
    assert_nil Trellis.status("no_such_target"), "the first handle still works"
  end

  # Connecting doesn't reach the database unless it starts background work
  # (the pool connects on first use), so an unreachable one surfaces on the
  # first call, as the connectivity error.
  def test_an_unreachable_database_raises_a_connectivity_error
    # A socket directory with no server in it.
    Trellis.connect(url: "host=/nonexistent/trellis-test port=5432 user=postgres dbname=x")
    error = assert_raises(Trellis::ConnectivityError) { Trellis.status("widget_prices") }
    assert_equal :connectivity, error.code
  end

  def test_options_are_validated_before_connecting
    [
      [{ url: :nope }, "url must be a String"],
      [{ url: TestCluster.dsn, schema: nil }, "schema must be a String"],
      [{ url: TestCluster.dsn, target_schema: 1 }, "target_schema must be a String"],
      [{ url: TestCluster.dsn, staging: "yes" }, "staging must be true or false"],
      [{ url: TestCluster.dsn, drain_threads: -1 }, "drain_threads must be a non-negative"],
      [{ url: TestCluster.dsn, worker_threads: 0 }, "worker_threads must be a positive"]
    ].each do |options, message|
      error = assert_raises(Trellis::ValidationError, options.inspect) { Trellis.connect(**options) }
      assert_match message, error.message
      refute Trellis.connected?
    end
    assert_raises(ArgumentError) { Trellis.connect(url: TestCluster.dsn, stagin: true) }
  end

  private

  def rows(pg)
    pg.exec("select id, price from widget_prices order by id").values.map { |r| r.map(&:to_i) }
  end
end
