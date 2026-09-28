# frozen_string_literal: true

require "test_helper"
require_relative "support/parity"
require_relative "support/rails_app"

# The cross-language parity suite (issue #155): clients/parity/cases.json,
# a script of (operation, input, expected shape) steps that the Elixir suite
# runs too. Neither suite owns it; both are checked against it, so a field
# name, a nil, a symbol, a time or a page that one binding shapes
# differently from the other fails here or there. See
# clients/parity/README.md.
class ParityTest < Minitest::Test
  include TrellisTestCase

  # The script, top to bottom, on a cluster of its own: it runs the staging
  # worker and ends by poisoning a key, which would disturb the shared
  # cluster's other tests (#588).
  def test_every_live_step_returns_its_expected_shape
    TestCluster.private_cluster do |cluster|
      pg = TestCluster.pg(cluster)
      saved = {}
      Parity.fixture.fetch("live").each_with_index do |step, index|
        run_step(step, index, pg:, cluster:, saved:)
      end
    ensure
      # Before the private cluster goes away under it.
      Trellis.shutdown
      pg&.close
    end
  end

  # The host half of each conversion: the native map the extension would
  # hand over, turned into this binding's value.
  def test_every_conversion_returns_its_expected_shape
    Parity.fixture.fetch("conversions").each do |conversion|
      native = Parity.native(conversion.fetch("native"))
      value = if conversion.fetch("type") == "Error"
                Trellis::Error.from_native(native.fetch(:code), native.fetch(:message))
              else
                Trellis.const_get(conversion.fetch("type")).from_native(native)
              end
      assert_shape(conversion.fetch("expect"), value, {}, conversion.fetch("about"))
    end
  end

  # Every error code trellis-embed maps has a live step that raises it, so a
  # code's class is checked against a real engine error, not only a table.
  def test_every_error_code_has_a_live_step
    raised = Parity.each_shape(Parity.fixture.fetch("live")).filter_map do |shape|
      shape["error"] if shape.is_a?(Hash)
    end
    missing = Trellis::Native.error_codes - raised
    assert_empty missing, "no live step in #{Parity::FIXTURE_PATH} raises these codes"
  end

  # Every statement form the grammar has is applied by some step, named by
  # its trellis::StatementKind.
  def test_every_statement_kind_has_a_live_step
    covered = Parity.fixture.fetch("live").filter_map { |step| step.dig("covers", "statement_kind") }
    assert_equal Trellis::Native.statement_kinds.sort, covered.sort
  end

  # The fixture's records are exactly this binding's value types, field for
  # field, and every one is checked by some step or conversion.
  def test_the_fixture_describes_every_record_this_binding_returns
    records = Parity.fixture.fetch("records")
    host = Trellis.constants.map { |name| Trellis.const_get(name) }
                  .select { |value| value.is_a?(Class) && value < Data }
                  .to_h { |klass| [klass.name.delete_prefix("Trellis::"), klass.members.map(&:to_s).sort] }
    assert_equal records.transform_values { |fields| fields.keys.sort }, host

    checked = Parity.each_shape([Parity.fixture.fetch("live"), Parity.fixture.fetch("conversions")])
                    .filter_map { |shape| shape["record"] if shape.is_a?(Hash) }
    assert_empty records.keys - checked, "records no step or conversion checks"
  end

  # Every public call of this binding is a fixture operation, and every
  # operation is used by some step.
  def test_every_public_call_is_a_fixture_operation_that_some_step_runs
    public_calls = Trellis.singleton_methods(false).map { |name| name.to_s.delete_suffix("?") }
    expected = public_calls - Parity::NOT_OPERATIONS.keys.map { |name| name.delete_suffix("?") }
    assert_equal expected.sort, Parity::OPERATIONS.keys.sort

    used = Parity.fixture.fetch("live").map { |step| step.fetch("op") }.uniq
    assert_equal (Parity::OPERATIONS.keys + Parity::HARNESS_OPS).sort, used.sort
  end

  # The Rails integration adds no call to `Trellis`: each migration helper
  # is the fixture operation of the same name, and every other public call
  # it has is a listed Rails-only exception.
  def test_the_rails_integration_is_fixture_operations_or_listed_exceptions
    # exec_migration is ActiveRecord's hook, which the helpers wrap, not a
    # helper.
    helpers = Trellis::Migration.public_instance_methods(false).map(&:to_s) - ["exec_migration"]
    assert_equal %w[apply define status], helpers.sort
    assert_empty helpers - Parity::OPERATIONS.keys

    calls = { "Trellis::Railtie" => Trellis::Railtie, "Trellis::Migration" => Trellis::Migration }
            .flat_map { |name, mod| mod.singleton_methods(false).map { |call| "#{name}.#{call}" } }
    assert_equal Parity::RAILS_ONLY.keys.sort, calls.sort
  end

  private

  def run_step(step, index, pg:, cluster:, saved:)
    op = step.fetch("op")
    what = "step #{index} (#{op}#{step['about'] && ": #{step['about']}"})"
    case op
    when "sql" then step.fetch("args").each { |sql| pg.exec(sql) }
    when "now" then saved[step.fetch("save")] = Time.now.utc.floor(6)
    else
      args = Parity.argument(step.fetch("args", []), saved:, cluster:)
      opts = Parity.argument(step.fetch("opts", {}), saved:, cluster:)
      call = -> { call_operation(op, args, opts) }
      value = if step.key?("until")
                poll(step, saved, what, &call)
              else
                call.call.tap { |result| assert_shape(step.fetch("expect"), result, saved, what) }
              end
      saved[step["save"]] = value if step.key?("save")
    end
  end

  # The operation's result, or the Trellis::Error it raised.
  def call_operation(op, args, opts)
    Parity::OPERATIONS.fetch(op).call(args, opts)
  rescue Trellis::Error => e
    e
  end

  # Calls the operation until its result matches the step's `until` shape,
  # for at most UNTIL_SECONDS, then fails naming what it waited for (#297).
  def poll(step, saved, what)
    waiting_for = step.fetch("waiting_for")
    eventually_value("#{waiting_for} (#{what})",
                     ->(seen) { shape_mismatches(step.fetch("until"), seen, saved).empty? },
                     seconds: Parity::UNTIL_SECONDS) { yield }
  end

  def assert_shape(shape, value, saved, what)
    problems = shape_mismatches(shape, value, saved)
    assert_empty problems, "#{what} returned #{value.inspect}"
  end

  def shape_mismatches(shape, value, saved)
    Parity::Matcher.new(saved).mismatches(shape, Parity.canonical(value))
  rescue Parity::Mismatch => e
    [e.message]
  end
end
