# frozen_string_literal: true

require "test_helper"

class MetricsTest < Minitest::Test
  include TrellisTestCase

  EXPOSITION_LINE = /\A(# (HELP|TYPE) trellis_\w+ .+|trellis_\w+(\{[^}]*\})? \S+)\z/

  # The registry is process-wide, so rendering needs no handle. What it holds
  # depends on what earlier tests in this process recorded, so this only
  # checks the text's shape.
  def test_renders_the_process_wide_registry_as_prometheus_text_without_a_handle
    refute Trellis.connected?
    body = Trellis::Metrics.render_prometheus
    assert_kind_of String, body

    body.each_line(chomp: true).reject(&:empty?).each do |line|
      assert_match EXPOSITION_LINE, line, "not a Prometheus exposition line: #{line.inspect}"
    end
  end

  # trellis_instance_up is rendered from the running handles themselves, so
  # it is there as soon as connect returns, and stays at 0 after shutdown.
  def test_an_instance_is_up_while_its_handle_runs_and_down_after_shutdown
    instance = Trellis::Instance.connect(url: TestCluster.dsn, schema: "metrics_up")
    assert_equal 1, instance_up("metrics_up")

    instance.shutdown
    assert_equal 0, instance_up("metrics_up")
  end

  private

  def instance_up(schema)
    sample = Trellis::Metrics.render_prometheus[
      %r{^trellis_instance_up\{trellis_instance="[^"/]+/#{schema}"\} (\d+)$}, 1
    ]
    refute_nil sample, "no trellis_instance_up series for #{schema}"
    Integer(sample)
  end
end
