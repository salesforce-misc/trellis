# frozen_string_literal: true

require "net/http"
require "socket"
require "test_helper"

# connect's metrics_bind: the process's registry served over HTTP by the
# CLI's --prometheus-bind listener (trellis-embed's metrics_http), one per
# process. Every test binds port 0, or a port it holds itself, and scrapes
# the real listener.
class MetricsBindTest < Minitest::Test
  include TrellisTestCase

  CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

  def test_serves_the_process_registry_as_prometheus_text
    instance = connect("metrics_bind_serve", "127.0.0.1:0")
    address = instance.metrics_address
    assert_match(/\A127\.0\.0\.1:\d+\z/, address)
    refute_equal "127.0.0.1:0", address

    response = http(address) { |h| h.get("/metrics") }
    assert_equal "200", response.code
    assert_equal CONTENT_TYPE, response["content-type"]
    assert_equal response.body.bytesize.to_s, response["content-length"]
    assert_match(%r{^trellis_instance_up\{trellis_instance="[^"/]+/metrics_bind_serve"\} 1$}, response.body)
  end

  # As RackApp: GET and HEAD on any path, a 405 for any other method.
  def test_answers_get_and_head_on_any_path_and_refuses_other_methods
    address = connect("metrics_bind_any", "127.0.0.1:0").metrics_address

    get = http(address) { |h| h.get("/elsewhere") }
    assert_equal "200", get.code
    head = http(address) { |h| h.head("/") }
    assert_equal ["200", CONTENT_TYPE], [head.code, head["content-type"]]
    assert_nil head.body
    post = http(address) { |h| h.post("/metrics", "") }
    assert_equal ["405", "GET, HEAD"], [post.code, post["allow"]]
  end

  def test_an_instance_without_metrics_bind_serves_nothing
    assert_nil connect("metrics_bind_none", nil).metrics_address
  end

  def test_an_address_that_can_t_be_bound_fails_the_connect
    held = TCPServer.new("127.0.0.1", 0)
    address = "127.0.0.1:#{held.addr[1]}"

    error = assert_raises(Trellis::ValidationError) { connect("metrics_bind_in_use", address) }
    assert_includes error.message, "metrics_bind #{address}: can't listen there"
    assert_includes error.message, "another process holds it"
    assert_empty Trellis::Instance.connected
  ensure
    held&.close
  end

  # Bound before the engine connects, and given back when that fails.
  def test_a_connect_failing_after_the_bind_releases_the_port
    held = TCPServer.new("127.0.0.1", 0)
    address = "127.0.0.1:#{held.addr[1]}"
    held.close

    # The staging worker reads Trellis's tables as it starts: unmigrated, it fails.
    assert_raises(Trellis::Error) do
      Trellis::Instance.connect(url: TestCluster.dsn, schema: "metrics_bind_unmigrated", staging: true,
                                metrics_bind: address)
    end
    assert_raises(Errno::ECONNREFUSED) { TCPSocket.new(*host_port(address)).close }
    assert_equal address, connect("metrics_bind_after_failure", address).metrics_address
  end

  def test_metrics_bind_is_an_ip_and_port
    error = assert_raises(Trellis::ValidationError) { connect("metrics_bind_bad", "localhost:9464") }
    assert_includes error.message, "metrics_bind expects an IP address and a port"
    assert_raises(Trellis::ValidationError) { connect("metrics_bind_bad", 9464) }
    assert_empty Trellis::Instance.connected
  end

  # One listener per process: the same address (as given, or as bound)
  # shares it, another is refused, and it closes with the last instance that
  # asked for it, releasing the port.
  def test_instances_share_one_listener_until_the_last_shuts_down
    first = connect("metrics_bind_first", "127.0.0.1:0")
    address = first.metrics_address
    as_given = connect("metrics_bind_as_given", "127.0.0.1:0")
    as_bound = connect("metrics_bind_as_bound", address)
    assert_equal [address] * 2, [as_given.metrics_address, as_bound.metrics_address]

    error = assert_raises(Trellis::ValidationError) { connect("metrics_bind_other", "127.0.0.2:0") }
    assert_includes error.message, "this process already serves its metrics on #{address}"

    first.shutdown
    as_given.shutdown
    assert_equal "200", http(address) { |h| h.get("/") }.code

    as_bound.shutdown
    assert_raises(Errno::ECONNREFUSED) { TCPSocket.new(*host_port(address)).close }
    assert_equal address, connect("metrics_bind_again", address).metrics_address
  end

  # A child forked while the parent serves can't connect at all, so it never
  # binds; the parent goes on serving. Once the parent has shut down, one
  # child may serve the address, and a sibling asking for it is refused.
  def test_a_forked_child_never_takes_its_parents_or_a_siblings_address
    address = connect("metrics_bind_parent", "127.0.0.1:0").metrics_address
    output = in_child { connect("metrics_bind_child", address) }
    assert_match(/\ATrellis::ForkedHandleError: .*was forked from process #{Process.pid}/, output)
    assert_equal "200", http(address) { |h| h.get("/") }.code

    Trellis::Instance.shutdown_all
    go_r, go_w = IO.pipe
    ready_r, ready_w = IO.pipe
    serving = fork do
      ready_r.close
      go_w.close
      instance = connect("metrics_bind_worker", address)
      ready_w.puts instance.metrics_address
      ready_w.close
      go_r.read
      instance.shutdown
      exit!(0)
    end
    ready_w.close
    go_r.close
    assert_equal address, ready_r.gets&.chomp
    assert_equal "200", http(address) { |h| h.get("/") }.code

    output = in_child { connect("metrics_bind_sibling", address) }
    assert_match(/\ATrellis::ValidationError: metrics_bind #{address}: can't listen there: .*another process holds it/,
                 output)
  ensure
    go_w&.close
    assert wait_for_child(serving, seconds: 30).success? if serving
  end

  private

  def connect(schema, metrics_bind)
    Trellis::Instance.connect(url: TestCluster.dsn, schema: schema, metrics_bind: metrics_bind)
  end

  def host_port(address)
    host, port = address.split(":")
    [host, Integer(port)]
  end

  def http(address, &)
    Net::HTTP.start(*host_port(address), open_timeout: 5, read_timeout: 5, &)
  end

  # Runs the block in a forked child; returns what it raised (class and
  # message) or "connected".
  def in_child
    out_r, out_w = IO.pipe
    child = fork do
      out_r.close
      begin
        yield
        out_w.puts "connected"
        Trellis::Instance.shutdown_all
      rescue StandardError => e
        out_w.puts "#{e.class}: #{e.message}"
      end
    ensure
      out_w.close
      exit!(0)
    end
    out_w.close
    assert wait_for_child(child, seconds: 30).success?
    out_r.read
  ensure
    out_r&.close
  end
end
