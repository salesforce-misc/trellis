# frozen_string_literal: true

module Trellis
  # The engine's metrics, in Prometheus text exposition format: the Rust
  # crate's trellis::Metrics#render_prometheus
  # (docs/decisions/0009-observability-decisions.md).
  #
  # The registry is process-wide: one body holds the series of every Trellis
  # handle in this process, so it needs no handle and works before
  # Trellis.connect and after Trellis.shutdown. Every series carries a
  # trellis_instance="<database>/<catalog schema>" label, so instances in one
  # process stay apart. trellis_instance_up is 1 while an instance has a
  # running handle and 0 after its last one shuts down; the other gauges of a
  # stopped instance disappear after 60 seconds. See docs/observability.md.
  #
  # Trellis::Metrics::RackApp serves the text from a route the app already
  # has; Trellis.connect's metrics_bind serves it on a port of its own, for a
  # process with no web server. Most series are set only in the process
  # running the staging or drain work, so scrape that one (README, Metrics).
  #
  # A process forked while its parent had an engine running may have
  # inherited the registry's lock held, so rendering there raises
  # Trellis::ForkedHandleError, as Trellis.connect does.
  module Metrics
    # Renders the registry's current contents: # HELP and # TYPE lines, then
    # each series' samples.
    def self.render_prometheus
      Native.render_prometheus
    end

    # A Rack app answering GET and HEAD with render_prometheus, in the
    # content type Prometheus expects, at whatever path it is mounted; every
    # other method is a 405. It needs no Rack gem. Unauthenticated, as a
    # Prometheus target is: guard it if the port is public.
    #
    #   mount Trellis::Metrics::RackApp, at: "/metrics/trellis"   # Rails
    #   map("/metrics/trellis") { run Trellis::Metrics::RackApp } # config.ru
    class RackApp
      CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

      def self.call(env)
        new.call(env)
      end

      def call(env)
        case env["REQUEST_METHOD"]
        when "GET"
          body = Metrics.render_prometheus
          [200, headers(body), [body]]
        when "HEAD"
          [200, headers(Metrics.render_prometheus), []]
        else
          [405, { "allow" => "GET, HEAD", "content-length" => "0" }, []]
        end
      end

      private

      def headers(body)
        { "content-type" => CONTENT_TYPE, "content-length" => body.bytesize.to_s }
      end
    end
  end
end
