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
  # The binding opens no port. Serve the text from a route the app already
  # has, for example in Rails:
  #
  #   # config/routes.rb
  #   get "/metrics", to: proc {
  #     [200, { "content-type" => "text/plain; version=0.0.4" }, [Trellis::Metrics.render_prometheus]]
  #   }
  #
  # In a forking server each worker process has its own registry, holding
  # only its own handles' series, so scrape every worker (or run the worker
  # handle, staging: true, in a process of its own and scrape that).
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
  end
end
