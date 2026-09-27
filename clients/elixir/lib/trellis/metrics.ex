defmodule Trellis.Metrics do
  @moduledoc """
  The engine's metrics, in Prometheus text exposition format: the Rust
  crate's `trellis::Metrics::render_prometheus`
  (`docs/decisions/0009-observability-decisions.md`).

  The registry is process-wide. It aggregates everything every Trellis
  handle in this OS process has recorded, so it needs no handle and works
  before `Trellis.connect/1` and after `Trellis.shutdown/1`.

  The binding opens no port. Serve the text from a route the host already
  has, for example with Plug:

      get "/metrics" do
        conn
        |> put_resp_content_type("text/plain; version=0.0.4")
        |> send_resp(200, Trellis.Metrics.render_prometheus())
      end
  """

  alias Trellis.Native

  @doc """
  Renders the registry's current contents: `# HELP` and `# TYPE` lines, then
  each series' samples.
  """
  @spec render_prometheus() :: String.t()
  def render_prometheus do
    {:ok, body} = Native.render_prometheus()
    body
  end
end
