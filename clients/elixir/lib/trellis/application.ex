defmodule Trellis.Application do
  @moduledoc false
  # Starts `Trellis.LogBridge` unless the host opted out with
  # `config :trellis_pg, log_bridge: false`.

  use Application

  @impl true
  def start(_type, _args) do
    Supervisor.start_link(children(), strategy: :one_for_one, name: Trellis.Supervisor)
  end

  @doc false
  def children do
    if Application.get_env(:trellis_pg, :log_bridge, true), do: [Trellis.LogBridge], else: []
  end
end
