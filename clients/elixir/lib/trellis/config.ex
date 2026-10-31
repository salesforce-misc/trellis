defmodule Trellis.Config do
  @moduledoc """
  The configuration a handle connected with, as `Trellis.config/1` returns
  it: `connect/1`'s `:schema` and `:target_schema`, and the handle's call
  pool as it was built: `:max_call_pool_size` and `:call_pool_wait_timeout_ms`.

  Not the `:url`: a connection string can carry a password, and a config is
  the kind of value that ends up in a log line or crash report whole. The
  caller already has the URL it connected with.
  """

  @type t :: %__MODULE__{
          schema: String.t(),
          target_schema: String.t(),
          max_call_pool_size: pos_integer(),
          call_pool_wait_timeout_ms: non_neg_integer()
        }

  @enforce_keys [:schema, :target_schema, :max_call_pool_size, :call_pool_wait_timeout_ms]
  defstruct @enforce_keys

  @doc false
  def from_native(map) when is_map(map), do: struct!(__MODULE__, map)
end
