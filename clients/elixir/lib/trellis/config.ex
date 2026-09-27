defmodule Trellis.Config do
  @moduledoc """
  The configuration a handle connected with, as `Trellis.config/1` returns
  it: `connect/1`'s `:schema` and `:target_schema`, and the connection pool's
  size cap and how long a call waits for a free connection.

  Not the `:url`: a connection string can carry a password, and a config is
  the kind of value that ends up in a log line or crash report whole. The
  caller already has the URL it connected with.
  """

  @type t :: %__MODULE__{
          schema: String.t(),
          target_schema: String.t(),
          pool_max_size: pos_integer(),
          pool_wait_timeout_ms: non_neg_integer()
        }

  @enforce_keys [:schema, :target_schema, :pool_max_size, :pool_wait_timeout_ms]
  defstruct @enforce_keys

  @doc false
  def from_native(map) when is_map(map), do: struct!(__MODULE__, map)
end
