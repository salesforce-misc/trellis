defmodule Trellis.Config do
  @moduledoc """
  The configuration a handle connected with, as `Trellis.config/1` returns
  it: `connect/1`'s `:url`, `:schema` and `:target_schema`, and the
  connection pool's size cap and how long a call waits for a free connection.

  `url` is the connection string exactly as it was passed to
  `Trellis.connect/1`, password and all: don't log it.
  """

  @type t :: %__MODULE__{
          url: String.t(),
          schema: String.t(),
          target_schema: String.t(),
          pool_max_size: pos_integer(),
          pool_wait_timeout_ms: non_neg_integer()
        }

  @enforce_keys [:url, :schema, :target_schema, :pool_max_size, :pool_wait_timeout_ms]
  defstruct @enforce_keys

  @doc false
  def from_native(map) when is_map(map), do: struct!(__MODULE__, map)
end
