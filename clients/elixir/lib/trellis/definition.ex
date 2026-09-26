defmodule Trellis.Definition do
  @moduledoc """
  A registered transform definition, as `Trellis.define/2` returns it.

  `target_table` and `source_table` are fully qualified (`schema.table`).
  `source_columns` maps each source column the definition was validated
  against to its type's name (`"bigint"`, `"numeric"`, `"text"`, ...).
  """

  @type t :: %__MODULE__{
          id: integer(),
          target_table: String.t(),
          source_table: String.t(),
          source_version: integer(),
          status: Trellis.Status.status(),
          source_columns: %{String.t() => String.t()}
        }

  @enforce_keys [:id, :target_table, :source_table, :source_version, :status, :source_columns]
  defstruct @enforce_keys

  @doc false
  def from_native(map) when is_map(map), do: struct!(__MODULE__, map)
end
