defmodule Trellis.Relationship do
  @moduledoc """
  A relationship declaration a `RELATIONSHIP` statement registered, as
  `Trellis.apply/2` reports it.

  `cardinality` is `:one` when `to_col` is the sole column of a primary key
  or unique index on `to_table` (at most one related row), and `:many`
  otherwise. `warnings` holds each non-fatal caveat the declaration raised,
  such as a missing index on `from_col`, as its message.
  """

  @typedoc "Whether a relationship's to-side has at most one row per from-row."
  @type cardinality :: :one | :many

  @type t :: %__MODULE__{
          id: integer(),
          name: String.t(),
          from_schema: String.t(),
          from_table: String.t(),
          from_col: String.t(),
          to_schema: String.t(),
          to_table: String.t(),
          to_col: String.t(),
          cardinality: cardinality(),
          warnings: [String.t()]
        }

  @enforce_keys [
    :id,
    :name,
    :from_schema,
    :from_table,
    :from_col,
    :to_schema,
    :to_table,
    :to_col,
    :cardinality,
    :warnings
  ]
  defstruct @enforce_keys

  @doc false
  def from_native(map) when is_map(map), do: struct!(__MODULE__, map)
end

defmodule Trellis.RelationshipSummary do
  @moduledoc """
  A registered relationship, as `Trellis.relationships/1` lists it:
  `Trellis.Relationship`'s fields, with the time it was declared in place of
  its creation-time warnings.
  """

  @type t :: %__MODULE__{
          id: integer(),
          name: String.t(),
          from_schema: String.t(),
          from_table: String.t(),
          from_col: String.t(),
          to_schema: String.t(),
          to_table: String.t(),
          to_col: String.t(),
          cardinality: Trellis.Relationship.cardinality(),
          created_at: DateTime.t()
        }

  @enforce_keys [
    :id,
    :name,
    :from_schema,
    :from_table,
    :from_col,
    :to_schema,
    :to_table,
    :to_col,
    :cardinality,
    :created_at
  ]
  defstruct @enforce_keys

  @doc false
  def from_native(%{created_at_micros: micros} = summary) do
    summary
    |> Map.delete(:created_at_micros)
    |> Map.put(:created_at, Trellis.Time.from_micros(micros))
    |> then(&struct!(__MODULE__, &1))
  end
end
