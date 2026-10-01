defmodule Trellis.DefinitionSummary do
  @moduledoc """
  A registered transform definition, as `Trellis.definitions/1` lists it:
  `Trellis.Definition`'s fields, with the time it was registered in place of
  its source columns.

  `target_table` and `source_table` are fully qualified (`schema.table`).
  `backfill_failure` is set while its build keeps failing, as in
  `Trellis.Status`.
  """

  @type t :: %__MODULE__{
          id: integer(),
          target_table: String.t(),
          source_table: String.t(),
          source_version: integer(),
          status: Trellis.Status.status(),
          created_at: DateTime.t(),
          backfill_failure: Trellis.BackfillFailure.t() | nil
        }

  @enforce_keys [
    :id,
    :target_table,
    :source_table,
    :source_version,
    :status,
    :created_at,
    :backfill_failure
  ]
  defstruct @enforce_keys

  @doc false
  def from_native(%{created_at_micros: micros, backfill_failure: failure} = summary) do
    summary
    |> Map.delete(:created_at_micros)
    |> Map.put(:created_at, Trellis.Time.from_micros(micros))
    |> Map.put(:backfill_failure, failure && Trellis.BackfillFailure.from_native(failure))
    |> then(&struct!(__MODULE__, &1))
  end
end
