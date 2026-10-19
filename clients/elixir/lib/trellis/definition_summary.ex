defmodule Trellis.DefinitionSummary do
  @moduledoc """
  A registered transform definition, as `Trellis.definitions/1` lists it.

  `target_table` is fully qualified (`schema.table`) and identifies the
  transform. `created_at` is when it was registered.
  `backfill_failure` is set while its build keeps failing, as in
  `Trellis.Status`. `halt` is set while the drain, or its build's merge,
  has halted on it: a `Trellis.CaptureFailure` of kind `:halt`, the
  definition `:paused` until the cause is fixed and it is resumed. Listing
  the definitions and looking for a `halt` is the health check for halts.
  """

  @type t :: %__MODULE__{
          target_table: String.t(),
          status: Trellis.Status.status(),
          created_at: DateTime.t(),
          backfill_failure: Trellis.BackfillFailure.t() | nil,
          halt: Trellis.CaptureFailure.t() | nil
        }

  @enforce_keys [
    :target_table,
    :status,
    :created_at,
    :backfill_failure,
    :halt
  ]
  defstruct @enforce_keys

  @doc false
  def from_native(%{created_at_micros: micros, backfill_failure: failure, halt: halt} = summary) do
    summary
    |> Map.delete(:created_at_micros)
    |> Map.put(:created_at, Trellis.Time.from_micros(micros))
    |> Map.put(:backfill_failure, failure && Trellis.BackfillFailure.from_native(failure))
    |> Map.put(:halt, halt && Trellis.CaptureFailure.from_native(halt))
    |> then(&struct!(__MODULE__, &1))
  end
end
