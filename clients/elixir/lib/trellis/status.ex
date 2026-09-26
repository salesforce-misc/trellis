defmodule Trellis.Status do
  @moduledoc """
  Where a transform is in its lifecycle, as `Trellis.status/2` reports it.

  A newly defined transform starts `:waiting_to_backfill` and reaches `:live`
  once its backfill has run and it has caught up. That takes a connection
  somewhere in the fleet running the staging worker and drain threads (see
  `Trellis.connect/1`); with none, a definition waits forever.

  `backfill_failure` is set while the backfill of the definition's source
  table keeps failing, and names the cause.
  """

  @typedoc "A transform's lifecycle status."
  @type status ::
          :waiting_to_backfill | :backfilling | :catching_up | :live | :quarantined | :paused

  @type t :: %__MODULE__{status: status(), backfill_failure: Trellis.BackfillFailure.t() | nil}

  @enforce_keys [:status, :backfill_failure]
  defstruct @enforce_keys

  @doc false
  def from_native(%{status: status, backfill_failure: failure}) do
    %__MODULE__{
      status: status,
      backfill_failure: failure && Trellis.BackfillFailure.from_native(failure)
    }
  end
end

defmodule Trellis.BackfillFailure do
  @moduledoc """
  A source table's backfill that keeps failing: how many attempts have failed,
  the latest error, and when the staging worker tries again. It retries on
  its own; fix the cause and the next attempt goes through.
  """

  @type t :: %__MODULE__{
          source_table: String.t(),
          attempts: non_neg_integer(),
          last_error: String.t(),
          next_attempt_at: DateTime.t()
        }

  @enforce_keys [:source_table, :attempts, :last_error, :next_attempt_at]
  defstruct @enforce_keys

  @doc false
  def from_native(%{next_attempt_at_micros: micros} = failure) do
    %__MODULE__{
      source_table: failure.source_table,
      attempts: failure.attempts,
      last_error: failure.last_error,
      next_attempt_at: Trellis.Time.from_micros(micros)
    }
  end
end
