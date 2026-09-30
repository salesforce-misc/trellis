defmodule Trellis.Status do
  @moduledoc """
  Where a transform is in its lifecycle, as `Trellis.status/2` reports it.

  A newly defined transform starts `:waiting_to_backfill` and reaches `:live`
  once its backfill has run and it has caught up. That takes a connection
  somewhere in the fleet running the staging worker and drain threads (see
  `Trellis.connect/1`); with none, a definition waits forever.

  The other fields say what the transform is stuck on, and are `nil` when
  nothing holds it up:

    * `backfill_failure` is set while the backfill of its source table keeps
      failing, and names the cause.
    * `capture_wait` is set while installing or widening capture on a table
      it reads waits for a lock another session holds. It clears once that
      session lets go.
    * `capture_failure` is set while capture of a table it reads is broken:
      a schema change paused it (resume it once fixed), or installing
      capture keeps failing (it clears once the cause is fixed).

  Every process sees them, whichever one runs the staging worker.
  """

  @typedoc "A transform's lifecycle status."
  @type status ::
          :waiting_to_backfill | :backfilling | :catching_up | :live | :quarantined | :paused

  @type t :: %__MODULE__{
          status: status(),
          backfill_failure: Trellis.BackfillFailure.t() | nil,
          capture_wait: Trellis.CaptureWait.t() | nil,
          capture_failure: Trellis.CaptureFailure.t() | nil
        }

  @enforce_keys [:status, :backfill_failure, :capture_wait, :capture_failure]
  defstruct @enforce_keys

  @doc false
  def from_native(%{
        status: status,
        backfill_failure: failure,
        capture_wait: wait,
        capture_failure: capture_failure
      }) do
    %__MODULE__{
      status: status,
      backfill_failure: failure && Trellis.BackfillFailure.from_native(failure),
      capture_wait: wait && Trellis.CaptureWait.from_native(wait),
      capture_failure: capture_failure && Trellis.CaptureFailure.from_native(capture_failure)
    }
  end
end

defmodule Trellis.CaptureWait do
  @moduledoc """
  What a transform's capture waits on: the staging worker's install, widen or
  uninstall (`operation`) of the capture triggers on `table` needs
  `lock_mode`, and another session holds or is queued for a conflicting lock.
  `blockers` has one line per such session. Nothing cancels them; the wait
  ends when they let go.
  """

  @type t :: %__MODULE__{
          table: String.t(),
          operation: String.t(),
          lock_mode: String.t(),
          waiting_since: DateTime.t(),
          observed_at: DateTime.t(),
          blockers: [String.t()]
        }

  @enforce_keys [:table, :operation, :lock_mode, :waiting_since, :observed_at, :blockers]
  defstruct @enforce_keys

  @doc false
  def from_native(%{waiting_since_micros: since, observed_at_micros: observed} = wait) do
    %__MODULE__{
      table: wait.table,
      operation: wait.operation,
      lock_mode: wait.lock_mode,
      waiting_since: Trellis.Time.from_micros(since),
      observed_at: Trellis.Time.from_micros(observed),
      blockers: wait.blockers
    }
  end
end

defmodule Trellis.CaptureFailure do
  @moduledoc """
  Why capture of `source_table` is broken: `error` is a sentence naming the
  cause, `columns` the columns it is about (empty when it isn't about a
  column), and `detected_at` when it was first found.
  """

  @type t :: %__MODULE__{
          source_table: String.t(),
          columns: [String.t()],
          error: String.t(),
          detected_at: DateTime.t()
        }

  @enforce_keys [:source_table, :columns, :error, :detected_at]
  defstruct @enforce_keys

  @doc false
  def from_native(%{detected_at_micros: micros} = failure) do
    %__MODULE__{
      source_table: failure.source_table,
      columns: failure.columns,
      error: failure.error,
      detected_at: Trellis.Time.from_micros(micros)
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
