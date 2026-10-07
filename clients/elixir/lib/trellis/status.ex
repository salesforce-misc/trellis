defmodule Trellis.Status do
  @moduledoc """
  Where a transform is in its lifecycle, as `Trellis.status/2` reports it.

  A newly defined transform starts `:waiting_to_backfill` and reaches `:live`
  once its backfill has run and it has caught up. That takes a connection
  somewhere in the fleet running the staging worker and drain threads (see
  `Trellis.connect/1`); with none, a definition waits forever.

  The other fields say what the transform is stuck on, and are `nil` when
  nothing holds it up:

    * `backfill_failure` is set while its build keeps failing (one of its
      build chunks, or the backfill of its source table), and names the
      cause.
    * `capture_wait` is set while installing or widening capture on a table
      it reads waits for a lock another session holds. It clears once that
      session lets go.
    * `capture_failure` is set while capture of a table it reads is broken,
      or the drain halted on it: a schema change paused it (resume it once
      fixed), installing capture keeps failing (it clears once the cause is
      fixed), or, with `kind` `:halt`, a failure no retry gets past reached
      it, so the drain paused it and what depends on it (resume it once
      fixed). A transform an operator paused has none.
    * `held_keys` is set while it holds keys in quarantine: source keys
      whose changes kept failing in its apply, so it leaves them out and
      their target rows stay as they were, whatever its status, `:live`
      included. `Trellis.sample_quarantined/3` lists them, and
      `Trellis.release_key/4` releases one once its cause is fixed.
    * `drain_failure` is set while the drain keeps failing on a page holding
      changes to a table it reads, with nothing charged or paused (see
      `Trellis.DrainFailure`), so its target stops short of that page,
      whatever its status, `:live` included. Every drain pass retries the
      page; fix the cause its error names. A paused or quarantined
      transform has none.

  Every process sees them, whichever one runs the staging worker.
  """

  @typedoc "A transform's lifecycle status."
  @type status ::
          :waiting_to_backfill | :backfilling | :catching_up | :live | :quarantined | :paused

  @type t :: %__MODULE__{
          status: status(),
          backfill_failure: Trellis.BackfillFailure.t() | nil,
          capture_wait: Trellis.CaptureWait.t() | nil,
          capture_failure: Trellis.CaptureFailure.t() | nil,
          held_keys: Trellis.HeldKeys.t() | nil,
          drain_failure: Trellis.DrainFailure.t() | nil
        }

  @enforce_keys [
    :status,
    :backfill_failure,
    :capture_wait,
    :capture_failure,
    :held_keys,
    :drain_failure
  ]
  defstruct @enforce_keys

  @doc false
  def from_native(%{
        status: status,
        backfill_failure: failure,
        capture_wait: wait,
        capture_failure: capture_failure,
        held_keys: held_keys,
        drain_failure: drain_failure
      }) do
    %__MODULE__{
      status: status,
      backfill_failure: failure && Trellis.BackfillFailure.from_native(failure),
      capture_wait: wait && Trellis.CaptureWait.from_native(wait),
      capture_failure: capture_failure && Trellis.CaptureFailure.from_native(capture_failure),
      held_keys: held_keys && Trellis.HeldKeys.from_native(held_keys),
      drain_failure: drain_failure && Trellis.DrainFailure.from_native(drain_failure)
    }
  end
end

defmodule Trellis.DrainFailure do
  @moduledoc """
  A drain page that keeps failing with nothing charged or paused, as
  `Trellis.status/2` and `Trellis.self_check/3` report it: the segment whose
  page fails (`seg_seq`), the qualified source tables it holds changes to
  (`tables`), the latest failure's `error` and its `sqlstate` (`nil` when it
  didn't come from Postgres), when a drain first and last failed on it
  (`since`, `last_seen`), and how many drain passes have (`attempts`).
  """

  @type t :: %__MODULE__{
          seg_seq: integer(),
          tables: [String.t()],
          error: String.t(),
          sqlstate: String.t() | nil,
          since: DateTime.t(),
          last_seen: DateTime.t(),
          attempts: pos_integer()
        }

  @enforce_keys [:seg_seq, :tables, :error, :sqlstate, :since, :last_seen, :attempts]
  defstruct @enforce_keys

  @doc false
  def from_native(%{since_micros: since, last_seen_micros: last_seen} = failure) do
    %__MODULE__{
      seg_seq: failure.seg_seq,
      tables: failure.tables,
      error: failure.error,
      sqlstate: failure.sqlstate,
      since: Trellis.Time.from_micros(since),
      last_seen: Trellis.Time.from_micros(last_seen),
      attempts: failure.attempts
    }
  end
end

defmodule Trellis.HeldKeys do
  @moduledoc """
  The keys a transform holds in quarantine, as `Trellis.status/2` and
  `Trellis.self_check/3` report them: how many (`count`), and when the one
  held longest was last poisoned (`oldest_poisoned_at`).
  """

  @type t :: %__MODULE__{
          count: pos_integer(),
          oldest_poisoned_at: DateTime.t()
        }

  @enforce_keys [:count, :oldest_poisoned_at]
  defstruct @enforce_keys

  @doc false
  def from_native(%{count: count, oldest_poisoned_at_micros: micros}) do
    %__MODULE__{count: count, oldest_poisoned_at: Trellis.Time.from_micros(micros)}
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
  Why capture of `source_table` is broken, or why the drain halted on the
  transform: `kind` is `:capture` or `:halt`, `error` is a sentence naming
  the cause, `columns` the columns it is about (empty when it isn't about a
  column, and for a halt), and `detected_at` when it was first found.
  """

  @typedoc "Whether capture broke or the drain halted."
  @type kind :: :capture | :halt

  @type t :: %__MODULE__{
          kind: kind(),
          source_table: String.t(),
          columns: [String.t()],
          error: String.t(),
          detected_at: DateTime.t()
        }

  @enforce_keys [:kind, :source_table, :columns, :error, :detected_at]
  defstruct @enforce_keys

  @doc false
  def from_native(%{detected_at_micros: micros} = failure) do
    %__MODULE__{
      kind: failure.kind,
      source_table: failure.source_table,
      columns: failure.columns,
      error: failure.error,
      detected_at: Trellis.Time.from_micros(micros)
    }
  end
end

defmodule Trellis.BackfillFailure do
  @moduledoc """
  A build that keeps failing: how many attempts have failed, the latest
  error, and when it is tried again. It retries on its own, except that a
  build failing for a reason no row explains pauses the transform after a
  few attempts (`next_attempt_at` is then when it paused). Fix the cause,
  resume a paused transform, and the next attempt goes through.
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
