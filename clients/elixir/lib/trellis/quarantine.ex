defmodule Trellis.QuarantineEntry do
  @moduledoc """
  One target's quarantine state, as `Trellis.quarantined/1` lists it and
  `Trellis.quarantine_status/2` reports it.

  `target` is an address: a transform's bare target table name
  (`"order_totals"`) for the whole transform, or `"order_totals.total"` for
  one of its columns. Pass it straight back to `Trellis.quarantine_status/2`,
  `Trellis.sample_quarantined/3`, or a `RESUME TRANSFORM` statement.

  `paused_at` and `last_error` are set only for a paused column: when its
  pause tripped, and its most recent failure (`nil` for a pause that
  cascaded from an upstream column rather than failing on its own).
  """

  @typedoc """
  A target's state. A column is `:live` or `:paused`; a whole transform
  reports its lifecycle status (see `t:Trellis.Status.status/0`).
  """
  @type state ::
          :waiting_to_backfill | :backfilling | :catching_up | :live | :quarantined | :paused

  @type t :: %__MODULE__{
          target: String.t(),
          state: state(),
          paused_at: DateTime.t() | nil,
          last_error: String.t() | nil
        }

  @enforce_keys [:target, :state, :paused_at, :last_error]
  defstruct @enforce_keys

  @doc false
  def from_native(%{paused_at_micros: micros} = entry) do
    %__MODULE__{
      target: entry.target,
      state: entry.state,
      paused_at: micros && Trellis.Time.from_micros(micros),
      last_error: entry.last_error
    }
  end
end

defmodule Trellis.PoisonEntry do
  @moduledoc """
  One source row the apply path gave up on, as `Trellis.poisoned_since/2`
  lists it: its fully-qualified `src_table`, its `key` as Trellis renders it,
  the error that poisoned it, and when.
  """

  @type t :: %__MODULE__{
          src_table: String.t(),
          key: String.t(),
          last_error: String.t(),
          poisoned_at: DateTime.t()
        }

  @enforce_keys [:src_table, :key, :last_error, :poisoned_at]
  defstruct @enforce_keys

  @doc false
  def from_native(%{poisoned_at_micros: micros} = entry) do
    %__MODULE__{
      src_table: entry.src_table,
      key: entry.key,
      last_error: entry.last_error,
      poisoned_at: Trellis.Time.from_micros(micros)
    }
  end
end

defmodule Trellis.PoisonSample do
  @moduledoc """
  One quarantined source row, as `Trellis.sample_quarantined/3` pages them:
  its fully-qualified `src_table`, its `key` as Trellis renders it, and the
  error it failed with.
  """

  @type t :: %__MODULE__{src_table: String.t(), key: String.t(), error_message: String.t()}

  @enforce_keys [:src_table, :key, :error_message]
  defstruct @enforce_keys

  @doc false
  def from_native(map) when is_map(map), do: struct!(__MODULE__, map)
end

defmodule Trellis.SamplePage do
  @moduledoc """
  One page of `Trellis.sample_quarantined/3`, ordered by `(src_table, key)`.

  `next_cursor` is opaque: pass it back as the `:after` option for the next
  page, and don't read or build one. The last page is the one with fewer
  rows than the limit. An empty page hands back the cursor it was asked for,
  so polling from where you got to never falls back to the first page;
  `next_cursor` is `nil` only for an empty first page.
  """

  @typedoc "An opaque `sample_quarantined/3` cursor."
  @opaque cursor :: String.t()

  @type t :: %__MODULE__{samples: [Trellis.PoisonSample.t()], next_cursor: cursor() | nil}

  @enforce_keys [:samples, :next_cursor]
  defstruct @enforce_keys

  @doc false
  def from_native(%{samples: samples, next_cursor: cursor}) do
    %__MODULE__{
      samples: Enum.map(samples, &Trellis.PoisonSample.from_native/1),
      next_cursor: cursor
    }
  end
end
