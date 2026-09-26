defmodule Trellis.SelfCheckReport do
  @moduledoc """
  What `Trellis.self_check/3` found auditing one page of a target table
  against a fresh recompute of its definition from the source.

  - `outcome` is `:converged` (every compared cell, row and column matched),
    `:not_caught_up` (the target didn't catch up within `:timeout_ms`; not a
    verdict on correctness, and nothing was compared), or `:diverged` (see
    `divergences`).
  - `divergences` lists what differed; it is `[]` unless `outcome` is
    `:diverged`.
  - `rows_compared` counts the distinct keys compared; `0` when the target
    didn't catch up.
  - `next_after` is the cursor to pass as the next call's `:after` to audit
    the following page. `nil` means this page reached the end of the target.
    A page that fills `:limit` exactly still returns a cursor, so a sweep
    can end on a page that compares nothing.
  - `checked_through` is the watermark the outcome holds through, a token
    `Trellis.await_converged/3` takes.
  """

  @typedoc "The verdict of one `Trellis.self_check/3` page."
  @type outcome :: :converged | :not_caught_up | :diverged

  @typedoc "An opaque `Trellis.self_check/3` cursor."
  @opaque cursor :: String.t()

  @type t :: %__MODULE__{
          target: String.t(),
          outcome: outcome(),
          divergences: [Trellis.Divergence.t()],
          rows_compared: non_neg_integer(),
          next_after: cursor() | nil,
          checked_through: Trellis.watermark()
        }

  @enforce_keys [:target, :outcome, :divergences, :rows_compared, :next_after, :checked_through]
  defstruct @enforce_keys

  @doc false
  def from_native(%{divergences: divergences} = report) do
    %__MODULE__{
      target: report.target,
      outcome: report.outcome,
      divergences: Enum.map(divergences, &Trellis.Divergence.from_native/1),
      rows_compared: report.rows_compared,
      next_after: report.next_after,
      checked_through: report.checked_through
    }
  end
end

defmodule Trellis.Divergence do
  @moduledoc """
  One thing `Trellis.self_check/3` found out of step, by `kind`:

  - `:cell`: the row exists on both sides but `column` differs at `key`.
    `persisted` is the target table's value and `recomputed` the fresh
    recompute's, both as Postgres renders them as text (`nil` for `NULL`).
  - `:missing_row`: the recompute produced `key`, but the target has no row
    for it.
  - `:extra_row`: the target has a row for `key` that the recompute didn't
    produce.
  - `:missing_column`: the definition expects `column`, but the target table
    doesn't have it.
  - `:extra_column`: the target table has `column`, but the definition
    doesn't expect it.

  Fields a kind doesn't carry are `nil`.
  """

  @typedoc "What kind of divergence this is."
  @type kind :: :cell | :missing_row | :extra_row | :missing_column | :extra_column

  @type t :: %__MODULE__{
          kind: kind(),
          key: String.t() | nil,
          column: String.t() | nil,
          persisted: String.t() | nil,
          recomputed: String.t() | nil
        }

  @enforce_keys [:kind, :key, :column, :persisted, :recomputed]
  defstruct @enforce_keys

  @doc false
  def from_native(map) when is_map(map), do: struct!(__MODULE__, map)
end
