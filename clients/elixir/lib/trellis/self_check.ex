defmodule Trellis.SelfCheckJob do
  @moduledoc """
  A background check of a target table, as `Trellis.self_check/3` starts it
  and `Trellis.self_check_job/2` reads it back.

  - `id` is what `Trellis.self_check_job/2` takes.
  - `target` is the audited target's bare table name.
  - `mode` is `:standard` or `:strict`, as the job was started. A second
    `Trellis.self_check/3` of a target whose job is still running returns
    that job, so its mode is the first call's.
  - `state` is `:queued` (no worker has taken it up yet; with no drain
    worker anywhere in the fleet it stays so), `:running`, then `:done`,
    `:failed` (`error` says why) or `:cancelled` (the worker running it shut
    down; `error` says so). Only `:done` carries a report.
  - `rows_compared` counts the distinct keys the pages so far compared; it
    moves while the job is `:running`.
  - `report` is the `Trellis.SelfCheckReport`, once the job is `:done`.
  - `error` says why a `:failed` or `:cancelled` job ended.

  A job ends when its transform is dropped, and `Trellis.self_check_job/2`
  then returns `nil` for it.
  """

  @type state :: :queued | :running | :done | :failed | :cancelled

  @type t :: %__MODULE__{
          id: integer(),
          target: String.t(),
          mode: :standard | :strict,
          state: state(),
          rows_compared: non_neg_integer(),
          report: Trellis.SelfCheckReport.t() | nil,
          error: String.t() | nil
        }

  @enforce_keys [:id, :target, :mode, :state, :rows_compared, :report, :error]
  defstruct @enforce_keys

  @doc "Whether the job has ended, so polling it again changes nothing."
  @spec finished?(t()) :: boolean()
  def finished?(%__MODULE__{state: state}), do: state in [:done, :failed, :cancelled]

  @doc false
  def from_native(job) do
    %__MODULE__{
      id: job.id,
      target: job.target,
      mode: job.mode,
      state: job.state,
      rows_compared: job.rows_compared,
      report: job.report && Trellis.SelfCheckReport.from_native(job.report),
      error: job.error
    }
  end
end

defmodule Trellis.SelfCheckReport do
  @moduledoc """
  What a finished `Trellis.self_check/3` job found auditing the whole of a
  target table against a fresh recompute of its definition from the source.

  - `outcome` is `:converged` (every compared cell, row and column matched),
    `:not_caught_up` (the target didn't catch up within `:timeout_ms`; not a
    verdict on correctness), `:not_live` (the transform isn't `:live`, so
    nothing was awaited or compared; see `status`), or `:diverged` (see
    `divergences`).
  - `status` is the transform's status when `outcome` is `:not_live` (a
    rebuild `Trellis.request_backfill/2` starts reads `:backfilling` from the
    call's return, so poll the status until it is `:live`, then check
    again), and `nil` for every other outcome.
  - `divergences` lists what differed; it is `[]` unless `outcome` is
    `:diverged`.
  - `rows_compared` counts the distinct keys compared.
  - `truncated` is `true` when the job stopped before the end of the
    target's keys: it found 1,000 divergences, or the transform stopped
    being `:live`, stopped catching up or lost its capture partway.
    `rows_compared` says how far it got.
  - `checked_through` is the watermark the outcome holds through, a token
    `Trellis.await_converged/3` takes.
  - `held_keys` is set, whatever the outcome, while the transform holds keys
    in quarantine (see `Trellis.HeldKeys`), read when the job is polled:
    their target rows are ones the audit can't vouch for, and a key with
    held changes keeps the target from catching up, so the outcome is
    `:not_caught_up` until it is released.
  - `drain_failures` lists, whatever the outcome, every page the drain keeps
    failing on with nothing charged or paused (see `Trellis.DrainFailure`),
    oldest first, whichever transforms read its tables, read when the job is
    polled; `[]` when there is none. Each holds back the targets of the
    tables it holds changes to, and with them the convergence the audit
    waits on.
  """

  @typedoc "The verdict of a `Trellis.self_check/3` job."
  @type outcome :: :converged | :not_caught_up | :not_live | :diverged

  @type t :: %__MODULE__{
          target: String.t(),
          outcome: outcome(),
          status: Trellis.Status.status() | nil,
          divergences: [Trellis.Divergence.t()],
          rows_compared: non_neg_integer(),
          truncated: boolean(),
          checked_through: Trellis.watermark(),
          held_keys: Trellis.HeldKeys.t() | nil,
          drain_failures: [Trellis.DrainFailure.t()]
        }

  @enforce_keys [
    :target,
    :outcome,
    :status,
    :divergences,
    :rows_compared,
    :truncated,
    :checked_through,
    :held_keys,
    :drain_failures
  ]
  defstruct @enforce_keys

  @doc false
  def from_native(%{divergences: divergences} = report) do
    %__MODULE__{
      target: report.target,
      outcome: report.outcome,
      status: report.status,
      divergences: Enum.map(divergences, &Trellis.Divergence.from_native/1),
      rows_compared: report.rows_compared,
      truncated: report.truncated,
      checked_through: report.checked_through,
      held_keys: report.held_keys && Trellis.HeldKeys.from_native(report.held_keys),
      drain_failures: Enum.map(report.drain_failures, &Trellis.DrainFailure.from_native/1)
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
  - `:capture`: a table the target is computed from isn't being captured as
    Trellis installed it (a capture trigger missing, disabled or calling the
    wrong function, a capture function missing or with the wrong owner, the
    Trellis role missing a privilege, or the table joined a partition or
    inheritance hierarchy). `detail` says what, and `table` names the table
    (`nil` for a missing privilege). `Trellis.self_check/3` reports these
    before, and instead of, comparing any rows.

  Fields a kind doesn't carry are `nil`.
  """

  @typedoc "What kind of divergence this is."
  @type kind ::
          :cell | :missing_row | :extra_row | :missing_column | :extra_column | :capture

  @type t :: %__MODULE__{
          kind: kind(),
          key: String.t() | nil,
          column: String.t() | nil,
          persisted: String.t() | nil,
          recomputed: String.t() | nil,
          table: String.t() | nil,
          detail: String.t() | nil
        }

  @enforce_keys [:kind, :key, :column, :persisted, :recomputed, :table, :detail]
  defstruct @enforce_keys

  @doc false
  def from_native(map) when is_map(map), do: struct!(__MODULE__, map)
end
