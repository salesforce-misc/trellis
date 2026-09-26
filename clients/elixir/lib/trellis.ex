defmodule Trellis do
  @moduledoc """
  Embedded Trellis for Elixir: define streaming transforms over Postgres
  tables from the BEAM, through the `trellis` Rust crate itself
  (`docs/decisions/0010-embeddable-clients.md`).

  The surface mirrors the Rust crate's `BlockingTrellis`:

  - **Lifecycle:** `connect/1`, `migrate/1`, `shutdown/1`.
  - **Every statement form:** `apply/2` runs one statement of Trellis's
    grammar (`TRANSFORM`, `RELATIONSHIP`, `PAUSE TRANSFORM`,
    `RESUME TRANSFORM`, `DROP TRANSFORM`, `DROP RELATIONSHIP`,
    `ALTER TRANSFORM`) and reports what it did as a `t:Trellis.Applied.t/0`.
    `define/2` is the one guarded convenience: an `apply/2` that refuses
    anything but a `TRANSFORM` statement before applying it.
  - **Reads:** `status/2`, `definitions/1`, `relationships/1`.
  - **Quarantine:** `quarantined/1`, `quarantine_status/2`,
    `sample_quarantined/3`, `poisoned_since/2`. Pausing and resuming are
    statements: `apply(trellis, "RESUME TRANSFORM order_totals.total")`.
  - **Operations:** `request_backfill/2`, `has_live_drain_workers/1`,
    `has_live_staging_worker/1`, and the read-your-writes pair
    `watermark_token/1` and `await_converged/3`.

      # A deploy's migration step: the defaults run nothing in the background.
      {:ok, migrator} = Trellis.connect(url: "postgres://localhost/app")
      :ok = Trellis.migrate(migrator)
      :ok = Trellis.shutdown(migrator)

      # The app, once migrated. The staging worker reads the catalog as it
      # starts, so a `staging: true` handle can't connect before `migrate/1`.
      {:ok, trellis} = Trellis.connect(url: "postgres://localhost/app", staging: true, drain_threads: 2)
      {:ok, definition} = Trellis.define(trellis, "TRANSFORM widget_prices FROM widgets SELECT price AS price")
      {:ok, %Trellis.Status{status: :live}} = Trellis.status(trellis, "widget_prices")
      :ok = Trellis.shutdown(trellis)

  Every function blocks the calling process on a database round trip, on a
  dirty IO scheduler, so no normal scheduler is held up. Non-bang functions
  return `{:ok, value}` (or `:ok`) and `{:error, %Trellis.Error{}}`; the bang
  variants return the value or raise the `Trellis.Error`.

  ## Conventions

  - Times are `DateTime`s in UTC, at microsecond precision.
  - A quarantine target is an address string: a transform's bare target
    table name (`"order_totals"`) or `"transform.column"`
    (`"order_totals.total"`), exactly as `quarantined/1` reports it.
  - `sample_quarantined/3`'s cursor and `watermark_token/1`'s token are
    opaque: pass back what the previous call returned.
  - Every atom in a result comes from a closed set allocated when the NIF
    loads; none is ever built from a string the database returned.

  ## One handle per OS process

  A handle owns a connection pool and a small Rust runtime, and optionally
  the background workers. Connect once, at boot, and share the handle; don't
  connect per request. The handle is shut down when `shutdown/1` is called,
  or, as a backstop, when it is garbage collected.
  """

  # `apply/2` is this module's own; `Kernel.apply/2` is never called here.
  import Kernel, except: [apply: 2]

  alias Trellis.{
    Applied,
    Definition,
    DefinitionSummary,
    Error,
    Native,
    PoisonEntry,
    QuarantineEntry,
    RelationshipSummary,
    SamplePage,
    Status
  }

  @enforce_keys [:ref]
  defstruct [:ref]

  @opaque t :: %__MODULE__{ref: reference()}

  @typedoc """
  Options for `connect/1`:

  - `:url` (required): the database, as a `postgres://` URL or a libpq
    `key=value` string. Nothing is read from the environment.
  - `:schema`: the schema Trellis keeps its own tables in. Default `"trellis"`.
  - `:target_schema`: the schema a bare target table name is created in.
    Default `"public"`.
  - `:staging`: whether this connection runs the staging worker (change
    capture from the source tables). Exactly one connection in a fleet should.
    Default `false`.
  - `:drain_threads`: how many threads apply staged changes to the targets.
    Default `0`.
  - `:worker_threads`: the Rust runtime's worker threads. Default `2`; the
    work is IO, so a small count is enough, and the BEAM has already sized
    its own schedulers to the cores.

  The defaults (`staging: false, drain_threads: 0`) run nothing in the
  background, which is right for a web process or a migration run. Some
  connection in the fleet has to set both, or no definition ever leaves
  `:waiting_to_backfill`.
  """
  @type option ::
          {:url, String.t()}
          | {:schema, String.t()}
          | {:target_schema, String.t()}
          | {:staging, boolean()}
          | {:drain_threads, non_neg_integer()}
          | {:worker_threads, pos_integer()}

  # The largest `limit` and `timeout_ms` the engine takes (an `i64` and a
  # `u64` millisecond count); anything larger is refused as `:validation`
  # rather than failing to decode in the NIF.
  @max_limit 9_223_372_036_854_775_807
  @max_timeout_ms 18_446_744_073_709_551_615

  @defaults %{
    schema: "trellis",
    target_schema: "public",
    staging: false,
    drain_threads: 0,
    worker_threads: 2
  }

  @doc """
  Connects to the database and starts whatever background work `options`
  asks for. See `t:option/0`.
  """
  @spec connect([option()] | %{optional(atom()) => term()}) :: {:ok, t()} | {:error, Error.t()}
  def connect(options) when is_list(options) or is_map(options) do
    with {:ok, options} <- validate_options(Map.new(options)),
         {:ok, ref} <- native(Native.connect(options)) do
      {:ok, %__MODULE__{ref: ref}}
    end
  end

  @doc "Like `connect/1`, but raises `Trellis.Error`."
  @spec connect!([option()] | %{optional(atom()) => term()}) :: t()
  def connect!(options), do: bang(connect(options))

  @doc """
  Creates or upgrades Trellis's own tables in the configured schema. Safe to
  run on every boot.

  Run it on a handle with the default options, before connecting one that
  runs background work: the staging worker reads Trellis's tables as it
  starts, so a `staging: true` connect to an unmigrated schema fails with a
  `:not_found` error.
  """
  @spec migrate(t()) :: :ok | {:error, Error.t()}
  def migrate(%__MODULE__{ref: ref}), do: unit(Native.migrate(ref))

  @doc "Like `migrate/1`, but raises `Trellis.Error`."
  @spec migrate!(t()) :: :ok
  def migrate!(trellis), do: bang(migrate(trellis))

  @doc """
  Registers a `TRANSFORM` statement and creates its target table.

  Returns once the definition is registered and its backfill queued, with the
  definition at `:waiting_to_backfill`; poll `status/2` for `:live`.

  Only `TRANSFORM` statements belong here. Any other statement form (`DROP`,
  `PAUSE`, `RELATIONSHIP`, ...) is refused with a `:validation` error before
  anything is applied, and a statement that doesn't parse is a `:parse` error.
  `apply/2` takes every form.

  Trellis runs this on its own connections, not in the caller's transaction:
  if an enclosing Ecto migration rolls back, the definition stays.
  """
  @spec define(t(), String.t()) :: {:ok, Definition.t()} | {:error, Error.t()}
  def define(%__MODULE__{ref: ref}, text) when is_binary(text) do
    with {:ok, definition} <- native(Native.define(ref, text)) do
      {:ok, Definition.from_native(definition)}
    end
  end

  @doc "Like `define/2`, but raises `Trellis.Error`."
  @spec define!(t(), String.t()) :: Definition.t()
  def define!(trellis, text), do: bang(define(trellis, text))

  @doc """
  The status of the transform that writes `target_table`, or `nil` if none
  does.
  """
  @spec status(t(), String.t()) :: {:ok, Status.t() | nil} | {:error, Error.t()}
  def status(%__MODULE__{ref: ref}, target_table) when is_binary(target_table) do
    case native(Native.status(ref, target_table)) do
      {:ok, nil} -> {:ok, nil}
      {:ok, status} -> {:ok, Status.from_native(status)}
      {:error, _} = error -> error
    end
  end

  @doc "Like `status/2`, but raises `Trellis.Error`."
  @spec status!(t(), String.t()) :: Status.t() | nil
  def status!(trellis, target_table), do: bang(status(trellis, target_table))

  @doc """
  Runs one statement of Trellis's grammar, whatever its form, and reports
  what it did (see `t:Trellis.Applied.t/0`).

      {:ok, {:transform_defined, definition}} =
        Trellis.apply(trellis, "TRANSFORM order_totals FROM orders SELECT price + tax AS total")

      {:ok, {:resumed, ["order_totals.total"]}} =
        Trellis.apply(trellis, "RESUME TRANSFORM order_totals.total")

  A statement that doesn't parse is a `:parse` error, and nothing is applied.
  Like `define/2`, this runs on Trellis's own connections, not in the
  caller's transaction.

  `{:ok, :unknown}` is a success like any other `{:ok, _}`: the statement
  took effect, and only its outcome is newer than this binding can describe.
  Don't retry it; read the result back with `status/2`, `definitions/1` or
  `relationships/1` if you need it.
  """
  @spec apply(t(), String.t()) :: {:ok, Applied.t()} | {:error, Error.t()}
  def apply(%__MODULE__{ref: ref}, text) when is_binary(text) do
    with {:ok, applied} <- native(Native.apply(ref, text)) do
      {:ok, Applied.from_native(applied)}
    end
  end

  @doc "Like `apply/2`, but raises `Trellis.Error`."
  @spec apply!(t(), String.t()) :: Applied.t()
  def apply!(trellis, text), do: bang(__MODULE__.apply(trellis, text))

  @doc "Every registered transform definition, oldest first."
  @spec definitions(t()) :: {:ok, [DefinitionSummary.t()]} | {:error, Error.t()}
  def definitions(%__MODULE__{ref: ref}) do
    list(Native.definitions(ref), &DefinitionSummary.from_native/1)
  end

  @doc "Like `definitions/1`, but raises `Trellis.Error`."
  @spec definitions!(t()) :: [DefinitionSummary.t()]
  def definitions!(trellis), do: bang(definitions(trellis))

  @doc "Every registered relationship, oldest first."
  @spec relationships(t()) :: {:ok, [RelationshipSummary.t()]} | {:error, Error.t()}
  def relationships(%__MODULE__{ref: ref}) do
    list(Native.relationships(ref), &RelationshipSummary.from_native/1)
  end

  @doc "Like `relationships/1`, but raises `Trellis.Error`."
  @spec relationships!(t()) :: [RelationshipSummary.t()]
  def relationships!(trellis), do: bang(relationships(trellis))

  @doc """
  Asks the staging worker to re-read `source_table` (a bare table name,
  resolved like one in a statement) for every definition that reads it, and
  returns once that re-read is queued.

  Each `:live` reader reports `:catching_up` until the re-read has
  re-derived every row the table still has and deleted any target row it no
  longer backs. A newly defined transform doesn't need this: its backfill is
  queued for it. Only a table Trellis already captures can be re-read; any
  other is refused.
  """
  @spec request_backfill(t(), String.t()) :: :ok | {:error, Error.t()}
  def request_backfill(%__MODULE__{ref: ref}, source_table) when is_binary(source_table) do
    unit(Native.request_backfill(ref, source_table))
  end

  @doc "Like `request_backfill/2`, but raises `Trellis.Error`."
  @spec request_backfill!(t(), String.t()) :: :ok
  def request_backfill!(trellis, source_table), do: bang(request_backfill(trellis, source_table))

  @doc """
  Every source row the apply path poisoned (gave up on) after `since`,
  oldest first. Poll it with the last entry's `poisoned_at` so a
  whole-table failure doesn't sit unnoticed.
  """
  @spec poisoned_since(t(), DateTime.t()) :: {:ok, [PoisonEntry.t()]} | {:error, Error.t()}
  def poisoned_since(%__MODULE__{ref: ref}, %DateTime{} = since) do
    list(Native.poisoned_since(ref, Trellis.Time.to_micros(since)), &PoisonEntry.from_native/1)
  end

  @doc "Like `poisoned_since/2`, but raises `Trellis.Error`."
  @spec poisoned_since!(t(), DateTime.t()) :: [PoisonEntry.t()]
  def poisoned_since!(trellis, since), do: bang(poisoned_since(trellis, since))

  @doc """
  Every quarantined transform and paused column, across every transform.
  Cheap enough for a dashboard or health check to poll.
  """
  @spec quarantined(t()) :: {:ok, [QuarantineEntry.t()]} | {:error, Error.t()}
  def quarantined(%__MODULE__{ref: ref}) do
    list(Native.quarantined(ref), &QuarantineEntry.from_native/1)
  end

  @doc "Like `quarantined/1`, but raises `Trellis.Error`."
  @spec quarantined!(t()) :: [QuarantineEntry.t()]
  def quarantined!(trellis), do: bang(quarantined(trellis))

  @doc """
  The state of one target: a transform (`"order_totals"`) or one of its
  columns (`"order_totals.total"`). A column that isn't paused is `:live`.
  """
  @spec quarantine_status(t(), String.t()) :: {:ok, QuarantineEntry.t()} | {:error, Error.t()}
  def quarantine_status(%__MODULE__{ref: ref}, target) when is_binary(target) do
    with {:ok, entry} <- native(Native.quarantine_status(ref, target)) do
      {:ok, QuarantineEntry.from_native(entry)}
    end
  end

  @doc "Like `quarantine_status/2`, but raises `Trellis.Error`."
  @spec quarantine_status!(t(), String.t()) :: QuarantineEntry.t()
  def quarantine_status!(trellis, target), do: bang(quarantine_status(trellis, target))

  @typedoc """
  Options for `sample_quarantined/3`:

  - `:limit`: the most rows to return. Default `100`.
  - `:after`: the `next_cursor` of the previous page. Default `nil`, the
    first page.
  """
  @type sample_option :: {:limit, pos_integer()} | {:after, SamplePage.cursor() | nil}

  @doc """
  One page of the rows quarantined under `target`, to diagnose a
  quarantine's cause: for a column (`"order_totals.total"`), the rows that
  failed evaluating it; for a whole transform (`"order_totals"`), the keys
  poisoned from its source table.

      {:ok, page} = Trellis.sample_quarantined(trellis, "order_totals.total", limit: 50)
      {:ok, next} = Trellis.sample_quarantined(trellis, "order_totals.total", limit: 50, after: page.next_cursor)
  """
  @spec sample_quarantined(t(), String.t(), [sample_option()]) ::
          {:ok, SamplePage.t()} | {:error, Error.t()}
  def sample_quarantined(%__MODULE__{ref: ref}, target, options \\ [])
      when is_binary(target) and is_list(options) do
    with {:ok, limit, cursor} <- sample_options(options),
         {:ok, page} <- native(Native.sample_quarantined(ref, target, cursor, limit)) do
      {:ok, SamplePage.from_native(page)}
    end
  end

  @doc "Like `sample_quarantined/3`, but raises `Trellis.Error`."
  @spec sample_quarantined!(t(), String.t(), [sample_option()]) :: SamplePage.t()
  def sample_quarantined!(trellis, target, options \\ []),
    do: bang(sample_quarantined(trellis, target, options))

  @doc """
  Whether at least one drain worker is alive anywhere in the fleet. With
  none, nothing reaches a target table: poll this from a health check.
  """
  @spec has_live_drain_workers(t()) :: {:ok, boolean()} | {:error, Error.t()}
  def has_live_drain_workers(%__MODULE__{ref: ref}),
    do: native(Native.has_live_drain_workers(ref))

  @doc "Like `has_live_drain_workers/1`, but raises `Trellis.Error`."
  @spec has_live_drain_workers!(t()) :: boolean()
  def has_live_drain_workers!(trellis), do: bang(has_live_drain_workers(trellis))

  @doc """
  Whether the staging worker (change capture) is alive anywhere in the
  fleet. The other half of the health check.
  """
  @spec has_live_staging_worker(t()) :: {:ok, boolean()} | {:error, Error.t()}
  def has_live_staging_worker(%__MODULE__{ref: ref}),
    do: native(Native.has_live_staging_worker(ref))

  @doc "Like `has_live_staging_worker/1`, but raises `Trellis.Error`."
  @spec has_live_staging_worker!(t()) :: boolean()
  def has_live_staging_worker!(trellis), do: bang(has_live_staging_worker(trellis))

  @typedoc "An opaque `watermark_token/1` token."
  @opaque watermark :: String.t()

  @doc """
  A read-your-writes token covering every write committed before this call.
  Take it after a source-table write commits, then pass it to
  `await_converged/3` to wait for that write to reach its targets.
  """
  @spec watermark_token(t()) :: {:ok, watermark()} | {:error, Error.t()}
  def watermark_token(%__MODULE__{ref: ref}), do: native(Native.watermark_token(ref))

  @doc "Like `watermark_token/1`, but raises `Trellis.Error`."
  @spec watermark_token!(t()) :: watermark()
  def watermark_token!(trellis), do: bang(watermark_token(trellis))

  @doc """
  Waits until every change committed at or before `token` has reached its
  target tables, or `timeout_ms` passes (an `:internal` error naming the
  timeout).

  It waits for captured changes only: a transform that isn't `:live` yet
  may still be missing rows when this returns (see `status/2`).

  The handle runs one call at a time, so every other call on it, from any
  process, waits behind this one for up to `timeout_ms`. Size it
  accordingly.
  """
  @spec await_converged(t(), watermark(), non_neg_integer()) :: :ok | {:error, Error.t()}
  def await_converged(%__MODULE__{ref: ref}, token, timeout_ms)
      when is_binary(token) and is_integer(timeout_ms) do
    if timeout_ms in 0..@max_timeout_ms do
      unit(Native.await_converged(ref, token, timeout_ms))
    else
      invalid(":timeout_ms must be a non-negative integer, got: #{inspect(timeout_ms)}")
    end
  end

  @doc "Like `await_converged/3`, but raises `Trellis.Error`."
  @spec await_converged!(t(), watermark(), non_neg_integer()) :: :ok
  def await_converged!(trellis, token, timeout_ms),
    do: bang(await_converged(trellis, token, timeout_ms))

  @doc """
  Stops the handle's background work and waits for its threads to exit. Any
  later call on the handle returns a `:validation` error; shutting down again
  is `:ok`.
  """
  @spec shutdown(t()) :: :ok | {:error, Error.t()}
  def shutdown(%__MODULE__{ref: ref}), do: unit(Native.shutdown(ref))

  @doc "Like `shutdown/1`, but raises `Trellis.Error`."
  @spec shutdown!(t()) :: :ok
  def shutdown!(trellis), do: bang(shutdown(trellis))

  defp validate_options(options) do
    with :ok <- check_keys(options),
         {:ok, url} <- fetch_url(options) do
      options = Map.merge(@defaults, Map.put(options, :url, url))

      cond do
        not is_binary(options.schema) ->
          invalid(":schema must be a string, got: #{inspect(options.schema)}")

        not is_binary(options.target_schema) ->
          invalid(":target_schema must be a string, got: #{inspect(options.target_schema)}")

        not is_boolean(options.staging) ->
          invalid(":staging must be a boolean, got: #{inspect(options.staging)}")

        not (is_integer(options.drain_threads) and options.drain_threads >= 0) ->
          invalid(
            ":drain_threads must be a non-negative integer, got: #{inspect(options.drain_threads)}"
          )

        not (is_integer(options.worker_threads) and options.worker_threads >= 1) ->
          invalid(
            ":worker_threads must be a positive integer, got: #{inspect(options.worker_threads)}"
          )

        true ->
          {:ok, options}
      end
    end
  end

  defp check_keys(options) do
    known = [:url | Map.keys(@defaults)]

    case Map.keys(options) -- known do
      [] -> :ok
      unknown -> invalid("unknown options #{inspect(unknown)}; known: #{inspect(known)}")
    end
  end

  defp fetch_url(options) do
    case Map.fetch(options, :url) do
      {:ok, url} when is_binary(url) and url != "" -> {:ok, url}
      {:ok, url} -> invalid(":url must be a non-empty string, got: #{inspect(url)}")
      :error -> invalid(":url is required")
    end
  end

  defp sample_options(options) do
    case Keyword.split(options, [:limit, :after]) do
      {_, [_ | _] = unknown} ->
        invalid("unknown options #{inspect(Keyword.keys(unknown))}; known: [:limit, :after]")

      {known, []} ->
        limit = Keyword.get(known, :limit, 100)
        cursor = Keyword.get(known, :after)

        cond do
          not (is_integer(limit) and limit in 1..@max_limit) ->
            invalid(":limit must be a positive integer, got: #{inspect(limit)}")

          not (is_nil(cursor) or is_binary(cursor)) ->
            invalid(":after must be a cursor from a previous page, got: #{inspect(cursor)}")

          true ->
            {:ok, limit, cursor}
        end
    end
  end

  defp invalid(message), do: {:error, Error.validation(message)}

  defp list(reply, from_native) do
    with {:ok, items} <- native(reply) do
      {:ok, Enum.map(items, from_native)}
    end
  end

  defp native({:ok, value}), do: {:ok, value}
  defp native({:error, error}), do: {:error, Error.from_native(error)}

  defp unit(reply) do
    case native(reply) do
      {:ok, :ok} -> :ok
      {:error, _} = error -> error
    end
  end

  defp bang(:ok), do: :ok
  defp bang({:ok, value}), do: value
  defp bang({:error, %Error{} = error}), do: raise(error)
end
