defmodule Trellis do
  @moduledoc """
  Embedded Trellis for Elixir: define streaming transforms over Postgres
  tables from the BEAM, through the `trellis` Rust crate itself
  (`docs/decisions/0010-embeddable-clients.md`).

  This is the vertical slice: `connect/1`, `migrate/1`, `define/2`,
  `status/2` and `shutdown/1`, enough to take a transform from nothing to
  `:live`.

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

  ## One handle per OS process

  A handle owns a connection pool and a small Rust runtime, and optionally
  the background workers. Connect once, at boot, and share the handle; don't
  connect per request. The handle is shut down when `shutdown/1` is called,
  or, as a backstop, when it is garbage collected.
  """

  alias Trellis.{Definition, Error, Native, Status}

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
  anything is applied.

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

  defp invalid(message), do: {:error, Error.validation(message)}

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
