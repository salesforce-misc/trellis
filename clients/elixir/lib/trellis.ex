defmodule Trellis do
  @moduledoc """
  Embedded Trellis for Elixir: define streaming transforms over Postgres
  tables from the BEAM, through the `trellis` Rust crate itself
  (`docs/decisions/0010-embeddable-clients.md`).

  The surface mirrors the Rust crate's `BlockingTrellis`:

  - **Lifecycle:** `connect/1`, `migrate/1`, `config/1`, `shutdown/1`, and
    `start_link/1` for a handle a supervisor owns.
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
    `has_live_staging_worker/1`, the read-your-writes pair
    `watermark_token/1` and `await_converged/3`, and the audit
    `self_check/3`, a background job that `self_check_job/2` reads back.
  - **Observability:** `Trellis.Metrics.render_prometheus/0` for a scrape
    route the host serves, and `Trellis.LogBridge`, which forwards the
    engine's log lines to `Logger` from the moment `:trellis_pg` starts.

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

  Every function here blocks the calling process on a database round trip,
  on a dirty IO scheduler, so no normal scheduler is held up. (Called through
  a supervised Trellis's name, only its owning process runs on one, and the
  caller waits for its reply.) Non-bang functions
  return `{:ok, value}` (or `:ok`) and `{:error, %Trellis.Error{}}`; the bang
  variants return the value or raise the `Trellis.Error`.

  ## Conventions

  - Times are `DateTime`s in UTC, at microsecond precision.
  - A quarantine target is an address string: a transform's bare target
    table name (`"order_totals"`) or `"transform.column"`
    (`"order_totals.total"`), exactly as `quarantined/1` reports it.
  - `sample_quarantined/3`'s cursor and `watermark_token/1`'s token are opaque: pass back what the previous call
    returned.
  - Every atom in a result comes from a closed set allocated when the NIF
    loads; none is ever built from a string the database returned.

  ## One handle per instance

  An instance is one catalog schema in one database (the `:schema` option). A
  handle belongs to one instance, and a VM holds a handle for each instance it
  uses: against different databases, or against different catalog schemas in
  one database. Connect each handle once, at boot, and share it; don't connect
  per request. The handle is shut down when `shutdown/1` is called, or, as a
  backstop, when it is garbage collected.

  Each handle pays for itself. It owns a connection pool of up to 20
  connections and a small Rust runtime (`:worker_threads` threads). A handle
  with `staging: true` or `drain_threads` above `0` also owns a second runtime
  and pool of the same size, plus connections outside both pools: one for the
  staging worker, and for each drain thread a `LISTEN` connection and up to two
  more while it drains.
  Handles share no budget, so size all of them together against the server's
  `max_connections`. Each instance has its own staging worker, so set
  `staging: true` in one process of the fleet per instance.

  In an application, let a supervisor own each handle: `{Trellis, options}` in
  the supervision tree starts a process that connects the handle as it starts
  and shuts it down when the supervisor stops it (see `start_link/1`). Every
  function here that takes a handle takes that process's name too, and the
  names tell the handles apart:

      # config/runtime.exs
      config :my_app, MyApp.Trellis,
        name: MyApp.Trellis,
        url: System.fetch_env!("DATABASE_URL")

      # lib/my_app/application.ex
      children = [
        MyApp.Repo,
        {Trellis, Application.fetch_env!(:my_app, MyApp.Trellis)},
        MyAppWeb.Endpoint
      ]

      # Anywhere in the app.
      {:ok, status} = Trellis.status(MyApp.Trellis, "widget_prices")

  A second instance in the same database is a second child with its own `:name`
  and `:schema`:

      {Trellis, name: MyApp.Billing, url: url, schema: "billing"}

  `Trellis.Metrics.render_prometheus/0` covers every handle in the VM at once,
  each instance's series labeled `trellis_instance`.

  For the migrations that define transforms, see `Trellis.Migration`.
  """

  # `apply/2` is this module's own; `Kernel.apply/2` is never called here.
  import Kernel, except: [apply: 2]

  alias Trellis.{
    Applied,
    Config,
    Definition,
    DefinitionSummary,
    Error,
    Native,
    PoisonEntry,
    QuarantineEntry,
    RelationshipSummary,
    SamplePage,
    SelfCheckJob,
    Status
  }

  @enforce_keys [:ref]
  defstruct [:ref]

  @opaque t :: %__MODULE__{ref: reference()}

  @typedoc """
  What every function but `connect/1` takes: a handle `connect/1` returned,
  or the name or pid of a supervised Trellis (`start_link/1`).
  """
  @type trellis :: t() | GenServer.server()

  # A handle, or something `GenServer.call/3` can address. `nil` and the
  # booleans are atoms but never a server's name.
  defguardp is_trellis(trellis)
            when is_struct(trellis, __MODULE__) or is_pid(trellis) or is_tuple(trellis) or
                   (is_atom(trellis) and trellis not in [nil, true, false])

  @typedoc """
  Options for `connect/1`:

  - `:url` (required): the database, as a `postgres://` URL or a libpq
    `key=value` string. Nothing is read from the environment.
  - `:schema`: the schema Trellis keeps its own tables in. Default `"trellis"`.
    Not `"public"`, not `:target_schema`, and not another instance's catalog
    or target schema; `migrate/1` refuses those.
  - `:target_schema`: the schema a bare target table name is created in.
    Default `"public"`.
  - `:staging`: whether this connection runs the staging worker, which
    installs change capture on the source tables and starts each new
    transform's backfill. Exactly one connection of each instance in a fleet
    should. Default `false`.
  - `:drain_threads`: how many threads apply staged changes to the targets.
    Default `0`.
  - `:worker_threads`: the worker threads of each Rust runtime a handle owns
    (its calls', and its background client's). Default `2`; the
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
  @min_job_id -9_223_372_036_854_775_808
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

  @typedoc """
  Options for `start_link/1`: every `t:option/0`, plus `:name`, the name to
  register the process under (see `GenServer.start_link/3`). Without one,
  call it by its pid.
  """
  @type start_option :: option() | {:name, GenServer.name()}

  @doc """
  Starts a process that owns a handle: it connects with `options` (see
  `t:start_option/0`) as it starts, and shuts the handle down as its
  supervisor stops it. Put it in a supervision tree as `{Trellis, options}`
  rather than calling this directly.

  Returns `{:error, %Trellis.Error{}}` if the connect fails, so the
  supervisor's start fails with the reason. A second `staging: true`
  process for the same instance fails that way (`:conflict`) while the first
  is alive: in a rolling deploy, stop the old one before the new one starts.

  Every call made through the process's name runs in the process, one at a
  time, as it would on the handle, which runs one call at a time anyway.
  Only the owning process waits on a dirty IO scheduler, and the callers
  wait in its mailbox. A caller's call has no timeout of its own, so a
  call made behind a long `await_converged/3` waits for it.

  `shutdown/1` refuses the process's name: its supervisor stops it. The
  child spec gives it 30 seconds to shut down, since `shutdown/1` waits for
  the background threads to exit and an in-flight call finishes first;
  change it with `Supervisor.child_spec/2`. A process killed before it shuts
  down leaves the handle to be shut down when it is garbage collected.
  """
  @spec start_link([start_option()]) :: GenServer.on_start()
  def start_link(options) when is_list(options), do: Trellis.Owner.start_link(options)

  @doc """
  The child spec `{Trellis, options}` stands for in a supervision tree. Its
  id is the `:name` option, so one tree can hold several.
  """
  @spec child_spec([start_option()]) :: Supervisor.child_spec()
  def child_spec(options) when is_list(options) do
    %{
      id: Keyword.get(options, :name, __MODULE__),
      start: {__MODULE__, :start_link, [options]},
      shutdown: 30_000
    }
  end

  @doc """
  Creates or upgrades Trellis's own tables in the configured schema. Safe to
  run on every boot.

  Run it on a handle with the default options, before connecting one that
  runs background work: the staging worker reads Trellis's tables as it
  starts, so a `staging: true` connect to an unmigrated schema fails with a
  `:not_found` error.
  """
  @spec migrate(trellis()) :: :ok | {:error, Error.t()}
  def migrate(trellis) when is_trellis(trellis), do: unit(run(trellis, :migrate, []))

  @doc "Like `migrate/1`, but raises `Trellis.Error`."
  @spec migrate!(trellis()) :: :ok
  def migrate!(trellis), do: bang(migrate(trellis))

  @doc "The configuration `trellis` connected with. See `Trellis.Config`."
  @spec config(trellis()) :: {:ok, Config.t()} | {:error, Error.t()}
  def config(trellis) when is_trellis(trellis) do
    with {:ok, config} <- native(run(trellis, :config, [])) do
      {:ok, Config.from_native(config)}
    end
  end

  @doc "Like `config/1`, but raises `Trellis.Error`."
  @spec config!(trellis()) :: Config.t()
  def config!(trellis), do: bang(config(trellis))

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
  @spec define(trellis(), String.t()) :: {:ok, Definition.t()} | {:error, Error.t()}
  def define(trellis, text) when is_trellis(trellis) and is_binary(text) do
    with {:ok, definition} <- native(run(trellis, :define, [text])) do
      {:ok, Definition.from_native(definition)}
    end
  end

  @doc "Like `define/2`, but raises `Trellis.Error`."
  @spec define!(trellis(), String.t()) :: Definition.t()
  def define!(trellis, text), do: bang(define(trellis, text))

  @doc """
  The status of the transform that writes `target_table`, or `nil` if none
  does.
  """
  @spec status(trellis(), String.t()) :: {:ok, Status.t() | nil} | {:error, Error.t()}
  def status(trellis, target_table) when is_trellis(trellis) and is_binary(target_table) do
    case native(run(trellis, :status, [target_table])) do
      {:ok, nil} -> {:ok, nil}
      {:ok, status} -> {:ok, Status.from_native(status)}
      {:error, _} = error -> error
    end
  end

  @doc "Like `status/2`, but raises `Trellis.Error`."
  @spec status!(trellis(), String.t()) :: Status.t() | nil
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
  @spec apply(trellis(), String.t()) :: {:ok, Applied.t()} | {:error, Error.t()}
  def apply(trellis, text) when is_trellis(trellis) and is_binary(text) do
    with {:ok, applied} <- native(run(trellis, :apply, [text])) do
      {:ok, Applied.from_native(applied)}
    end
  end

  @doc "Like `apply/2`, but raises `Trellis.Error`."
  @spec apply!(trellis(), String.t()) :: Applied.t()
  def apply!(trellis, text), do: bang(__MODULE__.apply(trellis, text))

  @doc "Every registered transform definition, oldest first."
  @spec definitions(trellis()) :: {:ok, [DefinitionSummary.t()]} | {:error, Error.t()}
  def definitions(trellis) when is_trellis(trellis) do
    list(run(trellis, :definitions, []), &DefinitionSummary.from_native/1)
  end

  @doc "Like `definitions/1`, but raises `Trellis.Error`."
  @spec definitions!(trellis()) :: [DefinitionSummary.t()]
  def definitions!(trellis), do: bang(definitions(trellis))

  @doc "Every registered relationship, oldest first."
  @spec relationships(trellis()) :: {:ok, [RelationshipSummary.t()]} | {:error, Error.t()}
  def relationships(trellis) when is_trellis(trellis) do
    list(run(trellis, :relationships, []), &RelationshipSummary.from_native/1)
  end

  @doc "Like `relationships/1`, but raises `Trellis.Error`."
  @spec relationships!(trellis()) :: [RelationshipSummary.t()]
  def relationships!(trellis), do: bang(relationships(trellis))

  @doc """
  Asks the staging worker to re-read `source_table` (a bare table name,
  resolved like one in a statement) for every definition that reads it, and
  returns once that re-read is queued.

  Each `:live` reader reports `:backfilling` (a plain aggregate or a plain
  1-1 transform, rebuilt by the call itself) or `:catching_up` (any other)
  until the re-read has re-derived every row the table still has and
  deleted any target row it no longer backs. A newly defined transform
  doesn't need this: its backfill is queued for it. Only a table Trellis
  already captures can be re-read; any other is refused.
  """
  @spec request_backfill(trellis(), String.t()) :: :ok | {:error, Error.t()}
  def request_backfill(trellis, source_table)
      when is_trellis(trellis) and is_binary(source_table) do
    unit(run(trellis, :request_backfill, [source_table]))
  end

  @doc "Like `request_backfill/2`, but raises `Trellis.Error`."
  @spec request_backfill!(trellis(), String.t()) :: :ok
  def request_backfill!(trellis, source_table), do: bang(request_backfill(trellis, source_table))

  @doc """
  Every source row the apply path poisoned (gave up on) after `since`,
  oldest first. Poll it with the last entry's `poisoned_at` so a
  whole-table failure doesn't sit unnoticed.
  """
  @spec poisoned_since(trellis(), DateTime.t()) :: {:ok, [PoisonEntry.t()]} | {:error, Error.t()}
  def poisoned_since(trellis, %DateTime{} = since) when is_trellis(trellis) do
    list(
      run(trellis, :poisoned_since, [Trellis.Time.to_micros(since)]),
      &PoisonEntry.from_native/1
    )
  end

  @doc "Like `poisoned_since/2`, but raises `Trellis.Error`."
  @spec poisoned_since!(trellis(), DateTime.t()) :: [PoisonEntry.t()]
  def poisoned_since!(trellis, since), do: bang(poisoned_since(trellis, since))

  @doc """
  Every quarantined transform and paused column, across every transform.
  Cheap enough for a dashboard or health check to poll.
  """
  @spec quarantined(trellis()) :: {:ok, [QuarantineEntry.t()]} | {:error, Error.t()}
  def quarantined(trellis) when is_trellis(trellis) do
    list(run(trellis, :quarantined, []), &QuarantineEntry.from_native/1)
  end

  @doc "Like `quarantined/1`, but raises `Trellis.Error`."
  @spec quarantined!(trellis()) :: [QuarantineEntry.t()]
  def quarantined!(trellis), do: bang(quarantined(trellis))

  @doc """
  The state of one target: a transform (`"order_totals"`) or one of its
  columns (`"order_totals.total"`). A column that isn't paused is `:live`.
  """
  @spec quarantine_status(trellis(), String.t()) ::
          {:ok, QuarantineEntry.t()} | {:error, Error.t()}
  def quarantine_status(trellis, target) when is_trellis(trellis) and is_binary(target) do
    with {:ok, entry} <- native(run(trellis, :quarantine_status, [target])) do
      {:ok, QuarantineEntry.from_native(entry)}
    end
  end

  @doc "Like `quarantine_status/2`, but raises `Trellis.Error`."
  @spec quarantine_status!(trellis(), String.t()) :: QuarantineEntry.t()
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
  @spec sample_quarantined(trellis(), String.t(), [sample_option()]) ::
          {:ok, SamplePage.t()} | {:error, Error.t()}
  def sample_quarantined(trellis, target, options \\ [])
      when is_trellis(trellis) and is_binary(target) and is_list(options) do
    with {:ok, limit, cursor} <- sample_options(options),
         {:ok, page} <- native(run(trellis, :sample_quarantined, [target, cursor, limit])) do
      {:ok, SamplePage.from_native(page)}
    end
  end

  @doc "Like `sample_quarantined/3`, but raises `Trellis.Error`."
  @spec sample_quarantined!(trellis(), String.t(), [sample_option()]) :: SamplePage.t()
  def sample_quarantined!(trellis, target, options \\ []),
    do: bang(sample_quarantined(trellis, target, options))

  @doc """
  Releases one key `transform` holds in quarantine, once its cause is fixed.
  `source_table` (`"public.orders"` or `"orders"`) and `key` are as
  `sample_quarantined/3` and `poisoned_since/2` report them.

  It stages a recompute of the key, which every transform reading the table
  applies from the key's current row, and discards the changes held for
  `transform` meanwhile. Another transform holding the same key keeps
  holding it. If the cause is still there, the key is poisoned again.
  Resuming the transform releases every key it holds.

  An unknown `transform` is a `:not_found` error, and so is a key it doesn't
  hold, which changes nothing. The release first waits for the drain pages in
  flight on the table to commit; a wait past the lock timeout (30 seconds) is
  a `:timeout` error and changes nothing: call it again.

      :ok = Trellis.release_key(trellis, "order_totals", "public.orders", "42")
  """
  @spec release_key(trellis(), String.t(), String.t(), String.t()) :: :ok | {:error, Error.t()}
  def release_key(trellis, transform, source_table, key)
      when is_trellis(trellis) and is_binary(transform) and is_binary(source_table) and
             is_binary(key) do
    unit(run(trellis, :release_key, [transform, source_table, key]))
  end

  @doc "Like `release_key/4`, but raises `Trellis.Error`."
  @spec release_key!(trellis(), String.t(), String.t(), String.t()) :: :ok
  def release_key!(trellis, transform, source_table, key),
    do: bang(release_key(trellis, transform, source_table, key))

  @doc """
  Whether at least one drain worker is alive anywhere in the fleet. With
  none, nothing reaches a target table: poll this from a health check.
  """
  @spec has_live_drain_workers(trellis()) :: {:ok, boolean()} | {:error, Error.t()}
  def has_live_drain_workers(trellis) when is_trellis(trellis),
    do: native(run(trellis, :has_live_drain_workers, []))

  @doc "Like `has_live_drain_workers/1`, but raises `Trellis.Error`."
  @spec has_live_drain_workers!(trellis()) :: boolean()
  def has_live_drain_workers!(trellis), do: bang(has_live_drain_workers(trellis))

  @doc """
  Whether the staging worker (which installs change capture and starts
  backfills) is alive anywhere in the fleet. The other half of the health check.
  """
  @spec has_live_staging_worker(trellis()) :: {:ok, boolean()} | {:error, Error.t()}
  def has_live_staging_worker(trellis) when is_trellis(trellis),
    do: native(run(trellis, :has_live_staging_worker, []))

  @doc "Like `has_live_staging_worker/1`, but raises `Trellis.Error`."
  @spec has_live_staging_worker!(trellis()) :: boolean()
  def has_live_staging_worker!(trellis), do: bang(has_live_staging_worker(trellis))

  @typedoc "An opaque `watermark_token/1` token."
  @opaque watermark :: String.t()

  @doc """
  A read-your-writes token covering every write committed before this call.
  Take it after a source-table write commits, then pass it to
  `await_converged/3` to wait for that write to reach its targets.
  """
  @spec watermark_token(trellis()) :: {:ok, watermark()} | {:error, Error.t()}
  def watermark_token(trellis) when is_trellis(trellis),
    do: native(run(trellis, :watermark_token, []))

  @doc "Like `watermark_token/1`, but raises `Trellis.Error`."
  @spec watermark_token!(trellis()) :: watermark()
  def watermark_token!(trellis), do: bang(watermark_token(trellis))

  @doc """
  Waits until every change committed at or before `token` has reached its
  target tables, or `timeout_ms` passes (a `:timeout` error; retry it).

  It waits for captured changes only: a transform that isn't `:live` yet
  may still be missing rows when this returns (see `status/2`).

  The handle runs one call at a time, so every other call on it, from any
  process, waits behind this one for up to `timeout_ms`. Size it
  accordingly.
  """
  @spec await_converged(trellis(), watermark(), non_neg_integer()) :: :ok | {:error, Error.t()}
  def await_converged(trellis, token, timeout_ms)
      when is_trellis(trellis) and is_binary(token) and is_integer(timeout_ms) do
    if timeout_ms in 0..@max_timeout_ms do
      unit(run(trellis, :await_converged, [token, timeout_ms]))
    else
      invalid(":timeout_ms must be a non-negative integer, got: #{inspect(timeout_ms)}")
    end
  end

  @doc "Like `await_converged/3`, but raises `Trellis.Error`."
  @spec await_converged!(trellis(), watermark(), non_neg_integer()) :: :ok
  def await_converged!(trellis, token, timeout_ms),
    do: bang(await_converged(trellis, token, timeout_ms))

  @typedoc """
  Options for `self_check/3`:

  - `:timeout_ms` (required): how long a page of the job waits for the
    target to catch up, per wait. A `:standard` check waits up to twice per
    page, a `:strict` one once.
  - `:mode`: `:standard` (default) re-checks anything that differs after a
    fresh wait, so a change still in flight isn't reported; it is safe while
    the source is being written. `:strict` skips the re-check, and is only
    sound once writes to the audited tables have stopped.
  """
  @type self_check_option ::
          {:timeout_ms, non_neg_integer()}
          | {:mode, :standard | :strict}

  @doc """
  Starts a background check of `target_table` against a fresh recompute of
  its definition from the source tables, and returns the `Trellis.SelfCheckJob`
  at once. See `t:self_check_option/0`.

  The comparison is a drain worker's, a page of keys at a time (with no
  drain worker anywhere in the fleet the job stays `:queued`, as a define
  does). Poll `self_check_job/2` with the job's `id` until it is finished
  (`Trellis.SelfCheckJob.finished?/1`); its `Trellis.SelfCheckReport` is then
  the verdict:

      {:ok, job} = Trellis.self_check(trellis, "order_totals", timeout_ms: 30_000)
      {:ok, %Trellis.SelfCheckJob{state: :done, report: report}} = Trellis.self_check_job(trellis, job.id)

  A target whose job is still `:queued` or `:running` gets that job back,
  whatever options this call passed; a finished job stays until the next
  `self_check/3` of its target replaces it. Only a one-row-per-source-key
  transform that reads no relationship can be audited; an aggregate or
  relationship-reading target is a `:validation` error, and an unknown one
  `:not_found`. A column that is paused is left out of the comparison.

  The call reads nothing of the target, so it returns well inside the
  30-second call limit.
  """
  @spec self_check(trellis(), String.t(), [self_check_option()]) ::
          {:ok, SelfCheckJob.t()} | {:error, Error.t()}
  def self_check(trellis, target_table, options)
      when is_trellis(trellis) and is_binary(target_table) and is_list(options) do
    with {:ok, opts} <- self_check_options(options),
         {:ok, job} <-
           native(
             run(trellis, :self_check, [
               target_table,
               Atom.to_string(opts.mode),
               opts.timeout_ms
             ])
           ) do
      {:ok, SelfCheckJob.from_native(job)}
    end
  end

  @doc "Like `self_check/3`, but raises `Trellis.Error`."
  @spec self_check!(trellis(), String.t(), [self_check_option()]) :: SelfCheckJob.t()
  def self_check!(trellis, target_table, options),
    do: bang(self_check(trellis, target_table, options))

  @doc """
  The `Trellis.SelfCheckJob` that `self_check/3` returned with this `id`, as
  it stands now: its state, how many keys it has compared so far, and, once
  it is `:done`, its report. `nil` when there is none: a newer `self_check/3`
  of its target replaced it, or its transform was dropped.
  """
  @spec self_check_job(trellis(), integer()) ::
          {:ok, SelfCheckJob.t() | nil} | {:error, Error.t()}
  def self_check_job(trellis, id) when is_trellis(trellis) and is_integer(id) do
    if id in @min_job_id..@max_limit//1 do
      with {:ok, job} <- native(run(trellis, :self_check_job, [id])) do
        {:ok, job && SelfCheckJob.from_native(job)}
      end
    else
      invalid("the job id is out of range, got: #{inspect(id)}")
    end
  end

  @doc "Like `self_check_job/2`, but raises `Trellis.Error`."
  @spec self_check_job!(trellis(), integer()) :: SelfCheckJob.t() | nil
  def self_check_job!(trellis, id), do: bang(self_check_job(trellis, id))

  @doc """
  Stops the handle's background work and waits for its threads to exit. Any
  later call on the handle returns a `:validation` error; shutting down again
  is `:ok`.
  """
  @spec shutdown(trellis()) :: :ok | {:error, Error.t()}
  def shutdown(%__MODULE__{ref: ref}), do: unit(Native.shutdown(ref))

  def shutdown(server) when is_trellis(server) do
    invalid(
      "#{inspect(server)} is a supervised Trellis, which its supervisor shuts down; " <>
        "stop it with Supervisor.terminate_child/2, not shutdown/1"
    )
  end

  @doc "Like `shutdown/1`, but raises `Trellis.Error`."
  @spec shutdown!(trellis()) :: :ok
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

  @self_check_keys [:timeout_ms, :mode]

  defp self_check_options(options) do
    case Keyword.split(options, @self_check_keys) do
      {_, [_ | _] = unknown} ->
        invalid(
          "unknown options #{inspect(Keyword.keys(unknown))}; known: #{inspect(@self_check_keys)}"
        )

      {known, []} ->
        opts = Map.merge(%{mode: :standard}, Map.new(known))
        timeout_ms = Map.get(opts, :timeout_ms)

        cond do
          not (is_integer(timeout_ms) and timeout_ms in 0..@max_timeout_ms) ->
            invalid(
              ":timeout_ms is required and must be a non-negative integer, got: #{inspect(timeout_ms)}"
            )

          opts.mode not in [:standard, :strict] ->
            invalid(":mode must be :standard or :strict, got: #{inspect(opts.mode)}")

          true ->
            {:ok, opts}
        end
    end
  end

  defp invalid(message), do: {:error, Error.validation(message)}

  # A native call on a handle, or on the handle a supervised Trellis owns,
  # run in its owning process (`Trellis.Owner`).
  defp run(%__MODULE__{ref: ref}, function, args),
    do: Kernel.apply(Native, function, [ref | args])

  defp run(server, function, args), do: Trellis.Owner.call(server, function, args)

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
