defmodule Trellis.LogBridge do
  @moduledoc """
  Forwards the engine's log lines to `Logger`.

  The Rust engine logs through the `tracing` facade and never installs a
  subscriber itself (`docs/decisions/0009-observability-decisions.md`,
  decision 3). When the `:trellis_pg` application starts, this process installs
  one in the Trellis NIF library, then drains what it queues into `Logger`,
  with `domain: [:trellis]` and the emitting Rust module as `:target`
  metadata. It needs no `Trellis` handle: every handle's lines come through
  it.

  ## Configuration

      # Forward events at this level and more severe ones. Defaults to
      # `Logger.level()` when `:trellis_pg` starts. Rust's `trace` level
      # arrives as `:debug`.
      config :trellis_pg, log_level: :info

      # Opt out: nothing is installed, this process doesn't start, and the
      # engine's lines go nowhere.
      config :trellis_pg, log_bridge: false

  The level is read once, when `:trellis_pg` starts. The engine drops events
  above it before formatting them, so a later `Logger.configure/1` can make
  `Logger` stricter but can't bring back a level the bridge filtered.

  ## What can be lost

  A log line can be dropped; logging never blocks or crashes the engine. The
  engine only appends to a bounded queue (10,000 lines), and this process
  drains it every 100 ms. If the queue fills, because this process is
  down, restarting, or falling behind, new lines are dropped and counted,
  and the next drain logs a warning with the count.

  ## Composing your own subscriber

  `tracing`'s global subscriber lives inside the native library that links
  it, and the Trellis NIF links its own copy. So a subscriber another NIF
  installs never sees the engine's lines, and doesn't stop this bridge.
  Sending them elsewhere, for example through the crate's `otlp` layer,
  means building your own NIF around the `trellis` crate that installs that
  subscriber. If a subscriber in the Trellis NIF was installed first, this
  process logs a warning and doesn't start, and the engine's lines go to
  that subscriber instead.
  """

  use GenServer

  require Logger

  alias Trellis.Native

  @interval_ms 100
  # Records per NIF call: small enough that one call never holds a normal
  # scheduler for long, which is why that NIF isn't dirty.
  @batch 500
  # Batches per drain: enough to empty a full queue (10,000 lines), and no
  # more, so a sustained flood still yields to `flush/0` and system messages.
  @max_batches 21

  @doc false
  def start_link(_options), do: GenServer.start_link(__MODULE__, :ok, name: __MODULE__)

  @doc false
  def child_spec(options) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [options]}}
  end

  @doc """
  Forwards every line queued so far to `Logger` before returning, rather
  than on the next tick. For tests, and for a host that wants the engine's
  last lines logged before it stops.
  """
  @spec flush() :: :ok
  def flush, do: GenServer.call(__MODULE__, :flush)

  @impl true
  def init(:ok) do
    level = Application.get_env(:trellis_pg, :log_level, Logger.level())

    case Native.install_log_bridge(filter(level)) do
      {:ok, :ok} ->
        # So `terminate/2` runs, and forwards what's left, on shutdown.
        Process.flag(:trap_exit, true)
        schedule()
        {:ok, nil}

      {:error, error} ->
        %Trellis.Error{message: message} = Trellis.Error.from_native(error)
        Logger.warning("Trellis's log lines won't reach Logger: #{message}")
        :ignore
    end
  end

  @impl true
  def handle_info(:drain, state) do
    if drain(@max_batches) == :more, do: send(self(), :drain), else: schedule()
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def handle_call(:flush, _from, state) do
    drain(@max_batches)
    {:reply, :ok, state}
  end

  @impl true
  def terminate(_reason, _state) do
    drain(@max_batches)
  end

  defp schedule, do: Process.send_after(self(), :drain, @interval_ms)

  defp drain(0), do: :more

  defp drain(batches) do
    {:ok, {records, dropped}} = Native.take_log_records(@batch)

    if dropped > 0 do
      Logger.warning("Trellis dropped #{dropped} log lines: the Logger bridge fell behind",
        domain: [:trellis]
      )
    end

    Enum.each(records, fn {level, target, message} ->
      Logger.bare_log(logger_level(level), message, domain: [:trellis], target: target)
    end)

    if length(records) < @batch, do: :done, else: drain(batches - 1)
  end

  # Rust's `tracing` levels, as `Logger` levels. `test/trellis/log_bridge_test.exs`
  # checks every level the NIF can send has a clause.
  @doc false
  def logger_level(:error), do: :error
  def logger_level(:warn), do: :warning
  def logger_level(:info), do: :info
  def logger_level(:debug), do: :debug
  def logger_level(:trace), do: :debug

  # A `Logger` level, as the most verbose `tracing` level worth forwarding.
  @doc false
  def filter(level) when level in [:emergency, :alert, :critical, :error], do: "error"
  def filter(level) when level in [:warning, :warn], do: "warn"
  def filter(level) when level in [:notice, :info], do: "info"
  def filter(:debug), do: "debug"
  def filter(:all), do: "trace"
  def filter(:none), do: "off"

  def filter(level) do
    raise ArgumentError,
          "config :trellis_pg, log_level: expected a Logger level, got: #{inspect(level)}"
  end
end
