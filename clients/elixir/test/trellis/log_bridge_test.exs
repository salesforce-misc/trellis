defmodule Trellis.LogBridgeTest do
  # Kills and restarts the one `Trellis.LogBridge`, so nothing else that
  # expects it to be up runs alongside.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Trellis.{LogBridge, TestCluster}

  setup_all do
    trellis = Trellis.connect!(url: TestCluster.info()["dsn"])
    :ok = Trellis.migrate!(trellis)
    on_exit(fn -> Trellis.shutdown(trellis) end)
    %{trellis: trellis}
  end

  # The engine logs this at `info` and it needs no background work: dropping
  # a transform that doesn't exist is a no-op.
  defp log_a_line(trellis, name) do
    {:ok, :dropped} = Trellis.apply(trellis, "DROP TRANSFORM #{name}")
  end

  # A `:logger` handler that sends every event to the test process.
  defmodule Forward do
    def log(event, %{config: %{test: pid}}), do: send(pid, {:logged, event})
  end

  test "an engine log line reaches Logger with its level, fields and metadata", %{
    trellis: trellis
  } do
    :ok = :logger.add_handler(:trellis_log_bridge_test, Forward, %{config: %{test: self()}})
    on_exit(fn -> :logger.remove_handler(:trellis_log_bridge_test) end)

    log_a_line(trellis, "no_such_transform_one")
    :ok = LogBridge.flush()

    assert_receive {:logged,
                    %{
                      level: :info,
                      msg: {:string, message},
                      meta: %{domain: [:elixir, :trellis], target: "trellis::defs::lifecycle"}
                    }}

    assert IO.chardata_to_string(message) ==
             "drop is a no-op; no such definition transform=no_such_transform_one"
  end

  test "lines logged while the bridge is down are forwarded once it restarts", %{
    trellis: trellis
  } do
    pid = Process.whereis(LogBridge)
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}

    # The engine doesn't notice: it only ever appends to the queue.
    log =
      capture_log(fn ->
        log_a_line(trellis, "no_such_transform_two")
        wait_for_restart(pid)
        LogBridge.flush()
      end)

    assert log =~ "no_such_transform_two"
  end

  test "every level the engine can send has a Logger level" do
    {:ok, levels} = Trellis.Native.log_levels()
    assert levels == [:error, :warn, :info, :debug, :trace]

    for level <- levels do
      assert LogBridge.logger_level(level) in Logger.levels()
    end
  end

  test "every Logger level maps to a filter the engine accepts" do
    for level <- [:all, :none | Logger.levels()] do
      assert LogBridge.filter(level) in ["off", "error", "warn", "info", "debug", "trace"]
    end

    assert_raise ArgumentError, ~r/log_level/, fn -> LogBridge.filter(:loud) end
  end

  test "an unknown filter word is a validation error, and the installed filter survives it", %{
    trellis: trellis
  } do
    assert {:error, {"validation", _}} = Trellis.Native.install_log_bridge("verbose")

    log =
      capture_log(fn ->
        log_a_line(trellis, "no_such_transform_three")
        LogBridge.flush()
      end)

    assert log =~ "no_such_transform_three"
  end

  test "opting out starts nothing" do
    assert Trellis.Application.children() == [LogBridge]

    Application.put_env(:trellis, :log_bridge, false)
    on_exit(fn -> Application.delete_env(:trellis, :log_bridge) end)
    assert Trellis.Application.children() == []
  end

  defp wait_for_restart(old_pid) do
    case Process.whereis(LogBridge) do
      pid when is_pid(pid) and pid != old_pid ->
        :ok

      _ ->
        Process.sleep(10)
        wait_for_restart(old_pid)
    end
  end
end
