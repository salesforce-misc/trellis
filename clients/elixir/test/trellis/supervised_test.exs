defmodule Trellis.SupervisedTest do
  # `{Trellis, options}` in a supervision tree: the process that owns a
  # handle, runs the calls made through its name, and shuts the handle down
  # when its supervisor stops it. The parity suite runs its whole script
  # through one too (`parity_test.exs`).
  #
  # Shares the suite's one database, so it doesn't run concurrently with the
  # other integration tests.
  use ExUnit.Case, async: false

  alias Trellis.{Config, Error, TestCluster}

  @name Trellis.SupervisedTest.Instance

  setup_all do
    trellis = Trellis.connect!(url: TestCluster.info()["dsn"])
    :ok = Trellis.migrate!(trellis)
    :ok = Trellis.shutdown!(trellis)
    :ok
  end

  test "the child spec is keyed on the name and allows time to shut down" do
    options = [name: @name, url: "postgres://localhost/app"]

    assert %{id: @name, start: {Trellis, :start_link, [^options]}, shutdown: 30_000} =
             Supervisor.child_spec({Trellis, options}, [])

    # Unnamed, it is addressed by its pid, and its id is the module's.
    assert %{id: Trellis} = Trellis.child_spec(url: "postgres://localhost/app")
  end

  test "a connect that fails is the start's error, with no exit signal to the caller" do
    assert {:error, %Error{code: :validation, message: message}} =
             Trellis.start_link(url: TestCluster.info()["dsn"], drain_threads: -1)

    assert message =~ ":drain_threads"

    assert {:error, {%Error{code: :validation}, _child}} =
             start_supervised({Trellis, name: @name, url: ""})
  end

  test "calls go through the name to the owned handle, and the supervisor's stop shuts it down" do
    pid = start_supervised!({Trellis, name: @name, url: TestCluster.info()["dsn"]})

    assert {:ok, nil} = Trellis.status(@name, "no_transform_writes_this")
    assert nil == Trellis.status!(pid, "no_transform_writes_this")
    assert {:ok, %Config{schema: "trellis"}} = Trellis.config(@name)

    # Callers wait in the owner's mailbox rather than each holding the handle.
    results =
      1..20
      |> Enum.map(fn _ -> Task.async(fn -> Trellis.status(@name, "nor_this") end) end)
      |> Task.await_many()

    assert Enum.uniq(results) == [{:ok, nil}]

    # Its supervisor shuts it down, not the caller.
    assert {:error, %Error{code: :validation, message: message}} = Trellis.shutdown(@name)
    assert message =~ "Supervisor.terminate_child/2"
    assert_raise Error, fn -> Trellis.shutdown!(@name) end
    assert {:ok, nil} = Trellis.status(@name, "no_transform_writes_this")

    # The handle the process owns, to check what stopping it did.
    handle = :sys.get_state(pid)
    assert {:ok, nil} = Trellis.status(handle, "no_transform_writes_this")

    :ok = stop_supervised(@name)

    # A shut-down handle refuses every call.
    assert {:error, %Error{code: :validation}} =
             Trellis.status(handle, "no_transform_writes_this")
  end

  test "something that can't name a process is refused before any call" do
    assert_raise FunctionClauseError, fn -> Trellis.status(nil, "widget_prices") end
    assert_raise FunctionClauseError, fn -> Trellis.definitions("MyApp.Trellis") end
  end
end
