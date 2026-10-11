defmodule Trellis.CallsTest do
  # How a call behaves while the database is slow (issue #599): it holds no
  # scheduler, a killed caller leaves at once, calls on one handle run in
  # parallel, and a shutdown cancels the calls in flight instead of waiting
  # for them.
  #
  # The stuck call is `relationships/1` against a catalog table another
  # session holds an exclusive lock on, so it queues on the server for as long
  # as the lock is held (the engine's own 30-second deadline would end it). A
  # test knows the call has reached the lock by reading `pg_stat_activity`
  # before it asserts anything, so nothing here depends on how fast the
  # call gets there.
  #
  # Uses a catalog schema of its own in the suite's one database, so the lock
  # blocks no other test's handle; it still doesn't run concurrently with them.
  use ExUnit.Case, async: false

  import Trellis.Eventually

  alias Trellis.{Error, TestCluster}

  @schema "calls_test"
  @target_schema "calls_test_targets"
  @name Trellis.CallsTest.Instance

  # The table `relationships/1` reads and `status/2` and `watermark_token/1`
  # don't, so a call stuck on it delays no other.
  @locked_table "relationship_definitions"

  # Far more than a test needs, and far less than the engine's 30 seconds:
  # what a call that should return at once is allowed.
  @prompt_ms 5_000

  setup_all do
    pg = TestCluster.postgrex!()
    Postgrex.query!(pg, "create schema if not exists #{@target_schema}", [])

    trellis = Trellis.connect!(options())
    :ok = Trellis.migrate!(trellis)
    :ok = Trellis.shutdown!(trellis)
    :ok
  end

  defp options do
    [url: TestCluster.info()["dsn"], schema: @schema, target_schema: @target_schema]
  end

  setup do
    observer = TestCluster.postgrex!()
    {:ok, observer: observer}
  end

  # Holds an exclusive lock on the catalog table until the returned function
  # is called (or the test ends), and returns once the lock is held.
  defp hold_lock! do
    test = self()
    holder = TestCluster.postgrex!()

    task =
      Task.async(fn ->
        Postgrex.transaction(
          holder,
          fn conn ->
            Postgrex.query!(
              conn,
              "lock table #{@schema}.#{@locked_table} in access exclusive mode",
              []
            )

            send(test, {:locked, self()})

            receive do
              :release -> :ok
            end
          end,
          timeout: :infinity
        )
      end)

    locked_holder =
      receive do
        {:locked, holder} -> holder
      after
        @prompt_ms -> flunk("the lock was not taken")
      end

    release = fn ->
      send(locked_holder, :release)
      {:ok, :ok} = Task.await(task)
      :ok
    end

    on_exit(fn ->
      # A test that failed before releasing: let the lock go.
      send(locked_holder, :release)
    end)

    release
  end

  # Returns once `count` backends are waiting on the held lock, which is when
  # that many calls have reached the server and are stuck on it.
  defp await_lock_waiters!(observer, count) do
    eventually("#{count} call(s) stuck on the lock", fn ->
      waiting = lock_waiters(observer)
      if waiting >= count, do: {:done, waiting}, else: {:waiting, waiting}
    end)
  end

  # The backends waiting for the lock `hold_lock!/0` holds, and only those:
  # a lock wait anywhere else in the database isn't counted.
  defp lock_waiters(observer) do
    %{rows: [[waiting]]} =
      Postgrex.query!(
        observer,
        "select count(*) from pg_locks " <>
          "where not granted and relation = '#{@schema}.#{@locked_table}'::regclass",
        []
      )

    waiting
  end

  defp connect! do
    trellis = Trellis.connect!(options())
    on_exit(fn -> Trellis.shutdown(trellis) end)
    trellis
  end

  # Runs `fun` in a task and returns its result, or fails if it takes longer
  # than a call that should return at once.
  defp promptly!(what, fun) do
    task = Task.async(fun)

    case Task.yield(task, @prompt_ms) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> result
      _ -> flunk("#{what} did not return within #{@prompt_ms} ms")
    end
  end

  test "calls stuck on the database hold no dirty scheduler", %{observer: observer} do
    trellis = connect!()
    release = hold_lock!()

    # More stuck calls than the VM has dirty IO schedulers: if each held one,
    # the last would not even start.
    stuck = :erlang.system_info(:dirty_io_schedulers) + 2
    tasks = for _ <- 1..stuck, do: Task.async(fn -> Trellis.relationships(trellis) end)
    assert await_lock_waiters!(observer, stuck) == stuck

    # A dirty IO job still runs at once. (Statistics would only count the
    # jobs queued, not whether one gets a scheduler; running one is the
    # property itself.)
    assert {:ok, [_ | _]} = promptly!("a dirty IO NIF", fn -> Trellis.Native.status_names() end)

    # So do the calls that don't touch the locked table.
    assert {:ok, nil} = promptly!("status/2", fn -> Trellis.status(trellis, "no_such_target") end)

    :ok = release.()
    assert Enum.all?(Task.await_many(tasks, @prompt_ms), &(&1 == {:ok, []}))
  end

  test "a killed caller exits at once, and the other callers go on", %{observer: observer} do
    trellis = connect!()
    release = hold_lock!()

    {caller, monitor} =
      spawn_monitor(fn -> Trellis.relationships(trellis) end)

    await_lock_waiters!(observer, 1)

    # It is in its `receive`, not in a NIF, so the kill takes effect now.
    Process.exit(caller, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^caller, :killed}, @prompt_ms

    # The call it abandoned is still stuck, and everyone else is unaffected.
    assert lock_waiters(observer) == 1
    assert {:ok, nil} = promptly!("status/2", fn -> Trellis.status(trellis, "no_such_target") end)

    assert {:ok, _token} =
             promptly!("watermark_token/1", fn -> Trellis.watermark_token(trellis) end)

    # Once the lock goes, the abandoned call ends and its reply is dropped; the
    # handle is as good as before.
    :ok = release.()
    assert {:ok, []} = promptly!("relationships/1", fn -> Trellis.relationships(trellis) end)
    assert lock_waiters(observer) == 0
  end

  test "a call blocked on a lock doesn't delay another call on the same supervised handle",
       %{observer: observer} do
    start_supervised!({Trellis, [{:name, @name} | options()]})
    release = hold_lock!()

    stuck = Task.async(fn -> Trellis.relationships(@name) end)
    await_lock_waiters!(observer, 1)

    # Through the name, as the stuck call is: neither the owner nor the
    # handle holds this behind it.
    {microseconds, result} =
      :timer.tc(fn ->
        promptly!("status/2", fn -> Trellis.status(@name, "no_such_target") end)
      end)

    assert {:ok, nil} = result
    assert microseconds < @prompt_ms * 1_000
    assert Task.yield(stuck, 0) == nil

    :ok = release.()
    assert {:ok, []} = Task.await(stuck, @prompt_ms)
  end

  test "a shutdown returns at once with a call stuck on a lock, and cancels the call",
       %{observer: observer} do
    start_supervised!({Trellis, [{:name, @name} | options()]})
    release = hold_lock!()

    stuck = Task.async(fn -> Trellis.relationships(@name) end)
    await_lock_waiters!(observer, 1)

    # The supervisor's stop is the shutdown. Waiting for the call would take
    # its 30-second deadline.
    {microseconds, :ok} = :timer.tc(fn -> stop_supervised(@name) end)
    assert microseconds < @prompt_ms * 1_000

    assert {:error, %Error{code: :validation, message: message}} =
             Task.await(stuck, @prompt_ms)

    assert message =~ "cancelled"
    assert message =~ "shut down"

    :ok = release.()
  end

  test "a handle's shutdown cancels the calls in flight" do
    trellis = connect!()
    release = hold_lock!()
    observer = TestCluster.postgrex!()

    stuck = Task.async(fn -> Trellis.relationships(trellis) end)
    await_lock_waiters!(observer, 1)

    {microseconds, :ok} = :timer.tc(fn -> Trellis.shutdown(trellis) end)
    assert microseconds < @prompt_ms * 1_000

    assert {:error, %Error{code: :validation, message: cancelled}} =
             Task.await(stuck, @prompt_ms)

    assert cancelled =~ "cancelled"

    # Later calls are refused outright.
    assert {:error, %Error{code: :validation, message: refused}} = Trellis.relationships(trellis)
    assert refused =~ "has been shut down"

    :ok = release.()
  end

  @tag timeout: 5_000
  test "the wait for a reply has a backstop, and it is a timeout" do
    # A reply that never comes, as a lost one would be.
    assert {:error, {"timeout", message}} = Trellis.Native.await(make_ref(), 50)
    assert message =~ "no reply"

    assert %Error{code: :timeout} = Error.from_native({"timeout", message})
  end

  test "a handle from another node is refused with a validation error, not a bad argument" do
    # A reference made on node `elsewhere@nohost` (NEWER_REFERENCE_EXT), as
    # a handle another node connected arrives here: a plain reference that is
    # no NIF resource on this node.
    node = "elsewhere@nohost"

    foreign =
      :erlang.binary_to_term(
        <<131, 90, 3::16, 119, byte_size(node), node::binary, 1::32, 1::32, 2::32, 3::32>>
      )

    assert node(foreign) == :elsewhere@nohost
    elsewhere = %Trellis{ref: foreign}

    assert {:error, %Error{code: :validation, message: message}} =
             Trellis.status(elsewhere, "no_such_target")

    assert message =~ "elsewhere@nohost"
    assert {:error, %Error{code: :validation}} = Trellis.config(elsewhere)
  end

  test "a call leaves nothing in the caller's mailbox" do
    trellis = connect!()

    assert {:ok, nil} = Trellis.status(trellis, "no_such_target")
    assert {:ok, []} = Trellis.definitions(trellis)
    assert {:error, %Error{code: :parse}} = Trellis.apply(trellis, "not a statement")

    assert Process.info(self(), :messages) == {:messages, []}
  end
end
