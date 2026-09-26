defmodule TrellisTest do
  # Shares the suite's one database, and the slice test owns its replication
  # slot, so these don't run concurrently.
  use ExUnit.Case, async: false

  alias Trellis.{Definition, Error, Status, TestCluster}

  @converge_ms 30_000

  setup_all do
    # Migrating is what a deploy's migration step does, on a handle that
    # runs nothing in the background (the defaults).
    trellis = Trellis.connect!(url: TestCluster.info()["dsn"])
    :ok = Trellis.migrate!(trellis)
    :ok = Trellis.shutdown!(trellis)
    :ok
  end

  # ADR-0010's definition of done for the slice: define a 1-1 transform,
  # watch it reach :live, insert a source row, see it in the target.
  #
  # Something has to run the staging worker and drain threads or the
  # definition never leaves :waiting_to_backfill. Here that is the test's own
  # handle: this BEAM is the whole fleet, so it is the one process that
  # should own the replication slot, and a second handle to do the draining
  # would be a second handle in one OS process, which ADR-0010 decision 3
  # rules out. `setup_all` migrated first because the staging worker reads
  # the catalog as it starts.
  test "a defined transform goes live and keeps its target converged" do
    pg = TestCluster.postgrex!()
    Postgrex.query!(pg, "create table widgets (id integer primary key, price integer)", [])
    Postgrex.query!(pg, "insert into widgets (id, price) values (1, 5)", [])

    trellis =
      Trellis.connect!(url: TestCluster.info()["dsn"], staging: true, drain_threads: 1)

    on_exit(fn -> Trellis.shutdown(trellis) end)

    assert {:ok, %Definition{} = definition} =
             Trellis.define(trellis, "TRANSFORM widget_prices FROM widgets SELECT price AS price")

    assert definition.target_table == "public.widget_prices"
    assert definition.source_table == "public.widgets"
    assert definition.status == :waiting_to_backfill
    assert definition.source_columns == %{"id" => "integer", "price" => "integer"}

    assert %Status{status: :live, backfill_failure: nil} =
             eventually(fn ->
               case Trellis.status!(trellis, "widget_prices") do
                 %Status{status: :live} = status -> status
                 _ -> nil
               end
             end)

    # The backfill carried the existing row across.
    assert rows(pg) == [[1, 5]]

    Postgrex.query!(pg, "insert into widgets (id, price) values (2, 7)", [])
    assert eventually(fn -> if rows(pg) == [[1, 5], [2, 7]], do: true end)

    assert :ok = Trellis.shutdown(trellis)
  end

  test "a statement that doesn't parse is a :parse error" do
    trellis = Trellis.connect!(url: TestCluster.info()["dsn"])
    on_exit(fn -> Trellis.shutdown(trellis) end)

    assert {:error, %Error{code: :parse}} = Trellis.define(trellis, "TRANSFORM oops")
    assert_raise Error, fn -> Trellis.define!(trellis, "TRANSFORM oops") end
  end

  test "a table no transform writes has no status" do
    trellis = Trellis.connect!(url: TestCluster.info()["dsn"])
    on_exit(fn -> Trellis.shutdown(trellis) end)

    assert {:ok, nil} = Trellis.status(trellis, "no_such_target")
  end

  test "a shut-down handle refuses calls, and shutting down again is :ok" do
    trellis = Trellis.connect!(url: TestCluster.info()["dsn"])
    assert :ok = Trellis.shutdown(trellis)
    assert :ok = Trellis.shutdown(trellis)

    assert {:error, %Error{code: :validation, message: message}} =
             Trellis.status(trellis, "widget_prices")

    assert message =~ "shut down"
  end

  defp rows(pg) do
    Postgrex.query!(pg, "select id, price from widget_prices order by id", []).rows
  end

  # Polls `check` until it returns something truthy, or fails the test.
  defp eventually(check, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @converge_ms

    cond do
      result = check.() ->
        result

      System.monotonic_time(:millisecond) > deadline ->
        flunk("did not converge within #{@converge_ms}ms")

      true ->
        Process.sleep(50)
        eventually(check, deadline)
    end
  end
end
