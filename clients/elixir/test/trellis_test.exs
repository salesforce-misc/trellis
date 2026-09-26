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
             eventually("widget_prices to reach :live", fn ->
               case Trellis.status!(trellis, "widget_prices") do
                 %Status{status: :live} = status -> {:done, status}
                 status -> {:waiting, status}
               end
             end)

    # The backfill carried the existing row across.
    assert rows(pg) == [[1, 5]]

    Postgrex.query!(pg, "insert into widgets (id, price) values (2, 7)", [])

    eventually("the new source row to reach widget_prices", fn ->
      case rows(pg) do
        [[1, 5], [2, 7]] -> {:done, :ok}
        rows -> {:waiting, rows}
      end
    end)

    assert :ok = Trellis.shutdown(trellis)
  end

  # `define/2` is `apply` underneath, which carries out every statement form,
  # so it must refuse the others before applying them, not after. The handle
  # runs nothing in the background, so the definition stays put at
  # :waiting_to_backfill unless one of these statements reaches the engine.
  test "define/2 refuses any other statement form without applying it" do
    pg = TestCluster.postgrex!()
    Postgrex.query!(pg, "create table gadgets (id integer primary key, price integer)", [])

    trellis = Trellis.connect!(url: TestCluster.info()["dsn"])
    on_exit(fn -> Trellis.shutdown(trellis) end)

    assert {:ok, %Definition{status: :waiting_to_backfill}} =
             Trellis.define(trellis, "TRANSFORM gadget_prices FROM gadgets SELECT price AS price")

    for statement <- [
          "PAUSE TRANSFORM gadget_prices",
          "  drop transform gadget_prices",
          "RELATIONSHIP owner FROM gadgets.id TO gadgets.id",
          "DROP RELATIONSHIP gadgets.owner"
        ] do
      assert {:error, %Error{code: :validation, message: message}} =
               Trellis.define(trellis, statement)

      assert message =~ "nothing was applied"
      assert_raise Error, fn -> Trellis.define!(trellis, statement) end

      assert {:ok, %Status{status: :waiting_to_backfill}} =
               Trellis.status(trellis, "gadget_prices"),
             "#{inspect(statement)} took effect"
    end
  end

  test "a statement that doesn't parse is a :parse error" do
    trellis = Trellis.connect!(url: TestCluster.info()["dsn"])
    on_exit(fn -> Trellis.shutdown(trellis) end)

    assert {:error, %Error{code: :parse}} = Trellis.define(trellis, "TRANSFORM oops")
    assert_raise Error, fn -> Trellis.define!(trellis, "TRANSFORM oops") end

    # A malformed statement of another form is the same :parse error, not a
    # :validation refusal naming a form it never managed to be.
    assert {:error, %Error{code: :parse}} = Trellis.define(trellis, "DROP gadget_prices")
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

  # Polls `check` until it returns `{:done, result}`, or fails the test
  # naming what it waited for and the last `{:waiting, seen}` value.
  defp eventually(what, check, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @converge_ms

    case check.() do
      {:done, result} ->
        result

      {:waiting, seen} ->
        if System.monotonic_time(:millisecond) > deadline do
          flunk("waited #{@converge_ms}ms for #{what}; last saw #{inspect(seen)}")
        else
          Process.sleep(50)
          eventually(what, check, deadline)
        end
    end
  end
end
