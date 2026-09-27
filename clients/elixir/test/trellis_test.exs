defmodule TrellisTest do
  # Shares the suite's one database, and the slice test owns its replication
  # slot, so these don't run concurrently.
  use ExUnit.Case, async: false

  import Trellis.Eventually

  alias Trellis.{Definition, Error, Status, TestCluster}

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

    # The apply shows up in the process-wide registry. The drain records it
    # just after its transaction commits, so the row can be visible first.
    eventually("widget_prices' applied changes to reach the metrics", fn ->
      metrics = Trellis.Metrics.render_prometheus()

      if metrics =~ ~r/^trellis_changes_applied_total\{transform="[^"]*widget_prices"\} \d+$/m,
        do: {:done, :ok},
        else: {:waiting, metrics}
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

  test "config/1 reads back the options the handle connected with, but never the url" do
    # The test cluster trusts every local connection, so the password is
    # accepted and ignored: it's here to be looked for in the output.
    password = "s3cret-hunter2"
    url = TestCluster.info()["dsn"] <> " password=" <> password
    trellis = Trellis.connect!(url: url, target_schema: "reporting")
    on_exit(fn -> Trellis.shutdown(trellis) end)

    assert {:ok,
            %Trellis.Config{
              schema: "trellis",
              target_schema: "reporting",
              pool_max_size: pool_max_size,
              pool_wait_timeout_ms: pool_wait_timeout_ms
            } = config} = Trellis.config(trellis)

    assert pool_max_size >= 1
    assert pool_wait_timeout_ms >= 1

    # A config is the kind of value that gets logged whole.
    refute Map.has_key?(config, :url)
    refute inspect(config) =~ password
    refute inspect(Map.from_struct(config)) =~ password
    refute inspect(trellis) =~ password
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
end
