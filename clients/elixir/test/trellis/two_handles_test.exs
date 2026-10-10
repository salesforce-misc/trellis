defmodule Trellis.TwoHandlesTest do
  # Two supervised handles in one BEAM, each registered under its own `:name`
  # and each an instance of its own (its own catalog schema and target
  # schema) in the suite's one database (epic #806, issue #879). Every call
  # goes through a name, so the two never share a handle.
  #
  # Both shapes planned for production use are here: the instances define
  # transforms over one source table, and one reads the other's target.
  # Neither waits for a result by polling it: a write is followed by
  # `await_converged/3` on a watermark token taken after it, on the first
  # instance and then on the one that reads from it, and only then are the
  # targets compared with the source. The one poll is the documented one, for
  # a definition to leave `:waiting_to_backfill` before the first write
  # (embedding: "Poll to live, don't wait").
  #
  # Each instance runs its own staging worker, so this doesn't run
  # concurrently with the other integration tests.
  use ExUnit.Case, async: false

  import Trellis.Eventually

  alias Trellis.{Status, TestCluster}

  @timeout_ms 30_000

  # Handles are started under these names, which are also their child ids.
  @a Trellis.TwoHandlesTest.A
  @b Trellis.TwoHandlesTest.B

  setup_all do
    pg = TestCluster.postgrex!()

    # Nothing in the engine creates a target schema.
    for schema <-
          ~w(two_shared_a_targets two_shared_b_targets two_chain_a_targets two_chain_b_targets) do
      Postgrex.query!(pg, "create schema if not exists #{schema}", [])
    end

    # Migrating is what a deploy's migration step does, on a handle that runs
    # nothing in the background.
    for {schema, target_schema} <- [
          {"two_shared_a", "two_shared_a_targets"},
          {"two_shared_b", "two_shared_b_targets"},
          {"two_chain_a", "two_chain_a_targets"},
          {"two_chain_b", "two_chain_b_targets"}
        ] do
      trellis =
        Trellis.connect!(
          url: TestCluster.info()["dsn"],
          schema: schema,
          target_schema: target_schema
        )

      :ok = Trellis.migrate!(trellis)
      :ok = Trellis.shutdown!(trellis)
    end

    :ok
  end

  # Starts the instance `schema` under `name` in the test's supervision tree,
  # with its staging worker and a drain thread.
  defp start_instance!(name, schema, target_schema) do
    start_supervised!({Trellis, instance_options(schema, target_schema, name)})
  end

  defp instance_options(schema, target_schema, name) do
    [
      name: name,
      url: TestCluster.info()["dsn"],
      schema: schema,
      target_schema: target_schema,
      staging: true,
      drain_threads: 1
    ]
  end

  test "two supervised handles with distinct names are two instances" do
    tree =
      start_supervised!(%{
        id: :two_handles,
        type: :supervisor,
        start:
          {Supervisor, :start_link,
           [
             [
               {Trellis, instance_options("two_shared_a", "two_shared_a_targets", @a)},
               {Trellis, instance_options("two_shared_b", "two_shared_b_targets", @b)}
             ],
             [strategy: :one_for_one]
           ]}
      })

    # One tree holds both: the child ids are the names.
    assert [@a, @b] == Supervisor.which_children(tree) |> Enum.map(&elem(&1, 0)) |> Enum.sort()
    pid_a = Process.whereis(@a)
    pid_b = Process.whereis(@b)
    assert is_pid(pid_a) and is_pid(pid_b) and pid_a != pid_b

    assert {:ok, %Trellis.Config{schema: "two_shared_a", target_schema: "two_shared_a_targets"}} =
             Trellis.config(@a)

    assert {:ok, %Trellis.Config{schema: "two_shared_b", target_schema: "two_shared_b_targets"}} =
             Trellis.config(@b)

    # Stopping one leaves the other running and answering.
    :ok = Supervisor.terminate_child(tree, @a)
    refute Process.alive?(pid_a)
    assert Process.alive?(pid_b)
    assert {:ok, nil} = Trellis.status(@b, "no_transform_writes_this")
  end

  test "two instances define transforms over one source table, and both keep their targets converged" do
    pg = TestCluster.postgrex!()
    Postgrex.query!(pg, "create table two_shared_src (id integer primary key, price integer)", [])
    Postgrex.query!(pg, "insert into two_shared_src (id, price) values (1, 5), (2, 7)", [])

    start_instance!(@a, "two_shared_a", "two_shared_a_targets")
    start_instance!(@b, "two_shared_b", "two_shared_b_targets")

    Trellis.define!(@a, "TRANSFORM shared_prices FROM two_shared_src SELECT price AS price")

    Trellis.define!(
      @b,
      "TRANSFORM shared_doubled FROM two_shared_src SELECT price + price AS doubled"
    )

    await_live(@a, "shared_prices")
    await_live(@b, "shared_doubled")

    # Existing rows were carried across by the backfills.
    converge([@a, @b])
    assert_shared_converged(pg)

    # Churn through the one table: both instances' capture sees every write.
    for statements <- [
          ["insert into two_shared_src (id, price) values (3, 11), (4, null)"],
          [
            "update two_shared_src set price = price + 100 where id in (1, 3)",
            "delete from two_shared_src where id = 2"
          ],
          [
            "insert into two_shared_src (id, price) values (2, 20)",
            "update two_shared_src set price = null where id = 1"
          ]
        ] do
      for statement <- statements, do: Postgrex.query!(pg, statement, [])
      converge([@a, @b])
      assert_shared_converged(pg)
    end

    # Each instance captured the one table with triggers of its own.
    triggers =
      rows(
        pg,
        "select tgname::text from pg_trigger " <>
          "where tgrelid = 'public.two_shared_src'::regclass and not tgisinternal"
      )
      |> List.flatten()

    assert "two_shared_a_capture_insert" in triggers, inspect(triggers)
    assert "two_shared_b_capture_insert" in triggers, inspect(triggers)
  end

  test "an instance defines a transform over another instance's target, and matches the oracle over it" do
    pg = TestCluster.postgrex!()
    Postgrex.query!(pg, "create table two_chain_src (id integer primary key, price integer)", [])
    Postgrex.query!(pg, "insert into two_chain_src (id, price) values (1, 5), (2, 7)", [])

    start_instance!(@a, "two_chain_a", "two_chain_a_targets")
    start_instance!(@b, "two_chain_b", "two_chain_b_targets")

    Trellis.define!(@a, "TRANSFORM chain_prices FROM two_chain_src SELECT price AS price")
    await_live(@a, "chain_prices")

    # B reads A's 1-1 target like any table with a primary key.
    Trellis.define!(
      @b,
      "TRANSFORM chain_doubled FROM two_chain_a_targets.chain_prices SELECT price + price AS doubled"
    )

    await_live(@b, "chain_doubled")
    converge([@a, @b])
    assert_chain_converged(pg)

    for statements <- [
          ["insert into two_chain_src (id, price) values (3, 11), (4, null)"],
          [
            "update two_chain_src set price = price + 100 where id in (1, 3)",
            "delete from two_chain_src where id = 2"
          ],
          [
            "insert into two_chain_src (id, price) values (2, 20)",
            "update two_chain_src set price = null where id = 1"
          ]
        ] do
      for statement <- statements, do: Postgrex.query!(pg, statement, [])
      # A first: B's token then covers A's writes to its target.
      converge([@a, @b])
      assert_chain_converged(pg)
    end

    # B's target follows A's, row for row, through A's transform.
    assert Postgrex.query!(pg, "select id, price from two_chain_a_targets.chain_prices", []).rows
           |> Enum.sort() ==
             Postgrex.query!(pg, "select id, price from two_chain_src", []).rows |> Enum.sort()
  end

  # Waits for each instance in turn: the one that reads another's target must
  # come after it.
  defp converge(names) do
    for name <- names do
      assert :ok = Trellis.await_converged(name, Trellis.watermark_token!(name), @timeout_ms)
    end
  end

  defp assert_shared_converged(pg) do
    source = rows(pg, "select id, price from two_shared_src order by id")

    assert rows(pg, "select id, price from two_shared_a_targets.shared_prices order by id") ==
             source

    assert rows(pg, "select id, doubled from two_shared_b_targets.shared_doubled order by id") ==
             Enum.map(source, fn [id, price] -> [id, price && price * 2] end)
  end

  defp assert_chain_converged(pg) do
    source = rows(pg, "select id, price from two_chain_src order by id")

    assert rows(pg, "select id, doubled from two_chain_b_targets.chain_doubled order by id") ==
             Enum.map(source, fn [id, price] -> [id, price && price * 2] end)
  end

  defp rows(pg, sql), do: Postgrex.query!(pg, sql, []).rows

  defp await_live(name, target) do
    eventually("#{target} to reach :live", fn ->
      case Trellis.status!(name, target) do
        %Status{status: :live} = status -> {:done, status}
        status -> {:waiting, status}
      end
    end)
  end
end
