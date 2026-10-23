defmodule Trellis.ApplyTest do
  # `apply/2` against a running engine, once per statement form, checking
  # the flattened result each one returns, plus the reads and operational
  # calls that sit alongside it.
  #
  # Shares the suite's one database and runs its one staging worker, so it
  # doesn't run concurrently with the other integration tests.
  use ExUnit.Case, async: false

  import Trellis.Eventually

  alias Trellis.{
    DefinitionSummary,
    Error,
    Status,
    TestCluster
  }

  setup_all do
    trellis = Trellis.connect!(url: TestCluster.info()["dsn"])
    :ok = Trellis.migrate!(trellis)
    :ok = Trellis.shutdown!(trellis)
    :ok
  end

  setup do
    pg = TestCluster.postgrex!()
    trellis = Trellis.connect!(url: TestCluster.info()["dsn"], staging: true, drain_threads: 1)
    on_exit(fn -> Trellis.shutdown(trellis) end)
    %{pg: pg, trellis: trellis}
  end

  test "every statement form round-trips through apply/2", %{pg: pg, trellis: trellis} do
    Postgrex.query!(
      pg,
      "create table pets (id integer primary key, owner_id integer, weight integer)",
      []
    )

    Postgrex.query!(pg, "insert into pets (id, owner_id, weight) values (1, 1, 4)", [])

    # Keyed on each kind's `trellis::StatementKind` name, so a statement form
    # the engine adds fails the last assertion until it is covered here.
    covered = MapSet.new()

    assert {:ok, {:transform_defined, "public.pet_weights"}} =
             Trellis.apply(trellis, "TRANSFORM pet_weights FROM pets SELECT weight AS weight")

    covered = MapSet.put(covered, "define_transform")

    assert [
             %DefinitionSummary{
               target_table: "public.pet_weights",
               created_at: %DateTime{},
               backfill_failure: nil,
               halt: nil
             }
           ] =
             Enum.filter(
               Trellis.definitions!(trellis),
               &(&1.target_table == "public.pet_weights")
             )

    await_status(trellis, "pet_weights", :live)

    assert {:ok, {:altered, altered}} =
             Trellis.apply(trellis, "ALTER TRANSFORM pet_weights ADD weight + weight AS doubled")

    assert %{
             target_table: "public.pet_weights",
             added: ["doubled"],
             dropped: [],
             altered: []
           } = altered

    covered = MapSet.put(covered, "alter_transform")

    assert {:ok, :paused} = Trellis.apply(trellis, "PAUSE TRANSFORM pet_weights")
    assert %Status{status: :paused} = Trellis.status!(trellis, "pet_weights")
    covered = MapSet.put(covered, "pause_transform")

    # A whole-transform resume has no columns to report, and rebuilds.
    assert {:ok, {:resumed, []}} = Trellis.apply(trellis, "RESUME TRANSFORM pet_weights")
    covered = MapSet.put(covered, "resume_transform")
    await_status(trellis, "pet_weights", :live)

    # A definition is paused before it is dropped. Pausing a paused one is the
    # same success.
    assert {:ok, :paused} = Trellis.apply(trellis, "PAUSE TRANSFORM pet_weights")
    assert {:ok, :paused} = Trellis.apply(trellis, "PAUSE TRANSFORM pet_weights")
    assert {:ok, :dropped} = Trellis.apply(trellis, "DROP TRANSFORM pet_weights")
    assert {:ok, nil} = Trellis.status(trellis, "pet_weights")
    covered = MapSet.put(covered, "drop_transform")

    assert {:ok, kinds} = Trellis.Native.statement_kinds()
    assert covered == MapSet.new(kinds)
  end

  test "a statement apply/2 can't carry out is an error, and apply!/2 raises it",
       %{trellis: trellis} do
    assert {:error, %Error{code: :parse}} = Trellis.apply(trellis, "TRANSFORM oops")

    for statement <- [
          "RELATIONSHIP owner FROM pets.owner_id TO owners.id",
          "DROP RELATIONSHIP pets.owner"
        ] do
      assert {:error, %Error{code: :parse}} = Trellis.apply(trellis, statement)
    end

    assert_raise Error, fn -> Trellis.apply!(trellis, "TRANSFORM oops") end

    assert {:error, %Error{code: :not_found}} =
             Trellis.apply(trellis, "PAUSE TRANSFORM no_such_transform")
  end

  test "request_backfill re-reads a captured table and refuses an unknown one",
       %{pg: pg, trellis: trellis} do
    Postgrex.query!(pg, "create table crates (id integer primary key, size integer)", [])
    Trellis.apply!(trellis, "TRANSFORM crate_sizes FROM crates SELECT size AS size")
    await_status(trellis, "crate_sizes", :live)

    assert :ok = Trellis.request_backfill(trellis, "crates")
    await_status(trellis, "crate_sizes", :live)

    assert {:error, %Error{code: :not_found}} =
             Trellis.request_backfill(trellis, "no_such_table")

    assert_raise Error, fn -> Trellis.request_backfill!(trellis, "no_such_table") end
  end

  test "a running handle reports live workers", %{trellis: trellis} do
    assert eventually("a live staging worker", fn ->
             case Trellis.has_live_staging_worker!(trellis) do
               true -> {:done, true}
               false -> {:waiting, false}
             end
           end)

    assert eventually("a live drain worker", fn ->
             case Trellis.has_live_drain_workers!(trellis) do
               true -> {:done, true}
               false -> {:waiting, false}
             end
           end)
  end

  test "await_converged waits out a write, by a token read back unchanged",
       %{pg: pg, trellis: trellis} do
    Postgrex.query!(pg, "create table bolts (id integer primary key, length integer)", [])
    Trellis.apply!(trellis, "TRANSFORM bolt_lengths FROM bolts SELECT length AS length")
    await_status(trellis, "bolt_lengths", :live)

    Postgrex.query!(pg, "insert into bolts (id, length) values (1, 30)", [])
    assert {:ok, token} = Trellis.watermark_token(trellis)
    assert is_binary(token)
    assert :ok = Trellis.await_converged(trellis, token, 30_000)

    # No polling: the write has reached the target once the call returns.
    assert Postgrex.query!(pg, "select id, length from bolt_lengths", []).rows == [[1, 30]]

    assert {:error, %Error{code: :validation}} =
             Trellis.await_converged(trellis, "not a token", 1_000)

    for timeout <- [-1, 2 ** 64] do
      assert {:error, %Error{code: :validation}} =
               Trellis.await_converged(trellis, token, timeout)
    end
  end

  defp await_status(trellis, target, wanted) do
    eventually("#{target} to reach #{inspect(wanted)}", fn ->
      case Trellis.status!(trellis, target) do
        %Status{status: ^wanted} = status -> {:done, status}
        status -> {:waiting, status}
      end
    end)
  end
end
