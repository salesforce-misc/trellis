defmodule Trellis.TestSupportTest do
  # Trellis.TestSupport (issue #1181): a fixture load that disables the
  # triggers, the way Rails' disable_referential_integrity does, pauses the
  # readers of the table (a capture failure of kind :disruption), and the
  # helper resumes them and waits for exact targets. Mirrors the Ruby
  # suite's test_support_test.rb.
  #
  # Each test has a cluster of its own: the shared one holds definitions
  # other tests dropped the sources of, and the helper rightly pauses and
  # refuses those.
  use ExUnit.Case, async: false

  import Trellis.Eventually

  alias Trellis.{CaptureFailure, Status, TestCluster, TestSupport}

  setup do
    cluster = TestCluster.private!()
    dsn = cluster["dsn"]
    pg = TestCluster.postgrex!(cluster)

    migrator = Trellis.connect!(url: dsn)
    :ok = Trellis.migrate!(migrator)
    :ok = Trellis.shutdown!(migrator)

    # Registered after `private!/0`'s teardown, so it runs first.
    trellis = Trellis.connect!(url: dsn, staging: true, drain_threads: 1)
    on_exit(fn -> Trellis.shutdown(trellis) end)
    %{pg: pg, trellis: trellis}
  end

  test "a fixture load is resumed and the targets equal the oracle", %{pg: pg, trellis: trellis} do
    sql(pg, "create table loads (id integer primary key, g integer not null, n integer not null)")
    sql(pg, "insert into loads select i, i % 3, i from generate_series(1, 10) i")
    Trellis.apply!(trellis, "TRANSFORM load_copy FROM loads SELECT n AS n")

    Trellis.apply!(
      trellis,
      "TRANSFORM load_sums FROM loads GROUP BY g WITHOUT SOURCE INDEX SELECT g AS g, SUM(n) AS total"
    )

    Trellis.apply!(trellis, "TRANSFORM load_operator FROM loads SELECT n AS n")
    for target <- ~w(load_copy load_sums load_operator), do: await_live(trellis, target)
    Trellis.apply!(trellis, "PAUSE TRANSFORM load_operator")

    # What a fixture loader does: every trigger disabled for the load, and
    # ENABLE TRIGGER ALL after it.
    sql(pg, "alter table loads disable trigger all")
    sql(pg, "insert into loads select i, i % 3, i from generate_series(11, 40) i")
    sql(pg, "alter table loads enable trigger all")

    # Nothing has told Trellis yet; the helper's own check pauses them.
    assert {:ok, resumed} = TestSupport.resume_capture_disruptions(trellis)
    assert Enum.sort(resumed) == ["public.load_copy", "public.load_sums"]

    assert column(pg, "select n::text from load_copy order by 1") ==
             column(pg, "select i::text from generate_series(1, 40) i order by 1")

    assert rows(pg, "select g::text, total::text from load_sums order by g") ==
             rows(pg, "select g::text, sum(n)::text from loads group by g order by g")

    # The operator's pause is the operator's: still paused. The disruption
    # seen while it was paused is recorded as kind :capture, not
    # :disruption, so the helper leaves it to the operator.
    assert %Status{status: :paused, capture_failure: %CaptureFailure{kind: :capture}} =
             Trellis.status!(trellis, "load_operator")

    # And nothing is left to do.
    assert {:ok, []} = TestSupport.resume_capture_disruptions(trellis)
  end

  test "a disruption is a capture failure of kind :disruption", %{pg: pg, trellis: trellis} do
    sql(pg, "create table kinds (id integer primary key, n integer not null)")
    Trellis.apply!(trellis, "TRANSFORM kind_copy FROM kinds SELECT n AS n")
    await_live(trellis, "kind_copy")

    sql(pg, "alter table kinds disable trigger all")
    sql(pg, "alter table kinds enable trigger all")
    assert Trellis.detect_capture_disruptions!(trellis) == ["public.kinds"]

    assert %Status{status: :paused, capture_failure: %CaptureFailure{kind: :disruption} = failure} =
             Trellis.status!(trellis, "kind_copy")

    assert failure.error =~ "changed outside Trellis"
    assert TestSupport.resume_capture_disruptions!(trellis) == ["public.kind_copy"]
    assert %Status{status: :live} = Trellis.status!(trellis, "kind_copy")
  end

  test "a refused resume leaves the definition paused and is surfaced", %{
    pg: pg,
    trellis: trellis
  } do
    sql(pg, "create table gone (id integer primary key, n integer not null)")
    sql(pg, "create table kept (id integer primary key, n integer not null)")
    Trellis.apply!(trellis, "TRANSFORM gone_copy FROM gone SELECT n AS n")
    Trellis.apply!(trellis, "TRANSFORM kept_copy FROM kept SELECT n AS n")
    await_live(trellis, "gone_copy")
    await_live(trellis, "kept_copy")

    sql(pg, "alter table gone rename to gone_away")
    sql(pg, "alter table kept disable trigger all")
    sql(pg, "alter table kept enable trigger all")
    sql(pg, "insert into kept values (1, 7)")

    assert {:error, {:resume_refused, refused}} = TestSupport.resume_capture_disruptions(trellis)
    assert Map.keys(refused) == ["public.gone_copy"]
    assert refused["public.gone_copy"] =~ "gone"

    error =
      assert_raise TestSupport.ResumeRefused, fn ->
        TestSupport.resume_capture_disruptions!(trellis)
      end

    assert Map.keys(error.refused) == ["public.gone_copy"]

    # The refused one stays paused for its disruption; the other was resumed
    # and waited for, and holds its row.
    assert %Status{status: :paused, capture_failure: %CaptureFailure{kind: :disruption}} =
             Trellis.status!(trellis, "gone_copy")

    assert %Status{status: :live} = Trellis.status!(trellis, "kept_copy")
    assert column(pg, "select n::text from kept_copy") == ["7"]
  end

  # A table recreated with a wider column (a test schema reloaded after a
  # migration): the resume re-types Trellis's copy first, reporting a
  # "resuming:" capture failure meanwhile, which is a resume in progress, not
  # a refusal.
  test "a resume that re-types first is waited for", %{pg: pg, trellis: trellis} do
    sql(pg, "create table widened (id integer primary key, n integer not null)")
    sql(pg, "insert into widened values (1, 1)")
    Trellis.apply!(trellis, "TRANSFORM widened_copy FROM widened SELECT n AS n")
    await_live(trellis, "widened_copy")

    sql(pg, "drop table widened")
    sql(pg, "create table widened (id integer primary key, n bigint not null)")
    sql(pg, "insert into widened values (1, 5000000000)")

    assert TestSupport.resume_capture_disruptions!(trellis) == ["public.widened_copy"]
    assert %Status{status: :live} = Trellis.status!(trellis, "widened_copy")
    assert column(pg, "select n::text from widened_copy") == ["5000000000"]
  end

  defp sql(pg, statement), do: Postgrex.query!(pg, statement, [])
  defp rows(pg, statement), do: sql(pg, statement).rows
  defp column(pg, statement), do: pg |> rows(statement) |> List.flatten()

  defp await_live(trellis, target) do
    eventually("#{target} to reach :live", fn ->
      case Trellis.status!(trellis, target) do
        %Status{status: :live} = status -> {:done, status}
        status -> {:waiting, status}
      end
    end)
  end
end
