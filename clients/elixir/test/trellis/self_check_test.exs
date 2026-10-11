defmodule Trellis.SelfCheckTest do
  # `self_check/3` end to end: start a background check of a live target that
  # matches its source, read the job back by id, then corrupt one target cell
  # behind the engine's back and see the check name it.
  #
  # Shares the suite's one database and runs its one staging worker, so it
  # doesn't run concurrently with the other integration tests.
  use ExUnit.Case, async: false

  import Trellis.Eventually

  alias Trellis.{Divergence, SelfCheckJob, SelfCheckReport, Status, TestCluster}

  @timeout_ms 30_000

  setup_all do
    trellis = Trellis.connect!(url: TestCluster.info()["dsn"])
    :ok = Trellis.migrate!(trellis)
    :ok = Trellis.shutdown!(trellis)
    :ok
  end

  test "a matching target converges and a corrupted cell diverges" do
    pg = TestCluster.postgrex!()

    Postgrex.query!(
      pg,
      "create table audited_widgets (id integer primary key, price integer, tax integer)",
      []
    )

    trellis = Trellis.connect!(url: TestCluster.info()["dsn"], staging: true, drain_threads: 1)
    on_exit(fn -> Trellis.shutdown(trellis) end)

    Trellis.apply!(
      trellis,
      "TRANSFORM audited_widget_totals FROM audited_widgets SELECT price + tax AS total"
    )

    await_live(trellis, "audited_widget_totals")

    Postgrex.query!(
      pg,
      "insert into audited_widgets (id, price, tax) values (1, 10, 1), (2, 20, 2)",
      []
    )

    :ok = Trellis.await_converged!(trellis, Trellis.watermark_token!(trellis), @timeout_ms)

    # The call returns the job; a drain worker runs it.
    started = Trellis.self_check!(trellis, "audited_widget_totals", timeout_ms: @timeout_ms)

    assert %SelfCheckJob{target: "audited_widget_totals", mode: :standard} = started
    assert started.state in [:queued, :running, :done]

    assert %SelfCheckJob{
             state: :done,
             rows_compared: 2,
             error: nil,
             report: %SelfCheckReport{
               target: "audited_widget_totals",
               outcome: :converged,
               divergences: [],
               rows_compared: 2,
               truncated: false,
               drain_failures: [],
               checked_through: checked_through
             }
           } = job = await_job(trellis, started.id)

    assert SelfCheckJob.finished?(job)

    # The position it checked through is a real watermark token.
    assert :ok = Trellis.await_converged(trellis, checked_through, @timeout_ms)

    # The engine wrote 11 (10 + 1); this test overwrites it.
    Postgrex.query!(pg, "update audited_widget_totals set total = 9999 where id = '1'", [])

    # A new check replaces the finished one: its id is gone.
    second =
      Trellis.self_check!(trellis, "audited_widget_totals",
        timeout_ms: @timeout_ms,
        mode: :strict
      )

    assert {:ok, nil} = Trellis.self_check_job(trellis, started.id)

    assert %SelfCheckJob{
             report: %SelfCheckReport{
               outcome: :diverged,
               divergences: [
                 %Divergence{
                   kind: :cell,
                   key: "1",
                   column: "total",
                   persisted: "9999",
                   recomputed: "11"
                 }
               ]
             }
           } = await_job(trellis, second.id)

    # A source attached as a partition is no longer captured whole, which the
    # capture audit reports before it compares anything. The staging worker
    # doesn't undo an ATTACH, so the report is stable.
    Postgrex.query!(
      pg,
      "create table audited_widgets_all (id integer not null, price integer, tax integer) " <>
        "partition by range (id)",
      []
    )

    Postgrex.query!(
      pg,
      "alter table audited_widgets_all attach partition audited_widgets " <>
        "for values from (minvalue) to (maxvalue)",
      []
    )

    on_exit(fn ->
      pg = TestCluster.postgrex!()
      Postgrex.query!(pg, "alter table audited_widgets_all detach partition audited_widgets", [])
    end)

    third = Trellis.self_check!(trellis, "audited_widget_totals", timeout_ms: @timeout_ms)

    assert %SelfCheckJob{
             report: %SelfCheckReport{
               outcome: :diverged,
               rows_compared: 0,
               divergences: [
                 %Divergence{kind: :capture, table: "public.audited_widgets", detail: detail}
               ]
             }
           } = await_job(trellis, third.id)

    assert detail =~ "partition of public.audited_widgets_all"
  end

  # With no drain worker anywhere, a job is registered and stays queued, and a
  # second call for the target returns the same job.
  test "a job no worker runs stays queued, and a second call returns it" do
    pg = TestCluster.postgrex!()
    Postgrex.query!(pg, "create table queued_widgets (id integer primary key, price integer)", [])

    trellis = Trellis.connect!(url: TestCluster.info()["dsn"])
    on_exit(fn -> Trellis.shutdown(trellis) end)

    Trellis.apply!(
      trellis,
      "TRANSFORM queued_widget_prices FROM queued_widgets SELECT price AS price"
    )

    job = Trellis.self_check!(trellis, "queued_widget_prices", timeout_ms: 1_000, mode: :strict)

    assert %SelfCheckJob{state: :queued, mode: :strict, rows_compared: 0, report: nil, error: nil} =
             job

    refute SelfCheckJob.finished?(job)

    assert job == Trellis.self_check!(trellis, "queued_widget_prices", timeout_ms: 5_000)
    assert {:ok, job} == Trellis.self_check_job(trellis, job.id)
    assert {:ok, nil} = Trellis.self_check_job(trellis, job.id + 1_000)
  end

  test "an unknown target is :not_found, and bad options are :validation" do
    trellis = Trellis.connect!(url: TestCluster.info()["dsn"])
    on_exit(fn -> Trellis.shutdown(trellis) end)

    assert {:error, %Trellis.Error{code: :not_found, message: message}} =
             Trellis.self_check(trellis, "no_such_target", timeout_ms: 1_000)

    assert message =~ "no_such_target"

    for options <- [
          [],
          [timeout_ms: -1],
          [timeout_ms: 1_000, mode: :lenient],
          [limit: 10, timeout_ms: 1_000],
          [timeout_ms: 1_000, after: "1"]
        ] do
      assert {:error, %Trellis.Error{code: :validation}} =
               Trellis.self_check(trellis, "no_such_target", options),
             inspect(options)
    end
  end

  defp await_job(trellis, id) do
    eventually("self_check job #{id} to finish", fn ->
      case Trellis.self_check_job!(trellis, id) do
        %SelfCheckJob{} = job ->
          if SelfCheckJob.finished?(job), do: {:done, job}, else: {:waiting, job}

        nil ->
          {:waiting, nil}
      end
    end)
  end

  defp await_live(trellis, target) do
    eventually("#{target} to reach :live", fn ->
      case Trellis.status!(trellis, target) do
        %Status{status: :live} = status -> {:done, status}
        status -> {:waiting, status}
      end
    end)
  end
end
