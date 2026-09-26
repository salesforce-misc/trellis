defmodule Trellis.SelfCheckTest do
  # `self_check/3` end to end: audit a live target that matches its source,
  # page through it, then corrupt one target cell behind the engine's back
  # and see the audit name it.
  #
  # Shares the suite's one database and takes the replication slot, so it
  # doesn't run concurrently with the other integration tests.
  use ExUnit.Case, async: false

  import Trellis.Eventually

  alias Trellis.{Divergence, SelfCheckReport, Status, TestCluster}

  @timeout_ms 30_000

  setup_all do
    trellis = Trellis.connect!(url: TestCluster.info()["dsn"])
    :ok = Trellis.migrate!(trellis)
    :ok = Trellis.shutdown!(trellis)
    :ok
  end

  test "a matching target converges, pages by next_after, and a corrupted cell diverges" do
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

    assert %SelfCheckReport{
             target: "audited_widget_totals",
             outcome: :converged,
             divergences: [],
             rows_compared: 2,
             next_after: nil,
             checked_through: checked_through
           } =
             Trellis.self_check!(trellis, "audited_widget_totals",
               limit: 100,
               timeout_ms: @timeout_ms
             )

    # The position it checked through is a real watermark token.
    assert :ok = Trellis.await_converged(trellis, checked_through, @timeout_ms)

    # One key a page: sweeping through `next_after` visits each key once
    # and ends on a `nil` cursor. A page that fills its limit exactly still
    # hands back a cursor (the engine can't know no key follows), so the
    # sweep ends on an empty third page.
    pages = sweep(trellis, "audited_widget_totals", 1)

    assert Enum.map(pages, &{&1.rows_compared, is_binary(&1.next_after)}) ==
             [{1, true}, {1, true}, {0, false}]

    assert Enum.all?(pages, &(&1.outcome == :converged))

    # The engine wrote 11 (10 + 1); this test overwrites it.
    Postgrex.query!(pg, "update audited_widget_totals set total = 9999 where id = '1'", [])

    assert {:ok,
            %SelfCheckReport{
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
            }} =
             Trellis.self_check(trellis, "audited_widget_totals",
               limit: 100,
               timeout_ms: @timeout_ms
             )
  end

  test "an unknown target is :not_found, and bad options are :validation" do
    trellis = Trellis.connect!(url: TestCluster.info()["dsn"])
    on_exit(fn -> Trellis.shutdown(trellis) end)

    assert {:error, %Trellis.Error{code: :not_found, message: message}} =
             Trellis.self_check(trellis, "no_such_target", limit: 10, timeout_ms: 1_000)

    assert message =~ "no_such_target"

    for options <- [
          [timeout_ms: 1_000],
          [limit: 10],
          [limit: 0, timeout_ms: 1_000],
          [limit: 10, timeout_ms: -1],
          [limit: 10, timeout_ms: 1_000, mode: :lenient],
          [limit: 10, timeout_ms: 1_000, after: 5],
          [limit: 10, timeout_ms: 1_000, page: 2]
        ] do
      assert {:error, %Trellis.Error{code: :validation}} =
               Trellis.self_check(trellis, "no_such_target", options),
             inspect(options)
    end
  end

  # Every page of a strict audit of `target`, `limit` keys at a time,
  # through to the one whose `next_after` is `nil`.
  defp sweep(trellis, target, limit, cursor \\ nil) do
    report =
      Trellis.self_check!(trellis, target,
        limit: limit,
        timeout_ms: @timeout_ms,
        mode: :strict,
        after: cursor
      )

    case report.next_after do
      nil -> [report]
      next -> [report | sweep(trellis, target, limit, next)]
    end
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
