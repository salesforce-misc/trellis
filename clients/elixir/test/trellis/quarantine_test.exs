defmodule Trellis.QuarantineTest do
  # The quarantine path has the most structure crossing the NIF boundary
  # (addresses, state atoms, times, the opaque cursor), so this drives it end
  # to end through real source rows: poison a column, read it back every way
  # the API offers, resume it with a statement, and see it clear.
  #
  # Shares the suite's one database and takes the replication slot, so it
  # doesn't run concurrently with the other integration tests.
  use ExUnit.Case, async: false

  import Trellis.Eventually

  alias Trellis.{Definition, PoisonSample, QuarantineEntry, SamplePage, Status, TestCluster}

  # The column fuse trips once this many distinct rows have failed on one
  # column (`staging::quarantine::DEFAULT_COLUMN_DEATH_THRESHOLD`).
  @column_death_threshold 5

  setup_all do
    trellis = Trellis.connect!(url: TestCluster.info()["dsn"])
    :ok = Trellis.migrate!(trellis)
    :ok = Trellis.shutdown!(trellis)
    :ok
  end

  test "a poisoned column is listed, sampled page by page, resumed and cleared" do
    pg = TestCluster.postgrex!()
    Postgrex.query!(pg, "create table ledger (id integer primary key, n integer)", [])

    trellis = Trellis.connect!(url: TestCluster.info()["dsn"], staging: true, drain_threads: 1)
    on_exit(fn -> Trellis.shutdown(trellis) end)

    # `integer + integer` is an `integer`, so doubling anything past
    # 2^30 overflows it, on the engine exactly as on Postgres.
    assert {:ok, {:transform_defined, %Definition{}}} =
             Trellis.apply(
               trellis,
               "TRANSFORM doubled_ledger FROM ledger SELECT n + n AS doubled"
             )

    await_live(trellis, "doubled_ledger")
    assert Trellis.quarantined!(trellis) == []

    ids = Enum.to_list(1..@column_death_threshold)

    for id <- ids do
      Postgrex.query!(pg, "insert into ledger (id, n) values ($1, 2000000000)", [id])
    end

    paused =
      eventually("doubled_ledger.doubled to pause", fn ->
        case Trellis.quarantine_status!(trellis, "doubled_ledger.doubled") do
          %QuarantineEntry{state: :paused} = entry -> {:done, entry}
          entry -> {:waiting, entry}
        end
      end)

    assert %QuarantineEntry{
             target: "doubled_ledger.doubled",
             paused_at: %DateTime{} = paused_at,
             last_error: last_error
           } = paused

    assert last_error =~ "out of range"
    assert DateTime.diff(DateTime.utc_now(), paused_at, :second) in 0..60

    # The pause is visible while the batch that tripped it can still be in
    # flight, and a resume that lands before that batch settles sees it
    # retried against its original row images, which pauses the column
    # again (#585). Wait for everything written so far to drain first, the
    # way an operator resuming a column has to until that's fixed.
    assert :ok = Trellis.await_converged(trellis, Trellis.watermark_token!(trellis), 30_000)

    # The flat list carries the same entry, by the same address.
    assert paused in Trellis.quarantined!(trellis)

    # The transform itself stays live: a column pause is the column's alone.
    assert %QuarantineEntry{target: "doubled_ledger", state: :live} =
             Trellis.quarantine_status!(trellis, "doubled_ledger")

    # Two rows a page, so the cursor has to carry three pages.
    pages = sample_pages(trellis, "doubled_ledger.doubled", 2)
    assert Enum.map(pages, &length(&1.samples)) == [2, 2, 1, 0]

    samples = Enum.flat_map(pages, & &1.samples)
    assert Enum.map(samples, & &1.key) == Enum.map(ids, &to_string/1)

    for sample <- samples do
      assert %PoisonSample{src_table: "public.ledger", error_message: message} = sample
      assert message =~ "out of range"
    end

    # Polling past the end hands back the cursor it was asked for, not nil,
    # so the next poll doesn't start over from the first page.
    [last_full, empty] = Enum.take(pages, -2)
    assert empty.next_cursor == last_full.next_cursor

    # Fix the data, then resume: the resume recomputes the column from the
    # source rows as they are now.
    Postgrex.query!(pg, "update ledger set n = id", [])

    assert {:ok, {:resumed, ["doubled_ledger.doubled"]}} =
             Trellis.apply(trellis, "RESUME TRANSFORM doubled_ledger.doubled")

    assert %QuarantineEntry{state: :live, paused_at: nil, last_error: nil} =
             Trellis.quarantine_status!(trellis, "doubled_ledger.doubled")

    assert Trellis.quarantined!(trellis) == []

    assert %SamplePage{samples: []} =
             Trellis.sample_quarantined!(trellis, "doubled_ledger.doubled")

    eventually("doubled_ledger to hold the fixed rows", fn ->
      case Postgrex.query!(pg, "select id, doubled from doubled_ledger order by id", []).rows do
        [[1, 2], [2, 4], [3, 6], [4, 8], [5, 10]] = rows -> {:done, rows}
        rows -> {:waiting, rows}
      end
    end)
  end

  # A failure the column fuse can't pin on one column (here, the target
  # table's own check constraint refusing the write) poisons the whole key,
  # which `poisoned_since/2` reports and the transform's own address samples.
  #
  # No public call releases a poisoned key, and a held one stops
  # `await_converged/3` converging past it (#588), so this runs on a cluster
  # of its own rather than wedging the shared one.
  test "a poisoned key is reported by poisoned_since and sampled under its transform" do
    cluster = TestCluster.private!()
    dsn = cluster["dsn"]
    pg = TestCluster.postgrex!(cluster)
    Postgrex.query!(pg, "create table gizmos (id integer primary key, price integer)", [])

    migrator = Trellis.connect!(url: dsn)
    :ok = Trellis.migrate!(migrator)
    :ok = Trellis.shutdown!(migrator)

    # Registered after `private!/0`'s teardown, so it runs first: on_exit
    # callbacks run last-in, first-out.
    trellis = Trellis.connect!(url: dsn, staging: true, drain_threads: 1)
    on_exit(fn -> Trellis.shutdown(trellis) end)

    Trellis.apply!(trellis, "TRANSFORM gizmo_prices FROM gizmos SELECT price AS price")
    await_live(trellis, "gizmo_prices")
    Postgrex.query!(pg, "alter table gizmo_prices add constraint cheap check (price < 100)", [])

    before = DateTime.utc_now()
    Postgrex.query!(pg, "insert into gizmos (id, price) values (1, 5), (2, 500)", [])

    [entry] =
      eventually("gizmos key 2 to be poisoned", fn ->
        case Trellis.poisoned_since!(trellis, before) do
          [] -> {:waiting, []}
          entries -> {:done, entries}
        end
      end)

    assert %Trellis.PoisonEntry{
             src_table: "public.gizmos",
             key: "2",
             last_error: last_error,
             poisoned_at: %DateTime{} = poisoned_at
           } = entry

    assert last_error =~ "cheap"
    assert DateTime.compare(poisoned_at, before) == :gt

    # The watermark is exclusive: nothing was poisoned after the entry itself.
    assert Trellis.poisoned_since!(trellis, poisoned_at) == []

    assert %SamplePage{samples: [%PoisonSample{src_table: "public.gizmos", key: "2"}]} =
             Trellis.sample_quarantined!(trellis, "gizmo_prices")

    # One poisoned key is below the whole-transform fuse: the transform stays
    # live, and the good row went through.
    assert %QuarantineEntry{target: "gizmo_prices", state: :live} =
             Trellis.quarantine_status!(trellis, "gizmo_prices")

    assert Postgrex.query!(pg, "select id, price from gizmo_prices", []).rows == [[1, 5]]
  end

  test "sample_quarantined refuses a malformed cursor or limit before calling the engine" do
    trellis = Trellis.connect!(url: TestCluster.info()["dsn"])
    on_exit(fn -> Trellis.shutdown(trellis) end)

    assert {:error, %Trellis.Error{code: :validation}} =
             Trellis.sample_quarantined(trellis, "t.c", after: "not a cursor")

    for limit <- [0, -1, 1.5, 2 ** 63] do
      assert {:error, %Trellis.Error{code: :validation, message: message}} =
               Trellis.sample_quarantined(trellis, "t.c", limit: limit)

      assert message =~ ":limit"
    end

    assert {:error, %Trellis.Error{code: :validation}} =
             Trellis.sample_quarantined(trellis, "t.c", page: 2)
  end

  defp await_live(trellis, target) do
    eventually("#{target} to reach :live", fn ->
      case Trellis.status!(trellis, target) do
        %Status{status: :live} = status -> {:done, status}
        status -> {:waiting, status}
      end
    end)
  end

  # Every page of `target`'s sample, `limit` rows at a time, through to the
  # first empty page.
  defp sample_pages(trellis, target, limit, cursor \\ nil) do
    page = Trellis.sample_quarantined!(trellis, target, limit: limit, after: cursor)

    case page.samples do
      [] -> [page]
      _ -> [page | sample_pages(trellis, target, limit, page.next_cursor)]
    end
  end
end
