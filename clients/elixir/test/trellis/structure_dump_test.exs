defmodule Trellis.StructureDumpTest do
  # `mix trellis.dump` + `mix trellis.load` (which runs `mix ecto.load`), the
  # way a host runs them, against a cluster of its own; `dump/2`'s redo with
  # a pg_dump function that races a define deterministically. No polling:
  # the loaded database is read, not waited on.
  use ExUnit.Case, async: false

  alias Trellis.{Status, StructureDump, TestCluster, TestRepo}

  @moduletag :tmp_dir

  setup_all do
    cluster = TestCluster.private!()
    {:ok, _} = Application.ensure_all_started(:ecto_sql)
    {:ok, _} = Application.ensure_all_started(:postgrex)
    %{cluster: cluster}
  end

  setup %{cluster: cluster, tmp_dir: dir} do
    put_repo_config(cluster, dump_path: Path.join(dir, "structure.sql"))
    start_supervised!(TestRepo)
    %{pg: TestCluster.postgrex!(cluster)}
  end

  test "a structure file dumped and loaded restores the definitions",
       %{cluster: cluster, pg: pg, tmp_dir: dir} do
    source = define_widgets(cluster, pg, "new")
    on_exit(fn -> Trellis.shutdown(source) end)

    # `--prefix public` makes pg_dump write `CREATE SCHEMA public`.
    path = Path.join(dir, "structure.sql")

    Mix.Tasks.Trellis.Dump.run(
      ~w(-r Trellis.TestRepo --dump-path #{path} --prefix public --quiet)
    )

    text = File.read!(path)
    assert text =~ "CREATE TABLE public.dump_widget_prices_new"

    assert text =~
             ~r/insert into "trellis".restore_queue values \(\d+, 'public.dump_widgets_new', 'public.dump_widget_prices_new'/

    assert text =~ "CREATE SCHEMA IF NOT EXISTS public;"
    # Trellis's internals are left out: its schema, the capture triggers and
    # the 1-1 target's ledger.
    refute text =~ "CREATE SCHEMA trellis"
    refute text =~ "transform_definitions"
    refute text =~ "CREATE TRIGGER"
    refute text =~ "__ledger"

    assert_loads(cluster, pg, path, "new")
  end

  test "a structure file in the older format still loads",
       %{cluster: cluster, pg: pg, tmp_dir: dir} do
    source = define_widgets(cluster, pg, "old")
    on_exit(fn -> Trellis.shutdown(source) end)

    # What `mix trellis.dump` wrote before: `mix ecto.dump` with the Trellis
    # schema in it, then `append/2`.
    path = Path.join(dir, "structure.sql")

    config =
      Keyword.merge(TestRepo.config(), dump_path: path, dump_prefixes: ["public", "trellis"])

    {:ok, ^path} = Ecto.Adapters.Postgres.structure_dump(dir, config)
    assert {:ok, ^path} = StructureDump.append(TestRepo, dump_prefixes: ["public", "trellis"])
    text = File.read!(path)
    assert text =~ ~s(insert into "trellis"."transform_definitions")
    # Runtime state is never dumped.
    refute text =~ ~s(insert into "trellis"."backfill_chunks")

    assert_loads(cluster, pg, path, "old")
  end

  test "the target schema joins the dump's prefixes, and pg_dump leaves Trellis out" do
    {:ok, source} = Trellis.connect(url: TestRepo.config()[:trellis][:url])
    on_exit(fn -> Trellis.shutdown(source) end)
    Trellis.migrate!(source)
    me = self()

    pg_dump = fn plan, args ->
      send(me, {:pg_dump, plan.config[:dump_prefixes], args})
      File.write!(plan.path, "-- the dump\n")
    end

    assert {:ok, [path]} = StructureDump.dump([{TestRepo, [dump_prefixes: ["app"]]}], pg_dump)
    assert_received {:pg_dump, ["app", "public"], [~s(--exclude-schema="trellis") | _ledgers]}

    assert File.read!(path) =~
             ~r/\A-- the dump\n-- Trellis definitions \(trellis::dump::schema_dump\)/

    # Without prefixes every schema is dumped, and only the exclusions are added.
    assert {:ok, _} = StructureDump.dump([{TestRepo, [dump_prefixes: nil]}], pg_dump)
    assert_received {:pg_dump, nil, [~s(--exclude-schema="trellis") | _ledgers]}
  end

  test "a dump_prefixes without the Trellis schema is refused naming it" do
    assert {:error, message} = StructureDump.append(TestRepo, dump_prefixes: ["public"])
    assert message =~ ~s(the Trellis schema "trellis" is not in :dump_prefixes)
    assert message =~ ~s(dump_prefixes: ["public", "trellis"])
  end

  test "a repo with no :trellis key is refused naming it", %{cluster: cluster, tmp_dir: dir} do
    stop_supervised!(TestRepo)
    put_repo_config(cluster, dump_path: Path.join(dir, "structure.sql"), trellis: nil)
    start_supervised!(TestRepo)

    assert {:error, message} = StructureDump.append(TestRepo)
    assert message =~ "Trellis.TestRepo has no Trellis to dump"
  end

  test "a second append replaces the appendix and leaves the rest of the file", %{
    pg: pg,
    tmp_dir: dir
  } do
    Postgrex.query!(pg, "create table dump_gadgets (id integer primary key, price integer)", [])
    {:ok, source} = Trellis.connect(url: TestRepo.config()[:trellis][:url])
    on_exit(fn -> Trellis.shutdown(source) end)
    Trellis.migrate!(source)

    path = Path.join(dir, "structure.sql")
    File.write!(path, "create table t ();\n-- Trellis definitions (stale)\ninsert into stale;\n")

    assert {:ok, ^path} = StructureDump.append(TestRepo)
    text = File.read!(path)
    assert String.starts_with?(text, "create table t ();\n-- Trellis definitions (trellis::")
    refute text =~ "stale"
  end

  test "a define that commits during the pg_dump redoes the dump", %{pg: pg, tmp_dir: dir} do
    Postgrex.query!(pg, "create table dump_raced (id integer primary key, price integer)", [])
    {:ok, source} = Trellis.connect(url: TestRepo.config()[:trellis][:url])
    on_exit(fn -> Trellis.shutdown(source) end)
    Trellis.migrate!(source)
    runs = :counters.new(1, [])

    # The define commits right after the first pg_dump, which so misses its
    # target table; the definitions read after the dump then differ from
    # those read before it.
    dump = fn plan, args ->
      :ok = StructureDump.pg_dump(plan, args)
      :counters.add(runs, 1, 1)

      if :counters.get(runs, 1) == 1 do
        Trellis.define!(
          source,
          "TRANSFORM dump_raced_prices FROM dump_raced SELECT price AS price"
        )
      end

      :ok
    end

    assert {:ok, [path]} = StructureDump.dump([{TestRepo, []}], dump)
    assert :counters.get(runs, 1) == 2
    assert path == Path.join(dir, "structure.sql")
    text = File.read!(path)
    [dumped, appendix] = String.split(text, "-- Trellis definitions (")
    assert dumped =~ "CREATE TABLE public.dump_raced_prices"
    # The second dump's exclusions had the new 1-1 target's ledger.
    refute dumped =~ "dump_raced_prices__ledger"
    assert appendix =~ "dump_raced_prices"
  end

  test "definitions that change during every dump are refused", %{pg: pg} do
    Postgrex.query!(pg, "create table dump_churn (id integer primary key, price integer)", [])
    {:ok, source} = Trellis.connect(url: TestRepo.config()[:trellis][:url])
    on_exit(fn -> Trellis.shutdown(source) end)
    Trellis.migrate!(source)
    runs = :counters.new(1, [])

    dump = fn plan, args ->
      :ok = StructureDump.pg_dump(plan, args)
      :counters.add(runs, 1, 1)
      n = :counters.get(runs, 1)
      Trellis.define!(source, "TRANSFORM dump_churn_#{n} FROM dump_churn SELECT price AS price")
      :ok
    end

    assert {:error, message} = StructureDump.dump([{TestRepo, []}], dump)
    assert message =~ "changed during each of 3 dumps"
    assert :counters.get(runs, 1) == 3
  end

  test "append refuses a structure file that isn't there", %{tmp_dir: dir} do
    missing = Path.join(dir, "missing.sql")
    assert {:error, message} = StructureDump.append(TestRepo, dump_path: missing)
    assert message =~ "run `mix ecto.dump` first"
    refute File.exists?(missing)
  end

  test "an appendix starts a line of its own, and a marker line in the dump is kept", %{
    tmp_dir: dir
  } do
    {:ok, source} = Trellis.connect(url: TestRepo.config()[:trellis][:url])
    on_exit(fn -> Trellis.shutdown(source) end)
    Trellis.migrate!(source)
    path = Path.join(dir, "structure.sql")

    # No trailing newline: the second append still finds the first's
    # appendix at the start of a line, and replaces it.
    File.write!(path, "create table t ();")
    assert {:ok, ^path} = StructureDump.append(TestRepo)
    assert {:ok, ^path} = StructureDump.append(TestRepo)
    text = File.read!(path)
    assert String.starts_with?(text, "create table t ();\n-- Trellis definitions (trellis::")
    assert length(String.split(text, "-- Trellis definitions (")) == 2

    # The marker's text inside the dump (a function body) is not an appendix.
    body =
      "create function f() returns int language sql as $$\n-- Trellis definitions (x)\nselect 1\n$$;\n"

    File.write!(path, body)
    assert {:ok, ^path} = StructureDump.append(TestRepo)
    assert {:ok, ^path} = StructureDump.append(TestRepo)
    text = File.read!(path)
    assert String.starts_with?(text, body <> "-- Trellis definitions (trellis::")
    assert length(String.split(text, "-- Trellis definitions (")) == 3
  end

  # Defines dump_widget_prices_<suffix>, a 1-1 transform, in the repo's
  # database, which every test here shares.
  defp define_widgets(cluster, pg, suffix) do
    Postgrex.query!(
      pg,
      "create table dump_widgets_#{suffix} (id integer primary key, price integer)",
      []
    )

    {:ok, source} = Trellis.connect(url: cluster["dsn"])
    Trellis.migrate!(source)

    Trellis.define!(
      source,
      "TRANSFORM dump_widget_prices_#{suffix} FROM dump_widgets_#{suffix} SELECT price AS price"
    )

    source
  end

  # `mix trellis.load` into a fresh database, which then has
  # dump_widget_prices_<suffix>, restored and unbuilt, its ledger, and no
  # restore queue.
  defp assert_loads(cluster, pg, path, suffix) do
    database = "dump_loaded_#{suffix}"
    target = "dump_widget_prices_#{suffix}"
    Postgrex.query!(pg, "create database #{database}", [])
    stop_supervised!(TestRepo)
    url = String.replace(cluster["dsn"], cluster["dbname"], database)
    put_repo_config(cluster, database: database, dump_path: path, trellis: [url: url])
    Mix.Tasks.Trellis.Load.run(~w(-r Trellis.TestRepo --quiet))

    {:ok, copy} = Trellis.connect(url: url)
    on_exit(fn -> Trellis.shutdown(copy) end)
    assert %Status{status: :waiting_to_backfill} = Trellis.status!(copy, target)
    assert "public.#{target}" in Enum.map(Trellis.definitions!(copy), & &1.target_table)

    loaded = TestCluster.postgrex!(Map.put(cluster, "dbname", database))

    assert %{rows: [[true, false]]} =
             Postgrex.query!(
               loaded,
               "select to_regclass('public.#{target}__ledger') is not null, " <>
                 "to_regclass('trellis.restore_queue') is not null",
               []
             )

    GenServer.stop(loaded)
  end

  # The repo reads its configuration, `:trellis` included, from the
  # application environment, as a host repo does.
  defp put_repo_config(cluster, extra) do
    trellis = [url: cluster["dsn"]]

    config =
      [
        socket_dir: cluster["host"],
        port: cluster["port"],
        username: cluster["user"],
        database: cluster["dbname"],
        pool_size: 2,
        log: false,
        dump_prefixes: ["public", "trellis"],
        trellis: trellis
      ]
      |> Keyword.merge(extra)
      |> Enum.reject(fn {_, value} -> is_nil(value) end)

    Application.put_env(:trellis_pg, TestRepo, config)
  end
end
