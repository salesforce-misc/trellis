defmodule Trellis.StructureDumpTest do
  # `mix trellis.dump` (which runs `mix ecto.dump`) + `mix ecto.load`, the
  # way a host runs them, against a cluster of its own; `dump/2`'s redo with
  # a dump function that races a define deterministically. No polling: the
  # loaded database is read, not waited on.
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

  test "a structure file dumped, appended to and loaded carries the definitions",
       %{cluster: cluster, pg: pg, tmp_dir: dir} do
    Postgrex.query!(pg, "create table dump_widgets (id integer primary key, price integer)", [])
    {:ok, source} = Trellis.connect(url: cluster["dsn"])
    on_exit(fn -> Trellis.shutdown(source) end)
    Trellis.migrate!(source)

    Trellis.define!(
      source,
      "TRANSFORM dump_widget_prices FROM dump_widgets SELECT price AS price"
    )

    config = TestRepo.config()
    # The mix task runs `ecto.dump` itself, with its own arguments, and
    # appends; a second append replaces the appendix rather than loading it
    # twice. `--prefix public` makes pg_dump write `CREATE SCHEMA public`.
    path = Path.join(dir, "structure.sql")

    Mix.Tasks.Trellis.Dump.run(
      ~w(-r Trellis.TestRepo --dump-path #{path} --prefix public --prefix trellis --quiet)
    )

    assert {:ok, ^path} = StructureDump.append(TestRepo)

    text = File.read!(path)
    assert length(String.split(text, "-- Trellis definitions (")) == 2
    assert text =~ ~s(insert into "trellis"."transform_definitions")
    # Runtime state is never dumped.
    refute text =~ ~s(insert into "trellis"."backfill_chunks")
    assert text =~ "CREATE SCHEMA IF NOT EXISTS public;"

    # Load it into a fresh database.
    Postgrex.query!(pg, "create database dump_loaded", [])
    loaded = Keyword.put(config, :database, "dump_loaded")
    load_structure(loaded, dir, path)

    {:ok, copy} =
      Trellis.connect(url: String.replace(cluster["dsn"], cluster["dbname"], "dump_loaded"))

    on_exit(fn -> Trellis.shutdown(copy) end)

    # The migration history is current, so migrate is a no-op, and the
    # definition is there, defined and not built.
    assert :ok = Trellis.migrate(copy)
    assert %Status{status: :waiting_to_backfill} = Trellis.status!(copy, "dump_widget_prices")
    assert "public.dump_widget_prices" in Enum.map(Trellis.definitions!(copy), & &1.target_table)
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
    dump = fn ->
      {:ok, _} = Ecto.Adapters.Postgres.structure_dump(dir, TestRepo.config())
      :counters.add(runs, 1, 1)

      if :counters.get(runs, 1) == 1 do
        Trellis.define!(
          source,
          "TRANSFORM dump_raced_prices FROM dump_raced SELECT price AS price"
        )
      end
    end

    assert {:ok, [path]} = StructureDump.dump([{TestRepo, []}], dump)
    assert :counters.get(runs, 1) == 2
    text = File.read!(path)
    [dumped, appendix] = String.split(text, "-- Trellis definitions (")
    assert dumped =~ "CREATE TABLE public.dump_raced_prices"
    assert appendix =~ "dump_raced_prices"
  end

  test "definitions that change during every dump are refused", %{pg: pg, tmp_dir: dir} do
    Postgrex.query!(pg, "create table dump_churn (id integer primary key, price integer)", [])
    {:ok, source} = Trellis.connect(url: TestRepo.config()[:trellis][:url])
    on_exit(fn -> Trellis.shutdown(source) end)
    Trellis.migrate!(source)
    runs = :counters.new(1, [])

    dump = fn ->
      {:ok, _} = Ecto.Adapters.Postgres.structure_dump(dir, TestRepo.config())
      :counters.add(runs, 1, 1)
      n = :counters.get(runs, 1)
      Trellis.define!(source, "TRANSFORM dump_churn_#{n} FROM dump_churn SELECT price AS price")
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

  defp load_structure(config, dir, path) do
    case Ecto.Adapters.Postgres.structure_load(dir, Keyword.put(config, :dump_path, path)) do
      {:ok, _} -> :ok
      {:error, output} -> flunk("loading the structure file failed:\n#{output}")
    end
  end
end
