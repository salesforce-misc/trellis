defmodule Trellis.StructureDumpTest do
  # `mix ecto.dump` + `mix trellis.dump` + `mix ecto.load`, the way a host
  # runs them: the adapter's own structure_dump and structure_load around
  # `Trellis.StructureDump.append/2`, against a cluster of its own. No
  # polling: the loaded database is read, not waited on.
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
    {:ok, path} = Ecto.Adapters.Postgres.structure_dump(dir, config)
    assert {:ok, ^path} = StructureDump.append(TestRepo)
    # A second run, through the mix task, replaces the appendix rather than
    # loading it twice.
    Mix.Tasks.Trellis.Dump.run(["-r", "Trellis.TestRepo", "--dump-path", path, "--quiet"])

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
    assert [definition] = Trellis.definitions!(copy)
    assert definition.target_table == "public.dump_widget_prices"
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
    File.write!(path, "create table t ();\n-- Trellis definitions (stale)\ninsert stale;\n")

    assert {:ok, ^path} = StructureDump.append(TestRepo)
    text = File.read!(path)
    assert String.starts_with?(text, "create table t ();\n-- Trellis definitions (trellis::")
    refute text =~ "stale"
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
