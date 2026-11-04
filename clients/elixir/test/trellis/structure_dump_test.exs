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

  test "a target schema joins the prefixes as a pattern that matches exactly it" do
    assert StructureDump.exact_prefix("report_targets") == "report_targets"
    # pg_dump would fold it to lower case and read "." as a pattern character.
    assert StructureDump.exact_prefix("Audit.T") == ~s("Audit.T")
    assert StructureDump.exact_prefix(~s(a"b)) == ~s("a""b")
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

  test "definitions that change during every dump are refused", %{pg: pg, tmp_dir: dir} do
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
    # What pg_dump wrote has the capture triggers in and no definitions:
    # not left for `ecto.load` to load.
    refute File.exists?(Path.join(dir, "structure.sql"))
  end

  test "a dump that fails leaves no file to load, and a failure before pg_dump keeps the old one",
       %{cluster: cluster, tmp_dir: dir} do
    path = Path.join(dir, "structure.sql")
    {:ok, source} = Trellis.connect(url: TestRepo.config()[:trellis][:url])
    on_exit(fn -> Trellis.shutdown(source) end)
    Trellis.migrate!(source)

    failing = fn plan, args ->
      :ok = StructureDump.pg_dump(plan, args)
      {:error, "boom"}
    end

    assert {:error, "boom"} = StructureDump.dump([{TestRepo, []}], failing)
    refute File.exists?(path)

    raising = fn plan, args ->
      :ok = StructureDump.pg_dump(plan, args)
      raise "boom"
    end

    assert_raise RuntimeError, "boom", fn -> StructureDump.dump([{TestRepo, []}], raising) end
    refute File.exists?(path)

    # A pg_dump that fails before it opens the file (it can't connect, or
    # it is older than the server) leaves the file that was there...
    File.write!(path, "-- previous\n")
    unopened = fn _plan, _args -> {:error, "pg_dump: error: connection failed"} end
    assert {:error, _} = StructureDump.dump([{TestRepo, []}], unopened)
    assert File.read!(path) == "-- previous\n"

    # ...and one that wrote to it doesn't.
    assert {:error, "boom"} = StructureDump.dump([{TestRepo, []}], failing)
    refute File.exists?(path)

    # Refused before pg_dump runs: the file that was there stays.
    File.write!(path, "-- previous\n")
    point_trellis_at(cluster, "elsewhere")
    assert {:error, message} = StructureDump.dump([{TestRepo, dump_path: path}], failing)
    assert message =~ "elsewhere"
    assert File.read!(path) == "-- previous\n"
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

  test "the target schema's schema_migrations is not dumped", %{
    cluster: cluster,
    pg: pg,
    tmp_dir: dir
  } do
    Postgrex.query!(pg, "create schema dump_reports", [])
    Postgrex.query!(pg, "create table public.dump_versions (version bigint primary key)", [])

    Postgrex.query!(
      pg,
      "create table dump_reports.dump_versions (version bigint primary key)",
      []
    )

    Postgrex.query!(pg, "insert into public.dump_versions values (5)", [])
    Postgrex.query!(pg, "insert into dump_reports.dump_versions values (7)", [])

    trellis = [url: cluster["dsn"], target_schema: "dump_reports"]

    put_repo_config(cluster,
      migration_source: "dump_versions",
      dump_path: Path.join(dir, "structure.sql"),
      trellis: trellis
    )

    {:ok, source} = Trellis.connect(trellis)
    on_exit(fn -> Trellis.shutdown(source) end)
    Trellis.migrate!(source)

    assert {:ok, [path]} = StructureDump.dump([{TestRepo, dump_prefixes: ["public"]}])
    text = File.read!(path)
    # The target schema is dumped for its tables...
    assert text =~ "CREATE TABLE dump_reports.dump_versions"
    # ...but only the schemas `--prefix` names have their migrations dumped.
    assert text =~ ~s[INSERT INTO public."dump_versions" (version) VALUES (5);]
    refute text =~ ~s[INSERT INTO dump_reports]
  end

  test "a repo and a :trellis url at different databases are refused", %{
    cluster: cluster,
    tmp_dir: dir
  } do
    path = Path.join(dir, "structure.sql")
    point_trellis_at(cluster, "elsewhere")

    assert {:error, message} = StructureDump.dump([{TestRepo, []}])
    assert message =~ "elsewhere"
    assert message =~ cluster["dbname"]
    assert message =~ "Trellis.TestRepo"
    refute File.exists?(path)

    File.write!(path, "create table t ();")
    assert {:error, ^message} = StructureDump.append(TestRepo)
    assert File.read!(path) == "create table t ();"

    assert_raise Mix.Error, ~r/elsewhere/, fn ->
      Mix.Tasks.Trellis.Load.run(~w(-r Trellis.TestRepo --quiet))
    end

    assert_raise Mix.Error, ~r/elsewhere/, fn ->
      Mix.Tasks.Trellis.Dump.run(~w(-r Trellis.TestRepo --quiet))
    end
  end

  test "the databases a repo and a url name are compared by host, port and database" do
    same? = fn config, url ->
      Trellis.StructureDump.Database.verify(TestRepo, config, url: url) == :ok
    end

    socket = [socket_dir: "/run/pg", port: 5433, database: "app", username: "me"]
    assert same?.(socket, "host=/run/pg port=5433 dbname=app user=me")
    assert same?.(socket, "postgres://me@%2Frun%2Fpg:5433/app")
    assert same?.(socket, "postgres://me@localhost:5433/app")
    assert same?.(socket, "postgres:///app?host=/run/pg&port=5433&user=me")
    refute same?.(socket, "host=/run/pg port=5432 dbname=app")
    refute same?.(socket, "host=/run/pg port=5433 dbname=other")
    refute same?.(socket, "host=db.internal port=5433 dbname=app")

    tcp = [hostname: "db.internal", database: "app", username: "me"]
    assert same?.(tcp, "postgres://me:p%40ss@db.internal/app?sslmode=require")
    assert same?.(tcp, "host = db.internal dbname = 'app' password='a b\\'c'")
    assert same?.(tcp, "postgresql://db.internal:5432/app")
    # Hosts are not equated across spellings.
    refute same?.(tcp, "postgres://db.internal.example/app")
    refute same?.([hostname: "localhost", database: "app"], "postgres://127.0.0.1/app")
    refute same?.([hostname: "localhost", database: "app"], "host=/run/pg dbname=app")
    # A repo naming no host matches a local one, as a Rails config naming none
    # does in the Ruby client (when PGHOST names none either).
    if is_nil(System.get_env("PGHOST")) do
      assert same?.([database: "app"], "postgres://127.0.0.1/app")
      assert same?.([database: "app"], "host=/run/pg dbname=app")
      refute same?.([database: "app"], "postgres://db.internal/app")
    end

    # The database defaults to the user, as libpq's does.
    refute same?.(tcp, "postgres://me@db.internal")
    assert same?.([hostname: "h", database: "me"], "postgres://me@h")
    # A url the check can't read, or that names several hosts, is not compared.
    assert same?.(tcp, "not a url")
    assert same?.(tcp, "postgres://a.example,b.example/other")

    # The message doesn't quote the url.
    assert {:error, message} =
             Trellis.StructureDump.Database.verify(TestRepo, tcp,
               url: "postgres://me:hunter2@elsewhere/app"
             )

    refute message =~ "hunter2"
    assert message =~ "app on elsewhere:5432"
  end

  test "mix trellis.dump also dumps the repo's migration_repo, as mix ecto.dump does",
       %{cluster: cluster, tmp_dir: dir} do
    structure = Path.join(dir, "structure.sql")
    migrations = Path.join(dir, "migrations.sql")
    put_repo_config(cluster, migration_repo: Trellis.TestMigrationRepo, dump_path: structure)

    Application.put_env(
      :trellis_pg,
      Trellis.TestMigrationRepo,
      socket_dir: cluster["host"],
      port: cluster["port"],
      username: cluster["user"],
      database: cluster["dbname"],
      dump_path: migrations
    )

    {:ok, source} = Trellis.connect(url: cluster["dsn"])
    on_exit(fn -> Trellis.shutdown(source) end)
    Trellis.migrate!(source)

    Mix.Tasks.Trellis.Dump.run(~w(-r Trellis.TestRepo --prefix public --quiet))

    assert File.read!(migrations) =~ "PostgreSQL database dump"
    assert File.read!(structure) =~ "-- Trellis definitions ("
  end

  test "a declined ecto.load prompt is matched to its repo by the whole name" do
    # `mix ecto.load`'s two prompts, as it words them.
    new = fn repo ->
      "Are you sure you want to load a new structure for #{inspect(repo)}? Any existing " <>
        "data in this repo may be lost."
    end

    loaded = fn repo ->
      "It looks like a structure was already loaded for #{inspect(repo)}. Any attempt to " <>
        "load it again might fail.\nAre you sure you want to proceed?\n"
    end

    declined? = &Mix.Tasks.Trellis.Load.declined?/2
    assert declined?.([new.(MyApp.Repo)], MyApp.Repo)
    assert declined?.([loaded.(MyApp.Repo)], MyApp.Repo)
    # A "no" for a repo whose name extends this one's is not a "no" for it.
    refute declined?.([new.(MyApp.Repo.Replica)], MyApp.Repo)
    refute declined?.([loaded.(MyApp.Repo.Replica)], MyApp.Repo)
    refute declined?.([new.(MyApp.Repo)], MyApp.Repo.Replica)
  end

  describe "mix trellis.load leaves a repo alone that ecto.load left alone" do
    setup %{cluster: cluster, pg: pg, tmp_dir: dir} do
      # A database that already has its migrations table, and no Trellis.
      database = "dump_loaded_before_#{System.unique_integer([:positive])}"
      Postgrex.query!(pg, "create database #{database}", [])
      url = String.replace(cluster["dsn"], cluster["dbname"], database)
      stop_supervised!(TestRepo)

      put_repo_config(cluster,
        database: database,
        dump_path: Path.join(dir, "structure.sql"),
        trellis: [url: url]
      )

      loaded = TestCluster.postgrex!(Map.put(cluster, "dbname", database))
      Postgrex.query!(loaded, "create table schema_migrations (version bigint primary key)", [])
      File.write!(Path.join(dir, "structure.sql"), "create table dump_from_file ();\n")

      shell = Mix.shell()
      Mix.shell(Mix.Shell.Process)

      on_exit(fn ->
        Mix.shell(shell)
      end)

      %{loaded: loaded}
    end

    test "with --skip-if-loaded on a database that has its migrations table", %{loaded: loaded} do
      Mix.Tasks.Trellis.Load.run(~w(-r Trellis.TestRepo --skip-if-loaded --quiet))
      assert nothing_loaded?(loaded)
    end

    test "when the prompt is declined", %{loaded: loaded} do
      send(self(), {:mix_shell_input, :yes?, false})
      Mix.Tasks.Trellis.Load.run(~w(-r Trellis.TestRepo --quiet))
      assert nothing_loaded?(loaded)
      assert_received {:mix_shell, :yes?, [prompt]}
      assert prompt =~ "already loaded for Trellis.TestRepo"
    end

    test "but runs the restore when the prompt is accepted", %{loaded: loaded} do
      send(self(), {:mix_shell_input, :yes?, true})
      Mix.Tasks.Trellis.Load.run(~w(-r Trellis.TestRepo --quiet))

      assert %{rows: [[true, true]]} =
               Postgrex.query!(
                 loaded,
                 "select to_regclass('public.dump_from_file') is not null, " <>
                   "exists (select 1 from pg_namespace where nspname = 'trellis')",
                 []
               )
    end

    defp nothing_loaded?(loaded) do
      %{rows: [[false, false]]} =
        Postgrex.query!(
          loaded,
          "select to_regclass('public.dump_from_file') is not null, " <>
            "exists (select 1 from pg_namespace where nspname = 'trellis')",
          []
        )

      true
    end
  end

  # Points the repo's `:trellis` url at another database than its own.
  defp point_trellis_at(cluster, database) do
    config = Application.fetch_env!(:trellis_pg, TestRepo)
    url = String.replace(config[:trellis][:url], cluster["dbname"], database)
    Application.put_env(:trellis_pg, TestRepo, Keyword.put(config, :trellis, url: url))
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
