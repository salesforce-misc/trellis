defmodule Trellis.StructureDump do
  @moduledoc """
  Appends Trellis's definitions to the structure file `mix ecto.dump` writes,
  so `mix ecto.load` builds a database that has its transforms.

  `pg_dump --schema-only` carries the Trellis schema, the target tables and
  the capture triggers, but no rows, so a database loaded from it has targets
  and no definitions. Ecto appends `INSERT`s for `schema_migrations` to the
  dump for the same reason; this appends `Trellis.dump_definitions_sql/1`
  after them. `mix trellis.dump` runs `dump/2` around `mix ecto.dump`, and
  the app's alias runs it in place of `ecto.dump`.

  ## What the structure file needs

  - The Trellis schema in the dump. `mix ecto.dump` dumps every schema unless
    it is given `--prefix` (the repo's `:dump_prefixes` is read by the
    adapter, and as of ecto_sql 3.14 `mix ecto.dump` without `--prefix`
    leaves it out). When either names schemas, the list has to include the
    instance's `:schema` (default `"trellis"`), and this refuses to run
    without it.
  - The repo's `:trellis` configuration, `Trellis.connect/1`'s options, as
    `Trellis.Migration` reads it. The handle starts no background work.

  ## Loading

  The file loads with `mix ecto.load` or plain `psql`, and nothing else runs:
  the worker that starts on the loaded database finds definitions that aren't
  built and builds them. Every definition loads `:waiting_to_backfill`,
  including one that was paused or quarantined where it was dumped, because
  a pause is runtime state.

  ## Races

  `pg_dump` and `dump_definitions_sql/1` read separate snapshots. `dump/2`
  reads the definitions before and after the `pg_dump` and redoes the dump
  when they differ, up to three times, so a `define` or `DROP` committing
  in between can't leave a file whose DDL and definitions disagree.
  `append/2` reads once, after a dump someone else ran, and can't tell.

  ## `CREATE SCHEMA public`

  `pg_dump` 17 writes `CREATE SCHEMA public;` when it dumps `public` by
  name, which fails against a database that already has the schema, as
  every database `mix ecto.create` makes does, and `mix ecto.load` stops at
  the first error. This rewrites that line to
  `CREATE SCHEMA IF NOT EXISTS public;` so the file loads into a fresh
  database.
  """

  # The engine's appendix starts with this line; a second append replaces
  # the first's appendix rather than loading the definitions twice.
  @marker "-- Trellis definitions ("

  # How each line of the engine's appendix starts (`trellis::dump`).
  @appendix_starts [
    "--",
    "insert into ",
    "select setval(",
    "update ",
    "create temporary table trellis_resolved_types ",
    "delete from ",
    "drop table trellis_resolved_types;"
  ]

  # How many times `dump/2` redoes a dump that a define or drop raced.
  @attempts 3

  @doc """
  Runs `dump`, a function that writes the structure files (`mix trellis.dump`
  passes `mix ecto.dump` itself), between two reads of each repo's
  definitions, and appends them to each repo's file once the two agree.

  `repos` is a list of `{repo, options}`, with `append/2`'s options.

  Returns `{:ok, paths}`, or `{:error, message}`: a repo that can't be
  dumped, or definitions that changed during each of three dumps.
  """
  @spec dump([{module(), keyword()}], (-> term())) :: {:ok, [Path.t()]} | {:error, String.t()}
  def dump(repos, dump) when is_list(repos) and is_function(dump, 0) do
    with {:ok, plans} <- plans(repos) do
      with_handles(plans, fn handles -> attempt(plans, handles, dump, @attempts) end)
    end
  end

  @doc """
  Appends the definitions of `repo`'s Trellis to the structure file, which
  must exist: run it after `mix ecto.dump`. It reads the definitions once,
  so it can't tell a define that committed after the `pg_dump` (`dump/2`
  can).

  Options:

  - `:dump_path`: the file. Default: the repo's `:dump_path`, else
    `:default_path`.
  - `:default_path`: where the repo's `structure.sql` is when it has no
    `:dump_path`; `mix trellis.dump` passes Ecto's own default.
  - `:dump_prefixes`: the schemas the dump was restricted to, in place of
    the repo's `:dump_prefixes` (`mix ecto.dump`'s `--prefix`).

  Returns `{:ok, path}`, or `{:error, message}`.
  """
  @spec append(module(), keyword()) :: {:ok, Path.t()} | {:error, String.t()}
  def append(repo, options \\ []) when is_atom(repo) and is_list(options) do
    with {:ok, [plan]} <- plans([{repo, options}]),
         :ok <- readable(plan.path) do
      with_handles([plan], fn [handle] ->
        with {:ok, sql} <- read(handle),
             :ok <- write(plan.path, sql) do
          {:ok, plan.path}
        end
      end)
    end
  end

  # Checked before connecting, so a missing file is refused the same way
  # whatever state the database is in.
  defp readable(path) do
    case File.read(path) do
      {:ok, _} -> :ok
      {:error, reason} -> read_error(path, reason)
    end
  end

  defp read_error(path, :enoent) do
    {:error,
     "no structure file at #{path}: run `mix ecto.dump` first, or `mix trellis.dump`, " <>
       "which runs it"}
  end

  defp read_error(path, reason) do
    {:error, "could not read the structure file #{path}: #{:file.format_error(reason)}"}
  end

  defp plans(repos) do
    Enum.reduce_while(repos, {:ok, []}, fn {repo, options}, {:ok, plans} ->
      case plan(repo, options) do
        {:ok, plan} -> {:cont, {:ok, plans ++ [plan]}}
        error -> {:halt, error}
      end
    end)
  end

  defp plan(repo, options) do
    config = Keyword.merge(repo.config(), Keyword.take(options, [:dump_prefixes]))

    with {:ok, trellis_options} <- trellis_options(repo, config),
         {:ok, path} <- path(repo, config, options),
         :ok <- check_prefixes(config, trellis_options) do
      {:ok, %{path: path, trellis_options: trellis_options}}
    end
  end

  defp trellis_options(repo, config) do
    case Keyword.fetch(config, :trellis) do
      {:ok, options} when is_list(options) ->
        {:ok, Keyword.merge(options, staging: false, drain_threads: 0)}

      _ ->
        {:error,
         "#{inspect(repo)} has no Trellis to dump: configure one under the repo's :trellis " <>
           "key, with Trellis.connect/1's options, e.g. " <>
           "`config :my_app, #{inspect(repo)}, trellis: [url: database_url]`"}
    end
  end

  defp path(repo, config, options) do
    case options[:dump_path] || config[:dump_path] || options[:default_path] do
      nil -> {:error, "no structure file for #{inspect(repo)}: pass :dump_path"}
      path -> {:ok, path}
    end
  end

  defp check_prefixes(config, trellis_options) do
    schema = Keyword.get(trellis_options, :schema, "trellis")

    case config[:dump_prefixes] do
      prefixes when prefixes in [nil, []] ->
        :ok

      prefixes ->
        if schema in prefixes do
          :ok
        else
          {:error,
           "the Trellis schema #{inspect(schema)} is not in :dump_prefixes " <>
             "(#{inspect(prefixes)}), so the structure file would hold the definitions " <>
             "but not the tables they insert into. Add it: " <>
             "`dump_prefixes: #{inspect(prefixes ++ [schema])}`"}
        end
    end
  end

  # Connects a handle per plan, runs `fun` with them, and shuts them all down.
  defp with_handles(plans, fun) do
    {handles, error} =
      Enum.reduce_while(plans, {[], nil}, fn plan, {handles, nil} ->
        case Trellis.connect(plan.trellis_options) do
          {:ok, handle} ->
            {:cont, {handles ++ [handle], nil}}

          {:error, error} ->
            {:halt, {handles, {:error, "Trellis could not connect: #{error.message}"}}}
        end
      end)

    try do
      error || fun.(handles)
    after
      Enum.each(handles, &Trellis.shutdown/1)
    end
  end

  defp attempt(_plans, _handles, _dump, 0) do
    {:error,
     "Trellis's definitions changed during each of #{@attempts} dumps: dump when no " <>
       "migration is defining or dropping"}
  end

  defp attempt(plans, handles, dump, left) do
    with {:ok, before} <- read_all(handles) do
      dump.()

      with {:ok, now} <- read_all(handles) do
        if now == before do
          write_all(plans, now)
        else
          attempt(plans, handles, dump, left - 1)
        end
      end
    end
  end

  defp read_all(handles) do
    Enum.reduce_while(handles, {:ok, []}, fn handle, {:ok, sqls} ->
      case read(handle) do
        {:ok, sql} -> {:cont, {:ok, sqls ++ [sql]}}
        error -> {:halt, error}
      end
    end)
  end

  defp read(handle) do
    case Trellis.dump_definitions_sql(handle) do
      {:ok, sql} -> {:ok, sql}
      {:error, error} -> {:error, "Trellis could not dump its definitions: #{error.message}"}
    end
  end

  defp write_all(plans, sqls) do
    plans
    |> Enum.zip(sqls)
    |> Enum.reduce_while({:ok, []}, fn {plan, sql}, {:ok, paths} ->
      case write(plan.path, sql) do
        :ok -> {:cont, {:ok, paths ++ [plan.path]}}
        error -> {:halt, error}
      end
    end)
  end

  # Writes a copy and renames it over the file, so a reader (or a second run)
  # never sees it half written.
  defp write(path, sql) do
    case File.read(path) do
      {:ok, existing} ->
        text = existing |> strip() |> loadable() |> terminated()
        copy = "#{path}.#{System.pid()}.#{System.unique_integer([:positive])}.tmp"
        File.write!(copy, text <> sql)
        File.rename!(copy, path)

      {:error, reason} ->
        read_error(path, reason)
    end
  end

  # `pg_dump` 17 writes `CREATE SCHEMA public;`, and `ecto.load` stops at the
  # first error. Every database `ecto.create` makes has a public schema.
  defp loadable(text) do
    String.replace(text, ~r/^CREATE SCHEMA public;$/m, "CREATE SCHEMA IF NOT EXISTS public;")
  end

  # The appendix starts on a line of its own.
  defp terminated(""), do: ""
  defp terminated(text), do: if(String.ends_with?(text, "\n"), do: text, else: text <> "\n")

  # Everything from a previous appendix's first line on. The appendix is last
  # in the file, and none of its later lines starts with the marker (its
  # values are JSON, whose newlines are escaped), so it starts at the last
  # line that does, if everything from there on is appendix-shaped: comments
  # and the statements the engine writes (`trellis::dump`: the `insert`s and
  # `setval`s that load the definitions, then the statements that resolve
  # each recorded type's OID by name). A marker line in the dump itself (a
  # function body, say) is followed by something else, and is kept.
  defp strip(text) do
    starts =
      if(String.starts_with?(text, @marker), do: [0], else: []) ++
        Enum.map(:binary.matches(text, "\n" <> @marker), fn {at, _} -> at + 1 end)

    case List.last(starts) do
      nil ->
        text

      at ->
        tail = binary_part(text, at, byte_size(text) - at)
        if appendix?(tail), do: binary_part(text, 0, at), else: text
    end
  end

  defp appendix?(tail) do
    tail
    |> String.split("\n")
    |> Enum.all?(fn line ->
      line == "" or String.starts_with?(line, @appendix_starts)
    end)
  end
end
