defmodule Trellis.StructureDump do
  @moduledoc """
  Writes the structure file `mix trellis.dump` writes in place of
  `mix ecto.dump`'s, so `mix trellis.load` (`mix ecto.load`, then
  `Trellis.migrate/1`) builds a database that has its transforms.

  The file is `mix ecto.dump`'s: `pg_dump --schema-only` of the repo's
  database, then the `INSERT`s for `schema_migrations`. The `pg_dump` runs
  with `Trellis.schema_dump/1`'s arguments, which leave out Trellis's own
  schema and the 1-1 targets' ledgers; the capture triggers on source tables
  are taken out of its output (`Trellis.filter_schema_dump/2`); and the
  schema dump's appendix follows, one row per definition in a restore queue.
  `Trellis.migrate/1` on a database loaded from the file creates Trellis's
  tables and restores each definition into its dumped target. Regenerating
  the file changes it only where a definition changed.

  `mix ecto.dump` can't pass `pg_dump` extra arguments, so `dump/2` runs
  `pg_dump` itself, through the adapter's `Ecto.Adapters.Postgres.dump_cmd/3`,
  with the arguments `mix ecto.dump` gives it.

  ## What the structure file needs

  - The repo's `:trellis` configuration, `Trellis.connect/1`'s options, as
    `Trellis.Migration` reads it. The handle starts no background work.
  - The target schema in the dump. When `--prefix` (`:dump_prefixes`)
    names schemas, the instance's `:target_schema` (default `"public"`) is
    dumped as well. Its `schema_migrations` rows are not: those are the
    named schemas'.
  - The repo and the `:trellis` url naming the same database, by host, port
    and database name. The definitions of one database don't belong in the
    structure file of another, so a `dump/2` or `append/2` at a url that
    names another refuses, and so does `mix trellis.load`. `localhost` is
    not `127.0.0.1`; a repo connecting over a socket directory, or naming
    no host, matches a local Trellis host.

  ## Loading

  `mix trellis.load` runs `mix ecto.load` and then `Trellis.migrate/1` for
  each repo with a `:trellis` configuration; the app's alias runs it in
  place of `ecto.load`. A file loaded another way (`psql -f`) needs
  `Trellis.migrate/1` after it. Every definition is restored unbuilt,
  including one that was paused or quarantined where it was dumped, because
  a pause is runtime state, and the worker builds it.

  A structure file in the older format, which carries the Trellis schema and
  `Trellis.dump_definitions_sql/1`'s rows, still loads: `append/2` writes
  that format, for a dump someone else ran.

  ## Races

  `pg_dump` and `Trellis.schema_dump/1` read separate snapshots. `dump/2`
  reads the definitions before and after the `pg_dump` and redoes the dump
  when they differ, up to three times, so a `define` or `DROP` committing
  in between can't leave a file whose DDL and definitions disagree.
  `append/2` reads once, after a dump someone else ran, and can't tell.

  A dump that fails once `pg_dump` has written the file (definitions that
  changed during each attempt, a failed filter) removes it: what `pg_dump`
  wrote has the capture triggers in and no definitions, and would load
  without a word. A file no `pg_dump` wrote to is left as it was.

  ## `CREATE SCHEMA public`

  `pg_dump` 17 writes `CREATE SCHEMA public;` when it dumps `public` by
  name, which fails against a database that already has the schema, as
  every database `mix ecto.create` makes does, and `mix ecto.load` stops at
  the first error. This rewrites that line to
  `CREATE SCHEMA IF NOT EXISTS public;` so the file loads into a fresh
  database.
  """

  alias Trellis.StructureDump.Database

  # The engine's appendix starts with this line; a second append replaces
  # the first's appendix rather than loading the definitions twice.
  @marker "-- Trellis definitions ("

  # How each line of the engine's appendixes starts (`trellis::dump`, both
  # formats; each statement is one line). The engine's `dump_definitions_sql`
  # and `schema_dump_restore` tests read this list and check every line of an
  # appendix against it.
  @appendix_starts [
    "--",
    "create schema if not exists ",
    "create table ",
    "insert into ",
    "select setval(",
    "update ",
    "create temporary table trellis_resolved_",
    "delete from ",
    "drop table trellis_resolved_"
  ]

  # How many times `dump/2` redoes a dump that a define or drop raced.
  @attempts 3

  @doc """
  Writes each repo's structure file: `pg_dump` with the repo's Trellis's
  `schema_dump/1` arguments between two reads of its definitions, then,
  once the two agree, the capture triggers taken out and the appendix
  appended.

  `repos` is a list of `{repo, options}`, with `append/2`'s options
  (`:dump_prefixes` is `mix ecto.dump`'s `--prefix`, and nil dumps every
  schema, as `mix ecto.dump` does). `pg_dump` writes the dump before the
  appendix: given a map with the repo's `:repo`, `:config` (its
  configuration with the options applied) and `:path`, and the extra
  `pg_dump` arguments, it returns `:ok` or `{:error, message}`. The default,
  `pg_dump/2`, is what `mix ecto.dump` runs, plus the extra arguments.

  Returns `{:ok, paths}`, or `{:error, message}`: a repo that can't be
  dumped, or definitions that changed during each of three dumps.
  """
  @spec dump([{module(), keyword()}], (map(), [String.t()] -> :ok | {:error, String.t()})) ::
          {:ok, [Path.t()]} | {:error, String.t()}
  def dump(repos, pg_dump \\ &pg_dump/2) when is_list(repos) and is_function(pg_dump, 2) do
    with {:ok, plans} <- plans(repos, false) do
      Enum.reduce_while(plans, {:ok, []}, fn plan, {:ok, paths} ->
        case with_handles([plan], fn [handle] -> dump_one(plan, handle, pg_dump) end) do
          :ok -> {:cont, {:ok, paths ++ [plan.path]}}
          error -> {:halt, error}
        end
      end)
    end
  end

  @doc """
  What `mix ecto.dump` runs for a repo, with `extra_args` added: `pg_dump
  --schema-only --no-acl --no-owner` into `plan.path`, with `-n` for each of
  `plan.config`'s `:dump_prefixes` (when there are any), then an `INSERT`
  for each migration version in each prefix's (or `public`'s)
  `schema_migrations`. The prefixes are `plan.config`'s `:migration_prefixes`
  when it has them: `dump/2` adds the instance's target schema to
  `:dump_prefixes` so the target tables are dumped, and that schema's
  `schema_migrations` is not part of the dump.
  """
  @spec pg_dump(map(), [String.t()]) :: :ok | {:error, String.t()}
  def pg_dump(%{repo: repo, config: config, path: path}, extra_args) do
    prefixes = config[:dump_prefixes] || []
    args = ["--file", path, "--schema-only", "--no-acl", "--no-owner"]
    args = Enum.reduce(prefixes, args, fn prefix, acc -> ["-n", prefix | acc] end) ++ extra_args
    File.mkdir_p!(Path.dirname(path))

    with {:ok, versions} <- migration_versions(repo, config) do
      case Ecto.Adapters.Postgres.dump_cmd(args, [], config) do
        {_output, 0} ->
          File.write!(path, versions, [:append])
          :ok

        {output, _} ->
          {:error, "The structure for #{inspect(repo)} couldn't be dumped: #{output}"}
      end
    end
  end

  # The `schema_migrations` rows as `mix ecto.dump` appends them: each
  # prefix's (`public` without one) versions, in order, the last prefix's
  # first.
  defp migration_versions(repo, config) do
    table = config[:migration_source] || "schema_migrations"
    prefixes = config[:migration_prefixes] || config[:dump_prefixes] || ["public"]

    read = fn repo ->
      Enum.reduce_while(prefixes, {:ok, []}, fn prefix, {:ok, versions} ->
        sql = ~s[SELECT version FROM #{prefix}."#{table}" ORDER BY version]

        case Ecto.Adapters.SQL.query(repo, sql, [], log: false) do
          {:ok, %{rows: rows}} ->
            {:cont, {:ok, Enum.map(rows, &{prefix, hd(&1)}) ++ versions}}

          {:error, %{postgres: %{code: :undefined_table}}} ->
            {:cont, {:ok, versions}}

          {:error, error} ->
            {:halt, {:error, Exception.message(error)}}
        end
      end)
    end

    case Ecto.Migrator.with_repo(repo, read) do
      {:ok, {:ok, versions}, _} ->
        {:ok,
         Enum.map_join(versions, fn {prefix, version} ->
           ~s[INSERT INTO #{prefix}."#{table}" (version) VALUES (#{version});\n]
         end)}

      {:ok, {:error, message}, _} ->
        {:error, "could not read #{inspect(repo)}'s migration versions: #{message}"}

      {:error, reason} ->
        {:error, "could not start #{inspect(repo)}: #{inspect(reason)}"}
    end
  end

  @doc """
  Appends the definitions of `repo`'s Trellis to a structure file in the
  older format, which must exist and carry the Trellis schema: a dump
  someone else ran (`mix ecto.dump` with the Trellis schema in it). It
  appends `Trellis.dump_definitions_sql/1`, replacing an earlier appendix,
  and the file loads with no Trellis step. `dump/2` writes the current
  format. It reads the definitions once, so it can't tell a define that
  committed after the `pg_dump` (`dump/2` can).

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
    with {:ok, [plan]} <- plans([{repo, options}], true),
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

  # `old_format?`: the plan of `append/2`, whose dump has to carry the
  # Trellis schema.
  defp plans(repos, old_format?) do
    Enum.reduce_while(repos, {:ok, []}, fn {repo, options}, {:ok, plans} ->
      case plan(repo, options, old_format?) do
        {:ok, plan} -> {:cont, {:ok, plans ++ [plan]}}
        error -> {:halt, error}
      end
    end)
  end

  defp plan(repo, options, old_format?) do
    config = Keyword.merge(repo.config(), Keyword.take(options, [:dump_prefixes]))

    with {:ok, trellis_options} <- trellis_options(repo, config),
         :ok <- Database.verify(repo, config, trellis_options),
         {:ok, path} <- path(repo, config, options),
         :ok <- if(old_format?, do: check_prefixes(config, trellis_options), else: :ok) do
      config = Keyword.put(config, :dump_path, path) |> with_target_schema(trellis_options)
      {:ok, %{repo: repo, config: config, path: path, trellis_options: trellis_options}}
    end
  end

  # The target tables are in the dump: the instance's target schema joins
  # any `:dump_prefixes`. It is not one of the schemas whose migrations
  # `mix ecto.dump` dumps (`:migration_prefixes`: the ones `--prefix` names).
  defp with_target_schema(config, trellis_options) do
    case config[:dump_prefixes] do
      prefixes when prefixes in [nil, []] ->
        config

      prefixes ->
        target = Keyword.get(trellis_options, :target_schema, "public")

        config
        |> Keyword.put(:dump_prefixes, Enum.uniq(prefixes ++ [exact_prefix(target)]))
        |> Keyword.put(:migration_prefixes, prefixes)
    end
  end

  # `schema` as a `pg_dump` pattern that matches exactly it, which is also
  # how SQL names it: as is when it is lower case letters, digits and
  # underscores, and double-quoted otherwise, so its case and the pattern
  # characters (`* ? .`) are taken literally.
  @doc false
  def exact_prefix(schema) do
    if schema =~ ~r/\A[a-z_][a-z0-9_]*\z/ do
      schema
    else
      ~s["#{String.replace(schema, ~s["], ~s[""])}"]
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

  # The dump of one plan. Once `pg_dump` has written the file, it holds raw
  # `pg_dump` output, with the capture triggers in and no definitions; any
  # way out but success removes it, so `mix ecto.load` can't load it without
  # a word. A file no `pg_dump` wrote to (a failure before the first, or a
  # `pg_dump` that failed before opening it) is the previous dump: left
  # alone.
  defp dump_one(plan, handle, pg_dump) do
    previous = File.read(plan.path)

    try do
      case attempt(plan, handle, pg_dump, @attempts) do
        :ok ->
          :ok

        error ->
          discard_unfinished(plan.path, previous)
          error
      end
    catch
      kind, reason ->
        discard_unfinished(plan.path, previous)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  defp discard_unfinished(path, previous) do
    case {File.read(path), previous} do
      {{:ok, same}, {:ok, same}} -> :ok
      {{:ok, _}, _} -> File.rm(path)
      _ -> :ok
    end
  end

  # One dump, between two reads of the definitions: done when they agree and
  # the file is finished, redone (`left` more times) when they differ.
  defp attempt(plan, handle, pg_dump, left) do
    with {:ok, before} <- read_schema_dump(handle),
         :ok <- pg_dump.(Map.take(plan, [:repo, :config, :path]), before.pg_dump_args),
         {:ok, now} <- read_schema_dump(handle) do
      cond do
        now == before ->
          finish(plan.path, handle, before.appendix)

        left > 1 ->
          attempt(plan, handle, pg_dump, left - 1)

        true ->
          {:error,
           "Trellis's definitions changed during each of #{@attempts} dumps: dump when no " <>
             "migration is defining or dropping"}
      end
    end
  end

  defp read_schema_dump(handle) do
    case Trellis.schema_dump(handle) do
      {:ok, parts} -> {:ok, parts}
      {:error, error} -> {:error, "Trellis could not dump its definitions: #{error.message}"}
    end
  end

  # The dump at `path` without the capture triggers, loadable into a database
  # that has `public`, then the appendix; written to a copy renamed over it.
  defp finish(path, handle, appendix) do
    with {:ok, text} <- read_file(path),
         {:ok, filtered} <- filter(handle, loadable(text)) do
      replace(path, terminated(filtered) <> appendix)
    end
  end

  defp filter(handle, text) do
    case Trellis.filter_schema_dump(handle, text) do
      {:ok, filtered} -> {:ok, filtered}
      {:error, error} -> {:error, "Trellis could not filter the dump: #{error.message}"}
    end
  end

  defp read_file(path) do
    case File.read(path) do
      {:ok, text} -> {:ok, text}
      {:error, reason} -> read_error(path, reason)
    end
  end

  defp replace(path, text) do
    copy = "#{path}.#{System.pid()}.#{System.unique_integer([:positive])}.tmp"
    File.write!(copy, text)
    File.rename!(copy, path)
    :ok
  end

  defp read(handle) do
    case Trellis.dump_definitions_sql(handle) do
      {:ok, sql} -> {:ok, sql}
      {:error, error} -> {:error, "Trellis could not dump its definitions: #{error.message}"}
    end
  end

  # Writes a copy and renames it over the file, so a reader (or a second run)
  # never sees it half written.
  defp write(path, sql) do
    case File.read(path) do
      {:ok, existing} ->
        replace(path, (existing |> strip() |> loadable() |> terminated()) <> sql)

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
