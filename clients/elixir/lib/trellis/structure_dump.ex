defmodule Trellis.StructureDump do
  @moduledoc """
  Appends Trellis's definitions to the structure file `mix ecto.dump` wrote,
  so `mix ecto.load` builds a database that has its transforms.

  `pg_dump --schema-only` carries the Trellis schema, the target tables and
  the capture triggers, but no rows, so a database loaded from it has targets
  and no definitions. Ecto appends `INSERT`s for `schema_migrations` to the
  dump for the same reason; this appends `Trellis.dump_definitions_sql/1`
  after them. It is `mix trellis.dump`, which sets up the alias that runs it
  after `ecto.dump`.

  ## What the structure file needs

  - The Trellis schema in the dump. Ecto dumps every schema unless the repo
    sets `:dump_prefixes`; when it does, the list has to include the
    instance's `:schema` (default `"trellis"`), and `append/2` refuses to run
    without it.
  - The repo's `:trellis` configuration, `Trellis.connect/1`'s options, as
    `Trellis.Migration` reads it.

  ## Loading

  The file loads with `mix ecto.load` or plain `psql`, and nothing else runs:
  the worker that starts on the loaded database finds definitions that aren't
  built and builds them. Every definition loads `:waiting_to_backfill`,
  including one that was paused or quarantined where it was dumped, because
  a pause is runtime state.

  ## Limits

  - `pg_dump` and `dump_definitions_sql/1` read separate snapshots, as
    Ecto's `schema_migrations` appendix does. A `define` or `DROP` that
    commits between the two leaves a file whose DDL and definitions
    disagree. Trellis statements run in migrations, which don't overlap a
    dump in a normal workflow; don't dump while one is running.
  - `pg_dump` 17 writes `CREATE SCHEMA public;`, which fails against a
    database that already has the schema, as every database `mix ecto.create`
    makes does, and `mix ecto.load` stops at the first error. `append/2`
    rewrites that line to `CREATE SCHEMA IF NOT EXISTS public;` so the file
    loads into a fresh database.
  - A definition that was paused or quarantined loads
    `:waiting_to_backfill`, as above.
  """

  # The engine's appendix starts with this line; a second run replaces the
  # first's appendix rather than loading the definitions twice.
  @marker "-- Trellis definitions ("

  @doc """
  Appends the definitions of `repo`'s Trellis to the structure file.

  Options:

  - `:dump_path`: the file. Default: the repo's `:dump_path`, else
    `:default_path`.
  - `:default_path`: where the repo's `structure.sql` is when it has no
    `:dump_path`; `mix trellis.dump` passes Ecto's own default.
  - `:dump_prefixes`: overrides the repo's `:dump_prefixes`.

  Returns `{:ok, path}`, or `{:error, message}`.
  """
  @spec append(module(), keyword()) :: {:ok, Path.t()} | {:error, String.t()}
  def append(repo, options \\ []) when is_atom(repo) and is_list(options) do
    config = Keyword.merge(repo.config(), Keyword.take(options, [:dump_prefixes]))

    with {:ok, trellis_options} <- trellis_options(repo, config),
         {:ok, path} <- path(repo, config, options),
         :ok <- check_prefixes(config, trellis_options),
         {:ok, sql} <- dump_sql(trellis_options) do
      write(path, sql)
      {:ok, path}
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

  defp dump_sql(trellis_options) do
    with {:ok, trellis} <- Trellis.connect(trellis_options) do
      try do
        case Trellis.dump_definitions_sql(trellis) do
          {:ok, sql} -> {:ok, sql}
          {:error, error} -> {:error, "Trellis could not dump its definitions: #{error.message}"}
        end
      after
        Trellis.shutdown(trellis)
      end
    else
      {:error, error} -> {:error, "Trellis could not connect: #{error.message}"}
    end
  end

  defp write(path, sql) do
    existing = if File.exists?(path), do: File.read!(path), else: ""
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, existing |> strip() |> loadable() |> Kernel.<>(sql))
  end

  # `pg_dump` 17 writes `CREATE SCHEMA public;`, and `ecto.load` stops at the
  # first error. Every database `ecto.create` makes has a public schema.
  defp loadable(text) do
    String.replace(text, ~r/^CREATE SCHEMA public;$/m, "CREATE SCHEMA IF NOT EXISTS public;")
  end

  # Everything from a previous appendix's first line on. The appendix is last
  # in the file, so nothing after it is lost.
  defp strip(text) do
    case :binary.match(text, "\n" <> @marker) do
      {at, _} -> binary_part(text, 0, at + 1)
      :nomatch -> if String.starts_with?(text, @marker), do: "", else: text
    end
  end
end
