if Code.ensure_loaded?(Mix.Tasks.Ecto.Dump) do
  defmodule Mix.Tasks.Trellis.Dump do
    use Mix.Task

    @shortdoc "Appends Trellis's definitions to the structure file ecto.dump wrote"

    @moduledoc """
    Appends the repo's Trellis definitions to its structure file, after
    `mix ecto.dump` wrote it, so `mix ecto.load` builds a database that has
    its transforms (see `Trellis.StructureDump`).

        mix trellis.dump [-r Repo] [-d path] [--prefix schema]

    Run it after `ecto.dump` every time, by an alias in the app's `mix.exs`:

        defp aliases do
          ["ecto.dump": ["ecto.dump", "trellis.dump"]]
        end

    A second run replaces the appendix the first wrote. The task reads the
    repo's `:trellis` configuration, as `Trellis.Migration` does, and takes
    the same options as `ecto.dump`: `-r`/`--repo`, `-d`/`--dump-path`,
    `--prefix` (repeatable) and `-q`/`--quiet`.
    """

    @aliases [d: :dump_path, q: :quiet, r: :repo]
    @switches [
      dump_path: :string,
      quiet: :boolean,
      repo: [:string, :keep],
      no_compile: :boolean,
      no_deps_check: :boolean,
      prefix: [:string, :keep]
    ]

    @impl Mix.Task
    def run(args) do
      {opts, _} = OptionParser.parse!(args, strict: @switches, aliases: @aliases)
      {:ok, _} = Application.ensure_all_started(:trellis_pg)

      options =
        case Keyword.get_values(opts, :prefix) do
          [] -> []
          prefixes -> [dump_prefixes: prefixes]
        end

      options = Keyword.merge(options, Keyword.take(opts, [:dump_path]))

      Enum.each(Mix.Ecto.parse_repo(args), fn repo ->
        Mix.Ecto.ensure_repo(repo, args)

        options =
          Keyword.put(
            options,
            :default_path,
            Path.join(Mix.EctoSQL.source_repo_priv(repo), "structure.sql")
          )

        case Trellis.StructureDump.append(repo, options) do
          {:ok, path} ->
            unless opts[:quiet], do: Mix.shell().info("Trellis definitions appended to #{path}")

          {:error, message} ->
            Mix.raise(message)
        end
      end)
    end
  end
end
