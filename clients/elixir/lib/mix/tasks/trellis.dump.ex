if Code.ensure_loaded?(Mix.Tasks.Ecto.Dump) do
  defmodule Mix.Tasks.Trellis.Dump do
    use Mix.Task

    @shortdoc "Runs ecto.dump and appends Trellis's definitions to the structure file"

    @moduledoc """
    Runs `mix ecto.dump` and appends the repo's Trellis definitions to the
    structure file it wrote, so `mix ecto.load` builds a database that has
    its transforms (see `Trellis.StructureDump`).

        mix trellis.dump [-r Repo] [-d path] [--prefix schema]

    It takes `ecto.dump`'s options and passes them to it unchanged:
    `-r`/`--repo`, `-d`/`--dump-path`, `--prefix` (repeatable) and
    `-q`/`--quiet`. Make it what `mix ecto.dump` runs, by an alias in the
    app's `mix.exs`:

        defp aliases do
          ["ecto.dump": "trellis.dump"]
        end

    The definitions are read before and after the `pg_dump`, and the dump is
    redone (up to three times) when a define or drop committed in between.

    Every repo is dumped; the definitions are appended for each one with a
    `:trellis` configuration (as `Trellis.Migration` reads it). A repo with
    none is dumped as `ecto.dump` dumps it, and the task refuses to run when
    no repo has one.
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

      repos = Mix.Ecto.parse_repo(args)
      Enum.each(repos, &Mix.Ecto.ensure_repo(&1, args))

      options =
        case Keyword.get_values(opts, :prefix) do
          [] -> []
          prefixes -> [dump_prefixes: prefixes]
        end
        |> Keyword.merge(Keyword.take(opts, [:dump_path]))

      dumped =
        for repo <- repos, is_list(repo.config()[:trellis]) do
          default = Path.join(Mix.EctoSQL.source_repo_priv(repo), "structure.sql")
          {repo, Keyword.put(options, :default_path, default)}
        end

      if dumped == [] do
        Mix.raise(
          "none of #{inspect(repos)} has a Trellis to dump: configure one under the repo's " <>
            ":trellis key, with Trellis.connect/1's options, or run `mix ecto.dump`"
        )
      end

      # The task module itself, not `Mix.Task.run("ecto.dump")`: the alias
      # names `ecto.dump`, and Mix runs a task once per invocation, where a
      # redone dump runs it again.
      case Trellis.StructureDump.dump(dumped, fn -> Mix.Tasks.Ecto.Dump.run(args) end) do
        {:ok, paths} ->
          unless opts[:quiet] do
            Enum.each(paths, &Mix.shell().info("Trellis definitions appended to #{&1}"))
          end

        {:error, message} ->
          Mix.raise(message)
      end
    end
  end
end
