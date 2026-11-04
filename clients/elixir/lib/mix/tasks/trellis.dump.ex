if Code.ensure_loaded?(Mix.Tasks.Ecto.Dump) do
  defmodule Mix.Tasks.Trellis.Dump do
    use Mix.Task

    @shortdoc "Dumps the structure as ecto.dump does, with Trellis's definitions"

    @moduledoc """
    Writes the structure file `mix ecto.dump` writes, with the repo's
    Trellis definitions in it and Trellis's internal tables left out, so
    `mix trellis.load` builds a database that has its transforms (see
    `Trellis.StructureDump`).

        mix trellis.dump [-r Repo] [-d path] [--prefix schema]

    It takes `ecto.dump`'s options and passes them to it unchanged:
    `-r`/`--repo`, `-d`/`--dump-path`, `--prefix` (repeatable) and
    `-q`/`--quiet`. Make it what `mix ecto.dump` runs, by an alias in the
    app's `mix.exs`:

        defp aliases do
          ["ecto.dump": "trellis.dump"]
        end

    For a repo with a `:trellis` configuration (as `Trellis.Migration` reads
    it), it runs `pg_dump` itself, as `mix ecto.dump` does but with the
    arguments that leave Trellis's own schema and the ledgers out
    (`Trellis.schema_dump/1`); takes the capture triggers out of its output;
    and appends the definitions. They are read before and after the
    `pg_dump`, and the dump is redone (up to three times) when a define or
    drop committed in between.

    A repo with none is dumped by `mix ecto.dump` itself, and the task
    refuses to run when no repo has one. A repo's `:migration_repo` is dumped
    by `mix ecto.dump` as well, as it is for any repo, unless it is among the
    repos given. The repo and its `:trellis` url have to name the same
    database (host, port and database name, spelled alike), or the task
    refuses: the definitions of one database don't belong in the structure
    file of another.
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
      {trellis_repos, other_repos} = Enum.split_with(repos, &is_list(&1.config()[:trellis]))

      if trellis_repos == [] do
        Mix.raise(
          "none of #{inspect(repos)} has a Trellis to dump: configure one under the repo's " <>
            ":trellis key, with Trellis.connect/1's options, or run `mix ecto.dump`"
        )
      end

      # `mix ecto.dump` without `--prefix` dumps every schema, whatever the
      # repo's :dump_prefixes says.
      options =
        case Keyword.get_values(opts, :prefix) do
          [] -> [dump_prefixes: nil]
          prefixes -> [dump_prefixes: prefixes]
        end
        |> Keyword.merge(Keyword.take(opts, [:dump_path]))

      dumped =
        for repo <- trellis_repos do
          default = Path.join(Mix.EctoSQL.source_repo_priv(repo), "structure.sql")
          {repo, Keyword.put(options, :default_path, default)}
        end

      # `mix ecto.dump` also dumps a repo's `:migration_repo`. It is dumped
      # first: given a `--dump-path`, `ecto.dump` writes every repo it dumps
      # to that path, and the Trellis file has to be the one left there.
      for repo <- migration_repos(trellis_repos, repos) do
        Mix.Tasks.Ecto.Dump.run(without_repos(args) ++ ["-r", inspect(repo)])
      end

      case Trellis.StructureDump.dump(dumped) do
        {:ok, paths} ->
          unless opts[:quiet] do
            Enum.each(
              paths,
              &Mix.shell().info(
                "The structure with Trellis's definitions has been dumped to #{&1}"
              )
            )
          end

        {:error, message} ->
          Mix.raise(message)
      end

      # The task module itself, not `Mix.Task.run("ecto.dump")`: the alias
      # names `ecto.dump`, and would run this task again.
      for repo <- other_repos do
        Mix.Tasks.Ecto.Dump.run(without_repos(args) ++ ["-r", inspect(repo)])
      end
    end

    # The migration repos of `trellis_repos` that are not themselves among
    # `repos`, which dump on their own.
    defp migration_repos(trellis_repos, repos) do
      for repo <- trellis_repos,
          migration_repo = repo.config()[:migration_repo],
          migration_repo != repo,
          migration_repo not in repos,
          uniq: true,
          do: migration_repo
    end

    # `args` without its `-r`/`--repo` options.
    defp without_repos([flag, _repo | rest]) when flag in ["-r", "--repo"],
      do: without_repos(rest)

    defp without_repos(["--repo=" <> _ | rest]), do: without_repos(rest)
    defp without_repos([arg | rest]), do: [arg | without_repos(rest)]
    defp without_repos([]), do: []
  end
end
