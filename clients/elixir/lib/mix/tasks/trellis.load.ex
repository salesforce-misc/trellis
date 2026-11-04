if Code.ensure_loaded?(Mix.Tasks.Ecto.Load) do
  defmodule Mix.Tasks.Trellis.Load do
    use Mix.Task

    @shortdoc "Runs ecto.load, then restores Trellis's definitions"

    @moduledoc """
    Runs `mix ecto.load`, then `Trellis.migrate/1` for each repo with a
    `:trellis` configuration (as `Trellis.Migration` reads it), which creates
    Trellis's tables and restores the definitions a structure file from
    `mix trellis.dump` carries (see `Trellis.StructureDump`). A file in the
    older format needs nothing more and loads as before.

        mix trellis.load [-r Repo] [-d path] [--skip-if-loaded]

    It takes `ecto.load`'s options and passes them to it unchanged. Make it
    what `mix ecto.load` runs, by an alias in the app's `mix.exs`:

        defp aliases do
          ["ecto.dump": "trellis.dump", "ecto.load": "trellis.load"]
        end

    The handle it migrates with starts no background work.
    """

    @impl Mix.Task
    def run(args) do
      {:ok, _} = Application.ensure_all_started(:trellis_pg)
      repos = Mix.Ecto.parse_repo(args)

      # The task module itself, not `Mix.Task.run("ecto.load")`: the alias
      # names `ecto.load`, and would run this task again.
      Mix.Tasks.Ecto.Load.run(args)

      for repo <- repos, options = repo.config()[:trellis], is_list(options) do
        restore(repo, Keyword.merge(options, staging: false, drain_threads: 0))
      end

      :ok
    end

    defp restore(repo, options) do
      case Trellis.connect(options) do
        {:ok, handle} ->
          try do
            case Trellis.migrate(handle) do
              :ok ->
                :ok

              {:error, error} ->
                Mix.raise(
                  "Trellis could not restore #{inspect(repo)}'s definitions: #{error.message}"
                )
            end
          after
            Trellis.shutdown(handle)
          end

        {:error, error} ->
          Mix.raise("Trellis could not connect for #{inspect(repo)}: #{error.message}")
      end
    end
  end
end
