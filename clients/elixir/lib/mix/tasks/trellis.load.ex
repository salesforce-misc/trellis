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

    It leaves a repo alone that `ecto.load` left alone: with
    `--skip-if-loaded` on a database that has its migrations table, or when
    the answer to `ecto.load`'s prompt is no. Before loading anything, it
    refuses a repo whose `:trellis` url names another database than the repo
    does (host, port and database name, spelled alike), since the
    definitions of one database don't belong in another.

    The handle it migrates with starts no background work.
    """

    alias __MODULE__.RecordingShell

    @switches [force: :boolean, skip_if_loaded: :boolean]
    @aliases [f: :force]

    @impl Mix.Task
    def run(args) do
      {:ok, _} = Application.ensure_all_started(:trellis_pg)
      {opts, _, _} = OptionParser.parse(args, strict: @switches, aliases: @aliases)

      trellis_repos =
        for repo <- Mix.Ecto.parse_repo(args),
            Mix.Ecto.ensure_repo(repo, args),
            options = repo.config()[:trellis],
            is_list(options) do
          {repo, Keyword.merge(options, staging: false, drain_threads: 0)}
        end

      # Before anything is loaded: a url at another database can't restore
      # what the file holds.
      for {repo, options} <- trellis_repos do
        case Trellis.StructureDump.Database.verify(repo, repo.config(), options) do
          :ok -> :ok
          {:error, message} -> Mix.raise(message)
        end
      end

      skipped_as_loaded =
        if opts[:skip_if_loaded],
          do: for({repo, _} <- trellis_repos, loaded?(repo), do: repo),
          else: []

      # The task module itself, not `Mix.Task.run("ecto.load")`: the alias
      # names `ecto.load`, and would run this task again.
      declined = load(args)

      # A repo whose structure `ecto.load` left alone (`--skip-if-loaded`
      # on a database that has it, or a "no" to its prompt) is left alone.
      for {repo, options} <- trellis_repos,
          repo not in skipped_as_loaded,
          not declined?(declined, repo) do
        restore(repo, options)
      end

      :ok
    end

    # Whether the repo's database has its migrations table, as `ecto.load`
    # decides it has been loaded.
    defp loaded?(repo) do
      config = repo.config()
      migration_repo = config[:migration_repo] || repo
      source = config[:migration_source] || "schema_migrations"

      {:ok, loaded?, _} =
        Ecto.Migrator.with_repo(migration_repo, &Ecto.Adapters.SQL.table_exists?(&1, source))

      loaded?
    end

    # Runs `mix ecto.load`, and returns the prompts it was answered "no" to.
    defp load(args), do: RecordingShell.run(fn -> Mix.Tasks.Ecto.Load.run(args) end)

    # `ecto.load`'s prompt names the repo: "... a new structure for Repo?
    # Any ..." or "... already loaded for Repo. Any ...". The white space
    # after the `?` or `.` keeps `MyApp.Repo` from matching a prompt for
    # `MyApp.Repo.Replica`.
    @doc false
    def declined?(declined, repo) do
      pattern = ~r/for #{Regex.escape(inspect(repo))}[?.]\s/
      Enum.any?(declined, &(&1 =~ pattern))
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

    defmodule RecordingShell do
      @moduledoc false
      # The shell `ecto.load` runs under, so the task can tell a structure
      # load that its prompt declined: it forwards to the shell in use, and
      # remembers each question answered "no".

      @behaviour Mix.Shell

      @inner :trellis_load_inner_shell
      @declined :trellis_load_declined

      @doc "Runs `fun`, and returns the questions it was answered \"no\" to."
      def run(fun) do
        shell = Mix.shell()
        Process.put(@inner, shell)
        Process.put(@declined, [])
        Mix.shell(__MODULE__)

        try do
          fun.()
          Process.get(@declined)
        after
          Mix.shell(shell)
          Process.delete(@inner)
          Process.delete(@declined)
        end
      end

      @impl true
      def print_app, do: inner().print_app()
      @impl true
      def info(message), do: inner().info(message)
      @impl true
      def error(message), do: inner().error(message)
      @impl true
      def prompt(message), do: inner().prompt(message)
      @impl true
      def cmd(command), do: inner().cmd(command)
      @impl true
      def cmd(command, options), do: inner().cmd(command, options)
      @impl true
      def yes?(message), do: record(message, inner().yes?(message))
      @impl true
      def yes?(message, options), do: record(message, inner().yes?(message, options))

      defp inner, do: Process.get(@inner)

      defp record(_message, true), do: true

      defp record(message, false) do
        Process.put(@declined, [message | Process.get(@declined)])
        false
      end
    end
  end
end
