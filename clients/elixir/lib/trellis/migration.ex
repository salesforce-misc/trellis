if Code.ensure_loaded?(Ecto.Migration) do
  defmodule Trellis.Migration do
    @moduledoc """
    Trellis statements in an Ecto migration, so a transform is defined, and
    dropped, the way the tables it reads are created.

        defmodule MyApp.Repo.Migrations.DefineWidgetPrices do
          use Ecto.Migration
          use Trellis.Migration

          # Required: see "Transactions" below.
          @disable_ddl_transaction true

          def up do
            create table(:widgets) do
              add :price, :integer
            end

            define "TRANSFORM widget_prices FROM widgets SELECT price AS price"
          end

          def down do
            apply "PAUSE TRANSFORM widget_prices"
            apply "DROP TRANSFORM widget_prices"
            drop table(:widgets)
          end
        end

    `use Trellis.Migration` imports three helpers, each a thin call through
    to the function of the same name on `Trellis`:

    - `define/1`: a `TRANSFORM` statement, and nothing else (`Trellis.define/2`).
    - `apply/1`: any statement of the grammar (`Trellis.apply/2`), such as the
      `PAUSE TRANSFORM` and `DROP TRANSFORM` a `down/0` undoes a define with.
      (`DROP` refuses a transform that isn't paused, and drops its target
      table.)
    - `status/1`: the status of the transform writing a target, or `nil`
      (`Trellis.status/2`).

    `define/1` and `apply/1` are queued like Ecto's own commands, so they run
    in order with them: a transform can read a table created earlier in the
    same migration. `status/1` runs when it's called, since a migration reads
    it to decide what to queue. Each raises the `Trellis.Error` if the call
    fails, which fails the migration.

    ## Transactions

    Trellis doesn't join the migration's transaction. It runs every call on
    connections of its own, and each call commits before it returns. If the
    migration's transaction rolled back after a define, the host's changes
    would be undone and the definition and its target table would not.

    So a migration that uses these helpers has to turn its transaction off
    with `@disable_ddl_transaction true`. `define/1` and `apply/1` check, as
    they run, that the migration's repo has no transaction open, and raise
    an `Ecto.MigrationError` before applying anything if it does. In a
    migration that forgot the attribute, the migration's transaction then
    rolls back with nothing applied on either side.

    Without the transaction, a migration that fails partway leaves whatever
    ran before the failure in place, on both sides. Give a migration that
    defines transforms little else to do.

    ## `up/0` and `down/0`, not `change/0`

    Ecto can reverse `create table` on its own, but not a Trellis statement:
    the reverse of a define is a pause and a drop of a target that only the
    statement names. So a module that uses `Trellis.Migration` must define
    `up/0` and `down/0`, and defining `change/0` is a compile error, rather
    than a rollback that fails partway.

    ## Which Trellis

    Ecto runs migrations without starting the application, so the
    application's supervised Trellis isn't running. Each helper connects a
    handle of its own instead, from the `:trellis` key of the repo's
    configuration, runs `Trellis.migrate/1` on it (creating Trellis's own
    tables the first time), makes its one call, and shuts the handle down:

        # config/runtime.exs
        config :my_app, MyApp.Repo,
          url: database_url,
          trellis: [url: database_url]

    The options are `Trellis.connect/1`'s. A migration's handle never runs
    background work: `:staging` and `:drain_threads` are always the defaults,
    whatever the configuration says.

    ## Running a migration once

    A define isn't idempotent: a second define of a target that exists fails
    with a `:conflict` error. Ecto's `schema_migrations` already records which
    migrations have run, so a migration's define runs once per database, and
    these helpers add no guard of their own.

    A migration that has to tolerate a transform defined some other way (by
    hand, or by a release task) can guard the define itself:

        def up do
          if status("widget_prices") == nil do
            define "TRANSFORM widget_prices FROM widgets SELECT price AS price"
          end
        end

    The guard checks the target's name, not its definition: a changed
    statement for a target that exists is an `ALTER TRANSFORM`.
    """

    require Logger

    @doc false
    defmacro __using__(_options) do
      quote do
        import Trellis.Migration, only: [define: 1, apply: 1, status: 1]
        @before_compile Trellis.Migration
      end
    end

    @doc false
    defmacro __before_compile__(env) do
      if Module.defines?(env.module, {:change, 0}) do
        raise CompileError,
          file: env.file,
          line: env.line,
          description:
            "#{inspect(env.module)} uses Trellis.Migration, so it must define up/0 and " <>
              "down/0 rather than change/0: Ecto can't reverse a Trellis statement. " <>
              "Undo a define in down/0 with apply(\"PAUSE TRANSFORM <target>\") and " <>
              "apply(\"DROP TRANSFORM <target>\")."
      end
    end

    @doc """
    Queues a `TRANSFORM` statement, to run in order with the migration's
    other commands. It is `Trellis.define/2`: any other statement form is
    refused before anything is applied.
    """
    @spec define(String.t()) :: :ok
    def define(statement) when is_binary(statement),
      do: queue(statement, &Trellis.define!(&1, statement))

    @doc """
    Queues any statement of Trellis's grammar, to run in order with the
    migration's other commands. It is `Trellis.apply/2`.
    """
    @spec apply(String.t()) :: :ok
    def apply(statement) when is_binary(statement),
      do: queue(statement, &Trellis.apply!(&1, statement))

    @doc """
    The status of the transform that writes `target_table`, or `nil` if none
    does. It is `Trellis.status!/2`, run as it's called rather than queued.
    """
    @spec status(String.t()) :: Trellis.Status.t() | nil
    def status(target_table) when is_binary(target_table),
      do: with_handle(Ecto.Migration.repo(), &Trellis.status!(&1, target_table))

    # Queued with a reverse that raises: a module that `use`s this one can't
    # define `change/0`, but one calling the helpers by their full name can,
    # and rolling that back fails naming the statement.
    defp queue(statement, call) do
      repo = Ecto.Migration.repo()

      Ecto.Migration.execute(
        fn -> run(repo, statement, call) end,
        fn -> irreversible(statement) end
      )
    end

    defp run(repo, statement, call) do
      if repo.in_transaction?() do
        raise Ecto.MigrationError,
          message:
            "Trellis doesn't join the migration's transaction, so this statement wasn't " <>
              "applied: if the transaction rolled back, the definition would stay. Add " <>
              "`@disable_ddl_transaction true` to the migration. The statement: " <>
              statement
      end

      Logger.info("trellis #{statement}")
      with_handle(repo, call)
      :ok
    end

    defp irreversible(statement) do
      raise Ecto.MigrationError,
        message:
          "cannot reverse a Trellis statement: #{statement}. Define up/0 and down/0 in " <>
            "the migration, and undo a define in down/0 with PAUSE TRANSFORM and DROP " <>
            "TRANSFORM statements."
    end

    defp with_handle(repo, call) do
      trellis = Trellis.connect!(connect_options(repo))

      try do
        Trellis.migrate!(trellis)
        call.(trellis)
      after
        Trellis.shutdown(trellis)
      end
    end

    defp connect_options(repo) do
      case Keyword.fetch(repo.config(), :trellis) do
        {:ok, options} when is_list(options) ->
          Keyword.merge(options, staging: false, drain_threads: 0)

        _ ->
          raise Ecto.MigrationError,
            message:
              "#{inspect(repo)} has no Trellis to run migrations against: configure one " <>
                "under the repo's :trellis key, with Trellis.connect/1's options, e.g. " <>
                "`config :my_app, #{inspect(repo)}, trellis: [url: database_url]`"
      end
    end
  end
end
