defmodule Trellis.MigrationTest do
  # `Trellis.Migration`, run the way a host app runs its migrations: through
  # `Ecto.Migrator` and an Ecto repo, against a cluster of its own, so the
  # definitions it leaves don't reach the other tests' staging workers.
  use ExUnit.Case, async: false

  alias Trellis.{Error, Status, TestCluster, TestRepo}

  defmodule DefineWidgets do
    use Ecto.Migration
    use Trellis.Migration

    @disable_ddl_transaction true

    def up do
      create table(:mig_widgets) do
        add :price, :integer
      end

      define "TRANSFORM mig_widget_prices FROM mig_widgets SELECT price AS price"
    end

    def down do
      apply "PAUSE TRANSFORM mig_widget_prices"
      apply "DROP TRANSFORM mig_widget_prices"
      drop table(:mig_widgets)
    end
  end

  # The same, but in the migration's transaction. Its define reads a table
  # committed before the migration runs, which Trellis's own connections
  # can see, so only the transaction check stops it being applied.
  defmodule DefineOrdersInTransaction do
    use Ecto.Migration
    use Trellis.Migration

    def up do
      create table(:mig_orders) do
        add :total, :integer
      end

      define "TRANSFORM mig_order_totals FROM mig_committed_orders SELECT total AS total"
    end

    def down, do: :ok
  end

  # The optional guard `Trellis.Migration`'s docs show, for a target defined
  # some other way first.
  defmodule DefineGadgetsUnlessDefined do
    use Ecto.Migration
    use Trellis.Migration

    @disable_ddl_transaction true

    def up do
      if status("mig_gadget_prices") == nil do
        define "TRANSFORM mig_gadget_prices FROM mig_gadgets SELECT price AS price"
      end
    end

    def down, do: :ok
  end

  defmodule DefineWithoutSource do
    use Ecto.Migration
    use Trellis.Migration

    @disable_ddl_transaction true

    def up, do: define("TRANSFORM mig_lost_prices FROM mig_widgets SELECT price AS price")
    def down, do: :ok
  end

  defmodule ApplyWithoutConfig do
    use Ecto.Migration
    use Trellis.Migration

    @disable_ddl_transaction true

    def up, do: apply("PAUSE TRANSFORM mig_widget_prices")
    def down, do: :ok
  end

  setup_all do
    cluster = TestCluster.private!()
    {:ok, _} = Application.ensure_all_started(:ecto_sql)
    {:ok, _} = Application.ensure_all_started(:postgrex)

    Application.put_env(:trellis_pg, TestRepo,
      socket_dir: cluster["host"],
      port: cluster["port"],
      username: cluster["user"],
      database: cluster["dbname"],
      # The migration lock takes a connection of its own.
      pool_size: 2,
      log: false,
      trellis: [url: cluster["dsn"]]
    )

    start_supervised!(TestRepo)
    %{cluster: cluster}
  end

  setup %{cluster: cluster} do
    trellis = Trellis.connect!(url: cluster["dsn"])
    on_exit(fn -> Trellis.shutdown(trellis) end)
    %{trellis: trellis, pg: TestCluster.postgrex!(cluster)}
  end

  test "up defines against a table the same migration creates, and down drops both",
       %{trellis: trellis, pg: pg} do
    # Each helper runs `Trellis.migrate/1` first, so this works on a
    # database Trellis has never seen, whichever test runs first.
    assert :ok = Ecto.Migrator.up(TestRepo, 1, DefineWidgets, log: false)

    assert %Status{} = Trellis.status!(trellis, "mig_widget_prices")
    assert table?(pg, "mig_widgets")
    assert table?(pg, "mig_widget_prices")

    assert :ok = Ecto.Migrator.down(TestRepo, 1, DefineWidgets, log: false)

    assert Trellis.status!(trellis, "mig_widget_prices") == nil
    refute table?(pg, "mig_widget_prices")
    refute table?(pg, "mig_widgets")
  end

  test "a migration in a transaction raises before Trellis applies anything",
       %{trellis: trellis, pg: pg} do
    Postgrex.query!(
      pg,
      "create table mig_committed_orders (id integer primary key, total integer)",
      []
    )

    error =
      assert_raise Ecto.MigrationError, fn ->
        Ecto.Migrator.up(TestRepo, 2, DefineOrdersInTransaction, log: false)
      end

    assert error.message =~ "@disable_ddl_transaction true"
    assert error.message =~ "TRANSFORM mig_order_totals"

    # Neither side kept anything: the host's table rolled back with the
    # transaction, and Trellis was never asked.
    refute table?(pg, "mig_orders")
    Trellis.migrate!(trellis)
    assert Trellis.status!(trellis, "mig_order_totals") == nil
    refute table?(pg, "mig_order_totals")
  end

  test "status/1 lets a migration skip a target defined some other way",
       %{trellis: trellis, pg: pg} do
    Postgrex.query!(pg, "create table mig_gadgets (id integer primary key, price integer)", [])
    Trellis.migrate!(trellis)
    Trellis.define!(trellis, "TRANSFORM mig_gadget_prices FROM mig_gadgets SELECT price AS price")

    assert :ok = Ecto.Migrator.up(TestRepo, 3, DefineGadgetsUnlessDefined, log: false)
    assert %Status{} = Trellis.status!(trellis, "mig_gadget_prices")
  end

  test "a define that fails fails the migration with Trellis's error", %{pg: pg} do
    # No `mig_widgets` table: the define can't find its source.
    refute table?(pg, "mig_widgets")

    assert %Error{code: :not_found} =
             assert_raise(Error, fn ->
               Ecto.Migrator.up(TestRepo, 4, DefineWithoutSource, log: false)
             end)
  end

  test "a repo with no :trellis key raises naming it" do
    config = Application.fetch_env!(:trellis_pg, TestRepo)
    Application.put_env(:trellis_pg, TestRepo, Keyword.delete(config, :trellis))
    on_exit(fn -> Application.put_env(:trellis_pg, TestRepo, config) end)

    error =
      assert_raise Ecto.MigrationError, fn ->
        Ecto.Migrator.up(TestRepo, 5, ApplyWithoutConfig, log: false)
      end

    assert error.message =~ "Trellis.TestRepo"
    assert error.message =~ "trellis: [url: database_url]"
  end

  test "a module that uses Trellis.Migration can't define change/0" do
    error =
      assert_raise CompileError, fn ->
        Code.compile_quoted(
          quote do
            defmodule Trellis.MigrationTest.Reversible do
              use Ecto.Migration
              use Trellis.Migration

              def change, do: define("TRANSFORM t FROM s SELECT a AS a")
            end
          end
        )
      end

    assert Exception.message(error) =~ "must define up/0 and down/0 rather than change/0"

    # Ecto only runs a public change/0, so a private helper of that name in
    # an up/0 and down/0 migration is left alone.
    assert [{Trellis.MigrationTest.PrivateChange, _}] =
             Code.compile_quoted(
               quote do
                 defmodule Trellis.MigrationTest.PrivateChange do
                   use Ecto.Migration
                   use Trellis.Migration

                   def up, do: change()
                   def down, do: :ok
                   defp change, do: :ok
                 end
               end
             )
  end

  defp table?(pg, name) do
    %{rows: [[found]]} = Postgrex.query!(pg, "select to_regclass($1) is not null", [name])
    found
  end
end
