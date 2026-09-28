defmodule Trellis.TestRepo do
  @moduledoc false
  # The Ecto repo `test/trellis/migration_test.exs` runs its migrations
  # through, the way a host app's migrations run. It reads its configuration,
  # `:trellis` key included, from the application environment the test puts
  # there, as `Ecto.Migrator` reads a host repo's.

  use Ecto.Repo, otp_app: :trellis_pg, adapter: Ecto.Adapters.Postgres
end
