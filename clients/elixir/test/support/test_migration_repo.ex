defmodule Trellis.TestMigrationRepo do
  @moduledoc false
  # A repo `Trellis.TestRepo` names as its `:migration_repo` in
  # `test/trellis/structure_dump_test.exs`, which `mix trellis.dump` has to
  # dump as `mix ecto.dump` does.

  use Ecto.Repo, otp_app: :trellis_pg, adapter: Ecto.Adapters.Postgres
end
