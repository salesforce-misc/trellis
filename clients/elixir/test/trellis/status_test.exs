defmodule Trellis.StatusTest do
  use ExUnit.Case, async: true

  # The atoms `Trellis.Status.status/0` documents are exactly the ones the
  # NIF can return, which it allocates from the engine's own status list.
  test "the documented statuses are exactly the engine's" do
    assert Trellis.Native.status_names() ==
             {:ok,
              [:waiting_to_backfill, :backfilling, :catching_up, :live, :quarantined, :paused]}
  end

  test "a backfill failure's retry time becomes a DateTime" do
    status =
      Trellis.Status.from_native(%{
        status: :waiting_to_backfill,
        backfill_failure: %{
          source_table: "public.orders",
          attempts: 3,
          last_error: "permission denied",
          next_attempt_at_micros: 1_727_222_400_654_321
        }
      })

    assert %Trellis.Status{
             status: :waiting_to_backfill,
             backfill_failure: %Trellis.BackfillFailure{
               source_table: "public.orders",
               attempts: 3,
               last_error: "permission denied",
               next_attempt_at: next_attempt_at
             }
           } = status

    assert next_attempt_at == ~U[2024-09-25 00:00:00.654321Z]
  end
end
