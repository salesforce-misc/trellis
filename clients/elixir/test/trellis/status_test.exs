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
        },
        capture_wait: nil,
        capture_failure: nil,
        held_keys: nil,
        drain_failure: nil
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

  test "a capture wait, a capture failure and held keys become structs with DateTimes" do
    status =
      Trellis.Status.from_native(%{
        status: :catching_up,
        backfill_failure: nil,
        capture_wait: %{
          table: "public.orders",
          operation: "widen",
          lock_mode: "ShareRowExclusiveLock",
          waiting_since_micros: 1_727_222_400_654_321,
          observed_at_micros: 1_727_222_400_654_322,
          blockers: ["pid 42 holds RowExclusiveLock"]
        },
        capture_failure: %{
          kind: :capture,
          source_table: "public.lines",
          columns: ["qty"],
          error: "column \"qty\" was dropped",
          detected_at_micros: 1_727_222_400_654_321
        },
        held_keys: %{count: 2, oldest_poisoned_at_micros: 1_727_222_400_654_323},
        drain_failure: nil
      })

    assert %Trellis.Status{
             status: :catching_up,
             backfill_failure: nil,
             capture_wait: %Trellis.CaptureWait{
               table: "public.orders",
               operation: "widen",
               lock_mode: "ShareRowExclusiveLock",
               waiting_since: ~U[2024-09-25 00:00:00.654321Z],
               observed_at: ~U[2024-09-25 00:00:00.654322Z],
               blockers: ["pid 42 holds RowExclusiveLock"]
             },
             capture_failure: %Trellis.CaptureFailure{
               kind: :capture,
               source_table: "public.lines",
               columns: ["qty"],
               error: "column \"qty\" was dropped",
               detected_at: ~U[2024-09-25 00:00:00.654321Z]
             },
             held_keys: %Trellis.HeldKeys{
               count: 2,
               oldest_poisoned_at: ~U[2024-09-25 00:00:00.654323Z]
             }
           } = status
  end

  test "a drain failure becomes a struct with DateTimes" do
    status =
      Trellis.Status.from_native(%{
        status: :live,
        backfill_failure: nil,
        capture_wait: nil,
        capture_failure: nil,
        held_keys: nil,
        drain_failure: %{
          seg_seq: 17,
          tables: ["public.lines", "public.orders"],
          error: "permission denied for function audit_hook",
          sqlstate: "42501",
          since_micros: 1_727_222_400_654_321,
          last_seen_micros: 1_727_222_400_654_322,
          attempts: 5
        }
      })

    assert status.drain_failure == %Trellis.DrainFailure{
             seg_seq: 17,
             tables: ["public.lines", "public.orders"],
             error: "permission denied for function audit_hook",
             sqlstate: "42501",
             since: ~U[2024-09-25 00:00:00.654321Z],
             last_seen: ~U[2024-09-25 00:00:00.654322Z],
             attempts: 5
           }
  end

  test "a definition summary's halt becomes a capture failure of kind halt" do
    summary =
      Trellis.DefinitionSummary.from_native(%{
        id: 1,
        target_table: "public.t",
        source_table: "public.s",
        source_version: 1,
        status: :paused,
        created_at_micros: 1_727_222_400_654_321,
        backfill_failure: nil,
        halt: %{
          kind: :halt,
          source_table: "public.s",
          columns: [],
          error: "the drain halted",
          detected_at_micros: 1_727_222_400_654_321
        }
      })

    assert %Trellis.DefinitionSummary{
             status: :paused,
             halt: %Trellis.CaptureFailure{
               kind: :halt,
               source_table: "public.s",
               columns: [],
               error: "the drain halted",
               detected_at: ~U[2024-09-25 00:00:00.654321Z]
             }
           } = summary
  end
end
