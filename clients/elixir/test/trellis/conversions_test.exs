defmodule Trellis.ConversionsTest do
  # The Elixir half of the plain-data boundary: turning what the NIF returns
  # into the public structs and tagged values, with no database involved.
  use ExUnit.Case, async: true

  alias Trellis.{
    Applied,
    Definition,
    DefinitionSummary,
    PoisonEntry,
    QuarantineEntry,
    Relationship,
    RelationshipSummary,
    SamplePage,
    SelfCheckReport
  }

  @micros 1_727_222_400_654_321
  @time ~U[2024-09-25 00:00:00.654321Z]

  # Each closed atom set the NIF allocates at load is exactly the one the
  # public types document.
  test "the documented atoms are exactly the NIF's" do
    assert {:ok, states} = Trellis.Native.quarantine_state_names()

    assert Enum.sort(states) ==
             Enum.sort([
               :waiting_to_backfill,
               :backfilling,
               :catching_up,
               :live,
               :quarantined,
               :paused
             ])

    assert Trellis.Native.cardinality_names() == {:ok, [:one, :many]}

    assert Trellis.Native.applied_kinds() ==
             {:ok,
              [
                :transform_defined,
                :relationship_defined,
                :paused,
                :resumed,
                :dropped,
                :altered,
                :unknown
              ]}

    assert Trellis.Native.self_check_outcomes() ==
             {:ok, [:converged, :not_caught_up, :not_live, :diverged]}

    assert Trellis.Native.divergence_kinds() ==
             {:ok, [:cell, :missing_row, :extra_row, :missing_column, :extra_column, :capture]}

    assert Trellis.Native.capture_failure_kinds() == {:ok, [:capture, :halt]}
  end

  test "a self-check report's divergences become structs" do
    report =
      SelfCheckReport.from_native(%{
        target: "order_totals",
        checked_through: "1/16B3748",
        rows_compared: 2,
        next_after: "2",
        outcome: :diverged,
        status: nil,
        held_keys: nil,
        drain_failures: [],
        divergences: [
          %{
            kind: :cell,
            key: "1",
            column: "total",
            persisted: "9999",
            recomputed: nil,
            table: nil,
            detail: nil
          },
          %{
            kind: :capture,
            key: nil,
            column: nil,
            persisted: nil,
            recomputed: nil,
            table: "public.orders",
            detail: "the capture trigger trellis_capture_insert on public.orders is missing"
          }
        ]
      })

    assert report == %SelfCheckReport{
             target: "order_totals",
             checked_through: "1/16B3748",
             rows_compared: 2,
             next_after: "2",
             outcome: :diverged,
             status: nil,
             divergences: [
               %Trellis.Divergence{
                 kind: :cell,
                 key: "1",
                 column: "total",
                 persisted: "9999",
                 recomputed: nil,
                 table: nil,
                 detail: nil
               },
               %Trellis.Divergence{
                 kind: :capture,
                 key: nil,
                 column: nil,
                 persisted: nil,
                 recomputed: nil,
                 table: "public.orders",
                 detail: "the capture trigger trellis_capture_insert on public.orders is missing"
               }
             ],
             held_keys: nil,
             drain_failures: []
           }
  end

  test "a not-live self-check report names the status it found" do
    report =
      SelfCheckReport.from_native(%{
        target: "order_totals",
        checked_through: "1/16B3748",
        rows_compared: 0,
        next_after: nil,
        outcome: :not_live,
        status: :backfilling,
        divergences: [],
        held_keys: nil,
        drain_failures: []
      })

    assert report.outcome == :not_live
    assert report.status == :backfilling
    assert report.divergences == []
  end

  test "a self-check report's held keys become a struct with a DateTime" do
    report =
      SelfCheckReport.from_native(%{
        target: "order_totals",
        checked_through: "1/16B3748",
        rows_compared: 0,
        next_after: nil,
        outcome: :not_caught_up,
        status: nil,
        divergences: [],
        held_keys: %{count: 1, oldest_poisoned_at_micros: 1_727_222_400_654_321},
        drain_failures: []
      })

    assert report.held_keys == %Trellis.HeldKeys{
             count: 1,
             oldest_poisoned_at: ~U[2024-09-25 00:00:00.654321Z]
           }
  end

  test "a self-check report's drain failures become structs with DateTimes, whatever the outcome" do
    report =
      SelfCheckReport.from_native(%{
        target: "order_totals",
        checked_through: "1/16B3748",
        rows_compared: 2,
        next_after: nil,
        outcome: :converged,
        status: nil,
        divergences: [],
        held_keys: nil,
        drain_failures: [
          %{
            seg_seq: 9,
            tables: ["public.orders"],
            error: "records fail only together",
            sqlstate: nil,
            since_micros: 1_727_222_400_654_321,
            last_seen_micros: 1_727_222_400_654_321,
            attempts: 1
          }
        ]
      })

    assert report.drain_failures == [
             %Trellis.DrainFailure{
               seg_seq: 9,
               tables: ["public.orders"],
               error: "records fail only together",
               sqlstate: nil,
               since: ~U[2024-09-25 00:00:00.654321Z],
               last_seen: ~U[2024-09-25 00:00:00.654321Z],
               attempts: 1
             }
           ]
  end

  defp native_definition do
    %{
      id: 7,
      target_table: "public.widget_prices",
      source_table: "public.widgets",
      source_version: 1,
      status: :live,
      source_columns: %{"price" => "integer"}
    }
  end

  # What the NIF's `definitions/1` returns for one definition.
  defp native_summary do
    native_definition()
    |> Map.delete(:source_columns)
    |> Map.merge(%{created_at_micros: @micros, backfill_failure: nil, halt: nil})
  end

  defp native_relationship do
    %{
      id: 2,
      name: "owner",
      from_schema: "public",
      from_table: "widgets",
      from_col: "owner_id",
      to_schema: "public",
      to_table: "users",
      to_col: "id",
      cardinality: :one,
      warnings: []
    }
  end

  defp native_applied(kind, fields \\ %{}) do
    Map.merge(
      %{
        kind: kind,
        definition: nil,
        relationship: nil,
        columns: nil,
        added: nil,
        dropped: nil,
        altered: nil
      },
      fields
    )
  end

  test "each apply outcome becomes its tagged value" do
    assert {:transform_defined, %Definition{id: 7, status: :live}} =
             Applied.from_native(
               native_applied(:transform_defined, %{definition: native_definition()})
             )

    assert {:relationship_defined, %Relationship{name: "owner", cardinality: :one}} =
             Applied.from_native(
               native_applied(:relationship_defined, %{relationship: native_relationship()})
             )

    assert Applied.from_native(native_applied(:paused)) == :paused
    assert Applied.from_native(native_applied(:dropped)) == :dropped

    assert Applied.from_native(native_applied(:resumed, %{columns: ["t.c"]})) ==
             {:resumed, ["t.c"]}

    assert {:altered,
            %{
              definition: %Definition{id: 7},
              added: ["doubled"],
              dropped: ["old"],
              altered: ["price"]
            }} =
             Applied.from_native(
               native_applied(:altered, %{
                 definition: native_definition(),
                 added: ["doubled"],
                 dropped: ["old"],
                 altered: ["price"]
               })
             )
  end

  # An outcome newer than the binding still reports success: the statement
  # was applied.
  test "an outcome the binding doesn't know is :unknown" do
    assert Applied.from_native(native_applied(:unknown)) == :unknown
  end

  test "creation times become DateTimes" do
    assert %DefinitionSummary{created_at: @time, backfill_failure: nil, halt: nil} =
             DefinitionSummary.from_native(native_summary())

    assert %RelationshipSummary{created_at: @time, cardinality: :many} =
             RelationshipSummary.from_native(
               native_relationship()
               |> Map.delete(:warnings)
               |> Map.merge(%{cardinality: :many, created_at_micros: @micros})
             )
  end

  test "a summary's backfill failure becomes a BackfillFailure" do
    failure = %{
      source_table: "public.orders",
      attempts: 2,
      last_error: "permission denied",
      next_attempt_at_micros: @micros
    }

    assert %DefinitionSummary{
             backfill_failure: %Trellis.BackfillFailure{
               source_table: "public.orders",
               attempts: 2,
               last_error: "permission denied",
               next_attempt_at: @time
             }
           } = DefinitionSummary.from_native(%{native_summary() | backfill_failure: failure})
  end

  test "a paused column's pause time becomes a DateTime, and a missing one stays nil" do
    assert %QuarantineEntry{
             target: "t.c",
             state: :paused,
             paused_at: @time,
             last_error: "integer out of range"
           } =
             QuarantineEntry.from_native(%{
               target: "t.c",
               state: :paused,
               paused_at_micros: @micros,
               last_error: "integer out of range"
             })

    assert %QuarantineEntry{paused_at: nil, last_error: nil} =
             QuarantineEntry.from_native(%{
               target: "t",
               state: :quarantined,
               paused_at_micros: nil,
               last_error: nil
             })
  end

  test "a poison time becomes a DateTime" do
    assert %PoisonEntry{poisoned_at: @time, key: "42"} =
             PoisonEntry.from_native(%{
               transform: "orders_view",
               src_table: "public.orders",
               key: "42",
               last_error: "boom",
               poisoned_at_micros: @micros
             })
  end

  test "a sample page keeps its cursor as given" do
    assert %SamplePage{
             samples: [%Trellis.PoisonSample{key: "1"}],
             next_cursor: "13:public.orders1"
           } =
             SamplePage.from_native(%{
               samples: [%{src_table: "public.orders", key: "1", error_message: "boom"}],
               next_cursor: "13:public.orders1"
             })
  end

  test "a DateTime crosses as microseconds and comes back unchanged" do
    assert Trellis.Time.to_micros(@time) == @micros
    assert Trellis.Time.from_micros(@micros) == @time
  end
end
