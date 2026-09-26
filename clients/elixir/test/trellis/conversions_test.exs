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
    SamplePage
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
    assert %DefinitionSummary{created_at: @time} =
             DefinitionSummary.from_native(
               native_definition()
               |> Map.delete(:source_columns)
               |> Map.put(:created_at_micros, @micros)
             )

    assert %RelationshipSummary{created_at: @time, cardinality: :many} =
             RelationshipSummary.from_native(
               native_relationship()
               |> Map.delete(:warnings)
               |> Map.merge(%{cardinality: :many, created_at_micros: @micros})
             )
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
