defmodule Trellis.Applied do
  @moduledoc """
  What `Trellis.apply/2` did, one shape per statement form:

  - `{:transform_defined, target_table}`: a `TRANSFORM` statement registered
    the transform writing `target_table` (fully qualified), at
    `:waiting_to_backfill`.
  - `{:relationship_defined, %Trellis.Relationship{}}`: a `RELATIONSHIP`
    statement registered a relationship.
  - `:paused`: a `PAUSE TRANSFORM` statement froze its subject, or found it
    already frozen.
  - `{:resumed, addresses}`: a `RESUME TRANSFORM` statement unfroze its
    subject. `addresses` lists every column a column resume resumed, as
    `"transform.column"`: the one it named, then any dependent whose pause
    was only that one's cascade. It is `[]` for a whole-transform resume,
    which drops the transform to `:waiting_to_backfill` to rebuild.
  - `:dropped`: a `DROP` statement removed its subject, or found it already
    gone.
  - `{:altered, %{target_table: target_table, added: names, dropped: names,
    altered: names}}`: an `ALTER TRANSFORM` statement edited its
    subject. The lists name only the fields this call actually changed.
  - `:unknown`: the statement was applied, but its outcome is newer than
    this version of the binding, which has no shape for it. It is still a
    success: don't retry the statement, which would apply it twice.
  """

  @type alteration :: %{
          target_table: String.t(),
          added: [String.t()],
          dropped: [String.t()],
          altered: [String.t()]
        }

  @type t ::
          {:transform_defined, String.t()}
          | {:relationship_defined, Trellis.Relationship.t()}
          | :paused
          | {:resumed, [String.t()]}
          | :dropped
          | {:altered, alteration()}
          | :unknown

  @doc false
  @spec from_native(map()) :: t()
  def from_native(%{kind: :transform_defined, target_table: target_table}),
    do: {:transform_defined, target_table}

  def from_native(%{kind: :relationship_defined, relationship: relationship}),
    do: {:relationship_defined, Trellis.Relationship.from_native(relationship)}

  def from_native(%{kind: :resumed, columns: columns}), do: {:resumed, columns}

  def from_native(%{kind: :altered} = altered) do
    {:altered,
     %{
       target_table: altered.target_table,
       added: altered.added,
       dropped: altered.dropped,
       altered: altered.altered
     }}
  end

  def from_native(%{kind: kind}) when kind in [:paused, :dropped, :unknown], do: kind
end
