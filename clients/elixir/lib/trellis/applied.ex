defmodule Trellis.Applied do
  @moduledoc """
  What `Trellis.apply/2` did, one shape per statement form:

  - `{:transform_defined, %Trellis.Definition{}}`: a `TRANSFORM` statement
    registered a definition, at `:waiting_to_backfill`.
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
  - `{:altered, %{definition: %Trellis.Definition{}, added: names, dropped:
    names, altered: names}}`: an `ALTER TRANSFORM` statement edited its
    subject. The lists name only the fields this call actually changed.
  - `:unknown`: the statement was applied, but its outcome is newer than
    this version of the binding, which has no shape for it. It is still a
    success: don't retry the statement, which would apply it twice.
  """

  @type alteration :: %{
          definition: Trellis.Definition.t(),
          added: [String.t()],
          dropped: [String.t()],
          altered: [String.t()]
        }

  @type t ::
          {:transform_defined, Trellis.Definition.t()}
          | {:relationship_defined, Trellis.Relationship.t()}
          | :paused
          | {:resumed, [String.t()]}
          | :dropped
          | {:altered, alteration()}
          | :unknown

  @doc false
  @spec from_native(map()) :: t()
  def from_native(%{kind: :transform_defined, definition: definition}),
    do: {:transform_defined, Trellis.Definition.from_native(definition)}

  def from_native(%{kind: :relationship_defined, relationship: relationship}),
    do: {:relationship_defined, Trellis.Relationship.from_native(relationship)}

  def from_native(%{kind: :resumed, columns: columns}), do: {:resumed, columns}

  def from_native(%{kind: :altered} = altered) do
    {:altered,
     %{
       definition: Trellis.Definition.from_native(altered.definition),
       added: altered.added,
       dropped: altered.dropped,
       altered: altered.altered
     }}
  end

  def from_native(%{kind: kind}) when kind in [:paused, :dropped, :unknown], do: kind
end
