defmodule Trellis.Error do
  @moduledoc """
  An error from Trellis: a stable `code` and the engine's message, and
  nothing else (ADR-0010 decision 4).

  Non-bang functions return `{:error, %Trellis.Error{}}`; bang functions
  raise it.

  `code` is one of `t:code/0`. The engine's error codes are an open set, so a
  code this version of the binding doesn't know arrives as `:unknown`, with
  the engine's code named in the message, rather than crashing.
  """

  @typedoc """
  - `:parse`: the statement text isn't valid grammar.
  - `:validation`: well-formed but rejected (a bad option, an unknown column,
    a type mismatch, a call on a handle that has been shut down).
  - `:connectivity`: the database couldn't be reached, or the connection
    dropped.
  - `:conflict`: clashes with something that already exists.
  - `:not_found`: names something that doesn't exist.
  - `:internal`: a Trellis bug or an unexpected database failure.
  - `:unknown`: a code newer than this binding.
  """
  @type code ::
          :parse | :validation | :connectivity | :conflict | :not_found | :internal | :unknown

  @type t :: %__MODULE__{code: code(), message: String.t()}

  defexception [:code, :message]

  # The closed set of code atoms, written out so no atom is ever built from a
  # string that crossed the NIF boundary. `test/trellis/error_test.exs`
  # fails when the engine reports a code this map lacks.
  @codes %{
    "parse" => :parse,
    "validation" => :validation,
    "connectivity" => :connectivity,
    "conflict" => :conflict,
    "not_found" => :not_found,
    "internal" => :internal
  }

  @doc false
  @spec from_native({String.t(), String.t()}) :: t()
  def from_native({code, message}) when is_binary(code) and is_binary(message) do
    case Map.fetch(@codes, code) do
      {:ok, atom} ->
        %__MODULE__{code: atom, message: message}

      :error ->
        %__MODULE__{code: :unknown, message: "(error code #{inspect(code)}) #{message}"}
    end
  end

  @doc false
  @spec validation(String.t()) :: t()
  def validation(message), do: %__MODULE__{code: :validation, message: message}
end
