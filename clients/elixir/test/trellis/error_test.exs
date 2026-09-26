defmodule Trellis.ErrorTest do
  use ExUnit.Case, async: true

  alias Trellis.Error

  # ADR-0010 decision 4: every code the engine reports today has its own
  # atom, so a new code is a deliberate change here rather than a silent
  # downgrade to :unknown.
  test "every error code the engine reports maps to its own atom" do
    {:ok, codes} = Trellis.Native.error_codes()
    assert codes != []

    for code <- codes do
      assert %Error{code: atom, message: "boom"} = Error.from_native({code, "boom"})

      assert Atom.to_string(atom) == code,
             "error code #{inspect(code)} has no atom in Trellis.Error; add it"
    end
  end

  test "a code this binding doesn't know becomes :unknown and keeps the code in the message" do
    assert %Error{code: :unknown, message: message} =
             Error.from_native({"brand_new_code", "something broke"})

    assert message =~ "brand_new_code"
    assert message =~ "something broke"
  end

  test "is an exception carrying the engine's message" do
    error = Error.from_native({"conflict", "already defined"})
    assert Exception.message(error) == "already defined"
    assert_raise Error, "already defined", fn -> raise error end
  end
end
