defmodule Trellis.ParityTest do
  # The cross-language parity suite (issue #155): `clients/parity/cases.json`,
  # a script of (operation, input, expected shape) steps that the Ruby suite
  # runs too. Neither suite owns it; both are checked against it, so a field
  # name, a nil, an atom, a time or a page that one binding shapes
  # differently from the other fails here or there. See
  # `clients/parity/README.md`.
  #
  # The live script runs on a cluster of its own, so it shares nothing with
  # the other integration tests; it is still `async: false`, like them, so
  # the suite never runs two staging workers' worth of load at once.
  use ExUnit.Case, async: false

  import Trellis.Eventually

  alias Trellis.{Parity, TestCluster}

  # The script, top to bottom. It runs the staging worker and ends by
  # poisoning a key, which would disturb the shared cluster (#588).
  @tag timeout: 300_000
  test "every live step returns its expected shape" do
    cluster = TestCluster.private!()
    pg = TestCluster.postgrex!(cluster)
    # The connected handle, if any. Unlinked, so it outlives the test
    # process for the `on_exit` below.
    {:ok, state} = Agent.start(fn -> nil end)

    # Registered after `private!/0`'s teardown, so it runs first: the handle
    # is shut down before its cluster goes away.
    on_exit(fn ->
      if handle = Agent.get(state, & &1), do: Trellis.shutdown(handle)
      Agent.stop(state)
    end)

    Parity.fixture()["live"]
    |> Enum.with_index()
    |> Enum.reduce(%{}, fn {step, index}, saved ->
      run_step(step, index, pg, cluster, state, saved)
    end)

    # The script ends with a shutdown; forward the engine's last log lines
    # while this test still captures them.
    Trellis.LogBridge.flush()
  end

  # The host half of each conversion: the native map the NIF would hand
  # over, turned into this binding's value.
  test "every conversion returns its expected shape" do
    # Loads every struct module, so each field name the native maps use
    # already exists as an atom.
    Enum.each(Application.spec(:trellis, :modules), &Code.ensure_loaded!/1)

    for conversion <- Parity.fixture()["conversions"] do
      native = Parity.native(conversion["native"])

      value =
        case conversion["type"] do
          "Error" -> Parity.canonical(Trellis.Error.from_native({native.code, native.message}))
          "Applied" -> Parity.canonical_applied(Trellis.Applied.from_native(native))
          type -> Parity.canonical(Module.concat(Trellis, type).from_native(native))
        end

      assert_shape(conversion["expect"], value, %{}, conversion["about"])
    end
  end

  # Every error code trellis-embed maps has a live step that returns it, so
  # a code's atom is checked against a real engine error, not only a table.
  test "every error code has a live step" do
    {:ok, codes} = Trellis.Native.error_codes()

    raised =
      for %{"error" => code} <- Parity.shapes(Parity.fixture()["live"]), do: code

    assert codes -- raised == [],
           "no live step in #{Parity.fixture_path()} returns these codes"
  end

  # Every statement form the grammar has is applied by some step, named by
  # its `trellis::StatementKind`.
  test "every statement kind has a live step" do
    {:ok, kinds} = Trellis.Native.statement_kinds()

    covered =
      for %{"covers" => %{"statement_kind" => kind}} <- Parity.fixture()["live"], do: kind

    assert Enum.sort(covered) == Enum.sort(kinds)
  end

  # The fixture's records are exactly this binding's value structs, field for
  # field, and every one is checked by some step or conversion. `Applied` is
  # the exception: here it is a tagged tuple, which `canonical_applied/1`
  # spreads into the fixture's record.
  test "the fixture describes every record this binding returns" do
    records = Parity.fixture()["records"]

    host =
      for module <- Application.spec(:trellis, :modules),
          Code.ensure_loaded!(module),
          function_exported?(module, :__struct__, 0),
          # The handle, and exceptions (`Trellis.Error` is checked by code).
          module != Trellis,
          not Map.has_key?(module.__struct__(), :__exception__),
          "Trellis." <> name = inspect(module),
          into: %{} do
        fields =
          module.__struct__() |> Map.from_struct() |> Map.keys() |> Enum.map(&Atom.to_string/1)

        {name, Enum.sort(fields)}
      end

    expected =
      records
      |> Map.delete("Applied")
      |> Map.new(fn {name, fields} -> {name, fields |> Map.keys() |> Enum.sort()} end)

    assert host == expected

    fixture = Parity.fixture()

    checked =
      for %{"record" => name} <- Parity.shapes([fixture["live"], fixture["conversions"]]),
          do: name

    assert Map.keys(records) -- checked == [], "records no step or conversion checks"
  end

  # Every public function of this binding is a fixture operation (its bang
  # variant alongside), and every operation is used by some step.
  test "every public function is a fixture operation that some step runs" do
    public =
      for {name, _arity} <- Trellis.__info__(:functions),
          name = Atom.to_string(name),
          not String.starts_with?(name, "__"),
          uniq: true,
          do: String.trim_trailing(name, "!")

    assert Enum.sort(public) == Enum.sort(Map.keys(Parity.operations()))

    used = Parity.fixture()["live"] |> Enum.map(& &1["op"]) |> Enum.uniq()
    assert Enum.sort(used) == Enum.sort(Map.keys(Parity.operations()) ++ Parity.harness_ops())
  end

  defp run_step(step, index, pg, cluster, state, saved) do
    op = step["op"]
    what = "step #{index} (#{op}#{if step["about"], do: ": " <> step["about"]})"

    case op do
      "sql" ->
        Enum.each(step["args"], &Postgrex.query!(pg, &1, []))
        saved

      "now" ->
        Map.put(saved, step["save"], DateTime.utc_now())

      _ ->
        args = Parity.argument(Map.get(step, "args", []), saved, cluster)
        opts = Parity.argument(Map.get(step, "opts", %{}), saved, cluster)
        call = fn -> call_operation(op, args, opts, state) end

        result =
          if Map.has_key?(step, "until") do
            poll(step, saved, what, call)
          else
            result = call.()
            assert_shape(step["expect"], canonical(op, result), saved, what)

            with %{"error" => code} <- step["expect"],
                 do: assert_bang_raises(code, op, args, opts, state, what)

            result
          end

        if step["save"], do: Map.put(saved, step["save"], unwrap(result)), else: saved
    end
  end

  # The operation's `{:ok, value}`, `:ok` or `{:error, error}`. A successful
  # `connect` hands its handle to the steps after it, and `shutdown` takes it
  # away again.
  defp call_operation(op, args, opts, state) do
    handle = Agent.get(state, & &1)
    fun = String.to_existing_atom(op)
    result = Parity.operations() |> Map.fetch!(op) |> then(& &1.(handle, args, opts, fun))

    case {op, result} do
      {"connect", {:ok, %Trellis{} = handle}} ->
        Agent.update(state, fn _ -> handle end)
        :ok

      {"shutdown", :ok} ->
        Agent.update(state, fn _ -> nil end)
        :ok

      _ ->
        result
    end
  end

  # The bang variant of a step that returned an error raises that error.
  defp assert_bang_raises(code, op, args, opts, state, what) do
    handle = Agent.get(state, & &1)
    fun = String.to_existing_atom(op <> "!")

    error =
      assert_raise Trellis.Error, fn ->
        Parity.operations() |> Map.fetch!(op) |> then(& &1.(handle, args, opts, fun))
      end

    assert Atom.to_string(error.code) == code, "#{what}: #{op}! raised #{inspect(error)}"
  end

  # Calls the operation until its result matches the step's `until` shape,
  # for at most `Trellis.Eventually`'s bound, then fails naming what it
  # waited for and the last result it saw (#297).
  defp poll(step, saved, what, call) do
    eventually("#{step["waiting_for"]} (#{what})", fn ->
      result = call.()

      if shape_mismatches(step["until"], canonical(step["op"], result), saved) == [],
        do: {:done, result},
        else: {:waiting, result}
    end)
  end

  defp canonical(op, result), do: Parity.canonical_result(op, result)

  defp unwrap({:ok, value}), do: value
  defp unwrap(other), do: other

  defp assert_shape(shape, canonical, saved, what) do
    problems = shape_mismatches(shape, canonical, saved)

    assert problems == [],
           "#{what} returned #{inspect(canonical)}:\n" <> Enum.join(problems, "\n")
  end

  defp shape_mismatches(shape, canonical, saved), do: Parity.mismatches(shape, canonical, saved)
end
