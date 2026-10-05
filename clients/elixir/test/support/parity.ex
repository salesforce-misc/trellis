defmodule Trellis.Parity do
  @moduledoc false
  # The Elixir half of the cross-language parity suite (issue #155): runs
  # `clients/parity/cases.json`, which the Ruby suite runs too, against this
  # binding. The shape language and the canonical form are described in
  # `clients/parity/README.md`; the Ruby suite's `test/support/parity.rb` is
  # this module's counterpart, and the two must read the fixture the same way.

  @fixture_path Path.expand("../../../parity/cases.json", __DIR__)
  @external_resource @fixture_path

  defmodule Mismatch do
    @moduledoc false
    defexception [:message]
  end

  def fixture_path, do: @fixture_path
  def fixture, do: @fixture_path |> File.read!() |> JSON.decode!()

  @doc """
  The closed word sets a `word_in` shape names, each read from the NIF (so
  from trellis-embed), never written out here.
  """
  def word_set("transform_status"), do: words(Trellis.Native.status_names())
  def word_set("quarantine_state"), do: words(Trellis.Native.quarantine_state_names())
  def word_set("cardinality"), do: words(Trellis.Native.cardinality_names())
  def word_set("applied_kind"), do: words(Trellis.Native.applied_kinds())
  def word_set("self_check_outcome"), do: words(Trellis.Native.self_check_outcomes())
  def word_set("divergence_kind"), do: words(Trellis.Native.divergence_kinds())
  def word_set("capture_failure_kind"), do: words(Trellis.Native.capture_failure_kinds())

  defp words({:ok, atoms}), do: Enum.map(atoms, &Atom.to_string/1)

  @doc """
  The fixture's operations. Each takes the connected handle (`nil` before
  `connect`), the step's positional arguments and its options, already
  turned into Elixir terms, and the function name to call (the bang variant
  for `name <> "!"`).
  """
  def operations do
    plain = fn handle, args, _opts, fun -> call(handle, args, fun) end

    %{
      "connect" => fn _handle, [], opts, fun ->
        Kernel.apply(Trellis, fun, [Map.to_list(opts)])
      end,
      "migrate" => plain,
      "config" => plain,
      "define" => plain,
      "apply" => plain,
      "status" => plain,
      "definitions" => plain,
      "relationships" => plain,
      "request_backfill" => plain,
      "poisoned_since" => plain,
      "quarantined" => plain,
      "quarantine_status" => plain,
      "sample_quarantined" => fn handle, args, opts, fun ->
        call(handle, args ++ [Map.to_list(opts)], fun)
      end,
      "has_live_drain_workers" => plain,
      "has_live_staging_worker" => plain,
      "watermark_token" => plain,
      "await_converged" => fn handle, [token], %{timeout_ms: timeout_ms}, fun ->
        call(handle, [token, timeout_ms], fun)
      end,
      "self_check" => fn handle, args, opts, fun ->
        call(handle, args ++ [Map.to_list(opts)], fun)
      end,
      "shutdown" => plain
    }
  end

  defp call(handle, args, fun), do: Kernel.apply(Trellis, fun, [handle | args])

  @doc """
  The public functions of `Trellis` with no fixture operation, and why. A
  supervisor owns a handle only on the BEAM; `supervised_test.exs` covers
  these, and the fixture's operations run through a supervised handle there
  too.
  """
  def not_operations do
    %{
      "child_spec" => "`{Trellis, options}` in a supervision tree",
      "start_link" => "`{Trellis, options}` in a supervision tree"
    }
  end

  @doc "The fixture's own steps, which drive the test rather than the binding."
  def harness_ops, do: ["sql", "now"]

  # The calls whose success is a bare `:ok` (Ruby's `nil`). Every other
  # call's success is `{:ok, value}`.
  @unit_ops ["connect", "migrate", "request_backfill", "await_converged", "shutdown"]

  @doc """
  What operation `op` returned, in the fixture's canonical form. The envelope
  goes first, and must be the one `op` has: `{:error, %Trellis.Error{}}` is
  the error; `:ok` from a unit call (`@unit_ops`) is `nil`; `{:ok, value}`
  from any other call is `value`, through `canonical/1` (`apply/2`'s through
  `canonical_applied/1`). Any other envelope raises `Mismatch`, so a call that
  returns a bare value, or `:ok` where it should return `{:ok, nil}`, fails
  its step.
  """
  def canonical_result(_op, {:error, %Trellis.Error{} = error}), do: canonical(error)
  def canonical_result(op, :ok) when op in @unit_ops, do: nil
  def canonical_result("apply", {:ok, applied}), do: canonical_applied(applied)
  def canonical_result(op, {:ok, value}) when op not in @unit_ops, do: canonical(value)

  def canonical_result(op, other) do
    success = if op in @unit_ops, do: ":ok", else: "{:ok, value}"
    mismatch("#{op} returned #{inspect(other)}, not #{success} or {:error, %Trellis.Error{}}")
  end

  @doc """
  A binding value in the fixture's canonical form: `nil`, booleans,
  integers, strings and lists as themselves; an atom as
  `%{"$word" => name}`; a `DateTime` as `%{"$time" => epoch microseconds}`;
  a struct as `%{"$record" => name, "fields" => ...}`; a map as
  `%{"$map" => ...}`; a `Trellis.Error` as `%{"$error" => code}`. A result's
  `{:ok, _}`/`:ok` envelope is `canonical_result/2`'s, so it is never
  unwrapped here, below the top.

  Checks the Elixir-side conventions on the way (UTC microsecond times,
  string map keys, errors as `Trellis.Error`) and raises `Mismatch` when one
  is broken.
  """
  def canonical(value) when is_nil(value) or is_boolean(value) or is_integer(value), do: value

  def canonical(value) when is_binary(value) do
    if String.valid?(value), do: value, else: mismatch("#{inspect(value)} isn't valid UTF-8")
  end

  def canonical(value) when is_atom(value), do: %{"$word" => Atom.to_string(value)}
  def canonical(value) when is_list(value), do: Enum.map(value, &canonical/1)

  def canonical(%DateTime{} = time) do
    cond do
      time.time_zone != "Etc/UTC" or time.utc_offset != 0 or time.std_offset != 0 ->
        mismatch("time #{inspect(time)} isn't UTC")

      elem(time.microsecond, 1) != 6 ->
        mismatch("time #{inspect(time)} isn't at microsecond precision")

      true ->
        %{"$time" => DateTime.to_unix(time, :microsecond)}
    end
  end

  def canonical(%Trellis.Error{code: code, message: message}) do
    unless is_atom(code) and is_binary(message),
      do: mismatch("error #{inspect({code, message})} isn't an atom code and a message")

    %{"$error" => Atom.to_string(code)}
  end

  def canonical(%module{} = struct) do
    case inspect(module) do
      "Trellis." <> name ->
        fields =
          struct
          |> Map.from_struct()
          |> Map.new(fn {k, v} -> {Atom.to_string(k), canonical(v)} end)

        %{"$record" => name, "fields" => fields}

      _ ->
        mismatch("#{inspect(struct)} isn't a Trellis record")
    end
  end

  def canonical(map) when is_map(map) do
    %{"$map" => Map.new(map, fn {k, v} -> {map_key(k), canonical(v)} end)}
  end

  def canonical(value), do: mismatch("#{inspect(value)} has no canonical form")

  defp map_key(key) when is_binary(key), do: key
  defp map_key(key), do: mismatch("map key #{inspect(key)} isn't a string")

  @applied_fields [:definition, :relationship, :columns, :added, :dropped, :altered]

  @doc """
  A `Trellis.Applied.t()` tagged tuple in canonical form, as the fixture's
  `Applied` record: its kind, the fields that kind carries, and `nil` for
  the rest (the shape the Ruby binding returns). `apply/2`'s tagged tuples
  are the one host-specific shape the canonical form absorbs.
  """
  def canonical_applied(kind) when is_atom(kind), do: applied_record(kind, %{})

  def canonical_applied({:transform_defined, definition}),
    do: applied_record(:transform_defined, %{definition: definition})

  def canonical_applied({:relationship_defined, relationship}),
    do: applied_record(:relationship_defined, %{relationship: relationship})

  def canonical_applied({:resumed, columns}), do: applied_record(:resumed, %{columns: columns})
  def canonical_applied({:altered, alteration}), do: applied_record(:altered, alteration)
  def canonical_applied(other), do: mismatch("#{inspect(other)} isn't a Trellis.Applied")

  defp applied_record(kind, carried) do
    nils = Map.new(@applied_fields, &{&1, nil})

    fields =
      nils
      |> Map.merge(carried)
      |> Map.put(:kind, kind)
      |> Map.new(fn {k, v} -> {Atom.to_string(k), canonical(v)} end)

    %{"$record" => "Applied", "fields" => fields}
  end

  defp mismatch(message), do: raise(Mismatch, message)

  @doc """
  Every mismatch between canonical `value` and `shape`, as "path: problem"
  (empty when it matches).
  """
  def mismatches(shape, value, saved), do: check(shape, value, "result", saved, [])

  defp check(nil, value, path, _saved, acc) do
    if is_nil(value), do: acc, else: acc ++ ["#{path}: expected nil, got #{inspect(value)}"]
  end

  defp check(type, value, path, _saved, acc) when is_binary(type) do
    ok =
      case type do
        "string" -> is_binary(value)
        "integer" -> is_integer(value)
        "boolean" -> is_boolean(value)
        "time" -> is_map(value) and Map.has_key?(value, "$time")
        _ -> raise ArgumentError, "#{path}: the fixture has no type #{inspect(type)}"
      end

    if ok, do: acc, else: acc ++ ["#{path}: expected a #{type}, got #{inspect(value)}"]
  end

  defp check(%{"record" => name} = shape, value, path, saved, acc)
       when map_size(shape) == 1 or (map_size(shape) == 2 and is_map_key(shape, "fields")) do
    check_record(name, Map.get(shape, "fields", %{}), value, path, saved, acc)
  end

  defp check(shape, value, path, saved, acc) when is_map(shape) and map_size(shape) == 1 do
    [{form, arg}] = Map.to_list(shape)
    check_form(form, arg, value, path, saved, acc)
  end

  defp check(shape, _value, path, _saved, _acc),
    do: raise(ArgumentError, "#{path}: the fixture has no shape #{inspect(shape)}")

  defp check_form("eq", arg, value, path, _saved, acc), do: equal(arg, value, path, acc)

  defp check_form("word", arg, value, path, _saved, acc),
    do: equal(%{"$word" => arg}, value, path, acc)

  defp check_form("time_micros", arg, value, path, _saved, acc),
    do: equal(%{"$time" => arg}, value, path, acc)

  defp check_form("error", arg, value, path, _saved, acc),
    do: equal(%{"$error" => arg}, value, path, acc)

  defp check_form("ref", arg, value, path, saved, acc),
    do: equal(canonical(resolve(saved, arg)), value, path, acc)

  defp check_form("nullable", _arg, nil, _path, _saved, acc), do: acc

  defp check_form("nullable", arg, value, path, saved, acc),
    do: check(arg, value, path, saved, acc)

  defp check_form("word_in", set, value, path, _saved, acc) do
    words = word_set(set)

    case value do
      %{"$word" => word} when is_binary(word) ->
        if word in words,
          do: acc,
          else: acc ++ ["#{path}: #{word} isn't in #{set} #{inspect(words)}"]

      _ ->
        acc ++ ["#{path}: expected a word from #{set} #{inspect(words)}, got #{inspect(value)}"]
    end
  end

  defp check_form(form, arg, value, path, saved, acc) when form in ["list", "list_of"] do
    cond do
      not is_list(value) ->
        acc ++ ["#{path}: expected a list, got #{inspect(value)}"]

      form == "list" and length(arg) != length(value) ->
        acc ++ ["#{path}: expected #{length(arg)} items, got #{length(value)}: #{inspect(value)}"]

      true ->
        shapes = if form == "list", do: arg, else: List.duplicate(arg, length(value))

        [shapes, value]
        |> Enum.zip()
        |> Enum.with_index()
        |> Enum.reduce(acc, fn {{shape, item}, i}, acc ->
          check(shape, item, "#{path}[#{i}]", saved, acc)
        end)
    end
  end

  defp check_form(form, arg, value, path, saved, acc) when form in ["map", "map_of"] do
    case value do
      %{"$map" => map} ->
        if form == "map" and Enum.sort(Map.keys(arg)) != Enum.sort(Map.keys(map)) do
          acc ++
            [
              "#{path}: expected keys #{inspect(Enum.sort(Map.keys(arg)))}, got #{inspect(Enum.sort(Map.keys(map)))}"
            ]
        else
          map
          |> Enum.sort()
          |> Enum.reduce(acc, fn {key, item}, acc ->
            shape = if form == "map", do: Map.fetch!(arg, key), else: arg
            check(shape, item, "#{path}[#{inspect(key)}]", saved, acc)
          end)
        end

      _ ->
        acc ++ ["#{path}: expected a map, got #{inspect(value)}"]
    end
  end

  defp check_form(form, _arg, _value, path, _saved, _acc),
    do: raise(ArgumentError, "#{path}: the fixture has no shape #{inspect(form)}")

  defp check_record(name, overrides, value, path, saved, acc) do
    schema =
      Map.get(fixture()["records"], name) ||
        raise ArgumentError, "#{path}: no record #{name} in the fixture"

    case Map.keys(overrides) -- Map.keys(schema) do
      [] -> :ok
      unknown -> raise ArgumentError, "#{path}: #{name} has no fields #{inspect(unknown)}"
    end

    case value do
      %{"$record" => ^name, "fields" => fields} ->
        if Enum.sort(Map.keys(fields)) == Enum.sort(Map.keys(schema)) do
          schema
          |> Enum.sort()
          |> Enum.reduce(acc, fn {field, field_shape}, acc ->
            shape = Map.get(overrides, field, field_shape)
            check(shape, Map.fetch!(fields, field), "#{path}.#{field}", saved, acc)
          end)
        else
          acc ++
            [
              "#{path}: #{name}'s fields are #{inspect(Enum.sort(Map.keys(fields)))}, " <>
                "expected #{inspect(Enum.sort(Map.keys(schema)))}"
            ]
        end

      _ ->
        acc ++ ["#{path}: expected a #{name}, got #{inspect(value)}"]
    end
  end

  defp equal(expected, value, _path, acc) when expected === value, do: acc

  defp equal(expected, value, path, acc),
    do: acc ++ ["#{path}: expected #{inspect(expected)}, got #{inspect(value)}"]

  @doc """
  A saved host value, then down `path` (`"page_1.next_cursor"`,
  `"poisoned.0.poisoned_at"`).
  """
  def resolve(saved, path) do
    [name | rest] = String.split(path, ".")

    value =
      case Map.fetch(saved, name) do
        {:ok, value} -> value
        :error -> raise ArgumentError, "nothing was saved as #{inspect(name)}"
      end

    Enum.reduce(rest, value, fn
      segment, list when is_list(list) ->
        Enum.fetch!(list, String.to_integer(segment))

      segment, struct when is_struct(struct) ->
        Map.fetch!(struct, String.to_existing_atom(segment))
    end)
  end

  @doc """
  A fixture argument as an Elixir term: `$ref`, `$word`, `$time_micros` and
  `$dsn` are resolved, an options object becomes a map with atom keys, and
  anything else is taken as it is.
  """
  def argument(list, saved, cluster) when is_list(list),
    do: Enum.map(list, &argument(&1, saved, cluster))

  def argument(%{"$ref" => path}, saved, _cluster), do: resolve(saved, path)
  def argument(%{"$word" => word}, _saved, _cluster), do: String.to_existing_atom(word)

  def argument(%{"$time_micros" => micros}, _saved, _cluster),
    do: DateTime.from_unix!(micros, :microsecond)

  def argument(%{"$dsn" => overrides}, _saved, cluster), do: dsn(cluster["dsn"], overrides)

  def argument(map, saved, cluster) when is_map(map),
    do: Map.new(map, fn {k, v} -> {String.to_existing_atom(k), argument(v, saved, cluster)} end)

  def argument(value, _saved, _cluster), do: value

  # `dsn` with each of `overrides` (`%{"user" => ...}`) set in place of its
  # own.
  defp dsn(dsn, overrides) do
    Enum.reduce(overrides, dsn, fn {key, value}, current ->
      pattern = ~r/(?<=\A|\s)#{Regex.escape(key)}=\S*/

      if Regex.match?(pattern, current),
        do: Regex.replace(pattern, current, "#{key}=#{value}"),
        else: "#{current} #{key}=#{value}"
    end)
  end

  @doc """
  A native map from the fixture's conversions, as the NIF would hand it
  over: atom keys, `$word` as an atom.
  """
  def native(%{"$word" => word}), do: String.to_existing_atom(word)
  def native(list) when is_list(list), do: Enum.map(list, &native/1)

  def native(map) when is_map(map),
    do: Map.new(map, fn {k, v} -> {String.to_existing_atom(k), native(v)} end)

  def native(value), do: value

  @doc "Every shape anywhere under `shape`, for the coverage checks."
  def shapes(shape) when is_list(shape), do: Enum.flat_map(shape, &shapes/1)
  def shapes(shape) when is_map(shape), do: [shape | Enum.flat_map(Map.values(shape), &shapes/1)]
  def shapes(shape), do: [shape]
end
