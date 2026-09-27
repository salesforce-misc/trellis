defmodule Trellis.MetricsTest do
  use ExUnit.Case, async: true

  # The registry is process-wide, so rendering needs no handle. It is empty
  # until something records into it (`TrellisTest`'s slice test checks a
  # drain's series shows up), so this only checks the text's shape.
  test "renders the process-wide registry as Prometheus text, without a handle" do
    body = Trellis.Metrics.render_prometheus()
    assert is_binary(body)

    for line <- String.split(body, "\n", trim: true) do
      assert line =~ ~r/^(# (HELP|TYPE) trellis_\w+ .+|trellis_\w+(\{[^}]*\})? \S+)$/,
             "not a Prometheus exposition line: #{inspect(line)}"
    end
  end
end
