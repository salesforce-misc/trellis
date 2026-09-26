defmodule Trellis.Eventually do
  @moduledoc false
  # Polling for the integration tests, which wait on the engine's background
  # workers: `eventually/2` calls `check` until it returns `{:done, result}`,
  # or fails the test naming what it waited for and the last
  # `{:waiting, seen}` value.

  import ExUnit.Assertions, only: [flunk: 1]

  @converge_ms 30_000

  def eventually(what, check),
    do: poll(what, check, System.monotonic_time(:millisecond) + @converge_ms)

  defp poll(what, check, deadline) do
    case check.() do
      {:done, result} ->
        result

      {:waiting, seen} ->
        if System.monotonic_time(:millisecond) > deadline do
          flunk("waited #{@converge_ms}ms for #{what}; last saw #{inspect(seen)}")
        else
          Process.sleep(50)
          poll(what, check, deadline)
        end
    end
  end
end
