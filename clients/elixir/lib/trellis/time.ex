defmodule Trellis.Time do
  @moduledoc false
  # Times cross the NIF boundary as signed microseconds since the Unix epoch
  # (ADR-0010 decision 4) and become `DateTime`s here, in Elixir, not in Rust.

  @spec from_micros(integer()) :: DateTime.t()
  def from_micros(micros) when is_integer(micros), do: DateTime.from_unix!(micros, :microsecond)

  @spec to_micros(DateTime.t()) :: integer()
  def to_micros(%DateTime{} = time), do: DateTime.to_unix(time, :microsecond)
end
