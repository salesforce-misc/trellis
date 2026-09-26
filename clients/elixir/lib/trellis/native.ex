defmodule Trellis.Native do
  @moduledoc false
  # The NIF boundary (native/trellis_nif). Every function here runs on a
  # dirty IO scheduler and returns `{:ok, value}` or `{:error, {code, message}}`
  # with `code` a binary; `Trellis` turns the error into a `Trellis.Error`.

  use Rustler,
    otp_app: :trellis,
    crate: "trellis_nif",
    # Debug outside :prod, so a dev or test build reuses the Cargo
    # workspace's own debug build of `trellis` (Cargo builds this workspace
    # member into the repository's `target/`).
    mode: if(Mix.env() == :prod, do: :release, else: :debug)

  def connect(_options), do: :erlang.nif_error(:nif_not_loaded)
  def migrate(_handle), do: :erlang.nif_error(:nif_not_loaded)
  def define(_handle, _text), do: :erlang.nif_error(:nif_not_loaded)
  def status(_handle, _target_table), do: :erlang.nif_error(:nif_not_loaded)
  def shutdown(_handle), do: :erlang.nif_error(:nif_not_loaded)
  def error_codes, do: :erlang.nif_error(:nif_not_loaded)
  def status_names, do: :erlang.nif_error(:nif_not_loaded)
end
