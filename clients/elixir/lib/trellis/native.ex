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
  def apply(_handle, _text), do: :erlang.nif_error(:nif_not_loaded)
  def status(_handle, _target_table), do: :erlang.nif_error(:nif_not_loaded)
  def definitions(_handle), do: :erlang.nif_error(:nif_not_loaded)
  def relationships(_handle), do: :erlang.nif_error(:nif_not_loaded)
  def request_backfill(_handle, _source_table), do: :erlang.nif_error(:nif_not_loaded)
  def poisoned_since(_handle, _watermark_micros), do: :erlang.nif_error(:nif_not_loaded)
  def quarantined(_handle), do: :erlang.nif_error(:nif_not_loaded)
  def quarantine_status(_handle, _target), do: :erlang.nif_error(:nif_not_loaded)

  def sample_quarantined(_handle, _target, _cursor, _limit),
    do: :erlang.nif_error(:nif_not_loaded)

  def has_live_drain_workers(_handle), do: :erlang.nif_error(:nif_not_loaded)
  def has_live_staging_worker(_handle), do: :erlang.nif_error(:nif_not_loaded)
  def watermark_token(_handle), do: :erlang.nif_error(:nif_not_loaded)
  def await_converged(_handle, _token, _timeout_ms), do: :erlang.nif_error(:nif_not_loaded)
  def shutdown(_handle), do: :erlang.nif_error(:nif_not_loaded)
  def error_codes, do: :erlang.nif_error(:nif_not_loaded)
  def status_names, do: :erlang.nif_error(:nif_not_loaded)
  def quarantine_state_names, do: :erlang.nif_error(:nif_not_loaded)
  def cardinality_names, do: :erlang.nif_error(:nif_not_loaded)
  def applied_kinds, do: :erlang.nif_error(:nif_not_loaded)
  def statement_kinds, do: :erlang.nif_error(:nif_not_loaded)
end
