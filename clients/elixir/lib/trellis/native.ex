defmodule Trellis.Native do
  @moduledoc false
  # The NIF boundary (native/trellis_nif). Every result is `{:ok, value}` or
  # `{:error, {code, message}}` with `code` a binary; `Trellis` turns the
  # error into a `Trellis.Error`.
  #
  # A call on a handle doesn't run on a dirty scheduler: the NIF submits it
  # on a normal one and returns `{:ok, call_ref}` at once, and the engine
  # sends `{call_ref, result}` to the calling process when the call ends
  # (the NIF crate's module docs have the rest). `call/3` is the `receive`
  # for that message. Nothing then holds a scheduler while the database
  # answers (a NIF on a dirty scheduler keeps it until it returns, even once
  # its caller has been killed). The NIFs that touch no handle, and `connect` and `shutdown`,
  # run on dirty IO schedulers; the NIF crate's module docs list the rest.
  #
  # A host gets the NIF precompiled: rustler_precompiled downloads the build
  # for its target from the `elixir-v<version>` GitHub release, which
  # .github/workflows/elixir-release.yml builds, and checks it against
  # checksum-Elixir.Trellis.Native.exs, which ships in the Hex package.
  # `TRELLIS_PG_BUILD=1` builds it from source instead (it needs Rust and
  # the host's own `{:rustler, ...}` dep), for a target not listed below.
  # Inside this repository (dev and test; a dependency always compiles in
  # :prod) it always builds from source, so CI tests the NIF it builds.

  version = Mix.Project.config()[:version]

  force_build =
    if System.get_env("TRELLIS_PG_BUILD") in ["1", "true"] or Mix.env() in [:dev, :test],
      # Otherwise unset, so rustler_precompiled's own switches still apply
      # (`config :rustler_precompiled, :force_build, trellis_pg: true`).
      do: [force_build: true],
      else: []

  use RustlerPrecompiled,
      [
        otp_app: :trellis_pg,
        crate: "trellis_nif",
        version: version,
        base_url:
          "https://github.com/salesforce-misc/trellis/releases/download/elixir-v#{version}",
        # The release workflow's matrix. Every build is for NIF version
        # 2.15, which loads on OTP 22 and up.
        targets: ~w(
          aarch64-apple-darwin
          x86_64-apple-darwin
          aarch64-unknown-linux-gnu
          x86_64-unknown-linux-gnu
          x86_64-unknown-linux-musl
        ),
        nif_versions: ["2.15"],
        # A source build: debug outside :prod, so a dev or test build reuses
        # the Cargo workspace's own debug build of `trellis` (Cargo builds
        # this workspace member into the repository's `target/`).
        mode: if(Mix.env() == :prod, do: :release, else: :debug)
      ] ++ force_build

  # How long `call/3` waits for a reply: the engine's 30-second call deadline,
  # its one-second client-side backstop after that, and a margin for the reply
  # to be encoded and delivered. The engine always replies by then, so this
  # only ends the wait for a reply that was lost (a bug), with the same
  # `timeout` error a call that ran out of its deadline gets.
  @reply_backstop_ms 35_000

  @doc """
  Runs `function` on `handle` (a handle's `ref`) with `args` and waits for its
  result, in the calling process.
  """
  @spec call(reference(), atom(), [term()]) :: {:ok, term()} | {:error, {String.t(), String.t()}}
  def call(handle, function, args) do
    case Kernel.apply(__MODULE__, function, [handle | args]) do
      {:ok, call_ref} when is_reference(call_ref) -> await(call_ref, @reply_backstop_ms)
      # Refused before it was submitted: a shut-down handle, a bad argument.
      {:error, _} = refused -> refused
    end
  end

  @doc false
  # The wait for call `call_ref`'s reply, public so a test can drive the
  # backstop with a short one.
  @spec await(reference(), non_neg_integer()) ::
          {:ok, term()} | {:error, {String.t(), String.t()}}
  def await(call_ref, backstop_ms) do
    receive do
      {^call_ref, result} -> result
    after
      backstop_ms ->
        {:error,
         {"timeout",
          "no reply from the engine within #{backstop_ms} ms, past the call's 30-second deadline"}}
    end
  end

  def connect(_options), do: :erlang.nif_error(:nif_not_loaded)
  def migrate(_handle), do: :erlang.nif_error(:nif_not_loaded)
  def define(_handle, _text), do: :erlang.nif_error(:nif_not_loaded)
  def apply(_handle, _text), do: :erlang.nif_error(:nif_not_loaded)
  def status(_handle, _target_table), do: :erlang.nif_error(:nif_not_loaded)
  def definitions(_handle), do: :erlang.nif_error(:nif_not_loaded)
  def dump_definitions_sql(_handle), do: :erlang.nif_error(:nif_not_loaded)
  def request_backfill(_handle, _source_table), do: :erlang.nif_error(:nif_not_loaded)
  def poisoned_since(_handle, _watermark_micros), do: :erlang.nif_error(:nif_not_loaded)
  def quarantined(_handle), do: :erlang.nif_error(:nif_not_loaded)
  def quarantine_status(_handle, _target), do: :erlang.nif_error(:nif_not_loaded)

  def sample_quarantined(_handle, _target, _cursor, _limit),
    do: :erlang.nif_error(:nif_not_loaded)

  def release_key(_handle, _transform, _source_table, _key),
    do: :erlang.nif_error(:nif_not_loaded)

  def has_live_drain_workers(_handle), do: :erlang.nif_error(:nif_not_loaded)
  def has_live_staging_worker(_handle), do: :erlang.nif_error(:nif_not_loaded)
  def detect_capture_disruptions(_handle), do: :erlang.nif_error(:nif_not_loaded)
  def watermark_token(_handle), do: :erlang.nif_error(:nif_not_loaded)
  def await_converged(_handle, _token, _timeout_ms), do: :erlang.nif_error(:nif_not_loaded)

  def self_check(_handle, _target_table, _mode, _timeout_ms),
    do: :erlang.nif_error(:nif_not_loaded)

  def self_check_job(_handle, _id), do: :erlang.nif_error(:nif_not_loaded)

  def config(_handle), do: :erlang.nif_error(:nif_not_loaded)
  def shutdown(_handle), do: :erlang.nif_error(:nif_not_loaded)
  def error_codes, do: :erlang.nif_error(:nif_not_loaded)
  def status_names, do: :erlang.nif_error(:nif_not_loaded)
  def quarantine_state_names, do: :erlang.nif_error(:nif_not_loaded)
  def applied_kinds, do: :erlang.nif_error(:nif_not_loaded)
  def statement_kinds, do: :erlang.nif_error(:nif_not_loaded)
  def self_check_outcomes, do: :erlang.nif_error(:nif_not_loaded)
  def self_check_modes, do: :erlang.nif_error(:nif_not_loaded)
  def self_check_states, do: :erlang.nif_error(:nif_not_loaded)
  def divergence_kinds, do: :erlang.nif_error(:nif_not_loaded)
  def capture_failure_kinds, do: :erlang.nif_error(:nif_not_loaded)
  def index_roles, do: :erlang.nif_error(:nif_not_loaded)
  def log_levels, do: :erlang.nif_error(:nif_not_loaded)
  def render_prometheus, do: :erlang.nif_error(:nif_not_loaded)
  def install_log_bridge(_level), do: :erlang.nif_error(:nif_not_loaded)
  def take_log_records(_max), do: :erlang.nif_error(:nif_not_loaded)
end
