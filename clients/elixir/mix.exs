defmodule Trellis.MixProject do
  use Mix.Project

  # Kept equal to the NIF crate's version (native/trellis_nif/Cargo.toml).
  # The precompiled NIFs are downloaded from the `elixir-v<version>` GitHub
  # release; see Trellis.Native and .github/workflows/elixir-release.yml.
  @version "0.1.0"
  @source_url "https://github.com/salesforce-misc/trellis"

  def project do
    [
      # Hex's `trellis` is taken and Hex names can't hold a hyphen, so the
      # package and the OTP app are `trellis_pg`; the modules stay `Trellis`.
      app: :trellis_pg,
      version: @version,
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      name: "Trellis",
      description:
        "Embedded Trellis: streaming transforms maintained in your own Postgres, " <>
          "run from an Elixir app through a precompiled Rust NIF.",
      source_url: @source_url,
      package: package(),
      docs: [
        main: "readme",
        extras: ["README.md"],
        source_url_pattern:
          "#{@source_url}/blob/elixir-v#{@version}/clients/elixir/%{path}#L%{line}"
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [mod: {Trellis.Application, []}, extra_applications: [:logger]]
  end

  defp deps do
    [
      # Downloads the NIF built for this machine from the GitHub release and
      # checks it against checksum-Elixir.Trellis.Native.exs.
      {:rustler_precompiled, "~> 0.9.0"},
      # Only to build the NIF from source (see Trellis.Native). Optional, so
      # a host using the precompiled NIF never fetches it; a host building
      # from source adds it to its own deps.
      {:rustler, "~> 0.38.0", optional: true, runtime: false},
      # `Trellis.Migration` is compiled only when the host depends on
      # `ecto_sql`; nothing else here uses it.
      {:ecto_sql, "~> 3.13", optional: true},
      # The integration suite writes source rows and reads target rows over
      # its own connection, the way a host app would.
      {:postgrex, "~> 0.22", only: :test},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @source_url},
      # The NIF's source ships too, for building it on a target the release
      # doesn't cover. The release workflow makes its Cargo.toml stand alone
      # outside this repository's workspace, and writes LICENSE.txt and the
      # checksum file, before it publishes
      # (.github/scripts/elixir-prepare-hex-package.sh).
      files: [
        "lib",
        "native/trellis_nif/src",
        "native/trellis_nif/.cargo",
        "native/trellis_nif/Cargo.toml",
        "native/trellis_nif/Cargo.lock",
        "checksum-Elixir.Trellis.Native.exs",
        "mix.exs",
        "README.md",
        "LICENSE.txt",
        ".formatter.exs"
      ]
    ]
  end
end
