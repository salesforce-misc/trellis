defmodule Trellis.MixProject do
  use Mix.Project

  def project do
    [
      app: :trellis,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      {:rustler, "~> 0.38.0", runtime: false},
      # The integration suite writes source rows and reads target rows over
      # its own connection, the way a host app would.
      {:postgrex, "~> 0.22", only: :test}
    ]
  end
end
