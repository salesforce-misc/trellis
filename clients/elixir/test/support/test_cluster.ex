defmodule Trellis.TestCluster do
  @moduledoc false
  # One throwaway Postgres for the whole suite, held by `trellis-testkit`
  # (testkit/src/bin/trellis-testkit.rs). The port's owner is a process that
  # lives until the VM exits; when it does, the port's pipe to testkit's stdin
  # closes and testkit stops the server and deletes its directory, however
  # the test run ended.

  use Agent

  @doc "Starts the cluster and waits for its connection details."
  def start do
    Agent.start(&open/0, name: __MODULE__)
  end

  @doc """
  The cluster's details: `"dsn"` (a libpq `key=value` string Trellis takes
  as its `:url`), and `"host"` (the socket directory), `"port"`, `"user"` and
  `"dbname"` for Postgrex.
  """
  def info, do: Agent.get(__MODULE__, & &1.info)

  @doc "A Postgrex connection to the shared cluster's database, linked to the caller."
  def postgrex!, do: postgrex!(info())

  @doc """
  Starts a second, private cluster for one test and returns its details
  (the shape `info/0` returns). It is torn down when the calling test
  process exits.

  For a test whose leftovers would disturb the shared cluster. A separate
  database isn't enough for one that runs the staging worker: Trellis's
  replication slot name is fixed, and slots are cluster-wide.
  """
  def private! do
    {:ok, agent} = Agent.start_link(&open/0)
    Agent.get(agent, & &1.info)
  end

  @doc "A Postgrex connection to the cluster `info` describes, linked to the caller."
  def postgrex!(info) do
    {:ok, pg} =
      Postgrex.start_link(
        socket_dir: info["host"],
        port: info["port"],
        username: info["user"],
        database: info["dbname"]
      )

    pg
  end

  defp open do
    port =
      Port.open({:spawn_executable, executable()}, [:binary, :exit_status, {:line, 65_536}])

    receive do
      {^port, {:data, {:eol, line}}} ->
        %{port: port, info: JSON.decode!(line)}

      {^port, {:exit_status, status}} ->
        raise "trellis-testkit exited with status #{status} before reporting a cluster"
    after
      120_000 -> raise "trellis-testkit reported no cluster within 120s"
    end
  end

  # CI puts `trellis-testkit` on PATH; locally, the workspace's debug build.
  defp executable do
    local = Path.expand("../../../../target/debug/trellis-testkit", __DIR__)

    cond do
      path = System.find_executable("trellis-testkit") ->
        path

      File.exists?(local) ->
        local

      true ->
        raise "trellis-testkit not found: run `cargo build -p testkit --bin trellis-testkit`"
    end
  end
end
