# frozen_string_literal: true

require "json"
require "pg"

# One throwaway Postgres for the whole suite, held by `trellis-testkit`
# (testkit/src/bin/trellis-testkit.rs). Its stdin is a pipe from this
# process: TestCluster.stop closes it at exit, and if this process dies
# instead the pipe closes all the same. Either way testkit stops the server
# and deletes its directory.
module TestCluster
  WORKSPACE = File.expand_path("../../../..", __dir__)

  class << self
    # The cluster's details: "dsn" (a libpq key=value string, which both
    # Trellis.connect and PG.connect take), and "host", "port", "user",
    # "dbname", "log_file".
    attr_reader :info

    # Starts the cluster and migrates Trellis's schema into its database, the
    # way a deploy's migration step would: on a handle that runs nothing in
    # the background.
    def start
      @testkit, @info = open

      Trellis.connect(url: dsn)
      Trellis.migrate
      Trellis.shutdown
    end

    # Starts a second, private cluster for one test, yields its details (the
    # shape #info returns), and tears it down when the block returns, however
    # it returns. Shut down any handle connected to it inside the block.
    #
    # For a test whose leftovers would disturb the shared cluster. A separate
    # database isn't enough for one that runs the staging worker: Trellis's
    # replication slot name is fixed, and slots are cluster-wide (#588).
    def private_cluster
      testkit, info = open
      yield info
    ensure
      testkit&.close
    end

    # Closes testkit's stdin and waits for it to tear the cluster down.
    def stop
      @testkit&.close
    end

    def dsn
      info.fetch("dsn")
    end

    # A new `pg` connection to the database `info` describes (the shared
    # cluster's by default). Close it when done.
    def pg(info = self.info)
      PG.connect(info.fetch("dsn"))
    end

    private

    # Spawns testkit and waits for the cluster it reports: its stdin pipe and
    # the cluster's details.
    def open
      testkit = IO.popen([executable], "r+")
      raise "trellis-testkit reported no cluster within 120s" unless testkit.wait_readable(120)

      line = testkit.gets or raise "trellis-testkit exited before reporting a cluster"
      [testkit, JSON.parse(line)]
    rescue StandardError
      # Closing its stdin tells testkit to tear down whatever it started.
      testkit&.close
      raise
    end

    # CI puts trellis-testkit on PATH; locally, the workspace's debug build.
    def executable
      on_path = ENV.fetch("PATH", "").split(File::PATH_SEPARATOR)
                   .map { |dir| File.join(dir, "trellis-testkit") }
                   .find { |path| File.executable?(path) }
      local = File.join(WORKSPACE, "target", "debug", "trellis-testkit")
      on_path || (File.executable?(local) && local) ||
        raise("trellis-testkit not found: run `cargo build -p testkit --bin trellis-testkit`")
    end
  end
end
