{:ok, _} = Trellis.TestCluster.start()
# The engine's log lines reach Logger through `Trellis.LogBridge`; print a
# test's only when it fails.
ExUnit.start(capture_log: true)
