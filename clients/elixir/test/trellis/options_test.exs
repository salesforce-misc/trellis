defmodule Trellis.OptionsTest do
  use ExUnit.Case, async: true

  alias Trellis.Error

  # All of these are rejected before anything reaches the database.
  @url "host=/nonexistent dbname=unused"

  test ":url is required, and nothing falls back to the environment" do
    assert {:error, %Error{code: :validation, message: ":url is required"}} = Trellis.connect([])
  end

  test "an unknown option is an error, not ignored" do
    assert {:error, %Error{code: :validation, message: message}} =
             Trellis.connect(url: @url, drain_thread: 2)

    assert message =~ ":drain_thread"
  end

  test "worker threads must be positive" do
    assert {:error, %Error{code: :validation, message: message}} =
             Trellis.connect(url: @url, worker_threads: 0)

    assert message =~ ":worker_threads"
  end

  test "drain threads can't be negative" do
    assert {:error, %Error{code: :validation}} = Trellis.connect(url: @url, drain_threads: -1)
  end

  test "the call pool must allow a connection, and its wait is a count of milliseconds" do
    assert {:error, %Error{code: :validation, message: message}} =
             Trellis.connect(url: @url, max_call_pool_size: 0)

    assert message =~ ":max_call_pool_size"

    assert {:error, %Error{code: :validation, message: message}} =
             Trellis.connect(url: @url, call_pool_wait_timeout_ms: -1)

    assert message =~ ":call_pool_wait_timeout_ms"
  end

  test "an invalid schema name is the engine's validation error" do
    assert {:error, %Error{code: :validation}} =
             Trellis.connect(url: @url, schema: "   ")
  end

  test "connect! raises the same error" do
    assert_raise Error, ":url is required", fn -> Trellis.connect!(%{}) end
  end
end
