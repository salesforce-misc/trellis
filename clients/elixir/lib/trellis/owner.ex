defmodule Trellis.Owner do
  @moduledoc false
  # The process a supervised Trellis is (`Trellis.start_link/1`): it connects
  # the handle as it starts, runs every call made through its name, one at a
  # time, and shuts the handle down when its supervisor stops it. The
  # handle's resource destructor is then only the backstop, for an owner
  # killed before `terminate/2` could run.
  #
  # The calls run here, rather than each caller being handed the handle, to
  # bound how many dirty IO schedulers they hold (#148). The handle runs one
  # call at a time anyway (`BlockingTrellis`'s job loop), and a caller parks
  # the dirty IO scheduler it calls from until its call returns, queued time
  # included. So N callers sharing the handle would park N of the BEAM's
  # dirty IO schedulers (10 by default) behind one slow call, and enough of
  # them would stall the node's file IO. Through here only this process
  # parks one; the callers wait in its mailbox.

  use GenServer

  @doc false
  def start_link(options) do
    {name, options} = Keyword.pop(options, :name)
    GenServer.start_link(__MODULE__, options, if(name, do: [name: name], else: []))
  end

  @doc false
  # `:infinity`, like the handle itself: every call that can wait long takes
  # a bound of its own (`await_converged/3`'s and `self_check/3`'s
  # `timeout_ms`), and a caller still exits if the owner does.
  def call(server, function, args),
    do: GenServer.call(server, {:native, function, args}, :infinity)

  @impl true
  def init(options) do
    # So a supervisor's shutdown runs `terminate/2` rather than killing the
    # owner outright.
    Process.flag(:trap_exit, true)

    # A failed connect is `{:error, %Trellis.Error{}}`, which `init/1` may
    # return: `start_link/1` returns it, and the caller it's linked to gets
    # no exit signal.
    Trellis.connect(options)
  end

  @impl true
  def handle_call({:native, function, args}, _from, handle) do
    {:reply, Kernel.apply(Trellis.Native, function, [handle.ref | args]), handle}
  end

  @impl true
  def terminate(_reason, handle) do
    Trellis.shutdown(handle)
  end
end
