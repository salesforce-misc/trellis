defmodule Trellis.Owner do
  @moduledoc false
  # The process a supervised Trellis is (`Trellis.start_link/1`): it connects
  # the handle as it starts, and shuts the handle down when its supervisor
  # stops it. The handle's resource destructor is then only the backstop, for
  # an owner killed before `terminate/2` could run.
  #
  # It keeps the handle's lifecycle and nothing else. Callers ask it for the
  # handle (`handle_ref/1`) and make their calls themselves, in their own
  # processes: a call submits its job and waits in the caller's `receive`
  # without holding a scheduler, and the engine runs the calls of different
  # callers in parallel, so nothing needs to be funnelled through one process
  # (#599). Each caller sequences its own calls by waiting for each reply.
  # Asking is a message to a process that is never busy, so it costs a
  # round trip and never waits on a call.

  use GenServer

  @doc false
  def start_link(options) do
    {name, options} = Keyword.pop(options, :name)
    GenServer.start_link(__MODULE__, options, if(name, do: [name: name], else: []))
  end

  @doc false
  # The NIF handle `server` owns. A caller exits if `server` isn't running.
  def handle_ref(server), do: GenServer.call(server, :handle_ref)

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
  def handle_call(:handle_ref, _from, handle), do: {:reply, handle.ref, handle}

  @impl true
  def terminate(_reason, handle) do
    Trellis.shutdown(handle)
  end
end
