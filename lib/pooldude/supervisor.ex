defmodule Pooldude.Supervisor do
  use Supervisor

  def start_link(mod, args) do
    Supervisor.start_link(__MODULE__, {mod, args})
  end

  def init({mod, args}) do
    children = [
      worker(mod, [args])
    ]

    opts = [
      strategy:     :simple_one_for_one,
      restart:      :temporary,
      max_restarts: 0,
      max_seconds:  1
    ]

    supervise(children, opts)
  end
end
