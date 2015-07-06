defmodule Poolboy.Server do
  use GenServer
  import Supervisor.Spec

  @timeout 5000

  defmodule State do
    defstruct supervisor: nil,
    workers: [],
    waiting: :queue.new,
    monitors: :ets.new(:monitors, [:private]),
    size: 5,
    overflow: 0,
    max_overflow: 10,
    strategy: :lifo         # NOTE: Why is this not LIFO by default?
  end

  # NOTE: Might move this into the main Poolboy

  def checkout(pool) do
    checkout(pool, true)
  end

  def checkout(pool, block) do
    checkout(pool, block, @timeout)
  end

  def checkout(pool, block, timeout) do
    try do
      GenServer.call(pool, {:checkout, block}, timeout)
    catch class, reason ->
        GenServer.cast(pool, {:cancel_waiting, self})
        raise(class, message: reason) # NOTE: how to include this :erlang.get_stacktrace)
    end
  end

  def checkin(pool, worker) when is_pid(worker) do
    GenServer.cast(pool, {:checkin, worker})
  end

  def transaction(pool, fun, timeout) do
    worker = checkout(pool, true, timeout)
    try do
      fun.(worker)
    after
      :ok = checkin(pool, worker)
    end
  end

  def child_spec(pool_id, pool_args) do
    child_spec(pool_id, pool_args, [])
  end

  # NOTE: Is pool_id a module name?
  def child_spec(pool_id, pool_args, worker_args) do
    worker(pool_id, [pool_args, worker_args])
  end

  def start(pool_args) do
    start(pool_args, pool_args) # NOTE: Why duplicate?
  end

  def start(pool_args, worker_args) do
    start_pool(:start, pool_args, worker_args)
  end

  def start_link(pool_args, worker_args) do
    start_pool(:start_link, pool_args, worker_args)
  end

  def stop(pool) do
    GenServer.call(pool, :stop)
  end

  def status(pool) do
    GenServer.call(pool, :status)
  end

  # NOTE: Woah, this is cool Go through the list, and initialize the various parts.
  # NOTE: Where is this callback called?
  def init({pool_args, worker_args}) do
    Process.flag(:trap_exit, true)
    init(pool_args, worker_args, %State{})
  end

  #############
  # Callbacks #
  #############

  def init([{:worker_module, mod}|rest], worker_args, state) when is_atom(mod) do
    {:ok, sup} = PoolboySup.start_link(mod, worker_args)
    init(rest, worker_args, %{state | supervisor: sup})
  end

  def init([{:size, size}|rest], worker_args, state) when is_integer(size) do
    init(rest, worker_args, %{state | size: size})
  end

  def init([{:max_overflow, max_overflow}|rest], worker_args, state) when is_integer(max_overflow) do
    init(rest, worker_args, %{state | max_overflow: max_overflow})
  end

  def init([{:strategy, :lifo}|rest], worker_args, state) do
    init(rest, worker_args, %{state | strategy: :lifo})
  end

  def init([{:strategy, :fifo}|rest], worker_args, state) do
    init(rest, worker_args, %{state | strategy: :fifo})
  end

  def init([_|rest], worker_args, state) do
    init(rest, worker_args, state)
  end

  def init([], _worker_arg, %State{size: size, supervisor: sup} = state) do
    workers = prepopulate(size, sup)
    {:ok, %{state | workers: workers}}
  end

  def handle_cast({:checkin, pid}, state = %State{monitors: monitors}) do
    case :ets.lookup(monitors, pid) do
      [{pid, ref}] ->
        true = Process.demonitor(ref)
        true = :ets.delete(monitors, ref)
        new_state = handle_checkin(pid, state)
        {:noreply, new_state}
      [] ->
        {:noreply, state}
    end
  end

  # NOTE: This means cancel waiting for all?
  # NOTE: Why would a demonitor return false?
  def handle_cast({:cancel_waiting, pid}, state = %State{waiting: waiting}) do
    new_waiting = waiting |> Enum.filter(fn {{p, _}, ref} ->
      # if pid is not what we are looking for ...
      # or process cannot be demonitored
      p != pid or not Process.demonitor(ref)
    end)

    {:noreply, %{state | waiting: new_waiting}}
  end

  def handle_cast(_msg, state) do
    {:noreply, state}
  end

  def handle_call({:checkout, block}, {from_pid, _} = from, state) do
    %State{supervisor: sup,
           workers: workers,
           monitors: monitors,
           overflow: overflow,
           max_overflow: max_overflow} = state

    case workers do
      [pid|left] ->
        ref = Process.monitor(from_pid)
        true = :ets.insert(monitors, {pid, ref})
        {:reply, pid, %{state | workers: left}}

      [] when max_overflow > 0 and overflow < max_overflow ->
        {pid, ref} = new_worker(sup, from_pid)
        true = :ets.insert(monitors, {pid, ref})
        {:reply, pid, %{state | overflow: overflow+1}}

      # When async but no more workers
      [] when block == false ->
        {:reply, :full, state}

      [] ->
        ref = Process.monitor(from_pid)
        waiting = :queue.in({from_pid, ref}, state[:waiting]) # NOTE: How to get value?
        {:noreply, %{state | waiting: waiting}}
    end
  end

  def handle_call(:status, _from, state) do
    %State{workers: workers,
           monitors: monitors,
           overflow: overflow} = state

    # NOTE: In the original implementation, ets:info(Monitors, size) is used
    {:reply, {state_name(state), length(workers), overflow, :ets.info(monitors, :size)}, state}
  end

  def handle_call(:get_avail_workers, _from, %State{workers: workers} = state) do
    {:reply, workers, state}
  end

  def handle_call(:get_all_workers, _from, %State{supervisor: sup} = state) do
    worker_list = Supervisor.which_children(sup)
    {:reply, worker_list, state}
  end

  def handle_call(:stop, _from, state) do
    {:stop, :normal, :ok, state}
  end

  def handle_call(_msg, _from, state) do
    {:reply, {:error, :invalid_message}, :ok, state}
  end

  def handle_info({:DOWN, ref, _, _, _}, state) do
    %State{monitors: monitors,
           waiting: waiting} = state

    case :ets.match(monitors, {:"$1", ref}) do
      [[pid]] ->
        true = :ets.delete(monitors, pid)
        new_state = handle_checkin(pid, state)
        {:noreply, new_state}

      [] ->
        # remove reference from waiting if worker is down
        new_waiting = :queue.filter(fn({_, r}) -> r != ref end, waiting)
        {:noreply, %{state | waiting: new_waiting}}

    end
  end

  def handle_info({:EXIT, pid, _reason}, state) do
    %State{supervisor: sup,
           monitors: monitors,
           workers: workers} = state

    case :ets.lookup(monitors, pid) do
      [{pid, ref}] ->
        true = Process.demonitor(ref)
        true = :ets.delete(monitors, pid)
        new_state = handle_worker_exit(pid, state)
        {:noreply, new_state}

      [] ->
        case Enum.member?(workers, pid) do
          true ->
            remaining_workers = workers |> Enum.filter(fn(p) -> p != pid end)
            {:noreply, %{state | workers: [new_worker(sup) | remaining_workers]}}

            false ->
              {:noreply, state}
        end
    end
  end

  def handle_info(_info, state) do
    {:noreply, state}
  end

  def terminate(_reason, %State{workers: workers, supervisor: sup}) do
    workers |> Enum.each(&(Process.unlink(&1)))
    true = Process.exit(sup, :shutdown)
    :ok
  end

  #####################
  # Private Functions #
  #####################

  def start_pool(start_fun, pool_args, worker_args) do
    # TODO: Check if this is the correct way to initialize
    case pool_args |> Keyword.fetch(:name) do
      nil ->
        apply(GenServer, start_fun, [__MODULE__, {pool_args, worker_args}, []])
      name ->
        apply(GenServer, start_fun, [name, {pool_args, worker_args}, []])
    end
  end

  def new_worker(sup) do
    {:ok, pid} = Supervisor.start_child(sup, [])
    true = Process.link(pid) # NOTE: Why link? Because trapping exits!
    pid
  end

  # NOTE: When use this?
  def new_worker(sup, from_pid) do
    pid = new_worker(sup)
    ref = Process.monitor(from_pid)
    {pid, ref}
  end

  def dismiss_worker(sup, pid) do
    true = Process.unlink(pid)
    Supervisor.terminate_child(sup, pid)
  end

  def prepopulate(n, _sup) when n < 1 do
    []
  end

  def prepopulate(n, sup) do
    prepopulate(n, sup, [])
  end

  def prepopulate(0, _sup, workers) do
    workers
  end

  def prepopulate(n, sup, workers) do
    prepopulate(n-1, sup, [new_worker(sup)|workers])
  end

  def handle_checkin(pid, state) do
    %State{supervisor: sup,
              waiting: waiting,
             monitors: monitors,
             overflow: overflow,
              workers: workers,
             strategy: strategy} = state

    case :queue.out(waiting) do
      {{:value, {from, ref}}, left} ->
        true = :ets.insert(monitors, {pid, ref})
        GenServer.reply(from, pid)
        %{state | waiting: left}

      # NOTE: What situation is this?
      {:empty, empty} when overflow > 0 ->
        # NOTE: Why dismiss worker
        :ok = dismiss_worker(sup, pid)
        %{state | waiting: empty, overflow: overflow-1}

      {:empty, empty} ->
        workers = case strategy do
          :lifo -> [pid | workers]
          :fifo -> workers ++ [pid]
        end
        %{state | workers: workers, waiting: empty, overflow: 0}
    end
  end

  def handle_worker_exit(pid, state) do
    %State{supervisor: sup,
              waiting: waiting,
             monitors: monitors,
             overflow: overflow,
              workers: workers} = state

    case :queue.out(waiting) do
      {{:value, {from, ref}}, left_waiting} ->
        new_worker = new_worker(sup)
        true = :ets.insert(monitors, {new_worker, ref})
        GenServer.reply(from, new_worker)
        %{state | waiting: left_waiting}

      {:empty, empty} when overflow > 0 ->
        %{state | overflow: overflow-1 , waiting: empty}

      # NOTE: Why create new worker?
      {:empty, empty} ->
        workers = [new_worker(sup)| workers |> Enum.filter(fn(p) -> p != pid end)]
        %{state | workers: workers, waiting: empty}
    end
  end

  def state_name(%State{overflow: overflow, max_overflow: max_overflow, workers: workers}) when overflow < 1 do
    case length(workers) == 0 do
      true ->
        if max_overflow < 1 do
          :full
        else
          :overflow
        end
      false ->
        :ready
    end
  end

  def state_name(%State{overflow: max_overflow, max_overflow: max_overflow}) do
    :full
  end

  def state_name(_state) do
    :overflow
  end

end
