defmodule EventSubscriber do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: opts[:name] || __MODULE__)
  end

  @impl true
  def init([type: type, name: name]) do
    IO.inspect(self(), label: :Genserver)
    {:ok, %{type: type, object: nil, name: name}}
  end

  def subscribe(pid, schema, opts \\ []) do
    GenServer.call(pid, {:subscribe, schema, opts})
    IO.inspect(pid, label: :subscribe_pid)
    Process.send_after(pid, :update, 10)
    |> IO.inspect(label: :send_after)
  end

  def unsubscribe(pid, schema) do
    GenServer.call(pid, {:unsubscribe, schema})
  end

  def events(pid, schema) do
    GenServer.call(pid, {:events, schema})
  end

  @impl true
  def handle_call({:subscribe, schema, preloads}, _from, state) do
    IO.inspect(self(), label: :Handle_subscribe_pid)
    EctoSync.subscribe_all(schema)
    |> IO.inspect(label: :after_subscribe)
    {:reply, :ok,  %{state | object: TestRepo.preload(schema, preloads, force: true)}}
  end

  @impl true
  def handle_call({:unsubscribe, schema}, _from, state) do
    EctoSync.unsubscribe(schema)
    {:noreply, state}
  end

  @impl true
  def handle_info(:update, state) do
    update(state.object)
    |> IO.inspect(label: :send_after)
    {:noreply, state}
  end

  @impl true
  def handle_info({{schema, event}, sync_fun}, state) do
    IO.inspect({schema, event}, label: :event)
    {:ok, value} = sync_fun.(state.object)
    update(value)
    {:noreply, %{state | object: value}}
  end

  defp update(schema) do
    {:ok, _} =
    schema.cases
    |> Enum.take_random(1)
    |> hd()
      |> Ecto.Changeset.change(%{name: "#{inspect(:erlang.make_ref())}"})
      |> TestRepo.update()
      |> IO.inspect(label: :update)
    Process.send_after(self(), :update, 500)
  end
end
System.no_halt(true)
Supervisor.start_link([
      TestRepo,
      {Phoenix.PubSub, name: EctoSync.PubSub},

      
        {EctoSync,
         cache_name: :dbcache,
         repo: TestRepo,
         pub_sub: EctoSync.PubSub,
         watchers:
           EctoSync.all_events(Test)
           |> EctoSync.all_events(TestCase, debug?: true, extra_columns: [:test_id])},
           Supervisor.child_spec({EventSubscriber, [type: :cache, name: CacheSubscriber]}, id: :cache),
           Supervisor.child_spec({EventSubscriber, [type: :cache, name: DBSubscriber]}, id: :db)
      
      ], [strategy: :one_for_one])

{:ok, test_with_cases} = TestRepo.insert(%Test{cases: [%TestCase{}, %TestCase{}]})
EventSubscriber.subscribe(CacheSubscriber, test_with_cases)
EventSubscriber.subscribe(DBSubscriber,test_with_cases)
