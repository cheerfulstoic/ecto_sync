defmodule EctoSync.EventHandler do
  @moduledoc false
  require Logger
  use GenServer

  import EctoSync.Helpers

  @events ~w/inserted updated deleted/a

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(state) do
    {:ok,
     %{
       config: %{
         repo: state[:repo],
         cache_name: state[:cache_name]
       },
       subscriptions: %{}
     }}
  end

  def subscribe(watcher_identifier, id \\ nil) do
    message = {:subscribe, {watcher_identifier, id}}
    GenServer.cast(__MODULE__, message)
  end

  def unsubscribe(schema) do
    GenServer.cast(__MODULE__, {:unsubscribe, schema})
  end

  def handle_cast(
        {:subscribe, {watcher_identifier, id} = message},
        state
      ) do
    # Logger.debug("subscribe #{inspect(self())}: #{inspect(message)}")

    counter =
      Map.get_lazy(state.subscriptions, message, fn ->
        # Logger.debug("Calling EctoWatch.subscribe(#{inspect(message)})")
        EctoWatch.subscribe(watcher_identifier, id)
        :counters.new(1, [:atomics])
      end)

    :counters.add(counter, 1, 1)

    subscriptions =
      Map.put(state.subscriptions, message, counter)

    {:noreply, %{state | subscriptions: subscriptions}}
  end

  def handle_cast({:broadcast, message}, state) do
    Logger.debug("broadcasting: #{inspect(message)}")
    send(self(), message)
    {:noreply, state}
  end

  def handle_call({:unsubscribe, value}, state) do
    schema = get_schema(value)

    Enum.each(@events, fn event ->
      if Registry.count_match(EventRegistry, {{schema, event}, value.id}, :_) == 0 do
        EctoWatch.unsubscribe({schema, event})
      end
    end)

    {:noreply, state}
  end

  def handle_info({schema_event, %{id: id} = identifiers} = message, state) do
    {_schema, event} = schema_event = normalize_event(schema_event)
    Logger.debug("EventHandler #{inspect(message)}")

    {registry_id, extra} =
      case event do
        :inserted -> {nil, Map.delete(identifiers, :id)}
        _ -> Map.pop(identifiers, :id)
      end

    {:ok, _key} = update_cache(schema_event, id, [], state.config)

    ref = :erlang.make_ref()

    [registry_id | Map.to_list(extra)]
    |> Enum.flat_map(fn identifier ->
      for {pid, _} <- Registry.lookup(EventRegistry, {schema_event, identifier}) do
        pid
      end
    end)
    |> Enum.uniq()
    |> Enum.each(fn pid ->
      broadcast(pid, [], schema_event, id, Map.put(state.config, :ref, ref))
    end)

    {:noreply, state}
  end

  defp broadcast(pid, opts, schema_event, id, config) do
    _preloads = opts[:preloads] || []
    get_fun = opts[:get_fun] || (&config.repo.get(&1, &2))

    config =
      Map.take(config, ~w/cache_name repo ref/a)
      |> Map.put(:get_fun, get_fun)

    args = {schema_event, id, config}

    send(pid, {schema_event, args})
  end

  defp normalize_event(label_event) when is_atom(label_event) do
    [event | table_parts] =
      to_string(label_event)
      |> String.split("_")
      |> Enum.reverse()

    table = Enum.join(table_parts, "_")
    {table, String.to_existing_atom(event)}
  end

  defp normalize_event(schema_event), do: schema_event
end
