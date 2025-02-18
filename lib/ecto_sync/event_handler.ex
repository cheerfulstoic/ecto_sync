defmodule EctoSync.EventHandler do
  @moduledoc false
  alias Ecto.Association.{BelongsTo, Has, ManyToMany}
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

  def sync(:cached, {_, :deleted}, id, _opts), do: {:ok, id}

  def sync(:cached, {schema, _event}, id, config),
    do: {:ok, get_from_cache([schema, id], config)}

  def sync(value_or_values, {{_, :deleted} = schema_event, id, config}),
    do: {:ok, do_sync(value_or_values, id, schema_event, config)}

  def sync(value_or_values, {{schema, _} = schema_event, id, config}) do
    new = get_from_cache([schema, id], config)

    {:ok, do_sync(value_or_values, new, schema_event, config)}
  end

  defp do_sync(values, new, schema_event, config) when is_list(values) do
    Enum.map(values, &update_all(&1, new, schema_event, config))
  end

  defp do_sync(value, new, schema_event, config),
    do: update_all(value, new, schema_event, config)

  defp get_from_cache([schema, id | _] = key, config) do
    key =
      List.to_tuple(key ++ [config.ref])

    {_, value} =
      Cachex.fetch(config.cache_name, key, fn _key ->
        {:commit, config.get_fun.(schema, id)}
      end)

    value
  end

  defp update_cache({schema, :deleted}, id, _, config) do
    Cachex.del(config.cache_name, {schema, id})
    {:ok, {schema, id}}
  end

  defp update_cache({schema, _event}, id, preloads, config) do
    key = {schema, id}

    record =
      config.repo.get!(schema, id)
      |> config.repo.preload(preloads)

    {:ok, true} = Cachex.put(config.cache_name, key, record)
    {:ok, key}
  end

  defp update_all(values, new, schema_event, config) when is_list(values),
    do: Enum.map(values, &update_all(&1, new, schema_event, config))

  defp update_all(value, new, {_schema, :inserted} = schema_event, config) do
    reduce_preloaded_assocs(value, fn {key, assoc_info}, acc ->
      {related?, resolved} =
        resolve_assoc(assoc_info, value, new, schema_event)

      Map.update!(acc, key, fn assoc ->
        if related? do
          do_insert(acc, key, assoc, resolved, schema_event, config)
        else
          update_all(assoc, new, schema_event, config)
        end
      end)
    end)
  end

  defp update_all(%{__struct__: schema} = value, new, {schema, :deleted}, _config) do
    if same_record?(value, {schema, new}) do
      nil
    else
      value
    end
  end

  defp update_all(value, id, {schema, :deleted} = schema_event, config) do
    reduce_preloaded_assocs(value, fn {key, _info}, acc ->
      Map.update!(acc, key, fn
        assocs when is_list(assocs) ->
          case find_by_primary_key(assocs, {schema, id}) do
            nil -> assocs
            index -> List.delete_at(assocs, index)
          end
          |> update_all(id, schema_event, config)

        assoc ->
          update_all(assoc, id, schema_event, config)
      end)
    end)
  end

  defp update_all(value, new, schema_event, config) do
    if same_record?(value, new) do
      preloads = find_preloads(value)

      get_preloaded(get_schema(value), new.id, preloads, config)
    else
      reduce_preloaded_assocs(value, fn {key, assoc_info}, acc ->
        Map.update!(acc, key, fn
          assocs when is_list(assocs) ->
            # if related? do
            do_assoc_updates(value, new, assoc_info, assocs, schema_event, config)

          # # else
          #   update_all(assocs, new, schema_event, config)
          # end

          assoc ->
            update_all(assoc, new, schema_event, config)
        end)
      end)
    end
  end

  defp do_assoc_updates(
         _value,
         new,
         %ManyToMany{related: schema},
         assocs,
         {schema, _} = schema_event,
         config
       ) do
    update_all(assocs, new, schema_event, config)
  end

  defp do_assoc_updates(_value, _new, %ManyToMany{}, assocs, _schema_event, _config) do
    assocs
  end

  defp do_assoc_updates(
         value,
         new,
         %{field: key} = assoc_info,
         assocs,
         {schema, _} = schema_event,
         config
       )
       when is_list(assocs) do
    possible_index = find_by_primary_key(assocs, new)
    related_id = Map.get(new, assoc_info.related_key)
    owner_id = Map.get(value, assoc_info.owner_key)

    cond do
      # Maybe we are removed as assoc
      not is_nil(possible_index) and related_id != owner_id and
          assoc_info.related == schema ->
        # Broadcast an insert to the new owner
        # assoc_moved(new, owner_id, assoc_info)

        List.delete_at(assocs, possible_index)
        |> update_all(new, schema_event, config)

      # Maybe we are assigned as assoc
      is_nil(possible_index) and related_id == owner_id and
          assoc_info.related == schema ->
        do_insert(value, key, assocs, new, schema_event, config)

      true ->
        update_all(assocs, new, schema_event, config)
    end
  end

  # defp assoc_moved(value, to, assoc_info) do
  #   identifiers =
  #     %{id: Map.get(value, assoc_info.owner_key)}
  #     |> Map.put(assoc_info.related_key, to)

  #   GenServer.cast(__MODULE__, {:broadcast, {{assoc_info.related, :inserted}, identifiers}})
  # end

  defp do_insert(_value, _key, assocs, resolved, {schema, _} = schema_event, config)
       when is_list(assocs) do
    preloads =
      case assocs do
        [] -> []
        _ -> find_preloads(assocs)
      end

    # In case a many to many assocs has to be inserted.
    {new, inserted} =
      case resolved do
        {new, {related_schema, id}} ->
          {new, get_preloaded(related_schema, id, preloads, config)}

        new ->
          {new, get_preloaded(schema, new.id, preloads, config)}
      end

    EctoSync.subscribe(inserted)

    Enum.map(assocs, &update_all(&1, new, schema_event, config)) ++ [inserted]
  end

  defp do_insert(_value, _key, assoc, new, {schema, _}, config) do
    preloads = find_preloads(assoc || config.preloads)

    new = get_preloaded(schema, new.id, preloads, config)
    EctoSync.subscribe(new)
    new
  end

  defp get_preloaded(schema, id, preloads, config) do
    repo = config.repo

    get_from_cache(
      [schema, id, preloads],
      Map.put(config, :get_fun, fn _schema, _id ->
        repo.get(schema, id) |> repo.preload(preloads, force: true)
      end)
    )
  end

  defp resolve_assoc(
         %ManyToMany{related: related_schema, join_through: schema, join_keys: join_keys},
         value,
         new,
         {schema, _}
       ) do
    [{related_key, parent_key}, {id_key, _}] =
      join_keys

    parent_id = Map.get(value, parent_key, false)

    child_id = Map.get(new, related_key)

    {parent_id == child_id, {new, {related_schema, Map.get(new, id_key)}}}
  end

  defp resolve_assoc(assoc_info, value, new, {schema, _event}) do
    case assoc_info do
      %ManyToMany{} ->
        {false, new}

      %Has{related: ^schema} ->
        parent_id = Map.get(value, assoc_info.owner_key, false)
        child_id = Map.get(new, assoc_info.related_key)
        {parent_id == child_id, new}

      %BelongsTo{related: ^schema} ->
        {false, new}

      _ ->
        {false, new}
    end
  end

  def find_by_primary_key([], _needle), do: nil

  def find_by_primary_key([value | _] = values, needle) when is_struct(value) do
    schema_mod = get_schema(value)

    [primary_key] =
      schema_mod.__schema__(:primary_key)

    Enum.find_index(values, &same_record?(&1, needle, primary_key))
  end

  defp same_record?(v1, v2, primary_key \\ nil)

  defp same_record?(%{__struct__: mod} = v1, %{__struct__: mod} = v2, primary_key) do
    primary_key =
      if primary_key == nil do
        mod.__schema__(:primary_key) |> hd()
      else
        primary_key
      end

    Map.get(v1, primary_key) == Map.get(v2, primary_key)
  end

  defp same_record?(%{__struct__: mod} = v1, {mod, id}, primary_key) do
    primary_key =
      if primary_key == nil do
        mod.__schema__(:primary_key) |> hd()
      else
        primary_key
      end

    Map.get(v1, primary_key) == id
  end

  defp same_record?(_v1, _v2, _primary_key), do: false

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
