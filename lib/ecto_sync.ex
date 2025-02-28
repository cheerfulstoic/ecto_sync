defmodule EctoSync do
  @moduledoc """
  A Cache updater and router for events emitted when database entries are updated. Subscribers can provide a list of records that they want to receive updates on. Additionally, they can provide a function that will act as a means of authorization on the updates they should get.

  Using the subscribe function a process can subscribe to all messages for a given struct.
  """

  @type subscriptions() :: list({EctoWatch.watcher_identifier(), term()})
  @type schema_or_list_of_schemas() :: Ecto.Schema.t() | list(Ecto.Schema.t())
  @events ~w/inserted updated deleted/a
  @cache_name :ecto_sync

  defstruct pub_sub: nil,
            repo: nil,
            cache_name: nil,
            watchers: [],
            schemas: []

  use Supervisor
  require Logger
  alias Ecto.Association.{BelongsTo, Has, ManyToMany}
  alias EctoSync.EventHandler
  import EctoSync.Helpers

  def start_link(opts \\ [name: __MODULE__]) do
    state =
      %__MODULE__{
        cache_name: opts[:cache_name] || @cache_name,
        repo: opts[:repo],
        pub_sub: opts[:pub_sub],
        watchers: opts[:watchers],
        schemas: opts[:schemas]
      }

    Supervisor.start_link(__MODULE__, state, name: __MODULE__)
  end

  @impl true
  def init(state) do
    children = [
      {Cachex, state.cache_name},
      {EctoWatch, [repo: state.repo, pub_sub: state.pub_sub, watchers: state.watchers]},
      {Registry, keys: :duplicate, name: EventRegistry},
      {EventHandler,
       [
         name: EventHandler,
         pub_sub: state.pub_sub,
         schemas: state.schemas,
         watchers: state.watchers,
         repo: state.repo,
         cache_name: state.cache_name
       ]}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @spec all_events(list(), module(), list()) :: list()
  def all_events(list \\ [], schema, opts \\ []) when is_atom(schema) do
    unless ecto_schema_mod?(schema) do
      raise ArgumentError, "Expected a module alias to an Ecto Schema"
    end

    opts = maybe_add_belongs_assocs(schema, opts)

    list ++ Enum.map(@events, &{schema, &1, opts})
  end

  @spec subscriptions(EctoWatch.watcher_identifier(), term()) :: [{pid(), Registry.value()}]
  @doc """
  Returns a list of pids that is subscribed to the given watcher identifier.
  """
  def subscriptions({schema, event}, id \\ nil) do
    Registry.lookup(EventRegistry, {{schema, event}, id})
  end

  @doc """
  Subscribe to Ecto.Schema(s) provided. The input can be one of following:
   - an Ecto.Schema struct, 
   - a list of Ecto.Schema struct, 
   - an EctoWatch identifier.

  When an Ecto.Schema struct or list of structs is provided, the process is subscribed to all `:updated` and `:deleted` events for the Ecto.Schema that represents the struct.

  ### Examples

  iex> defmodule Test do
  ...>   use Ecto.Schema
  ...>   schema do
  ...>     field :name, :string
  ...>   end
  ...> end
  iex> EctoSync.subscribe(Test)
  [{{Test, :inserted}, nil}, {{Test, :updated}, nil}, {{Test, :deleted}, nil}]
  iex> EctoSync.subscribe(%Test{id: 1})
  [{{Test, :updated}, 1}, {{Test, :deleted}, 1}]
  """
  @spec subscribe(schema_or_list_of_schemas() | EctoWatch.watcher_identifier(), term(), term()) ::
          subscriptions()
  def subscribe(values, id \\ nil, opts \\ [])

  def subscribe(values, id, opts) when is_list(values),
    do: Enum.flat_map(values, &subscribe(&1, id, opts))

  def subscribe(value, _id, opts) when is_struct(value) do
    events = subscribe_events(value)
    schema = get_schema(value)

    id =
      if ecto_schema_mod?(schema) do
        primary_key(value)
      else
        raise_no_ecto(schema)
      end

    do_subscribe(events, id, opts)
  end

  def subscribe(watcher_identifier, id, opts) do
    events = subscribe_events(watcher_identifier)

    do_subscribe(events, id, opts)
  end

  def unsubscribe(record) do
    EventHandler.unsubscribe(record)
  end

  @doc """
  Subscribe to insert/update/delete events for a given Ecto.Schema struct.
  """
  # TODO make this subscribe to everything related to this schema, even non preloaded assocs
  @spec subscribe_all(schema_or_list_of_schemas()) :: subscriptions()
  def subscribe_all(value), do: reduce_to_seen(value, &do_subscribe_all/2)

  @doc """
  Subscribe to all events related to the assocs of a given schema.
  """
  @spec subscribe_assocs(schema_or_list_of_schemas(), term()) :: subscriptions()
  def subscribe_assocs(schema, id \\ nil)

  def subscribe_assocs(schema, id) when is_struct(schema) do
    schema_mod = get_schema(schema)
    subscribe_assocs(schema_mod, id || schema.id)
  end

  def subscribe_assocs(schema_mod, _id) when is_atom(schema_mod) do
    reduce_to_seen(schema_mod, &do_subscribe_assocs/2)
  end

  defp do_subscribe_assocs(schema_mod, seen) do
    reduce_assocs(schema_mod, seen, fn {_key, assoc_info}, seen ->
      related = Map.get(assoc_info, :join_through, Map.get(assoc_info, :related))

      if is_binary(related) do
        seen
      else
        events =
          subscribe_events({related, :all})
          |> Enum.map(&{&1, nil})
          |> MapSet.new()

        all_events = MapSet.difference(events, seen)

        if all_events == MapSet.new([]) do
          seen
        else
          all_events
          |> Enum.each(fn {watcher_identifier, id} ->
            subscribe(watcher_identifier, id)
          end)

          do_subscribe_assocs(related, MapSet.union(all_events, seen))
        end
      end
    end)
  end

  # def subscribe_assocs(schema_mod, id) when is_atom(schema_mod) do
  #   schema_mod.__schema__(:associations)
  #   |> Enum.reduce(MapSet.new([]), fn key, seen ->
  #     %{related: related} =
  #       schema_mod.__schema__(:association, key)

  #     events =
  #       subscribe_events({related, :all})
  #       |> Enum.map(&{&1, id})
  #       |> MapSet.new()

  #     MapSet.difference(events, seen)
  #     |> Enum.reduce([], fn {identifier, id}, acc ->
  #       acc ++ subscribe(identifier, id)
  #     end)
  #     |> MapSet.new()
  #     |> MapSet.union(seen)
  #   end)
  #   |> MapSet.to_list()
  # end
  @doc """
  Subscribe to all events related to the assocs that are preloaded in the given value.
  """
  @spec subscribe_preloads(schema_or_list_of_schemas()) :: subscriptions()
  def subscribe_preloads(value) do
    callback = &do_subscribe_preloads/3

    reduce_to_seen(value, &do_subscribe_preloads(&1, &2, callback))
  end

  defdelegate sync(struct, config), to: EctoSync.Syncer

  defp do_subscribe_all(value, seen, _callback \\ nil) when is_struct(value) do
    schema_mod = get_schema(value)

    if ecto_schema_mod?(schema_mod) do
      subscriptions = subscribe(value) |> MapSet.new()

      seen = MapSet.union(seen, subscriptions)

      do_subscribe_preloads(value, seen, &do_subscribe_all/3)
    else
      seen
    end
  end

  defp do_subscribe_preloads(value, seen, callback) when is_function(callback) do
    schema_mod = get_schema(value)

    id = primary_key(value)

    if ecto_schema_mod?(schema_mod) do
      reduce_preloaded_assocs(value, seen, fn
        {_key, assoc_info}, assoc, seen ->
          subscribe_assoc(id, assoc, assoc_info, seen, callback)
      end)
    end
  end

  defp subscribe_assoc(id, assocs, assoc_info, seen, callback) when is_list(assocs),
    do:
      Enum.reduce(assocs, seen, fn assoc, seen ->
        subscribe_assoc(id, assoc, assoc_info, seen, callback)
      end)

  # defp subscribe_assoc(
  #        id,
  #        assoc,
  #        %ManyToMany{related: related, join_through: join_through, join_keys: join_keys} =
  #          assoc_info,
  #        seen,
  #        callback
  #      )
  #      when is_atom(join_through) do
  #   [{parent_key, _} | _] = join_keys
  #   # Subscribe to join_through inserts with id 
  #   # Subscribe to join_through updates, deletes

  #   join_through_inserted =
  #     [{{join_through, :inserted}, {parent_key, id}}]

  #   join_through_events =
  #     subscribe_events(join_through)
  #     |> Enum.map(&{&1, {parent_key, id}})

  #   # Subscribe to related updates, deletes
  #   assoc_events =
  #     subscribe_events(assoc)
  #     |> Enum.map(&{&1, assoc.id})
  #     |> MapSet.new()

  #   assoc_events =
  #     (IO.inspect(subscribe_events(assoc), label: :assoc_events) ++
  #        join_through_events ++ join_through_inserted)
  #     |> MapSet.new()
  #     |> MapSet.difference(seen)

  #   for {event, id} <- assoc_events do
  #     subscribe(event, id)
  #   end

  #   callback.(assoc, MapSet.union(assoc_events, seen), callback)
  # end

  defp subscribe_assoc(
         id,
         assoc,
         %ManyToMany{join_through: join_through, join_keys: [{parent_key, _} | _]} = assoc_info,
         seen,
         callback
       ) do
    event_label = fn
      event when is_binary(join_through) ->
        String.to_atom("#{join_through}_#{event}")

      event ->
        {join_through, event}
    end

    join_through_events =
      [:updated, :deleted]
      |> Enum.map(&{event_label.(&1), {parent_key, id}})

    # Subscribe to related updates, deletes
    assoc_events =
      [child_inserted(assoc_info, id) | join_through_events ++ assoc_events(assoc)]
      |> MapSet.new()
      |> MapSet.difference(seen)

    for {event, id} <- assoc_events do
      subscribe(event, id)
    end

    callback.(assoc, MapSet.union(assoc_events, seen), callback)
  end

  defp subscribe_assoc(id, assoc, assoc_info, seen, callback) do
    all_events =
      if is_nil(assoc) do
        [child_inserted(assoc_info, id)]
      else
        [child_inserted(assoc_info, id) | assoc_events(assoc)]
      end
      |> MapSet.new()
      |> MapSet.difference(seen)

    for {event, id} <- all_events do
      subscribe(event, id)
    end

    seen = MapSet.union(seen, all_events)

    if is_nil(assoc) do
      seen
    else
      callback.(assoc, seen, callback)
    end
  end

  defp assoc_events(assoc) do
    subscribe_events(assoc)
    |> Enum.map(&{&1, primary_key(assoc)})
  end

  defp do_subscribe(events, id, opts) do
    watch_id = (is_tuple(id) && nil) || id

    for watcher_identifier <- events do
      # with {:error, error} <- WatcherServer.pub_sub_subscription_details(watcher_identifier, id) do
      #   raise ArgumentError, error
      # end

      Registry.lookup(EventRegistry, {watcher_identifier, id})
      EventHandler.subscribe(watcher_identifier, watch_id)

      if Registry.count_match(EventRegistry, {watcher_identifier, id}, :_) == 0 do
        #   # Logger.debug("EventRegistry | #{inspect({watcher_identifier, id})}")
        Registry.register(EventRegistry, {watcher_identifier, id}, opts)
      end

      {watcher_identifier, id}
    end
  end

  defp subscribe_events(label_or_schema) when is_atom(label_or_schema) do
    if ecto_schema_mod?(label_or_schema) do
      subscribe_events({label_or_schema, :all})
    else
      List.wrap(label_or_schema)
    end
  end

  defp subscribe_events(values) when is_list(values) do
    Enum.map(values, &subscribe_events(&1)) |> List.flatten()
  end

  defp subscribe_events(value) when is_struct(value) do
    schema = get_schema(value)

    if ecto_schema_mod?(schema) do
      ~w/updated deleted/a
      |> Enum.map(&{schema, &1})
    else
    end
  end

  defp subscribe_events({schema, event} = watcher_identifier)
       when is_atom(schema) and event in [:all | @events] do
    case watcher_identifier do
      {schema, :all} -> Enum.map(@events, &{schema, &1})
      _ -> List.wrap(watcher_identifier)
    end
  end

  defp child_inserted(%ManyToMany{join_through: join_through, join_keys: join_keys}, parent_id) do
    [{parent_key, _} | _] = join_keys

    watcher_identifier =
      if is_binary(join_through) do
        String.to_atom("#{join_through}_inserted")
      else
        {join_through, :inserted}
      end

    {watcher_identifier, {parent_key, parent_id}}
  end

  defp child_inserted(%Has{related_key: related_key, related: schema}, parent_id) do
    assoc_field = {related_key, parent_id}

    {{schema, :inserted}, assoc_field}
  end

  defp child_inserted(%BelongsTo{related: schema}, _parent_id) do
    {{schema, :inserted}, nil}
  end

  defp reduce_to_seen(value, function) do
    List.wrap(value)
    |> Enum.reduce(MapSet.new([]), function)
    |> MapSet.to_list()
  end

  defp maybe_add_belongs_assocs(schema, opts) do
    {assoc_fields?, opts} =
      Keyword.pop(opts, :add_assocs, false)

    {columns, opts} = Keyword.pop(opts, :extra_columns, [])

    extra_columns =
      if assoc_fields? do
        schema.__schema__(:associations)
        |> Enum.map(&schema.__schema__(:association, &1))
        |> Enum.reduce(columns, fn
          %BelongsTo{owner_key: key}, acc ->
            [key | acc]

          _, acc ->
            acc
        end)
        |> Enum.reverse()
      else
        columns
      end

    Keyword.put(opts, :extra_columns, extra_columns)
  end

  defp raise_no_ecto(schema),
    do: raise(ArgumentError, "Expected an Ecto schema struct, got #{inspect(schema)}")
end
