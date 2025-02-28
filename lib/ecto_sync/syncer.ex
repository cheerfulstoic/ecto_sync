defmodule EctoSync.Syncer do
  @moduledoc """
  Internal module that holds all the syncing logic.
  """
  alias EctoSync.SyncConfig
  alias Ecto.Schema
  alias Ecto.Association.{BelongsTo, Has, ManyToMany}
  import EctoSync.Helpers

  @spec sync(atom() | Schema.t(), SyncConfig.t()) :: Schema.t() | term()
  def sync(:cached, {_, :deleted}, id, _config), do: {:ok, id}

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
end
