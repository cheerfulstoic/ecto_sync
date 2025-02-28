defmodule EctoSync.Helpers do
  @moduledoc false
  def ecto_schema_mod?(schema_mod) do
    schema_mod.__schema__(:fields)

    true
  rescue
    UndefinedFunctionError -> false
  end

  def get_schema([value | _]), do: get_schema(value)
  def get_schema(value) when is_struct(value), do: value.__struct__
  def get_schema(_), do: nil

  def find_preloads([value | _]), do: find_preloads(value)

  def find_preloads(value) when is_struct(value) do
    reduce_preloaded_assocs(value, [], fn {key, _}, acc ->
      case Map.get(value, key) do
        nil ->
          [{key, []} | acc]

        [] ->
          [{key, []} | acc]

        [assoc | _] ->
          [{key, find_preloads(assoc)} | acc]

        assoc ->
          [{key, find_preloads(assoc)} | acc]
      end
    end)
  end

  def reduce_assocs(schema_mod, acc \\ nil, function) when is_function(function) do
    schema_mod.__schema__(:associations)
    |> Enum.reduce(acc, fn key, acc ->
      assoc_info = schema_mod.__schema__(:association, key)
      function.({key, assoc_info}, acc)
    end)
  end

  def reduce_preloaded_assocs(%{__struct__: schema_mod} = value, acc \\ nil, function)
      when is_function(function) do
    reduce_assocs(schema_mod, (is_nil(acc) && value) || acc, fn {key, assoc_info}, acc ->
      case Map.get(value, key) do
        struct when not is_struct(struct, Ecto.Association.NotLoaded) ->
          maybe_call_with_struct(function, {key, assoc_info}, struct, acc)

        _ ->
          acc
      end
    end)
  end

  defp maybe_call_with_struct(function, key, struct, acc) do
    if is_function(function, 3) do
      function.(key, struct, acc)
    else
      function.(key, acc)
    end
  end

  def get_from_cache([schema, id | _] = key, config) do
    key =
      List.to_tuple(key ++ [config.ref])

    {_, value} =
      Cachex.fetch(config.cache_name, key, fn _key ->
        {:commit, config.get_fun.(schema, id)}
      end)

    value
  end

  def update_cache({schema, :deleted}, id, _, config) do
    Cachex.del(config.cache_name, {schema, id})
    {:ok, {schema, id}}
  end

  def update_cache({schema, _event}, id, preloads, config) do
    key = {schema, id}

    record =
      config.repo.get!(schema, id)
      |> config.repo.preload(preloads)

    {:ok, true} = Cachex.put(config.cache_name, key, record)
    {:ok, key}
  end
end
