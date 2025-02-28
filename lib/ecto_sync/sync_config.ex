defmodule EctoSync.SyncConfig do
  @moduledoc false
  defstruct id: nil,
            schema: nil,
            event: nil,
            repo: nil,
            cache_name: nil,
            ref: nil,
            get_fun: nil,
            preloads: []

  def new(id, {schema, event}, cache_name, repo) do
    ref = :erlang.make_ref()

    %__MODULE__{
      id: id,
      schema: schema,
      event: event,
      repo: repo,
      cache_name: cache_name,
      ref: ref,
      get_fun: &repo.get(&1, &2)
    }
  end

  def maybe_put_get_fun(config, nil), do: config
  def maybe_put_get_fun(config, get_fun), do: Map.put(config, :get_fun, get_fun)
end
