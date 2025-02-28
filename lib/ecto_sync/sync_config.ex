defmodule EctoSync.SyncConfig do
  defstruct repo: nil,
            cache_name: nil,
            ref: nil

  def new(values) do
    %__MODULE__{repo: values[:repo], cache_name: values[:cache_nam], ref: values[:ref]}
  end
end
