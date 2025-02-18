defmodule EctoSync.RepoCase do
  @moduledoc false
  use ExUnit.CaseTemplate

  using do
    quote do
      import Ecto
      # import Ecto.Query
      import EctoSync.RepoCase
    end
  end
end
