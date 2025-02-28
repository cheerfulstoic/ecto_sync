defmodule Person do
  @moduledoc false
  use Ecto.Schema

  schema "persons" do
    field(:name, :string)
    has_many(:posts, Post)
  end
end
