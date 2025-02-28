defmodule Tag do
  @moduledoc false
  use Ecto.Schema

  schema "tags" do
    field(:name, :string)
    many_to_many(:posts, Post, join_through: PostsTags)
  end
end
