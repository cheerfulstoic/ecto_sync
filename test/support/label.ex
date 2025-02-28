defmodule Label do
  @moduledoc false
  use Ecto.Schema

  schema "labels" do
    field(:name, :string)
    many_to_many(:posts, Post, join_through: "posts_labels")
  end
end
