defmodule TestRepo do
  @moduledoc false
  use Ecto.Repo, otp_app: :ecto_sync, adapter: Ecto.Adapters.Postgres
end

defmodule Person do
  @moduledoc false
  use Ecto.Schema

  schema "persons" do
    field(:name, :string)
    has_many(:posts, Post)
  end
end

defmodule Post do
  @moduledoc false
  use Ecto.Schema

  schema "posts" do
    field(:name, :string)
    field(:body, :string)
    belongs_to(:person, Person, on_replace: :update)
    many_to_many(:tags, Tag, join_through: PostsTags)
    many_to_many(:labels, Label, join_through: "posts_labels")
  end
end

defmodule PostsTags do
  @moduledoc false
  use Ecto.Schema

  schema "posts_tags" do
    belongs_to(:post, Post)
    belongs_to(:tag, Tag)
  end
end

defmodule Tag do
  @moduledoc false
  use Ecto.Schema

  schema "tags" do
    field(:name, :string)
    many_to_many(:posts, Post, join_through: PostsTags)
  end
end

defmodule Label do
  @moduledoc false
  use Ecto.Schema

  schema "labels" do
    field(:name, :string)
    many_to_many(:posts, Post, join_through: "posts_labels")
  end
end
