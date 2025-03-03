Assume the following schema is present:
```elixir
defmodule MyApp.Account do
  use Ecto.Schema

  schema "accounts" do
    has_one :user, MyApp.User
  end
end

defmodule MyApp.User do
  use Ecto.Schema

  schema "users" do
    field :name, :string
    has_many :posts, MyApp.Post
    belongs_to :account, MyApp.Account
  end
end

defmodule MyApp.Post do
  use Ecto.Schema

  schema "posts" do
    field :title, :string
    belongs_to :user, MyApp.User
    has_many :comments, MyApp.Comment
    many_to_many :tags, join_through: MyApp.PostsTags
  end
end

defmodule MyApp.Comment do
  use Ecto.Schema

  schema "comments" do
    field :content, :string
    belongs_to :post, MyApp.Post
    belongs_to :author, MyApp.User
  end
end

defmodule MyApp.Tags do
  use Ecto.Schema

  schema "tags" do
    many_to_many :posts, join_through: MyApp.PostsTags
  end
end

defmodule MyApp.PostsTags do
  use Ecto.Schema

  schema "posts_tags" do
    belongs_to :tag, MyApp.Tag
    belongs_to :post, MyApp.Post
  end
end
```


