defmodule TestRepo do
  @moduledoc false
  use Ecto.Repo, otp_app: :ecto_sync, adapter: Ecto.Adapters.Postgres
end
