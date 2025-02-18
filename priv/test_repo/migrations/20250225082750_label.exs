defmodule TestRepo.Migrations.Label do
  use Ecto.Migration

  def change do
    create table("labels") do
      add :name, :string
    end
  end
end
