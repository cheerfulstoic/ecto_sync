defmodule TestRepo.Migrations.Person do
  use Ecto.Migration

  def change do
    create table("persons") do
      add :name, :string
    end
  end
end
