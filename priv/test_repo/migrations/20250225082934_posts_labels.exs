defmodule TestRepo.Migrations.PostsLabels do
  use Ecto.Migration

  def change do
    create table(:posts_labels) do
      add :label_id, references(:labels, on_delete: :delete_all)
      add :post_id, references(:posts, on_delete: :delete_all)
    end

    create unique_index(:posts_labels, [:label_id, :post_id])
  end
end
