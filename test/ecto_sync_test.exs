defmodule EctoSyncTest do
  use EctoSync.RepoCase, async: false
  import EctoSync

  setup do
    do_setup()
  end

  describe "all_events/3" do
    test "all events are generated" do
      assert [
               {Post, :inserted, [extra_columns: []]},
               {Post, :updated, [extra_columns: []]},
               {Post, :deleted, [extra_columns: []]}
             ] == EctoSync.all_events([], Post)
    end

    test "add assocs generates belongs_to assocs" do
      assert [
               {Post, :inserted, [extra_columns: [:person_id]]},
               {Post, :updated, [extra_columns: [:person_id]]},
               {Post, :deleted, [extra_columns: [:person_id]]}
             ] == EctoSync.all_events([], Post, add_assocs: true)
    end

    test "add_assocs merges other columns" do
      assert [
               {Post, :inserted, [extra_columns: [:id, :person_id]]},
               {Post, :updated, [extra_columns: [:id, :person_id]]},
               {Post, :deleted, [extra_columns: [:id, :person_id]]}
             ] == EctoSync.all_events([], Post, add_assocs: true, extra_columns: [:id])
    end

    test "raises with invalid inputs" do
      assert_raise(ArgumentError, fn -> EctoSync.all_events(Unexisting) end)
    end
  end

  describe "subscribe events" do
    test "subscribe to Ecto.Schema" do
      assert [
               {{Post, :inserted}, nil},
               {{Post, :updated}, nil},
               {{Post, :deleted}, nil}
             ] ==
               subscribe({Post, :all})
    end

    test "subscribe to Ecto.Schema struct", %{person: person} do
      assert [
               {{Person, :updated}, person.id},
               {{Person, :deleted}, person.id}
             ] ==
               subscribe(person)
    end

    test "subscribe to a list of Ecto.Schema structs", %{
      person: person,
      person_with_posts: person2
    } do
      assert [
               {{Person, :updated}, person.id},
               {{Person, :deleted}, person.id},
               {{Person, :updated}, person2.id},
               {{Person, :deleted}, person2.id}
             ] ==
               subscribe([person, person2])
               |> Enum.sort_by(&elem(&1, 1))
    end

    test "subscribe to label" do
      assert [{:label, nil}] == subscribe(:label)
    end
  end

  describe "subscribe_all/1" do
    test "subscribe to a list of Ecto.Schema structs", %{
      person_with_posts: %{posts: [post1, post2]} = person,
      person: person2
    } do
      assert [
               {{Person, :deleted}, person2.id},
               {{Person, :deleted}, person.id},
               {{Person, :updated}, person2.id},
               {{Person, :updated}, person.id},
               {{Post, :deleted}, post1.id},
               {{Post, :deleted}, post2.id},
               {{Post, :inserted}, {:person_id, person.id}},
               {{Post, :updated}, post1.id},
               {{Post, :updated}, post2.id}
             ] ==
               subscribe_all([person, person2])
               |> Enum.sort()
    end
  end

  describe "subscribe_preloads/1" do
    test "subscribe to preloaded Ecto.Schemas",
         %{
           person_with_posts_and_tags:
             %{posts: [%{tags: [tag]} = post1, %{labels: [label]} = post2]} = person
         } do
      assert [
               {
                 :posts_labels_deleted,
                 {:post_id, post2.id}
               },
               {
                 :posts_labels_inserted,
                 {:post_id, post2.id}
               },
               {
                 :posts_labels_updated,
                 {:post_id, post2.id}
               },
               {{Label, :deleted}, label.id},
               {{Label, :updated}, label.id},
               {{Post, :deleted}, post1.id},
               {{Post, :deleted}, post2.id},
               {{Post, :inserted}, {:person_id, person.id}},
               {{Post, :updated}, post1.id},
               {{Post, :updated}, post2.id},
               {{PostsTags, :deleted}, {:post_id, post1.id}},
               {{PostsTags, :inserted}, {:post_id, post1.id}},
               {{PostsTags, :updated}, {:post_id, post1.id}},
               {{Tag, :deleted}, tag.id},
               {{Tag, :updated}, tag.id}
             ]
             |> Enum.sort() ==
               subscribe_preloads([person])
               |> Enum.sort()
    end
  end

  describe "subscribe_assocs/1" do
    test "subscribe to assocs on Ecto.Schemas" do
      assert [
               {{Person, :deleted}, nil},
               {{Person, :inserted}, nil},
               {{Person, :updated}, nil},
               {{Post, :deleted}, nil},
               {{Post, :inserted}, nil},
               {{Post, :updated}, nil},
               {{PostsTags, :deleted}, nil},
               {{PostsTags, :inserted}, nil},
               {{PostsTags, :updated}, nil},
               {{Tag, :deleted}, nil},
               {{Tag, :inserted}, nil},
               {{Tag, :updated}, nil}
             ] ==
               subscribe_assocs(Person)
               |> Enum.sort()
    end
  end

  describe "integrations" do
    test "subscribe/2 to inserts", %{person: person} do
      assert [{{Post, :inserted}, nil}] == subscribe({Post, :inserted})

      {:ok, post} = TestRepo.insert(%Post{person_id: person.id})

      receive do
        {{Post, :inserted}, sync_args} ->
          {:ok, synced} = EctoSync.sync(post, sync_args)
          assert synced == post
      after
        1000 ->
          raise "no inserts"
      end
    end

    test "subscribe/2 to updates", %{person: person} do
      {:ok, %{id: post_id} = post} = TestRepo.insert(%Post{person_id: person.id})

      assert [
               {{Post, :updated}, post_id}
             ] ==
               subscribe({Post, :updated}, post_id)

      {:ok, updated} =
        Ecto.Changeset.change(post, %{name: "updated"})
        |> TestRepo.update()

      receive do
        {{Post, :updated}, sync_args} ->
          {:ok, synced} = EctoSync.sync(post, sync_args)
          assert synced == updated
      after
        1000 ->
          raise "no updates"
      end
    end

    test "subscribe/2 to deletes", %{person: person} do
      {:ok, post} =
        TestRepo.insert(%Post{person_id: person.id})

      assert [
               {{Post, :updated}, post.id},
               {{Post, :deleted}, post.id}
             ] ==
               subscribe(post)

      {:ok, updated} =
        Ecto.Changeset.change(post, %{name: "updated"})
        |> TestRepo.update()

      person = TestRepo.preload(person, [:posts], force: true)

      receive do
        {{Post, :updated}, sync_args} ->
          {:ok, synced} = EctoSync.sync(post, sync_args)
          assert synced == updated
      after
        1000 ->
          raise "no updates"
      end

      {:ok, updated} = TestRepo.delete(post)
      expected = TestRepo.preload(person, [:posts], force: true)

      receive do
        {{Post, :deleted}, sync_args} ->
          {:ok, nil} = EctoSync.sync(updated, sync_args)
          {:ok, synced} = EctoSync.sync(person, sync_args)
          assert synced == expected
      after
        1000 ->
          raise "no deletes"
      end
    end

    test "subscriptions based on assocs work", %{person: person} do
      subscribe({Post, :all}, {:person_id, person.id})

      {:ok, post} =
        TestRepo.insert(%Post{person_id: person.id})

      receive do
        {{Post, :inserted}, sync_args} ->
          {:ok, synced} = EctoSync.sync(post, sync_args)
          assert synced == post
      after
        1000 ->
          raise "nothing"
      end

      {:ok, updated_post} =
        Ecto.Changeset.change(post, %{name: "updated"})
        |> TestRepo.update()

      receive do
        {{Post, :updated}, sync_args} ->
          {:ok, synced} = EctoSync.sync(post, sync_args)
          assert synced == updated_post
      after
        1000 ->
          raise "nothing"
      end
    end

    # TODO make this test more robust.
    test "assoc has moved to other row", %{
      person: person2,
      person_with_posts: person_with_posts
    } do
      preloads = [:posts]

      %{posts: [post1 | _]} = person1 = person_with_posts |> do_preload(preloads)
      person2 = do_preload(person2, preloads)

      # subscribe_all(person1)

      subscribe(Post, {:person_id, person2.id})
      # subscribe_all(person2)

      {:ok, _} =
        Ecto.Changeset.change(post1, %{person_id: person2.id})
        |> TestRepo.update()

      person1_expected_after_update = TestRepo.get(Person, person1.id) |> do_preload(preloads)

      person2_expected_after_update = TestRepo.get(Person, person2.id) |> do_preload(preloads)

      receive do
        {{Post, :updated}, sync_args} ->
          {:ok, synced} = EctoSync.sync(person1, sync_args)
          assert person1_expected_after_update == synced

          {:ok, synced} = EctoSync.sync(person2, sync_args)
          assert person2_expected_after_update == synced
      after
        1000 -> raise "no updates for person1"
      end

      # receive do
      #   {{Post, :inserted}, sync_args} ->
      #     {:ok, synced} = EctoSync.sync(person2, sync_args)
      #     assert person2_expected_after_update == synced
      # after
      #   1000 -> raise "no updates for person2"
      # end
    end

    test "assoc has been deleted", %{person_with_posts: person1} do
      preloads = [:posts]

      %{posts: [_case1, post2]} = person1 = do_preload(person1, preloads)

      subscribe_all(person1)

      {:ok, _} = TestRepo.delete(post2)

      expected_after_delete =
        TestRepo.get(Person, person1.id)
        |> do_preload(preloads)

      receive do
        {{Post, :deleted}, sync_args} ->
          {:ok, synced} = EctoSync.sync(person1, sync_args)
          assert expected_after_delete == synced
      after
        1000 ->
          raise "no deletes"
      end
    end

    test "sync fun with list returns updated list", %{
      person_with_posts: %{posts: [post1 | _]} = person
    } do
      preloads = [:posts]
      subscribe_all(person)

      {:ok, _} =
        Ecto.Changeset.change(post1, %{name: "updated"})
        |> TestRepo.update()

      expected = do_preload(person, preloads)

      sort = fn enum -> Enum.sort_by(enum, & &1.id) end

      receive do
        {{Post, :updated}, sync_args} ->
          {:ok, synced} = EctoSync.sync([person, person], sync_args)
          assert is_list(synced)

          for s <- synced do
            assert sort.(s.posts) == sort.(expected.posts)
          end
      after
        1000 -> raise "no updates"
      end
    end

    test "multiple updates result in distinct values" do
      preloads = [posts: [person: [:posts]]]

      {:ok, %{posts: [post1]} = person1} =
        TestRepo.insert(%Person{posts: [%Post{}]})
        |> do_preload(preloads)

      subscribe_all(person1)

      {:ok, _} =
        Ecto.Changeset.change(post1, %{name: "post1 update1"})
        |> TestRepo.update()

      person1_expected_after_update =
        TestRepo.get(Person, person1.id) |> do_preload(preloads)

      receive do
        {{Post, :updated}, sync_args} ->
          {:ok, synced} = EctoSync.sync(person1, sync_args)
          assert person1_expected_after_update == synced
      after
        1000 -> raise "no updates for update1"
      end

      {:ok, _} =
        Ecto.Changeset.change(post1, %{name: "post1 update2"})
        |> TestRepo.update()

      person1_expected_after_update_2 =
        TestRepo.get(Person, person1.id) |> do_preload(preloads)

      receive do
        {{Post, :updated}, sync_args} ->
          {:ok, synced} = EctoSync.sync(person1_expected_after_update, sync_args)
          assert person1_expected_after_update_2 == synced
      after
        1000 -> raise "no updates for update2"
      end

      # (Enum.sum(db_times) / Enum.count(db_times))
      # |> IO.inspect(label: :db_avg)

      # (Enum.sum(cache_times) / Enum.count(cache_times))
      # |> IO.inspect(label: :cache_avg)
    end
  end

  describe "belongs_to" do
    test "insert" do
      preloads = [:person]

      {:ok, post} =
        TestRepo.insert(%Post{})
        |> do_preload(preloads)

      subscribe_all(post)

      {:ok, _person} = TestRepo.insert(%Person{})
      {:ok, _person} = TestRepo.insert(%Person{posts: [post]})
      expected = TestRepo.get(Post, post.id) |> do_preload(preloads)

      receive do
        {{Post, :updated}, sync_args} ->
          {:ok, synced} = EctoSync.sync(post, sync_args)
          assert expected == synced

          # {{Person, :inserted}, sync_args} ->
          #   {:ok, synced} = EctoSync.sync(post, sync_args)
          #   assert do_preload(post, preloads) == synced
      after
        1000 -> raise "no post inserted"
      end
    end

    test "delete", %{person_with_posts_and_tags: person, person: other_person} do
      %{posts: [post1 | _]} = person
      preloads = [:person]
      post1 = do_preload(post1, preloads)

      subscribe_preloads(post1)

      for p <- [person, other_person] do
        TestRepo.delete(p)
      end

      receive do
        {{Person, :deleted}, sync_args} ->
          {:ok, synced} = EctoSync.sync(post1, sync_args)
          assert do_preload(post1, preloads) == synced
      after
        1000 -> raise "no post update"
      end

      refute_received({{Person, :updated}, _})
    end

    test "update", %{person_with_posts_and_tags: person, person: other_person} do
      preloads = [:person]
      %{posts: [post1 | _]} = person
      post1 = do_preload(post1, preloads)

      subscribe_preloads(post1)

      {:ok, _} =
        Ecto.Changeset.change(person, %{name: "updated"})
        |> TestRepo.update()

      {:ok, _} =
        Ecto.Changeset.change(other_person, %{name: "updated"})
        |> TestRepo.update()

      receive do
        {{Person, :updated}, sync_args} ->
          {:ok, synced} = EctoSync.sync(post1, sync_args)
          assert do_preload(post1, preloads) == synced
      after
        1000 -> raise "no post update"
      end

      refute_received({{Person, :updated}, _})
    end

    test "update assoc is changed", %{person_with_posts_and_tags: person, person: other_person} do
      preloads = [:person]
      %{posts: [post1, post2]} = person
      post1 = do_preload(post1, preloads)

      subscribe_all(post1)

      {:ok, _} =
        Ecto.Changeset.change(post2, %{person_id: other_person.id})
        |> TestRepo.update()

      {:ok, preloaded} =
        Ecto.Changeset.change(post1, %{person_id: other_person.id})
        |> TestRepo.update()
        |> do_preload(preloads)

      receive do
        {{Post, :updated}, sync_args} ->
          {:ok, synced} = EctoSync.sync(post1, sync_args)
          assert preloaded == synced
      after
        1000 -> raise "no post update"
      end

      refute_received({{Person, :updated}, _})
    end
  end

  describe "has_many" do
    test "inserted", %{person_with_posts_and_tags: person} do
      preloads = [posts: [:tags, :labels]]
      person = do_preload(person, preloads)

      subscribe_preloads(person)

      {:ok, _post} = TestRepo.insert(%Post{person_id: person.id})

      receive do
        {{Post, :inserted}, sync_args} ->
          {:ok, synced} = EctoSync.sync(person, sync_args)
          assert do_preload(person, preloads) == synced
          synced
      after
        1000 -> raise "nothing POSTS"
      end
    end

    test "updated", %{person_with_posts_and_tags: person} do
      preloads = [posts: [:tags, :labels]]
      %{posts: [post1 | _]} = person = do_preload(person, preloads)

      subscribe_preloads(person)

      {:ok, _} =
        Ecto.Changeset.change(post1, %{name: "updated"})
        |> TestRepo.update()

      receive do
        {{Post, :updated}, sync_args} ->
          {:ok, %{posts: synced_posts}} = EctoSync.sync(person, sync_args)
          %{posts: preloaded_posts} = do_preload(person, preloads)
          assert preloaded_posts |> Enum.sort() == synced_posts |> Enum.sort()
      after
        1000 -> raise "no post update"
      end
    end

    test "deleted", %{person_with_posts_and_tags: person} do
      preloads = [posts: [:tags, :labels]]
      %{posts: [post1 | _]} = person = do_preload(person, preloads)

      subscribe_preloads(person)

      {:ok, _} = TestRepo.delete(post1)

      receive do
        {{Post, :deleted}, sync_args} ->
          {:ok, %{posts: synced_posts}} = EctoSync.sync(person, sync_args)
          %{posts: preloaded_posts} = do_preload(person, preloads)
          assert preloaded_posts |> Enum.sort() == synced_posts |> Enum.sort()
      after
        1000 -> raise "no post update"
      end
    end
  end

  describe "many to many with join_through module" do
    test "inserted", %{person_with_posts_and_tags: person} do
      preloads = [posts: [:tags, :labels]]
      %{posts: [post1, _post2]} = person = do_preload(person, preloads)

      subscribe_preloads(person)

      {:ok, tag} = TestRepo.insert(%Tag{})
      {:ok, _assoc} = TestRepo.insert(%PostsTags{post_id: post1.id, tag_id: tag.id})

      receive do
        {{PostsTags, :inserted}, sync_args} ->
          {:ok, synced} = EctoSync.sync(person, sync_args)
          assert do_preload(person, preloads) == synced
          synced
      after
        1000 -> raise "nothing POSTS"
      end
    end

    test "updated", %{person_with_posts_and_tags: person} do
      preloads = [posts: [:tags, :labels]]
      %{posts: [%{tags: [tag]}, _post2]} = person = do_preload(person, preloads)

      subscribe_preloads(person)

      {:ok, other_tag} = TestRepo.insert(%Tag{})

      {:ok, _tag} =
        Ecto.Changeset.change(other_tag, %{name: "other_updated"})
        |> TestRepo.update()

      {:ok, _tag} =
        Ecto.Changeset.change(tag, %{name: "updated"})
        |> TestRepo.update()

      receive do
        {{Tag, :updated}, sync_args} ->
          {:ok, synced} = EctoSync.sync(person, sync_args)
          assert do_preload(person, preloads) == synced
      after
        1000 -> raise "no tag update"
      end
    end

    test "updated subscribe_assocs", %{person_with_posts_and_tags: person} do
      preloads = [posts: [:tags, :labels]]
      %{posts: [%{tags: [tag]}, _post2]} = person = do_preload(person, preloads)

      subscribe_assocs(person)

      {:ok, other_tag} = TestRepo.insert(%Tag{})

      {:ok, _tag} =
        Ecto.Changeset.change(other_tag, %{name: "other_updated"})
        |> TestRepo.update()

      {:ok, _tag} =
        Ecto.Changeset.change(tag, %{name: "updated"})
        |> TestRepo.update()

      flush()
      |> Enum.each(fn
        {{Tag, :updated}, sync_args} ->
          {:ok, synced} = EctoSync.sync(person, sync_args)
          assert do_preload(person, preloads) == synced

        {{Tag, :inserted}, _} ->
          false

        message ->
          raise "#{inspect(message)}"
      end)
    end

    test "deleted", %{person_with_posts_and_tags: person} do
      preloads = [posts: [:tags, :labels]]
      %{posts: [%{tags: [tag]}, _post2]} = person = do_preload(person, preloads)

      subscribe_preloads(person)
      TestRepo.delete(tag)

      receive do
        {{Tag, :deleted}, sync_args} ->
          {:ok, synced} = EctoSync.sync(person, sync_args)
          assert do_preload(person, preloads) == synced
      after
        1000 -> raise "no tag delete"
      end
    end

    # test "inserted with join_through table", %{person_with_posts_and_tags: person} do
    #   preloads = [posts: [:tags, :labels]]
    #   %{posts: [post1, post2]} = person = do_preload(person, preloads)

    #   {:ok, label} = TestRepo.insert(%Label{})

    #   subscribe_preloads(person)

    #   {:ok, _} =
    #     Ecto.Changeset.change(post2, %{labels: [label | post2.labels]})
    #     |> TestRepo.update()

    #   receive do
    #     {{event, :inserted}, sync_args} ->
    #       IO.inspect(event)
    #       {:ok, synced} = EctoSync.sync(person, sync_args)
    #       assert do_preload(person, preloads) |> IO.inspect() == synced
    #   after
    #     1000 -> raise "nothing POSTS"
    #   end
    # end
  end

  describe "unsubscribe/1" do
    test "unsubscribe", %{
      person: person
    } do
      subscribe({Post, :inserted})

      {:ok, post} =
        TestRepo.insert(%Post{person_id: person.id})

      receive do
        {{Post, :inserted}, sync_args} ->
          {:ok, synced} = EctoSync.sync(post, sync_args)
          assert synced == post
      after
        1000 ->
          raise "nothing"
      end

      EctoSync.unsubscribe(%Post{})
    end
  end

  defp do_setup() do
    start_supervised!(TestRepo)
    {:ok, person} = TestRepo.insert(%Person{})

    {:ok, person_with_post_and_tags} =
      TestRepo.insert(%Person{
        posts: [%Post{tags: [%Tag{name: "tag"}]}, %Post{labels: [%Label{name: "label"}]}]
      })

    {:ok, person_with_posts} = TestRepo.insert(%Person{posts: [%Post{}, %Post{}]})
    start_supervised!({Phoenix.PubSub, name: EctoSync.PubSub})

    start_supervised!({
      EctoSync,
      repo: TestRepo,
      pub_sub: EctoSync.PubSub,
      watchers:
        [
          {%{
             table_name: "posts_labels",
             primary_key: :id,
             columns: [:label_id, :post_id],
             association_columns: [:label_id, :post_id]
           }, :updated, extra_columns: [:label_id, :post_id], label: :posts_labels_updated},
          {%{
             table_name: "posts_labels",
             primary_key: :id,
             columns: [:label_id, :post_id],
             association_columns: [:label_id, :post_id]
           }, :inserted, extra_columns: [:label_id, :post_id], label: :posts_labels_inserted},
          {%{
             table_name: "posts_labels",
             primary_key: :id,
             columns: [:label_id, :post_id],
             association_columns: [:label_id, :post_id]
           }, :deleted, extra_columns: [:label_id, :post_id], label: :posts_labels_deleted}
        ]
        |> EctoSync.all_events(Post, extra_columns: [:person_id])
        |> EctoSync.all_events(Person, add_assocs: true)
        |> EctoSync.all_events(Tag, add_assocs: true)
        |> EctoSync.all_events(Label, add_assocs: true)
        |> EctoSync.all_events(PostsTags, add_assocs: true)
    })

    [
      person: person,
      preloads: [:person],
      person_with_posts: person_with_posts,
      person_with_posts_and_tags: person_with_post_and_tags
    ]
  end

  defp do_preload({:ok, value}, preloads) do
    {:ok, do_preload(value, preloads)}
  end

  defp do_preload(value, preloads) do
    Ecto.reset_fields(value, preloads)
    |> TestRepo.preload(preloads, force: true)
  end

  defp flush(messages \\ []) do
    receive do
      message -> flush([message | messages])
    after
      1000 ->
        messages
        |> Enum.reverse()
    end
  end
end
