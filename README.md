# EctoSync

**TLDR;**
```
Subscribe to events emitted by EctoWatch, cache the result, publish sync function to subscribers.
```

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `ecto_sync` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:ecto_sync, "~> 0.1.0"}
  ]
end
```
### Modify lib/my_app/application.ex
```
children = [
  ...,
  {EctoSync,
   repo: MyRepo,
   pub_sub: MyPubSub,
   watchers:
     EctoSync.all_events(Schema)
     |> EctoSync.all_events(OtherSchema, add_assocs: true)
     |> EctoSync.all_events(AnotherSchema, extra_columns: [:test_id])},
  ]
```

## Future improvements:
 - Generate better representation of schemas and associations to improve the naive method of syncing structs.
 - Implement a pool of EventHandlers.

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/ecto_sync>.

