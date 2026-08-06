# embedding

The one example here that is not itself a `workflow.yaml` `flow` runs — it is a Go
program that embeds Flowstate as a library, using `pkg/flowstate/embed`. Run it:

```console
$ go run ./examples/embedding
== running locally ==
2026/.. INFO hello, embedder!
local run outputs: map[message:literal:{string_value:"hello, embedder!"}]

(pass --durable to also run this against a real Temporal server)
```

`main.go` does the four things an embedder does:

1. Registers a custom Go task, `greet`, with an `embed.Tasks` set — no `.proto`
   descriptor, the nil-descriptor escape hatch `embed.Task` documents. See the
   comment on `registerGreetTask` for what that trades away.
2. Compiles `flowfile/workflow.yaml` from bytes with `embed.Compile`, after
   `Tasks.Install`ing the task set so validation can see it.
3. Runs the compiled workflow in-process with `embed.RunLocal`.
4. With `--durable`, also registers a worker and runs the same workflow durably
   against a Temporal server:

```console
$ temporal server start-dev &
$ go run ./examples/embedding --durable
```

Without `--durable`, or without a reachable server, the durable half is skipped
with a message rather than failing — this example's point is the embedding
surface, not standing up Temporal.

See [docs/EMBEDDING.md](../../docs/EMBEDDING.md) for the fuller guide this example
is the runnable half of.
