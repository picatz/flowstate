---
name: flowstate-plugin-author
description: Use when asked to write or change a Flowstate plugin (a task provider, the executable flowstate-plugin-<name>): decide if a plugin is needed, define the schema in Protobuf, test and rehearse it locally.
---

# Authoring a Flowstate plugin

A plugin is a separate executable a worker launches; its tasks appear to a
Flowfile as `<plugin>.<task>:`. The full walkthrough is `docs/PLUGINS.md`; the
worked example is `pkg/flowstate/v1/plugin/examples/flowstate-plugin-example`,
and `examples/plugins/` holds Flowfiles that use plugin tasks. Read both before
writing code.

## Decide first

- Run `flow tasks`. The built-ins are `exec`, `http`, and `log`; `flow tasks
  http` shows the inputs `http` takes (its response cap and egress come from
  operator policy). An API reachable with `http` is a
  Flowfile, not a plugin.
- Reuse a Flowfile with `call:` when the logic is a composition of steps.
- Write a plugin only for behavior a Flowfile cannot express: a protocol that is
  not an HTTP fetch, a client library, or a secrets backend.

## Build it the right way

- Name the binary `flowstate-plugin-<name>`; discovery ignores anything else,
  and the suffix is the qualifier a Flowfile writes.
- Use `sdk.Main(sdk.Plugin{...})` from `pkg/flowstate/v1/plugin/sdk`. Set
  `Version` and `Description` on `sdk.Plugin`, and a one-line `Summary` on every
  `Task`.
- Define the schema once, in Protobuf. Set `Input:` and `Output:` to the
  generated messages, decode with `sdk.DecodeInputs`, return `sdk.EncodeOutputs`.
  Never hand-write a second struct for the same shape. A task with no
  descriptors validates nothing: a misspelled input passes `flow validate`.
- Generate with `go tool buf generate proto`, using `protoc-gen-go` and `protoc-gen-flowstate-doc` (see
  the `buf.gen.yaml` in PLUGINS.md; the second carries field comments to
  editors). Regenerate, never hand-edit `*.pb.go`.
- Put bounds in the schema with standard protovalidate rules (`string.max_len`,
  `repeated.max_items`, `in`, `required`); the host enforces them at
  `flow validate`.
- Plugin `cel` rules (`(buf.validate.field).cel` and the message and predefined
  forms) are stripped host-side, so plugin CEL can never be a `flow validate`
  contract. Put any cross-field or computed check in the plugin's own process.
- Print nothing to stdout before the SDK serves; it corrupts the handshake. Log
  to stderr or with `sdk.WithLogger`.
- `Fn` runs concurrently. Guard package-level state with a mutex, `sync.Once`,
  or an atomic. Build the manifest from constants, never from the environment
  or a clock: a relaunched plugin that describes itself differently is refused.

## Secrets and errors

- Secrets stay references until the point of use. Name an input that takes one in
  `SecretInputs` (and `RequiredSecretInputs`), or declare a credential with
  `Credentials:` and `(flowstate.v1.input).credential`. The host resolves it
  worker-side and `DecodeInputs` hands your function the resolved string.
- Never put a secret, token, or credential-bearing backend message in a returned
  error, a log line, stderr, or a health message: errors are written to durable
  workflow history.
- Return errors through the constructors (`sdk.InvalidInput`, `sdk.Failed`,
  `sdk.Unavailable`, `sdk.UnavailableAfter`, `sdk.OutcomeUnknown`, ...). Only
  `Unavailable` is retried; pick the one that is true.

## Bound what you fetch

- Reach the network only through `sdk.HTTPClient()`. It applies the operator's
  egress policy and `max_response_bytes`; do not build your own `http.Client`.
- Call `sdk.WithCredentials(ctx)` when a secret travels somewhere the SDK cannot
  see (a query string, a custom header, a body).
- A non-HTTP protocol that needs its own bound uses
  `sdk.HTTPClientWithBounds(maxResponseBytes, timeout)`; it changes what is
  bounded, never what may be reached.
- Cap every list, page count, and read a remote party controls.

## Validate, test, run locally first

```sh
go build -o ./bin/flowstate-plugin-<name> .   # the name is not optional
flow plugins --plugin-dir ./bin               # launches it; shows tasks, inputs, outputs
flow tasks --plugin-dir ./bin <name>.<task>   # the task in full, with a step to copy
flow validate --plugin-dir ./bin workflow.yaml
flow plugins --plugin-dir ./bin -o json > plugins.lock.json
flow test --plugin-catalog plugins.lock.json workflow.test.yaml   # plugin tasks stubbed
flow run local workflow.yaml --plugin-dir ./bin --secret-env NAME --auth-policy auth.yaml
```

- `flow plugins` surfaces a manifest typo that stops a plugin at startup; add
  `--verbose` to read the plugin's own message. Run it on every build.
- A secret passed to an input not declared secret passes `flow validate` and is
  refused at run time, so cover it with a `flow run local` leg.
- In Go, test with `pkg/flowstate/v1/plugin/plugintest`: `Build`, `Launch`,
  `Run`/`Call`, then `s.Conform(t)`. Put it in its own package that imports
  neither `main` nor `gen/`, and skip it under `-short`. Test a refused input
  and a failure kind, not only the happy path, and assert that a secret value
  appears in no output and no error.
- `flow validate --plugin-catalog plugins.lock.json workflow.yaml` checks a file
  against the saved catalog without launching a plugin.

## Before you finish

- [ ] A built-in task or a `call:` could not do the job.
- [ ] The schema is Protobuf, generated, and used for `Input` and `Output`.
- [ ] Every task has a summary, and every field a comment.
- [ ] No secret in any error, log, or output; secret inputs declared.
- [ ] External responses and lists are bounded; network goes through the SDK.
- [ ] No rule depends on plugin `cel`.
- [ ] `flow plugins`, `flow validate`, and `flow run local` ran with `--plugin-dir`.
- [ ] `plugintest` with `Conform` passes, including a negative case.
