# The control-plane API

Everything the `flow` CLI does against a server goes through one
[ConnectRPC](https://connectrpc.com/) service, `flowstate.v1.WorkflowService`,
defined in [`proto/flowstate/v1/service.proto`](../proto/flowstate/v1/service.proto).
You can call it from any language: as JSON over plain HTTP, with a generated
Connect or gRPC client, or with the Go client in this repository.

This page shows how to call it, what it expects, and which parts are safe to
build on today.

## The service

| Area | RPCs |
| --- | --- |
| Authoring (execute nothing) | `Validate`, `Compile`, `GetCatalog` |
| Runs | `Run`, `Get`, `GetTimeline`, `List`, `Signal`, `SignalWithStart`, `Cancel`, `Terminate` |
| Schedules | `CreateSchedule`, `ListSchedules`, `DescribeSchedule`, `DeleteSchedule`, `PauseSchedule`, `ResumeSchedule`, `TriggerSchedule` |
| Debugging a durable run | `DebugAttach`, `DebugGet`, `DebugResume`, `DebugSetBreakpoints`, `DebugInspect`, and `DebugHistory` to read a run at a past point — see [Debugging](DEBUGGING.md#debugging-a-durable-run) |

Each RPC's documentation lives on it in the `.proto` file, including limits,
defaults, and error cases. The [MCP tool reference](reference/mcp.md) renders
the same comments.

The path from a file to a durable run is always `Compile`, then `Run`: `Run`
takes a compiled `flowstate.v1.Workflow`, never Flowfile source.

## Calling it with curl

Start a development stack with `flow server dev`; it listens on
`http://localhost:9233` and accepts anonymous callers. Connect serves every RPC
as a `POST` to `/<package>.<Service>/<Method>` with a JSON body.

```console
$ cat > hello.yaml <<'EOF'
edition: v2026.4
name: hello
inputs:
  who:
    type: string
    default: world
steps:
  - id: greet
    log:
      message: ${"hello, " + inputs.who}
outputs:
  greeting:
    value: ${"hello, " + inputs.who}
EOF
$ API=http://localhost:9233/flowstate.v1.WorkflowService
```

Validate and compile it:

```console
$ curl -sS -X POST $API/Validate -H 'Content-Type: application/json' \
    -d "$(jq -Rs '{files: [{name: "hello.yaml", source: .}]}' hello.yaml)"
{"report":{"files":[{"file":"hello.yaml"}]}}

$ curl -sS -X POST $API/Compile -H 'Content-Type: application/json' \
    -d "$(jq -Rs '{file: {name: "hello.yaml", source: .}}' hello.yaml)" > compiled.json
```

`source` is the Flowfile as plain YAML text, so `jq -Rs` is all the encoding it
needs; a document that is not valid UTF-8 is refused as a whole request, not
reported as a diagnostic. A file entry with no `diagnostics` is clean. A file that does not compile is not
an RPC error: `Compile` answers with the diagnostics in `report` and no
`workflow`.

Run it, passing the compiled workflow unchanged:

```console
$ jq '{workflow, inputs: {who: {literal: {stringValue: "curl"}}}, requestId: "hello-1"}' \
    compiled.json > run.json
$ curl -sS -X POST $API/Run -H 'Content-Type: application/json' -d @run.json
{"workflowId":"flowstate-request-8888…", "runId":"01a0e4e1-…", "status":"STATUS_RUNNING", "specificationAsSubmitted":true}

$ curl -sS -X POST $API/Get -H 'Content-Type: application/json' \
    -d '{"workflowId":"flowstate-request-8888…"}'
{"workflowId":"…", "status":"STATUS_COMPLETED",
 "runOutputs":{"values":{"greeting":{"literal":{"stringValue":"hello, curl"}}}},
 "starter":"flowstate:insecure-anonymous#anonymous", …}
```

`Run` returns as soon as the run starts; a workload may take a week. Poll `Get`,
or read its history with `GetTimeline`.

### JSON conventions

- Field names are protojson lowerCamelCase: `workflowId`, `requestId`.
- Enums are strings: `STATUS_COMPLETED`.
- `bytes` fields are base64. A Flowfile in a `source` field is plain YAML text, as
  it is for every other authoring tool. 64-bit integers are strings.
- A value is a tagged union. Inputs, signal payloads, and outputs use
  `{"literal": {"stringValue": "…"}}`, `{"literal": {"int64Value": "3"}}`,
  `{"literal": {"boolValue": true}}`, and so on. Inputs and signal payloads must
  be literals; an expression or a secret reference from a caller is refused.
- A signal payload is named values:

  ```json
  {"workflowId": "…", "name": "release-approved",
   "payload": {"namedValues": {"approved": {"literal": {"boolValue": true}}}}}
  ```

- Errors are Connect JSON errors, `{"code":"invalid_argument","message":"…"}`,
  with protovalidate violations in `details`.

The CLI's `-o json` output is friendlier than the raw API: it renders values as
plain JSON (`.runOutputs.greeting` is `"hello, curl"`) and renames
`stepValues` to `steps`. `flow get --raw` shows the raw protojson.

### Retrying safely

Set `requestId` on any `Run` a client might retry. A second request with the
same `requestId` in the same tenant is answered with the run the first one
started (`"reused": true`), even after that run has finished. A reused
`requestId` with a different workflow or inputs is refused with
`already_exists`. `flow run` sends a fresh one on every invocation, or yours
with `--request-id`.

To address a run by a business key instead, set `entityKey` (lowercase letters,
digits, and `-`). While a run holds that key, another `Run` with it is refused;
`SignalWithStart` signals the run holding the key, or starts one.

## Protocols and transports

| Client | Works against `flow server` |
| --- | --- |
| Connect, JSON or binary, over HTTP/1.1 | Yes, plaintext on loopback or over TLS. |
| gRPC-Web | Yes. |
| gRPC | Over TLS, where HTTP/2 is negotiated. A plaintext listener speaks HTTP/1.1 only (no h2c). |
| Browser JavaScript from another origin | No: the server sends no CORS headers. Put a proxy that adds them in front, or call from a backend. |
| Connect `GET` requests | No: every RPC is `POST`. |

`flow server` listens in plaintext only on loopback. Anywhere else it requires
TLS, or `--tls-terminated-upstream` behind a proxy that terminates it.

## Authentication

Every RPC requires a caller identity. `flow server` refuses to start without
either `--auth-policy`, naming the token issuers it trusts, or an explicit
`--insecure-no-auth`.

- **Bearer tokens.** Send `Authorization: Bearer <JWT>`. The server verifies it
  against the issuers in the auth policy (OIDC discovery and JWKS), checks the
  audience set by `--rpc-resource`, and derives the caller's tenant from the
  token, never from the request.
- **Client certificates.** With mTLS configured, a verified client certificate
  can identify the caller.
- **Development.** `flow server dev` is anonymous; `flow server dev --auth`
  generates a local issuer and prints a `flow jwt sign` command that mints a
  token for it.

The CLI finds credentials in `--token-file` (`FLOWSTATE_TOKEN_FILE`),
`FLOWSTATE_TOKEN`, or `--credential-source`, which obtains a CI job's own OIDC
token (`github-actions`, `gitlab`, `terraform-cloud`). See
[Deployment](DEPLOYMENT.md) for issuer configuration and
[Architecture](ARCHITECTURE.md#identity-in-both-directions) for how identity
flows into policy.

Tenancy is enforced on every call that addresses a run: a run another tenant
owns is reported as not found.

## The Go client

```go
import (
	"context"
	"net/http"

	"connectrpc.com/connect"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
)

func run(ctx context.Context, source []byte) (*v1.RunResponse, error) {
	client := flowstatev1connect.NewWorkflowServiceClient(
		http.DefaultClient, "http://localhost:9233")

	compiled, err := client.Compile(ctx, connect.NewRequest(&v1.CompileRequest{
		File: &v1.SourceFile{Name: "hello.yaml", Source: source},
	}))
	if err != nil {
		return nil, err
	}

	started, err := client.Run(ctx, connect.NewRequest(&v1.RunRequest{
		Workflow: compiled.Msg.GetWorkflow(),
		Inputs:   map[string]*v1.Value{"who": v1.NewLiteral("go")},
	}))
	if err != nil {
		return nil, err
	}
	return started.Msg, nil
}
```

Check `compiled.Msg.GetReport()` for diagnostics before running a workflow the
compiler refused. Other languages generate a client from [`proto/`](../proto/)
with [Buf](https://buf.build/); the module depends on `protovalidate` and
`googleapis`.

## Building a workflow without YAML

A Flowfile is one way to produce a `flowstate.v1.Workflow`. `Run` accepts any
valid message, including one a program builds directly, and the repository's
own tests and conformance cases build workflows in Go. Two things to know before
you build one:

- **Expressions are parsed CEL.** A `Value`'s `expr` field is a CEL syntax tree
  (`google.api.expr.v1alpha1.ParsedExpr`), not source text. In Go,
  `v1.NewExpr("inputs.who")` parses one. From another language, use a CEL
  parser that emits that message, or call `Compile` on a Flowfile fragment and
  reuse what it returns.
- **The server checks less than the compiler does.** `Run` applies the schema's
  own constraints, size and depth bounds, task existence, input binding, signal
  and debug policies, and plugin pinning. It does not run the Flowfile
  validator's cross-step checks: a reference to a step that does not exist, a
  duplicate step id, or an expression that does not type-check is accepted and
  fails when the run reaches it. In Go, call
  `flowfile.Validate(workflow)` yourself first. There is no RPC for this yet
  (tracked in [#1430](https://github.com/picatz/flowstate/issues/1430)).

`flowfile.Marshal` goes the other way, writing a `Workflow` back out as a
Flowfile, which is how `flow fmt` works.

## What is stable

Flowstate has not had a release, and [SUPPORT.md](../SUPPORT.md) makes no
compatibility promise for the CLI, the Flowfile edition, the protobuf API,
stored run history, or the plugin SDK. What the repository does enforce today:

| Surface | Mechanism |
| --- | --- |
| The protobuf API (`proto/flowstate/v1`) | `buf breaking` checks wire and source compatibility against `main` on every change. A deliberate break has to be declared; it has happened. |
| Flowfile syntax | Only the current edition compiles. `flow fix` rewrites files from older editions; there is no deprecation window. |
| Expression meaning in a running workflow | Each run records its language profile, so a stored expression keeps the vocabulary it was compiled with. |
| The plugin protocol | A version negotiated at launch; a plugin and host that disagree refuse each other. |
| Go packages | `pkg/flowstate/embed` is the curated surface for embedding. `pkg/flowstate/v1` holds the generated types plus the interpreter, and its Go API changes freely. |

Build on the API expecting to track changes, and pin the version you build
against.

## Next steps

- [Embedding](EMBEDDING.md): compile and run workflows inside your own Go program.
- [Plugins](PLUGINS.md): add tasks and secret providers in another process.
- [Using Flowstate from an agent](MCP.md): the same RPCs as MCP tools.
- [Concepts](CONCEPTS.md): where the API sits among the other components.
