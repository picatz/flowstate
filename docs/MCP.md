# Using Flowstate from an agent

`flow mcp` is a [Model Context Protocol](https://modelcontextprotocol.io/)
server. An MCP client (Claude Code, Claude Desktop, the Codex CLI, or any other)
launches it as a subprocess and talks to it over stdin and stdout. It gives an
agent what a person gets from the CLI and the editor: the language reference,
the task catalog, validation with line and column, stubbed tests, a scripted
debugger, local rehearsal, and, when you point it at a server, durable runs.

```sh
claude mcp add flowstate -- flow mcp
```

That one line is enough for authoring: validate, compile, test, debug, and run
locally. Nothing is reachable over the network and no secret resolves until you
start the server with flags that allow it.

## What it serves

### Resources

Read-only documents an agent can load before it writes anything. All are
compiled into the binary, so they describe the engine the agent is about to
call rather than whatever happens to be checked out nearby.

| URI | What it is |
| --- | --- |
| `flowstate://docs/language` | [The Flowfile language](LANGUAGE.md): every construct, its defaults, and where each expression root is in scope. Start here. |
| `flowstate://catalog/tasks` | What this build can execute, as JSON: every task with its typed inputs and outputs, and every CEL function. Always this process's own registry. `flowstate_get_catalog` gives the same answer unless `--address` or `FLOWSTATE_ADDRESS` names a deployment, in which case the tool asks that deployment and the resource stays local. |
| `flowstate://docs/examples/<name>` | One example workflow by its directory name under [`examples/`](../examples/), such as `flowstate://docs/examples/release-approval`. Each is also listed by name. |
| `flowstate://docs/dsl` | [Language design decisions](DSL.md): why each construct is shaped the way it is. Long; read it for rationale, not to learn the syntax. |
| `ui://flowstate/approval-card` | An [MCP Apps](https://modelcontextprotocol.io/) view a capable host renders for `flowstate_get`: a run's open approval gates. It displays; it grants no authority. |

### Tools

One tool per RPC of the [control-plane API](API.md), with input schemas derived
from the same protobuf messages, plus the tools that run in the MCP process
itself. The generated [MCP tool reference](reference/mcp.md) lists every tool with its
request and response messages.

| Tools | Need a server? | What they are for |
| --- | --- | --- |
| `flowstate_validate`, `flowstate_compile` | No | Author: diagnostics with positions, and the compiled specification. |
| `flowstate_get_catalog` | Only when `--address` or `FLOWSTATE_ADDRESS` is set | The tasks and functions available: this process's own without an address, or the addressed deployment's, refusing if that deployment is unreachable. |
| `flowstate_test` | No | Run `*.test.yaml` cases against stubbed tasks on a virtual clock. The first thing to reach for after validating. |
| `flowstate_debug` | No | Run a test case under a script of debugger commands (`break`, `continue`, `inspect`, …) and return the session transcript. At most 100 commands per call. |
| `flowstate_check_policy` | No | Ask whether an identity would be admitted by a Flowfile's `signals:`, `debug:` or `triggers.manual` policy, executing no step. The same check as `flow signals check`; a refusal is the engine's fixed sentence and never quotes a claim or an input. Stdio only. |
| `flowstate_debug_session_start`, `_attach`, `_observe`, `_command`, `_end` | Only `_attach` | Keep one debug session open across calls: over a test case, or attached to a durable run. Stdio only, and one test-case session at a time: while it is open, `flowstate_test` and `flowstate_debug` are refused. See [Debugging](DEBUGGING.md#a-session-that-outlives-the-call). |
| `flowstate_run_local` | No | Rehearse a workflow for real in this process, with inputs and signals, and return the run plus what its `log:` steps wrote. |
| `flowstate_run`, `flowstate_get`, `flowstate_get_timeline`, `flowstate_list`, `flowstate_signal`, `flowstate_signal_with_start`, `flowstate_cancel`, `flowstate_terminate` | Yes | Start and operate durable runs. |
| `flowstate_debug_attach`, `flowstate_debug_get`, `flowstate_debug_resume`, `flowstate_debug_set_breakpoints`, `flowstate_debug_inspect` | Yes | The durable debugger's RPCs, one call each; the run's `debug:` policy must name you. |
| `flowstate_create_schedule` and the other schedule tools | Yes | Manage a workflow's schedule. |

A tool that needs a server and has no `--address` says so instead of failing
obscurely. RPC-derived tools answer with the same JSON document the CLI prints
for `-o json`, plus the protojson in `structuredContent`, so a schema-aware
client can pass a `flowstate_compile` result straight to `flowstate_run`.

There are no MCP prompts.

## The authoring loop

The surface is shaped around this sequence:

1. **Read** `flowstate://docs/language` and `flowstate://catalog/tasks`. A task
   named correctly in prose but missing from the catalog is a mistake you can
   avoid before writing a line. `flowstate://docs/examples/<name>` has complete,
   tested files to adapt.
2. **Write** the Flowfile and a `*.test.yaml` beside it.
3. **Validate** with `flowstate_validate`. It is pure and safe to call in a
   loop. Its diagnostics carry a line, a column, a stable `code` from the
   [diagnostics reference](reference/diagnostics.md), and often a suggested edit.
4. **Test** with `flowstate_test`. Conditions, retries, compensation, and data
   flow are proved here against stubs, with nothing reached.
5. **Debug** a surprising case with `flowstate_debug`: set a breakpoint, inspect
   the values the step saw, and turn the expression you settle on into an
   `expect.check:`.
6. **Rehearse** with `flowstate_run_local` when a real task must run.
7. **Run durably**: `flowstate_compile`, then `flowstate_run` against a server,
   then `flowstate_get` or `flowstate_get_timeline` to follow it.

[`examples/agentic-loop`](../examples/agentic-loop/README.md) walks this loop
with real transcripts.

A local rehearsal is not a durable run. It has no run id, nothing can watch it,
it does not survive the process, and it never exercises Continue-As-New. The
two drivers agree on behavior; durability is what the server adds.

## Configuring it

Every flag is fixed when the process starts. A client talks to `flow mcp` over
stdio and cannot change any of it, so nothing an agent sends can widen what the
process may do.

| Flags | Effect |
| --- | --- |
| `--address` (or `FLOWSTATE_ADDRESS`), `--token-file`, `--credential-source`, `--audience`, TLS client flags | Which server the durable tools and `flowstate_get_catalog` call, and as whom. `flowstate_validate`, `flowstate_compile`, `flowstate_test`, `flowstate_debug`, `flowstate_check_policy`, and `flowstate_run_local` never dial. |
| `--egress-policy` | What `http:` steps in `flowstate_run_local` may reach. **Without it, egress is denied entirely**, which is stricter than `flow run local`: the caller here is a model, not the file's author. |
| `--secret-env`, `--secret-dir`, and the other [secret flags](SECRETS.md) | Which secret references `flowstate_run_local` may resolve. None, unless a flag says so. |
| `--as-subject`, `--as-issuer`, `--as-namespace`, `--as-deployment`, `--as-claim` | The identity a local run rehearses policy as. |
| `--task-policy` | Which tasks a dispatch may run, for `flowstate_run_local` and for the test cases `flowstate_test` and `flowstate_debug` run alike. A stubbed dispatch in a test is checked too, against the empty identity a test case runs as. Unset, every task is allowed. |
| `--run-local-timeout` (default `2m`) | Bounds one `flowstate_run_local` call, since `sleep: 24h` is a legal Flowfile and a tool call holds a model's turn open. |
| `--plugin-dir` | Load plugin tasks, so validation and rehearsal know them. |

A refused request means this process was not configured for it, not that the
workflow is wrong, and the refusal says which.

### Claude Code

```sh
claude mcp add flowstate -- flow mcp

# With a server for durable runs, and egress for local rehearsal:
claude mcp add flowstate -- flow mcp \
  --address https://flowstate.internal:9233 \
  --egress-policy /etc/flowstate/egress.yaml
```

To share one setup with a team, check a `.mcp.json` into the repository root:

```json
{
  "mcpServers": {
    "flowstate": {
      "command": "flow",
      "args": ["mcp", "--egress-policy", "/etc/flowstate/egress.yaml"],
      "env": {
        "FLOWSTATE_ADDRESS": "https://flowstate.internal:9233"
      }
    }
  }
}
```

### Claude Desktop

In `claude_desktop_config.json` (`~/Library/Application Support/Claude/` on
macOS, `%APPDATA%\Claude\` on Windows), the same `mcpServers` entry as the
`.mcp.json` above.

### Codex CLI

In `~/.codex/config.toml`:

```toml
[mcp_servers.flowstate]
command = "flow"
args = ["mcp", "--egress-policy", "/etc/flowstate/egress.yaml"]

[mcp_servers.flowstate.env]
FLOWSTATE_ADDRESS = "https://flowstate.internal:9233"
```

### Any other client

A stdio client needs a command, its arguments, and an environment:

```json
{
  "command": "flow",
  "args": ["mcp"],
  "env": {}
}
```

Two things that save a support round trip:

- Give the absolute path to `flow` if the client does not inherit your shell's
  `PATH`; `go install` puts it in `$(go env GOPATH)/bin`.
- Logs go to stderr. Stdout is the protocol, so anything else written there
  breaks the session. A malformed line from the client is answered with a
  JSON-RPC error for that line, and the session continues; it ends when stdin
  closes.

## Over HTTP: `flow mcp serve`

`flow mcp serve` offers a reduced surface over streamable HTTP for clients that
cannot launch a subprocess. Each request carries an OAuth bearer token, verified
against the deployment's trusted issuers, and the server publishes the
[RFC 9728](https://www.rfc-editor.org/rfc/rfc9728) metadata a client uses to
find where to get one.

It serves `flowstate_validate`, `flowstate_compile`, `flowstate_get_catalog`,
`flowstate_test`, and `flowstate_debug`, and the documentation resources.
`flowstate_run_local` is absent, because executing submitted workflows for
remote callers is remote code execution. The durable tools are absent too: they
would spend this process's own credential on a caller's behalf. An agent that
needs durable runs over HTTP talks to the [API](API.md) directly with its own
credential.

[MCP over HTTP](MCP_AUTHORIZATION.md) covers the authorization exchange,
configuration, and limits.

## Identity and approvals

Over stdio, every durable call goes out as the identity `flow mcp` was
configured with: the token file or credential source you gave it. An agent that
sends `flowstate_signal` to approve a gate is approving as that identity, and a
workflow's `signals:` policy judges it as that identity. If a gate should
require a human, give it a `signals:` rule the agent's credential does not
satisfy, such as a claim only people carry, or a
`sender.identity.principal != run.identity.principal` clause when the agent starts
the run. See
[the language guide](LANGUAGE.md#who-may-send-a-signal-signals).

Everything an agent reads from a run (outputs, payloads, prompts) is data a
workflow or a signal sender chose. The [threat model](../THREAT_MODEL.md#5-prompt-injection-through-agent-surfaces)
covers prompt injection through these surfaces.

## Next steps

- [MCP tool reference](reference/mcp.md): every tool, generated from the service.
- [examples/agentic-loop](../examples/agentic-loop/README.md): the loop with
  real transcripts, and a workflow that drives an agent step behind a human gate.
- [Editor setup](EDITORS.md): the same validation through `flow lsp`, for people.
