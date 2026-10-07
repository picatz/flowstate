# debugging

One small workflow stepped through by every front the debugger has: a prompt
over a test case or a local rehearsal, an editor, a durable run on a server, and
an agent over MCP. [Debugging](../../docs/DEBUGGING.md) is the reference; this
is the walkthrough.

The workflow charges a batch of orders in a `for_each`, flags the ones over a
threshold, runs two checks in a `parallel:` block, and calls
[`workflows/receipt.yaml`](workflows/receipt.yaml). Its `debug:` block names who
may debug a durable run of it: callers carrying `team: sre`. Each place it can
stop has an address:

| Address | Where |
| --- | --- |
| `settle`, `orders`, `flagged`, `checks`, `receipt`, `done` | the top-level steps |
| `orders[1]/charge` | `charge` in iteration 1 of `orders` (the 900 order) |
| `checks#0/stock`, `checks#1/fraud` | the two branches of `checks` |
| `receipt(receipt)/compose`, `receipt(receipt)/send` | the callee's steps, reached through the call `receipt` |

`settle` sleeps for the `settle` input, zero unless given, so a durable
debugger has time to attach before the run reaches `orders`.

```console
$ flow validate examples/debugging/workflow.yaml
$ flow test examples/debugging/
```

## At a prompt

`flow test --debug` steps through the one case in
[`workflow.test.yaml`](workflow.test.yaml), on stubs and a virtual clock. Type
the commands, or pipe them in:

```console
$ flow test --debug examples/debugging/workflow.test.yaml
debugging "one order over the threshold is flagged" — `help` lists the commands
break at settle (wait)
debug> break orders/charge if amount > inputs.threshold
breakpoint at orders/charge if amount > inputs.threshold
debug> continue
  settle -> timed_out: false
  charge completed
break at orders[1]/charge (task "log")
debug> inspect amount
900
debug> finish
  charge completed
  charge completed
  orders -> results: [{"charge":{}},{"charge":{}},{"charge":{}}]
break at flagged (value)
debug> break checks#1/fraud
breakpoint at checks#1/fraud
debug> continue
  flagged -> value: [900]
  stock completed
break at checks#1/fraud (task "log")
debug> until receipt/send
  fraud completed
  checks completed
  compose -> value: "charged 3 order(s)"
break at receipt(receipt)/send (task "log")
debug> backtrace
  #1 receipt.send (task "log")
  #2 debugging.receipt (call "receipt")
debug> continue
  send completed
  receipt -> summary: "charged 3 order(s)"
  done completed
PASS  examples/debugging/workflow.test.yaml: one order over the threshold is flagged
```

The condition is compiled when you type it and evaluated in the step's own
scope, which is why `amount` — the loop's binding — can be named before the loop
has started. `finish` leaves the loop; `next` would have stopped at the next
iteration's `charge`.

`flow run local --debug examples/debugging/workflow.yaml` is the same prompt over
a real local rehearsal. [`debug.script`](debug.script) is a recorded session,
with a logpoint, that replays the same way every time:

```console
$ flow debug replay examples/debugging/debug.script examples/debugging/workflow.yaml
```

## In an editor

`flow dap` is the same session behind the Debug Adapter Protocol. In VS Code,
with a `flowstate` debug type registered (see
[Editor setup](../../docs/EDITORS.md#visual-studio-code); `editors/vscode/`
does not contribute one yet):

```json
{
  "type": "flowstate",
  "request": "launch",
  "name": "Debug examples/debugging",
  "program": "${workspaceFolder}/examples/debugging/workflow.yaml"
}
```

A gutter breakpoint on the `message:` line under `charge` resolves to
`orders/charge`; give it the condition `amount > inputs.threshold`. *Step Into*
at `receipt` enters the callee, and *Step Over* runs it whole. The `attach`
configuration for the durable run below is in the same section of
[Editor setup](../../docs/EDITORS.md#visual-studio-code).

## A durable run

This half needs two terminals, `jq`, and the network once, to download the
Temporal CLI `flow server dev` starts. In the first, start the stack with a
local issuer, persisting the `team` claim the `debug:` block reads:

```console
$ flow server dev --auth --identity-claim team --db /tmp/flowstate-debugging.db -o json > /tmp/flowstate-debugging-stack.json
```

In the second, mint an hour-long credential that carries `team: sre`:

```sh
STACK=/tmp/flowstate-debugging-stack.json
ADDRESS=$(jq -r .flowstateAddress "$STACK")
KEY=$(jq -r .authKeyFile "$STACK")
TOKEN_DIR=$(dirname "$KEY")
flow jwt sign --key "$KEY" --id flowstate-dev --issuer "$(jq -r .authIssuer "$STACK")" \
  --subject sre@example.com --audience "$(jq -r .authResource "$STACK")" \
  --claim namespace="$(jq -r .authNamespace "$STACK")" --claim team=sre \
  --ttl 1h > "$TOKEN_DIR/sre.jwt"
chmod 600 "$TOKEN_DIR/sre.jwt"
AS="--address $ADDRESS --token-file $TOKEN_DIR/sre.jwt"
```

The dev server's policy lists every action, so its token may use all of them, including
`workload.debug` and `workload.debug_inspect`. Start a run that sleeps for half
a minute, and attach:

```console
$ WORKFLOW_ID=$(flow run --detach examples/debugging/workflow.yaml --input settle=30s $AS -o json | jq -r .workflowId)
$ flow debug attach "$WORKFLOW_ID" $AS
attached to flowstate-request-… — session 5ae9… (pending: delivered; the run applies commands at its next step boundary, and has not reached one yet. …)
held at orders (for_each) — pause, revision 2
  #1 debugging.orders (for_each)
  session 5ae9…, lease until …
debug> break receipt if size(steps.flagged.value) > 0
breakpoint at receipt if size(steps.flagged.value) > 0
debug> continue
held at receipt (call "receipt") — breakpoint receipt, revision 4
  #1 debugging.receipt (call "receipt")
debug> inspect steps.flagged.value
[900]
debug> step
held at receipt(receipt)/compose (value) — step, revision 6
  #1 receipt.compose (value)
  #2 debugging.receipt (call "receipt")
debug> finish
held at done (task "log") — step, revision 8
  #1 debugging.done (task "log")
debug> detach
applied
```

(The lease line after each later stop is left out here.) The attach answers pending while `settle` sleeps: a hold takes effect at the
next step boundary, and never interrupts work in flight. The run held at
`orders`, but a durable run holds only where it has one position — top-level
steps and a callee's top-level steps — so `continue` ran the loop and the
parallel block whole, and `step` from `receipt` entered the callee. A breakpoint
on `orders/charge` is reported not armed here, saying to break at the enclosing
step instead, and `until orders/charge` is refused with the run still held. (In
a program past `MaxDebugStaticSites` step sites the durable driver cannot list
every site, so it asks the program as written instead, and refuses the same
targets: a step declared only inside a body, or declared nowhere.)

`disconnect`, instead of `detach`, leaves the session attached for a later
command. Each of these is one call, for a script or an agent:

```console
$ flow debug get "$WORKFLOW_ID" $AS
$ flow debug do "$WORKFLOW_ID" --session <session-id> next $AS
$ flow debug do "$WORKFLOW_ID" --session <session-id> inspect 'steps.orders.results.size()' -o json $AS
$ flow debug do "$WORKFLOW_ID" --session <session-id> detach $AS
$ flow timeline "$WORKFLOW_ID" $AS
```

The timeline shows every command as a `flowstate_debug` signal and every lease
with its holder. A credential without `team: sre` is refused at the attach, by
the run's own policy:

```text
permission_denied: signal "flowstate_debug": the sender does not match any rule
this signal's policy declares
```

A session nobody renews lapses after its lease (two minutes by default), and the
run resumes on its own. Stop the first terminal with Ctrl-C, then remove
`/tmp/flowstate-debugging.db`, `/tmp/flowstate-debugging.db.flowstate-auth`, and
the stack JSON.

## From an agent

`flow mcp`, started with the same server flags, keeps a durable session open
across tool calls:

```console
$ flow mcp --address "$ADDRESS" --token-file "$TOKEN_DIR/sre.jwt"
```

```json
{"name": "flowstate_debug_session_attach", "arguments": {"workflow_id": "<workflow-id>"}}
{"name": "flowstate_debug_session_command", "arguments": {"session_id": "<session-id>", "command": "break receipt"}}
{"name": "flowstate_debug_session_command", "arguments": {"session_id": "<session-id>", "command": "continue"}}
{"name": "flowstate_debug_session_command", "arguments": {"session_id": "<session-id>", "command": "inspect steps.flagged.value"}}
{"name": "flowstate_debug_session_observe", "arguments": {"session_id": "<session-id>"}}
{"name": "flowstate_debug_session_end", "arguments": {"session_id": "<session-id>"}}
```

Each answer carries the typed snapshot — state, stop reason, the occurrence
address, frames, capabilities, revision — beside the text a person would read.
Ending detaches the run, which finishes on its own.

`flowstate_debug_session_start` and the one-shot `flowstate_debug` debug a test
case instead, but they take the workflow as text, so this example's
`call: ./workflows/receipt.yaml` has no directory to resolve against; point them
at a workflow without a relative `call:`.
