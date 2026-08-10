# The Flowstate command line

This is the contract the `flow` binary holds itself to, and the reasoning behind
each rule. It exists because a command line is a product surface with two distinct
audiences — a person reading a terminal and a program reading a pipe — and almost
every defect in this area comes from serving one of them in a way that breaks the
other.

[docs/ARCHITECTURE.md](ARCHITECTURE.md) describes what the system is.
[CLAUDE.md](../CLAUDE.md) describes how to change it. This describes what a person
meets.

It is the reasoning and not the enumeration. Every command and flag `flow` has —
with defaults, and which environment variable feeds each one — is
[reference/cli.md](reference/cli.md), generated from the command tree the binary
builds at startup and pinned in CI, so it is complete in a way a page somebody
maintains by hand is not. Its siblings answer the other three "what is there"
questions: [reference/tasks.md](reference/tasks.md),
[reference/mcp.md](reference/mcp.md), [reference/envvars.md](reference/envvars.md).

## Worker-side secrets

A Flowfile carries only a reference such as `${secret('env:API_TOKEN')}`. The
value is resolved inside the task activity, after the authenticated workload and
step are known, and is never placed in the workflow payload, outputs, or history.

Secret access requires two independent pieces of worker configuration:

```sh
flow worker \
  --deployment-name flowstate --build-id "$(git rev-parse --short HEAD)" \
  --secret-env API_TOKEN \
  --secret-dir /var/run/secrets/flowstate \
  --auth-policy /etc/flowstate/auth.yaml
```

`--secret-env` is an allowlist and may be repeated (or supplied as a
comma-separated list). It exposes only variables named
`FLOWSTATE_SECRET_<NAME>`. `--secret-dir` enables `file:` references rooted below
the given directory. Providers without an auth policy, a policy without a
`secrets:` section, an unknown scheme, and a rule that does not allow the exact
workload/step/reference all fail closed before a value is read.

`flow run local` accepts the same flags. That is intentional: a rehearsal under a
different secret policy is not a rehearsal of production. See
[`examples/http-secret`](../examples/http-secret/) for the Flowfile and policy
together.

## Just-in-time workload credentials

Prefer federation to a static secret when the downstream system supports token
exchange or workload identity. Configure `federation.targets` in the same reviewed
auth policy, give the worker a rotating PKCS#8 signing key, and name the target in
the HTTP task:

```yaml
http:
  url: https://api.partner.example.com/orders
  credential: partner-api
```

```sh
flow worker \
  --deployment-name flowstate --build-id "$(git rev-parse --short HEAD)" \
  --auth-policy /etc/flowstate/auth.yaml \
  --identity-key /var/run/flowstate/2026-08.pem
```

The activity evaluates the target's CEL assume policy against the authenticated
tenant, workflow, run, and step; mints an audience-scoped Flowstate assertion;
exchanges it; and applies the short-lived bearer token directly to the request.
The assertion and exchanged credential are never task outputs. An unknown target,
denied rule, missing key, non-expiring credential, or federation configuration
error fails closed. AWS session targets require SigV4 and are intentionally left
to AWS-aware tasks rather than being misrepresented as bearer credentials. See
[`examples/http-federated`](../examples/http-federated/).

For multi-tenant static providers, combine `--secret-require-namespace` with
`--secret-env-namespace team-a=TEAM_A_SECRET_` or
`--secret-dir-namespaced`. A tenant with no configured environment prefix is
refused; prefixes are checked for overlap; file tenants receive separate rooted
directories.

## The two audiences

**stdout carries the answer. stderr carries the account of it.**

`flow get x | jq` must receive a workload's outputs and nothing else. `flow list |
awk '{print $1}'` must receive rows. Everything else a run produces — what it is
doing, that it succeeded, that more remains, that a page failed — belongs on
stderr, where a person watching a terminal still sees it and a pipe does not.

The rule generalizes past the obvious cases, and the awkward ones are where it
earns its keep:

- A **diagnostic is an answer**, not commentary. `flow validate` exists to report
  problems, so its diagnostics are its output. An editor and a `make` wrapper both
  consume them, which is why they keep the `path:line:col: message` shape that
  every tool already parses.
- A **confirmation is an account**, not an answer. "asked X to stop" tells a person
  something happened; no pipeline wants it, so it does not go where a pipeline
  reads.
- **The same fact must not be written twice.** A result printed both to a log line
  and to stdout is a result a pipe reads once and a person reads twice, and the two
  copies drift.

When a command has no answer, stdout stays empty. An empty stdout is a meaningful
value: it is what "no runs" looks like to a program. A table header written when
there are no rows is worse than nothing, because it is indistinguishable from a
listing that succeeded and found none.

## Colour is a capability, never a preference

Styling is decided once, per stream, from what that stream can actually do:
whether it is a terminal, what colour depth it supports, and what the environment
has asked for. Nothing else in the CLI decides for itself.

The consequences are non-negotiable, because each of them is a way people get
burned:

- **A pipe gets no escape sequences.** Not fewer, none. Detection is per stream, so
  `flow get x | jq` may be plain while the status line on stderr is styled, in the
  same invocation.
- **`NO_COLOR` is honoured**, and so is `CLICOLOR_FORCE` for the person who wants
  colour through a pager. A CI job with no TTY gets plain text without being asked.
- **`TERM=dumb` means dumb.** No cursor movement, no repainting, no spinner.
- **Every style must survive removal.** Meaning is carried by the words and the
  layout; colour and weight only make the meaning faster to find. A line that reads
  correctly in black and white is the only kind allowed, because that is the line a
  log file, a screen reader, and a colour-blind reader receive.

## The palette works in both directions

A terminal's background is the user's choice, and a palette that assumes one is a
palette that is unreadable for half its audience. Colours are therefore declared as
a pair — one value for a light background, one for dark — and resolved once at
startup.

Two rules keep the pair honest. Both members must clear a contrast floor against
their own background, which rules out the mid-tone greys that look fine to whoever
picked them and vanish for everyone else. And a colour never carries meaning
alone: status is a word first and a colour second, so the palette is an
accelerator rather than the channel.

Where the depth is lower than the palette assumes, colour degrades to the nearest
of what is available and then to weight — bold and dim — and then to nothing. Each
step down loses emphasis and no information.

Which half of each pair to use is a question asked *of the terminal*, in the one
place the CLI does that: an OSC 11 query written out, and a reply read back. Two
things follow, and both are load-bearing.

It is asked at most once per process, and only when the answer can change a byte.
Below ANSI both halves resolve to the same styles, so a `NO_COLOR` reader and a
`TERM=dumb` terminal are never asked — which matters because those are among the
terminals least likely to reply.

And a terminal that replies to *nothing* is waited on for two seconds per file,
which is four seconds of a command printing nothing before it behaves normally.
That reads as a hung network or a wedged server, the two places somebody would look
and neither of them it.

That is narrower than it sounds, and worth stating precisely because the obvious
guess is wrong. The query asks two things at once — the background colour, and the
primary device attributes every terminal answers — so a terminal that simply does
not implement background reporting still ends the wait immediately. Measured
against a pty answering only the second: 0.02s. The four seconds belong to a pty
answering neither, which is automation holding a tty rather than a terminal
somebody is sitting at.

`FLOWSTATE_BACKGROUND=dark` or `=light` settles it without asking, for exactly that
case: 4.02s to 0.02s. Anything else in that variable — including empty — is ignored
rather than guessed at, since a variable somebody exported and left blank is not an
assertion about their terminal.

## Symbols, not emoji

The CLI uses no emoji. They render at inconsistent widths, break column alignment,
are read aloud unpredictably, and carry tone into places that should be reporting
facts.

What it does use is a small set of restrained typographic marks, and each one has a
plain ASCII fallback selected by the same capability detection that decides colour.
A symbol is decoration for a label, never a replacement: a status is `RUNNING`, and
the mark beside it helps the eye find the row.

`FLOWSTATE_SYMBOLS=unicode` or `=ascii` overrides that detection, on the same
principle as `FLOWSTATE_BACKGROUND`: the derivation can be wrong about a terminal,
and the person sitting at it is the only one who can see that.

## One vocabulary, everywhere

The words a person meets in `flow --help` are the words they write in a Flowfile,
the words the RPC uses, and the words the documentation uses. A concept with two
names is a concept the reader has to translate, and every translation is a place to
be wrong.

Two distinctions matter enough to state:

- A **workload** is the thing someone defines; a **run** is one execution of it. A
  workload is addressed by its workflow id; one attempt at it has a run id. They are
  different identifiers with different lifetimes, and `--run-id` exists precisely
  because approving a deploy means approving the workload, not one attempt.
- A **namespace** is a Flowstate tenant. Where a deployment also maps onto Temporal
  namespaces, the text says *Temporal namespace* in full, every time, because the
  two are different boundaries and the reader cannot tell from context.

Where a flag name must be spelled the same on two commands but means two different
things, that is a defect, not a convention.

## Errors say what to do next

A refusal names what was refused, why, and what would work instead. Three habits
carry most of that:

- **Name the thing and the verb.** "refused while listing runs" beats "permission
  denied", because the reader knows which of the six things they just ran was
  refused.
- **Do not narrow a cause you do not know.** A run that cannot be addressed may not
  exist, may belong to another tenant, or may have aged out of retention. Saying
  "check the id" when it might be any of the three sends people hunting for the
  wrong mistake.
- **Advice a reader can paste beats advice they must interpret.** Where a fix is a
  command, the message contains that command.

The exit status is part of the message. A run that finished as a failure exits
non-zero, so `flow get x && deploy` behaves the way the shell reader expects — the
query succeeded, and what it reports is a failure.

## A command is the act; a file is only a declaration

`flow schedule create` exists as a separate verb rather than as something `flow run`
notices, and the reason generalises beyond schedules.

A Flowfile may declare that a workload is meant to run every weekday at 07:00. That
declaration is reviewed with the steps it belongs to, which is the whole argument for
writing it in the file. It is also, on its own, inert: `flow run` does not create a
schedule and `flow run local` ignores the block, so nothing in this tool turns merging
a file into work being done. Somebody types the verb.

The cost of the alternative is not that a schedule appears — it is that its *first
firing looks like somebody meant it*. An unexpected run at 07:00 on a Tuesday is
indistinguishable from an intended one until somebody goes looking for who asked, which
is the kind of ambiguity an operator pays for at the worst moment. A verb somebody typed
leaves an answer to that question.

Two habits follow for anything else that reaches this shape:

- **Refuse everything refusable at the moment a person is present.** `flow schedule
  create` checks the specification, the cadence and the arguments there, rather than
  letting a firing discover them. A refusal at 03:00 in a worker's log, about a mistake
  made at a keyboard a week earlier, is a refusal nobody reads.
- **Answer with what makes the mistake visible.** `create` prints the next firing times
  without being asked, because a cadence that means something other than what was
  intended is almost always obvious there and almost never obvious in the expression
  that produced it. `--paused` exists so that answer arrives before anything fires.

## Interactive surfaces are optional, never required

Anything the CLI can do interactively it can also do non-interactively, because the
same task is done from a laptop and from a CI job. A terminal UI is an alternative
presentation of a capability that already exists as plain output and flags — never
the only way to reach one.

Whether a terminal may be *detected* depends on what the surface changes:

- A surface that changes **what a command does** — a picker that chooses the
  argument, a form that supplies input, a prompt that decides — is entered
  deliberately, by a flag or a subcommand. Detection there means the same invocation
  does two different things depending on where it ran, which is the defect the
  `--output` flag exists to avoid.
- A surface that changes only **how the same information is presented** may follow
  the terminal, on three conditions. The non-terminal shape has to be the same
  command carrying the same information; a flag has to be able to ask for the plain
  shape *on* a terminal, because a person reading with a screen reader or capturing
  under `script(1)` must not be trapped by having a TTY; and an explicitly requested
  `--output` format must win, since a document was asked for and a terminal was not.

`flow watch` is the second kind, and the way it splits the streams is what makes the
two shapes compose rather than compete: the live view is drawn on stderr and the
outputs go to stdout, so one invocation shows progress on the terminal and pipes its
answer to `jq`.

Animation follows from the same reasoning. It exists to say "still working" during a
wait whose length is not known, it stops the moment there is something to report,
and it never appears where output is not a terminal. Nothing that is only decorative
is worth a repaint — which is why a live view moves a number that answers a question
(how long has this been going) rather than a spinner that answers none.

Two rules about what such a view may claim, both of them the no-silent-caps rule
from `CLAUDE.md` applied to a screen. A list cut to fit says how many it cut and how
many there are, because a window that looks like a whole list is one a reader counts
wrongly. And a view that stopped being able to reach the server says so, rather than
becoming a still screen that cannot be told apart from a wedged one.

## The machine surface

Everything above serves a person; this names the contract for a program, and it
is one rule with consequences — the fuller reasoning lives in
[DSL.md](DSL.md#the-fourth-round-the-tool-is-a-product-surface):

- **Every command is a projection of an RPC.** `flow` is a thin client of the
  same Connect services the API serves; a capability that cannot be expressed as
  an RPC is a missing RPC, not a CLI feature.
- **`--output json` is the protojson of the RPC response.** There is no second
  encoder, so the JSON surface cannot drift from the API and needs no separate
  schema documentation.
- **The one exception is a mutation whose response is empty, and it is meant to
  stop being one.** `cancel`, `terminate`, `signal` and the four schedule
  mutations answer with empty messages (`CancelResponse{}` and its siblings), so
  there is no response protojson to render and a script had to re-`get` the run
  to learn what it had just done. They therefore write one shared envelope,
  `flowstate.v1.MutationResult`: `verb`, `workflowId`, `runId`, `scheduleName`,
  `signalName`, `result`. The envelope is a schema message like everything else
  here, rendered by the same protojson encoder, because a document scripts index
  by name is a contract and this project describes its contracts in the schema.
  Only its *values* come from the calling process rather than from the server.
  `result` is `applied` for an act that is true once the server answers,
  `requested` for one it has accepted and not yet performed, and `delivered` for
  a signal the server has taken, which is a claim about the server and not about
  the workflow: a signal held for a gate the run has not reached is dropped if
  the run continues as new with the pending set full, so a workflow that never
  observes it is a possible ending of a delivery that succeeded. The envelope
  carries only what this process knows for certain, because inventing a
  resulting state out of an empty response is exactly the claim the prose has
  always refused to make. That emptiness is the real defect
  (picatz/flowstate#374): when those responses gain fields, the envelope stops
  being the whole answer and the response's own protojson carries the rest.
- **Exit status is a contract with three values.** `0`: the command succeeded
  and the answer is not a refusal. `1`: the command worked and the answer is a
  refusal or a finding — diagnostics found, a check failed, a run that finished
  as a failure. `2`: the invocation itself was wrong — an unknown flag, an
  unknown command, the wrong number of arguments. A program branches on these;
  prose never replaces them. The classification is the same one `isUsageError`
  in `cmd/flow/execute.go` already draws to decide whether the report ends with
  "Try `flow --help`", read a second time rather than computed twice — a golden
  test on each branch (`cmd/flow/execute_test.go`) pins that a usage error exits
  2, a refusal or a finding exits 1, and a clean run exits 0.
- **Pure verbs stay pure.** `validate`, every `--check`, and every read are
  side-effect-free so a program — or an agent — can loop on them unattended.
  Mutations sit behind explicit confirmation in non-interactive streams.

## `flow mcp`: the same surface, for an agent

An agent is the second machine audience, and it needs the same thing a pipe does
— the RPCs, unembellished — plus one thing a pipe never asks for: somewhere to
read before it writes. `flow mcp` serves both over stdin and stdout as a Model
Context Protocol server, which an MCP client launches as a subprocess rather than
something you run yourself.

Nothing here is a second product. The tools are the Connect services projected,
with input schemas derived from the same protobuf messages the API speaks, so a
field added to a request reaches an agent the day the code is regenerated. There
is no hand-maintained tool list to fall behind the engine, and a test holds the
list to the service descriptor in both directions.

### What it serves

**One tool per RPC, plus two that are deliberately not RPCs.** The roster is
[docs/reference/mcp.md](reference/mcp.md), generated from the service descriptor
and pinned in CI — this page describes the shape and does not repeat the list,
because an earlier revision counted "ten tools" two sentences after explaining
why hand-maintained lists fall behind, and the count was wrong within weeks.

The per-RPC tools split by what the method needs. Validate, compile and the
catalog touch no run and no tenant, so they answer in this process: an agent
gets a working authoring loop with no server and no Temporal stood up. The
lifecycle and schedule verbs address durable state, which only a server has,
and without `--address` they say so rather than failing opaquely.

`flowstate_run_local` is not an RPC, and deliberately: it is the local driver
executing a submitted Flowfile in this process — the same rehearsal `flow run
local` performs — and giving it a service method would make a server executing
submitted workflows in-process, which is a different product. It answers with
the same `GetResponse` document `flowstate_get` returns, plus whatever `log:`
steps emitted, because stdout is the transport here and a workflow that
narrates itself must not write into the protocol.

`flowstate_test` is the other non-RPC, and the one to reach for first while
authoring: the `flow test` machinery over bytes submitted inline, every task
stubbed, time virtual, nothing real invoked — so it needs no egress policy and
no opt-in, and proves conditions, retries, compensation and data flow before
`flowstate_run_local` rehearses the one task deliberately left unstubbed.

**Resources**, read-only and listed before anything is called:

| URI | What it is |
| --- | --- |
| `flowstate://docs/dsl` | [docs/DSL.md](DSL.md) whole: the grammar, every step kind, expression scoping, retries, waits, secrets. |
| `flowstate://catalog/tasks` | The task catalog as JSON — the same `GetCatalogResponse` bytes `flowstate_get_catalog` answers with, from the same encoder. |
| `flowstate://docs/examples/<name>` | One example workflow, by its directory name under [`examples/`](../examples/) — `flowstate://docs/examples/hello-world`. A URI template, with each name also listed as a resource so nothing has to be guessed. |

They are resources rather than tools because a verb is the wrong shape for "what
is the language": an agent that must spend a call, a round trip, and a slice of
its context window to learn the vocabulary will guess instead, and a guessed
Flowfile costs that budget three times over in diagnostics.

All three are compiled into the binary. That is a deliberate trade, stated
plainly: what is served is frozen at build time, so a binary from March answers
with March's reference — and that is the better failure, because `flow` is
installed with `go install` and run from a container or a CI job with no checkout
anywhere near it. An answer read off whatever happens to be on disk describes
some other engine; a compiled-in one describes the engine the agent is about to
call. The copies are held to the originals by a test, the way generated protobuf
code is held to its schema.

### The flag surface

Every flag is taken at start-up, and that is the security posture rather than an
implementation detail: a client speaks to this over stdio and never gets to
choose any of it. An opt-in a caller can send is not an opt-in.

- `--address` (or `FLOWSTATE_ADDRESS`), `--token-file` — which server the
  lifecycle tools talk to, and how they authenticate. The local tools never dial.
- `--egress-policy` — what `http:` steps in a `flowstate_run_local` run may
  reach. **Without it, egress is denied entirely**, which is stricter than `flow
  run local`'s default and deliberately so: that command is run by the person who
  wrote the file, and this one serves a model the ability to compose a workflow
  and have this process fetch a URL of its choosing. An empty allowlist is the
  honest starting point for a surface whose caller is not a person.
- `--secret-env`, `--secret-dir`, `--secret-dir-namespaced`,
  `--secret-env-namespace`, `--secret-require-namespace`, `--auth-policy`,
  `--identity-key` — the same secret and federation opt-ins `flow worker` and
  `flow run local` take, with the same fail-closed rules. No scheme is registered
  unless a flag says so.
- `--as-subject`, `--as-issuer`, `--as-namespace`, `--as-deployment`,
  `--as-claim` — the identity a local run rehearses policy as.
- `--run-local-timeout` (default `2m`) — a bound `flow run local` does not need
  and this does. There, a workflow waiting on a gate nobody will answer is a
  terminal a person can interrupt; here it is a tool call holding a model's turn
  open for as long as the workflow asks, and the workflow is untrusted input.
  `sleep: 24h` is a legal Flowfile.

Nothing in a tool's arguments can widen any of it. A denied request means this
process was not configured for it, not that the workflow is wrong — which is what
the refusal says, so an agent corrects the right thing.

### Configuring a client

This section is for wiring an MCP client — an editor, a desktop app, a CLI
agent — into `flow mcp`. It is a different audience from
[AGENTS.md](../AGENTS.md) at the repository root, which is for an agent
*developing* Flowstate itself; this one is for anyone *using* the binary
Flowstate ships, from any repository.

**Claude Code**, which takes the command and its flags directly:

```sh
claude mcp add flowstate -- flow mcp

# With a server for the durable verbs, and egress for local runs:
claude mcp add flowstate -- flow mcp \
  --address flowstate.internal:9233 \
  --egress-policy /etc/flowstate/egress.yaml
```

That writes to Claude Code's own config; the equivalent, checked into a
project so a team shares one setup, is a `.mcp.json` at the repository root:

```json
{
  "mcpServers": {
    "flowstate": {
      "command": "flow",
      "args": ["mcp", "--egress-policy", "/etc/flowstate/egress.yaml"],
      "env": {
        "FLOWSTATE_ADDRESS": "flowstate.internal:9233"
      }
    }
  }
}
```

**Claude Desktop**, in `claude_desktop_config.json` — `~/Library/Application
Support/Claude/` on macOS, `%APPDATA%\Claude\` on Windows:

```json
{
  "mcpServers": {
    "flowstate": {
      "command": "flow",
      "args": ["mcp", "--egress-policy", "/etc/flowstate/egress.yaml"],
      "env": {
        "FLOWSTATE_ADDRESS": "flowstate.internal:9233"
      }
    }
  }
}
```

**OpenAI Codex CLI**, in `~/.codex/config.toml`:

```toml
[mcp_servers.flowstate]
command = "flow"
args = ["mcp", "--egress-policy", "/etc/flowstate/egress.yaml"]

[mcp_servers.flowstate.env]
FLOWSTATE_ADDRESS = "flowstate.internal:9233"
```

**Any other stdio client** takes the same three things, whatever it calls the
file — a command, its arguments, and an environment:

```json
{
  "command": "flow",
  "args": ["mcp"],
  "env": {}
}
```

Two notes that save a support round trip. Use an absolute path to `flow` if the
client does not inherit your shell's `PATH`; `$(go env GOPATH)/bin/flow` is where
`go install` puts it. And logs go to stderr — stdout is the protocol, so anything
written there breaks the session rather than appearing anywhere.

### For agents

The loop this surface is shaped around, in order:

1. **Read** `flowstate://docs/dsl` for the grammar and `flowstate://catalog/tasks`
   for what this build can actually execute — a task named correctly in prose and
   absent from the catalog is a compile error you can avoid before writing a line.
   `flowstate://docs/examples/<name>` has working files to pattern-match against.
2. **Author** the Flowfile.
3. **Validate** it with `flowstate_validate`. It is pure and safe to loop on, and
   its diagnostics carry line and column, so correct against the position rather
   than re-reading the whole file.
4. **Run it locally** with `flowstate_run_local`. This is the step that used to
   be missing: conditions, retries, timeouts, loops, waits and step outputs behave
   here the way they behave in production, so a rehearsal that is right is
   evidence rather than a hope.
5. **Iterate** on 2–4, which costs nothing and reaches nothing.
6. **Run it durably**: `flowstate_compile`, then `flowstate_run` against a server,
   then `flowstate_get` to watch it.

What step 4 does not prove is durability, and the tool's own description says so
at length: a local run has no run id, nothing can watch it, it does not survive
the process, Continue-As-New compaction never happens, and parallel steps are
rehearsed rather than genuinely distributed. Two execution drivers agreeing on
everything observable is what makes the rehearsal worth doing —
[CLAUDE.md](../CLAUDE.md) has why that agreement is enforced rather than hoped
for — but agreement about behavior is not a claim about durability.

## `flow test`: the local driver only, on purpose

`flow test` (design: #155) runs a workflow's own `*.test.yaml` files — stubbed
task responses in place of the real registry, scripted signals, a virtual
clock — entirely through the local driver, in process. That is a deliberate
boundary, not a gap to fill in later.

The whole reason this command exists is the feedback loop GitHub Actions never
had: *edit, run, read the result*, in well under a second, with nothing to
provision. Running cases against a dev Temporal server would answer a
different, real question — does this survive Continue-As-New, does a durable
timer actually fire, does versioning behave — at the cost of the property that
makes `flow test` worth reaching for on every edit: no infrastructure, and no
wait. `flow run` against a dev server is where that question belongs; `flow
test` does not try to also be it.

What has to stay true regardless of which driver a workload eventually runs
under is *what a workflow can observe of time*, not how fast a test suite
gets there. A [`v1.VirtualClock`](../pkg/flowstate/v1/clock.go) exists so the
*local* driver can advance instantly to the next deadline once nothing else is
runnable — the same trick Temporal's own test environment already gives the
*durable* driver, which is why nothing new was needed on that side. The
production default on both is real time; a `*.test.yaml` run is the only
caller that ever injects the virtual one, through
[`v1.NewContextWithClock`](../pkg/flowstate/v1/clock.go). Any case where the
two drivers could disagree about what `now` means, or about whether a wait
actually blocks for what it says, is covered by a shared case in
`pkg/flowstate/v1/tests` that both drivers run — see `WaitCases` in
`pkg/flowstate/v1/tests/wait.go` — which is what keeps "the local driver got
faster" from quietly becoming "the local driver stopped rehearsing production."

Stubbing happens at the task boundary and nowhere lower: a step's `if:`,
`retry:`, loops, and `undo:` registration all run for real, through the
ordinary local driver, and only the effect a task would have outside the
process is replaced. A stub can replace a task *inside a called workflow* —
the call itself still runs for real — which is what makes `flow test` able to
exercise composition rather than only a single file in isolation; see
`examples/call-a-workflow/workflow.test.yaml` for a worked case, run in CI by
the `Example workflows pass their own tests` job.

### What a stub can see

A stub's `where:` and its `returns:` are evaluated against **the scope the
stubbed step itself was evaluated in, plus `inputs`** — the task's own resolved
inputs. So a stub reads, with the same spellings a Flowfile uses:

| Name | What it holds |
| --- | --- |
| `inputs.<name>` | the task's own resolved inputs — the url an `http` step is about to fetch, the message a `log` step is about to write |
| a bare name | whatever is bound where the step is written: a `for_each`'s `as:` name (`item` when it writes none), the step's own `vars:` keys |
| `vars.<name>` | the ambient vars in scope |
| `steps.<id>.<output>` | what earlier steps reported |

The bare binding is the one that makes a loop testable. Some task inputs are
expressions the *task* evaluates itself (`http`'s `outputs:` and `expect:`,
against `response`), so they are not resolved values when a stub is consulted
and are simply absent from `inputs`. Where a loop body's outputs are shaped by
such an expression, `inputs` alone is identical on every iteration — the loop's
binding is what separates them:

```yaml
stubs:
  # One stub, answering each iteration with that iteration's own value.
  - task: http
    returns:
      name: '${service.name}'

  # Or one stub per iteration, matched by the binding.
  - task: http
    where: service.name == 'search'
    fails: {kind: Upstream, message: service unavailable}
```

A `returns:` value follows the Flowfile's own fence rule at any depth: a
whole-value `${...}` is an expression, anything else is literal, and a value
that mixes text with a fence is refused when the file loads rather than
carried into the run as text. `inputs` is bound over the run's own
`inputs.<name>` namespace for the length of a `where:` — a stub's `where:` has
named the task's inputs since stubs existed, and that meaning is kept.

`where:` cannot reach what a stub replaces. The task's own evaluation of
`expect:` or `outputs:` does not run when the task is stubbed — the stub *is*
the step's answer — which is why `examples/http-expect` and
`examples/http-output-shaping` assert the steps around those expressions rather
than the expressions themselves.

## What this means for a change

A change to this surface is finished when:

- Data goes to stdout, everything else to stderr, and neither is written twice.
- The output is correct with `NO_COLOR=1`, correct through a pipe, correct on a
  dumb terminal, and correct in a CI log — each verified, not assumed.
- Every added string uses the vocabulary above, and no string contains an emoji.
- A test asserts the *record* rather than that a value appeared somewhere: rows are
  checked as rows, in order, on the line they belong to. `CLAUDE.md` has the longer
  version of why.
