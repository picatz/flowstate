# Embedding Flowstate as a Go library

`pkg/flowstate/embed` is the curated surface for a Go program that wants
workflows without building a workflow system: compile a Flowfile from bytes,
run it locally in-process or durably against a Temporal worker the program
owns, and register the program's own Go functions as tasks a workflow can
call.

[examples/embedding](../examples/embedding) is the runnable version of
everything below, and [the architecture](ARCHITECTURE.md) explains the system
it embeds.

## Why this package and not `pkg/flowstate/v1` directly

`pkg/flowstate/v1` is the interpreter, and "v1" names the schema edition it
executes, not a Go compatibility promise — see that package's own doc. Its
types and functions change as the interpreter evolves. `pkg/flowstate/embed`
is deliberately small, built to be the thing an embedder holds onto across an
upgrade. Prefer it even where `v1` could do the same thing more directly.

Two limits, so nobody relies on more:

- **It does not hide `v1`'s types.** A task function is a `v1.TaskFunc`,
  `RunLocal` answers with a `*v1.Workflow_StepOutputs`, and a durable run is
  started with `engine.Run` over a `v1.RunState`, so an embedding program
  imports `v1` and `engine` too, as the example below does.
- **Nothing is tagged yet.** "Stable" is the intent this package is held to,
  not a versioned guarantee. Pin a module revision and re-test when you move
  it; [SUPPORT.md](../SUPPORT.md) says what is and is not supported.

## The four things an embedder does

```go
import (
	"context"
	"errors"
	"log"

	"github.com/picatz/flowstate/pkg/flowstate/embed"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"go.temporal.io/sdk/worker"
)

// 1. Register a custom task. No Input/Output message is given here, which is
// [embed.Task]'s nil-descriptor escape hatch: `flow validate`, a language
// server, and generated reference docs can then check and document nothing
// about this task's shape beyond its name. That is a reasonable trade-off for
// a task used in one program by its own author, and the wrong choice for a
// task anyone else will write a step against — see
// examples/embedding/main.go's registerGreetTask for a task that takes it.
tasks := embed.NewTasks()
tasks.Register(embed.Task{
	Name: "greet",
	Fn: func(_ context.Context, inputs map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
		name := inputs["name"].GetLiteral().GetStringValue()
		if name == "" {
			// A failure the caller caused: classified, reported as
			// InvalidInput, and not retried. A plain error here would be
			// retried five times and reported as Internal.
			return nil, embed.InvalidInput(errors.New("name is required"))
		}
		return &v1.Node_Outputs{NamedValues: v1.NewNamedValues(map[string]any{
			"message": "hello, " + name,
		})}, nil
	},
})
uninstall, err := tasks.Install()
if err != nil {
	// Another Tasks set already claims one of these names.
	log.Fatal(err)
}
defer uninstall()

// 2. Compile a Flowfile from bytes. data is the Flowfile's contents,
// however the embedding program obtained them — go:embed, os.ReadFile, ...
workflow, diags, err := embed.Compile(data)

// 3. Run it locally, and read a step's output back as a Go value.
ctx := context.Background()
outputs, err := embed.RunLocal(ctx, workflow, embed.RunOptions{
	Inputs: map[string]any{"name": "world"},
	Tasks:  tasks,
})
message, ok := embed.StepOutputString(outputs, "greet", "message")

// 4. Or run it durably. RunDurable registers the interpreter and the tasks on
// a Temporal worker the program owns; temporalClient is the client.Client the
// program dialed itself. Start the worker, then start runs with
// temporalClient.ExecuteWorkflow(ctx, opts, engine.Run, &v1.RunState{...}).
err = embed.RunDurable(worker.New(temporalClient, engine.RunTaskQueueName, worker.Options{}), tasks)
```

`data` and `temporalClient` are elided above: they are the two values an
embedding program supplies from its own setup, not something this package
provides. [examples/embedding/main.go](../examples/embedding/main.go) shows the
durable start in full, including the `RunState` fields to set.

`embed.StepOutput` and `embed.StepOutputString` are how a program reads what a
run returned: the raw `*v1.Workflow_StepOutputs` is a protobuf message, and
printing it gives `values:{key:"message" value:{literal:...}}` rather than the
string. Both report `ok=false` for a step that did not run or an output it did
not produce, rather than a zero value.

## Compile vs. validate

`embed.Compile` wraps [`flowfile.Parse`](../pkg/flowstate/v1/flowfile/parse.go) — the same
compile boundary `flow validate` starts from. It does **not** check whether a
step's task is one this build knows: that question is
[`flowfile.Validate`](../pkg/flowstate/v1/flowfile/validate.go)'s, which
`Compile` deliberately does not call. The same goes for a step reading
another that does not exist (`${steps.nope.x}`): `Compile` is the parse, and
the checks across steps are `Validate`'s. A Flowfile naming a task nobody
registered compiles cleanly and is refused before `RunLocal` runs its first
step, with `task "nosuchtask": unknown task: ...` naming what to register; the
ghost reference fails at the step that evaluates it. Call
`flowfile.Validate(workflow)` (or `flowfile.ValidateSource`) directly for the
richer, line-and-column diagnostic `flow validate` gives.

Compiling from bytes has no file identity, so a `call:` step cannot be
resolved and is refused with a diagnostic saying so — the same restriction
`flowfile.Parse` documents. An embedder that needs `call:` reads the file
itself and uses `flowfile.ParseFile`.

## Custom tasks: two registries, on purpose

Validating a Flowfile and running it ask two different questions about a
task's name, of two different registries:

- **Validation** (`flowfile.Validate`, a language server, `Compile`'s
  eventual promotion decision inside `flowfile.Parse`) asks "does this
  *build* know a task by this name at all" — a property of the process,
  answered by `v1.DefaultRegistry()`.
- **Execution** (`RunLocal`, and a durable worker's activities) asks "what
  does this Fn actually do" — a property of *this run*, answered by a
  registry scoped to it.

`embed.Tasks.Install()` registers a task set into `v1.DefaultRegistry()` so
validation can see it, and returns a func that undoes exactly that
registration — or refuses outright, returning a non-nil error and a nil
uninstall, when a task in the set names something a *different*,
still-installed `Tasks` set already claims. Two embedders (or an embedder
and a plugin) legitimately can both want to call a task `log`; refusing the
second Install rather than silently layering it over the first is what
keeps a later `uninstall` call from ever restoring the wrong thing.
`embed.RunOptions.Tasks` is read fresh by every `RunLocal` call to build a
run-scoped registry, independent of whether `Install` was ever called —
which is what makes it safe for two goroutines to call `RunLocal` with two
different `Tasks` sets, against two different workflows, at the same time,
and never see each other's tasks.

When an embedder *means* to overwrite an existing task — installing a custom
`http` task with a different egress policy, or swapping a task in a
conformance test — it calls `v1.DefaultRegistry().Replace` instead — a `pkg/flowstate/v1` API
outside this package's curated surface.
`Replace` validates the definition the same way `Register` does (grammar,
non-nil function, input coherence) but writes unconditionally. The
distinction is the audit trail: a `Register` call that silently succeeds
always added something new; a `Replace` call always overwrote on purpose.

One consequence is a real, deliberate divergence: a workflow value built
directly in Go — skipping `Compile` and `flowfile.Validate` entirely — runs
an `opts.Tasks` task even when that same set was never installed and so
would be reported "unknown task" by validation. Validation and execution are
answering different questions on purpose.

`RunDurable` has no per-run registry to hand a Temporal activity, because
activities execute in a context this package never sees. Custom tasks meant
to run durably must be `Install`ed and stay installed for as long as the
worker polls — `RunDurable` refuses to register a worker for a `Tasks` set
that is not installed, rather than starting a worker that would poll for
activities it can never execute.

## Fail-closed defaults

A zero `RunOptions` is the safest possible run, matching an unconfigured
`flow run local`:

| Field | Zero value means |
| --- | --- |
| `Inputs` | The workflow's own `inputs:` defaults apply; no undeclared input is accepted. |
| `Tasks` | Only this build's own tasks (`log`, `http`, `exec`) run; `exec` is denied unless the host installs an exec policy. |
| `Clock` | Real wall-clock time (`v1.RealClock`). |
| `Signals` | A `wait_for_signal:` step fails immediately (`v1.ErrNoSignalWaiter`) rather than blocking forever. |
| `EgressPolicy` | The same deny-by-default policy `flow run local` enforces with no flags: internal address ranges denied, loopback denied unless `FLOWSTATE_ALLOW_LOOPBACK_EGRESS=true` is set in the process environment, every redirect hop re-checked, the response body bounded. |
| `Secrets` | Every `${secret(...)}` reference and every `credential:` target is refused — no worker-side authority is installed on the run's context at all. |

What `RunLocal` returns is not redacted, whatever the options say. A
`sensitive:` declaration bounds what Flowstate itself renders — a terminal, a
test report, an agent's answer — and not what a run hands back to the program
that ran it: the outputs are the run's history, in the clear. An embedder
that prints or forwards them takes the same fail-closed line the CLI does
(`decideCarriedValues` and `redactStepValues` in `cmd/flow/sensitive.go`):
when the workflow declares anything sensitive, withhold every step's values
rather than redact by value. `v1.SensitiveInputValues` recognises the
declared values and what they contain, and nothing computed from them, so a
token upper-cased or embedded in a URL by a step passes value-based redaction
untouched; that is why the CLI withholds the whole transcript, and why an
embedder should.

Nothing becomes more permissive by being left unset. Configuring `Secrets`
at all still denies everything unless a `Policy` with an actual allow rule
is given — an `auth.SecretPolicy`'s own zero value permits nothing, the same
as a `Store` with no `Policy` at all.

`RunOptions.EgressPolicy`, unlike `flow run local --egress-policy`, governs
only the one `RunLocal` call it is passed to. The CLI flag mutates
`v1.DefaultRegistry()` for the whole process; `RunLocal` instead builds a
fresh, run-scoped registry and installs the policy's `http` task into that,
so two concurrent `RunLocal` calls with different policies never interfere
with each other.

## Testing the workflows you embed

An embedded workflow is code your program ships, and its `*.test.yaml` suite
belongs in the same CI that tests the rest of the program.
`pkg/flowstate/v1/flowtest/flowtesting` pins a suite into `go test` with one
call:

```go
func TestWorkflows(t *testing.T) {
	flowtesting.RunFile(t, "workflows/deploy.test.yaml")
}
```

Each case in the file becomes a real Go subtest named by the case's own
`name:`, so everything that addresses a Go test addresses a Flowfile case —
`go test -run 'TestWorkflows/rolls_back_on_a_500'` reruns one case, `-v`
shows per-case timing and the suite's warnings, an IDE's per-test rerun works,
and a CI failure names the case rather than the file. Because the name is the
address, a file whose cases share one is refused before anything runs; `flow
test` itself accepts duplicates, since it never addresses a case by name.

A suite built or loaded in Go rather than read from disk goes through
`flowtesting.Run(t, file, flowtesting.WithDir(dir))`, where `WithDir` supplies
the directory the cases' relative `workflow:` paths resolve against — the fact
a file on disk carries in its own path. Two more options match the CLI's two
opt-in bars: `WithCoverageRequired()` holds the suite to
`flow test --coverage-required` (every step and switch arm reached or
recorded, no stale records), and `WithSchedules(budget)` explores each case
under seeded schedules the way `flow test --seeds N` does, failing the case's
subtest with the seed to replay when an ordering changed what it observed.

The verdicts are `flow test`'s own, spelled the same way: the harness runs
each case through the same engine, stubbing and virtual clock the CLI uses,
so a case passing under `go test` and failing under `flow test` (or the
reverse) would be a bug in the harness, not a property of your suite.

Each case logs its transcript — what every step produced and when virtual
time moved, which stub answered it, scripted signals with their sender, the
`switch:` arm taken — through the subtest's own log, so `go test` shows it
exactly when the CLI would: on a failing case, and under `-v` for every case.

## Debugging an embedded run

`embed.Debug` starts a workflow under the step [debugger](DEBUGGING.md), in
this process, with the same registry, egress, secret and clock rules
`RunLocal` applies — no listener, no CLI, and nothing serialized. It returns
once the run is under way. The `*embed.Debugging` it returns is the same local
session `flow run local --debug`, `flow dap` and the MCP sessions drive, so a
snapshot, a breakpoint state or an inspection means here what it means there:

```go
debugging, err := embed.Debug(ctx, workflow, embed.DebugOptions{
	RunOptions:  embed.RunOptions{Tasks: tasks},
	Continue:    true, // run until something stops it; false holds at the first step
	Breakpoints: []*v1.DebugBreakpoint{{Step: "orders/charge", Condition: "amount > 500"}},
})
if err != nil {
	return err // a breakpoint that does not resolve or compile is refused here
}

// A revision can move before the hold is recorded, so wait through them.
held, err := debugging.WaitSnapshot(ctx, 0)
for err == nil && held.GetState() == v1.DebugRunState_DEBUG_RUN_STATE_RUNNING {
	held, err = debugging.WaitSnapshot(ctx, held.GetRevision())
}
if err != nil {
	return err
}
if held.GetState() != v1.DebugRunState_DEBUG_RUN_STATE_HELD {
	return errors.New("the run ended without stopping at the breakpoint")
}
answer, err := debugging.Inspect(ctx, &v1.DebugInspectRequest{
	Revision: held.GetRevision(), Expression: "amount * 2",
})
if err != nil {
	return err
}
next, err := debugging.Driver().Do(ctx, "next") // or Resume with a typed action
if err != nil {
	return err
}

_ = debugging.Close() // detach: breakpoints stop holding, the run finishes
outputs, err := debugging.Wait(ctx)
```

`Debugging` embeds the session, so `Snapshot`, `WaitSnapshot`, `Resume`,
`Pause`, `ReplaceBreakpoints` and `Inspect` are the typed contract, and
`Driver()` takes the debugger's command lines — `next`, `break charge if amount
> 500`, `inspect steps.fetch` — for a program that would rather speak those.
Each call returns a fresh driver, which adopts the breakpoints already set —
through `DebugOptions`, `ReplaceBreakpoints` or another driver — before a line
changes the set, so `break` adds to them rather than replacing them.
`Wait` returns what `RunLocal` would have; a run held at a stop does not finish
until something moves it or `Close` detaches. `Cancel`, or cancelling `ctx`,
ends the run. `DebugOptions.Output` receives the session's narration, and
`SourceMap` relates steps to lines when a program has one.

A debugger is a reveal: the narration and every inspection show values
unredacted. So `Debug` refuses a workflow that declares a sensitive input or
output — itself or in a workflow it calls — unless `DebugOptions.RevealSensitive`
authorizes it, as `flow run local --debug` and `flow dap` refuse one without
`--reveal-sensitive`.

A custom task is opaque to a debugger: the run stops before it and after it,
and nothing in between. `v1.NoteTask` is how its author says what happened in
between:

```go
Fn: func(ctx context.Context, inputs map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
	amount := inputs["amount"].GetLiteral().GetInt64Value()
	v1.NoteTask(ctx, fmt.Sprintf("authorizing %d", amount))
	// ...
}
```

A note reaches the session as an observation (`charge: authorizing 900`) and
its `Output`, and is a no-op when nobody is debugging. It is presentation, not
data: it never enters the step's outputs or the run's history, it is cut to 1
KiB, and it is rendered through the session's redaction — which is a transcript
control, not a boundary, so a task must not write a secret into one. Notes are
local-only; a durable run does not carry them.

`Example_debug` in `pkg/flowstate/embed/debug_example_test.go` is the whole
program, run as a test.

## Identity and policy when you embed the server

Two layers decide who may do what, and an embedder configures each in its own
place. [AUTHORIZATION.md](AUTHORIZATION.md) lists every decision point and what each
does when nothing is configured.

- **Deployment authority** is the trust policy: which issuers are trusted and,
  for each, the `actions:` list its callers hold. The list is required; an
  entry without one is refused at load, and a verified caller holds only what
  its entry lists. It is the outer bound on everything below.
- **Author authority** is the Flowfile's own `allow:` predicates, which decide
  who may answer one gate or start one workflow. The deployment does not
  evaluate or override them.

To add a rule of your own, such as a maintenance freeze or a per-tenant
allowlist, hand the server an `authz.Decider`:

```go
srv, err := server.New(temporalClient,
	server.WithDecider(authz.DeciderFunc(func(ctx context.Context, req authz.Request) authz.Decision {
		return authz.Decision{Allowed: !frozen(ctx, req.Principal), Scope: "maintenance.freeze"}
	})))
```

The trust policy answers first and your decider is asked only about what it
allows, so a decider can refuse and can never grant. A panic in it is a
refusal. The same seam exists on the codec server (`codecserver.Options.Decider`; its
`Insecure` loopback mode skips authorization altogether, decider included), and
every check goes through `authz`; a test refuses a new comparison of a caller's
actions anywhere else. A decider's refusal tells the caller only that the
deployment's rules refused it, never a scope to request, and the decider may be
asked more than once per request, so keep it cheap and stateless in its answer.
To change what a caller is granted, change its issuer entry, not the decider.

### Who is starting an in-process run

`flow run local` does not consult a workflow's `triggers.manual:` block: a
rehearsal on the author's machine has no one to attest. A program that runs
workflows in-process on behalf of authenticated callers can ask for the
server's answer with `RunOptions.Starter`:

```go
outputs, err := embed.RunLocal(ctx, workflow, embed.RunOptions{
	Starter: &embed.Starter{Principal: verified, Reason: "incident 42"},
})
```

The run is refused before any step if the block says `denied`, its `allow:`
predicate does not admit the caller, or it requires a reason and none was
given. A zero or anonymous principal satisfies no `allow:` predicate, and a
workflow with no `manual:` block admits any starter, as on a server. The
program is the authority on who the caller is, so authenticate first and pass
what the verifier returned. A predicate reads `sender.identity.claims.<name>`
for the claims the principal carries, which are the ones its issuer entry's
`carry_claims` and `groups_claim` produced (see `auth.MapClaims`, and
`auth.WithClaimMapper` to replace it), and a principal with no namespace falls into
`Starter.Namespace`, which never overrides the principal's own. With `Starter`
nil nothing is consulted.

## What is not curated here

- **`call:` across embedder files.** Compiling from bytes has no directory to
  resolve one against.
- **The Flowstate server and RPC surface.** A curation problem of its own: it spans a
  Temporal client, a verifier, audit and a decider, and wants one design rather than
  a re-export of `server.New`.
- **Schedules, plugin-process hosting.** Real capabilities of the system;
  neither is exposed by this package.
- **Schema version skew between an embedder and the Flowstate build it
  links against.** The `edition:` mechanism covers the DSL layer; there is
  no Go-layer answer yet. Pin your `go.mod` dependency the way you would any
  other library, and re-test against a Flowfile suite when you upgrade it.

## See also

- [examples/embedding](../examples/embedding) — the runnable version of
  everything above, including the durable path.
- [`pkg/flowstate/embed`](../pkg/flowstate/embed) package doc — the
  authoritative reference for every exported name.
- [docs/reference/tasks.md](reference/tasks.md) — every task this build
  ships, including the shape `embed.Task` mirrors a narrower slice of.
