# Embedding Flowstate as a Go library

`pkg/flowstate/embed` is the curated surface for a Go program that wants
workflows without building a workflow system: compile a Flowfile from bytes,
run it locally in-process or durably against a Temporal worker the program
owns, and register the program's own Go functions as tasks a workflow can
call.

[docs/ARCHITECTURE.md](ARCHITECTURE.md) describes what the system is.
[CLAUDE.md](../CLAUDE.md) describes how to change it. This describes what an
embedder meets. [examples/embedding](../examples/embedding) is the runnable
version of everything below.

## Why this package and not `pkg/flowstate/v1` directly

`pkg/flowstate/v1` is the interpreter, and "v1" names the schema edition it
executes, not a Go compatibility promise — see that package's own doc. Its
types and functions change as the interpreter evolves. `pkg/flowstate/embed`
is deliberately small, built to be the thing an embedder holds onto across an
upgrade, and reaches into `v1` on an embedder's behalf so a program does not
have to. Prefer it even where `v1` could do the same thing more directly.

## The four things an embedder does

```go
import (
	"context"
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

// 3. Run it locally.
ctx := context.Background()
outputs, err := embed.RunLocal(ctx, workflow, embed.RunOptions{
	Inputs: map[string]any{"name": "world"},
	Tasks:  tasks,
})

// 4. Or run it durably, against a Temporal worker the program owns.
// temporalClient is a *client.Client the embedding program dialed itself.
err = embed.RunDurable(worker.New(temporalClient, engine.RunTaskQueueName, worker.Options{}), tasks)
```

`data` and `temporalClient` are elided above — they are the two values an
embedding program supplies from its own setup, not something this package
provides. [examples/embedding](../examples/embedding) is the runnable version
with both filled in.

## Compile vs. validate

`embed.Compile` wraps [`flowfile.Parse`](reference/tasks.md) — the same
compile boundary `flow validate` starts from. It does **not** check whether a
step's task is one this build knows: that question is
[`flowfile.Validate`](../pkg/flowstate/v1/flowfile/validate.go)'s, which
`Compile` deliberately does not call. A Flowfile naming a task nobody
registered compiles cleanly and fails only once a run actually reaches that
step, with the engine's own `unknown task %q` error. Call
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
keeps a later `uninstall` call from ever restoring the wrong thing. `embed.
RunOptions.Tasks` is read fresh by every `RunLocal` call to build a
run-scoped registry, independent of whether `Install` was ever called —
which is what makes it safe for two goroutines to call `RunLocal` with two
different `Tasks` sets, against two different workflows, at the same time,
and never see each other's tasks (issue #195's lesson).

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
| `Tasks` | Only this build's own tasks (`log`, `http`) run. |
| `Clock` | Real wall-clock time (`v1.RealClock`). |
| `Signals` | A `wait_for_signal:` step fails immediately (`v1.ErrNoSignalWaiter`) rather than blocking forever. |
| `EgressPolicy` | The same deny-by-default policy `flow run local` enforces with no flags: internal address ranges denied, loopback denied unless `FLOWSTATE_ALLOW_LOOPBACK_EGRESS=true` is set in the process environment, every redirect hop re-checked, the response body bounded. |
| `Secrets` | Every `${secret(...)}` reference and every `credential:` target is refused — no worker-side authority is installed on the run's context at all. |

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

## What is not curated here

- **`call:` across embedder files.** Compiling from bytes has no directory to
  resolve one against.
- **The Flowstate server and RPC surface.** A curation problem of its own.
- **Schedules, plugin-process hosting.** Real capabilities of the system;
  neither is part of this slice.
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
