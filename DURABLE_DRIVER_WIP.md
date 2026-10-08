# Durable driver WIP (delete when applied)

The session building `flow test --driver both` (#1598 narrow cut) ran out of disk
before it could commit the edits to existing files. The new files are in the
previous commit; these are the exact edits to the existing ones. Status when it
stopped: with all of them, `go run ./cmd/flow test --driver both examples/` passed
every example case (343 pass, a few carrying a `local only:` note); the final
edits (items 3 and 4 below) and the tests were not rebuilt afterwards.

## 1. `pkg/flowstate/v1/flowtest/run.go`

- `RunOptions`: add, above `Mutate`:
  `Durable DurableRunner` with the doc "additionally runs each passing case on the
  durable interpreter with a Continue-As-New forced between every pair of steps,
  and fails the case where the two drivers disagree. Nil runs the local driver
  alone."
- In the case loop, just before the `runCase(` call:
  ```go
  caseCtx := ctx
  if reported && opts.Debugger == nil {
  	caseCtx = contextWithDurable(ctx, opts.Durable)
  }
  ```
  and pass `caseCtx` as runCase's first argument.
- In `runCase`, right after `ctx = v1.NewContextWithRegistry(ctx, registry)` and before
  `releaseRegistry()`:
  ```go
  var durableRegistry *v1.Registry
  durableUnanswered := &unstubbedTasks{}
  durableSkipped := ""
  if durableFrom(base) != nil && v1.SchedulerFromContext(base) == v1.WrittenOrder {
  	if durableSkipped = durableIneligible(test, workflow, stubs); durableSkipped == "" {
  		durableRegistry, err = freshCaseRegistry(test, workflow, boundaries, durableUnanswered)
  		if err != nil {
  			caseError("%s", err)
  			return
  		}
  	}
  }
  ```
- Just before `result.Passed = len(result.Failures) == 0` that precedes the autopsy block:
  ```go
  if durableRegistry != nil && len(result.Failures) == 0 && v1.DebuggerFromContext(ctx) == nil {
  	dctx := v1.NewContextWithRegistry(base, durableRegistry)
  	// Named for the workflow it serves, as the local driver names it, which is
  	// what a `step:` stub matches the step it answers by.
  	durableRuntime := runtime
  	durableRuntime.Step.Workflow = workflow.GetName()
  	dctx = v1.ContextWithTaskRuntime(dctx, durableRuntime)
  	dctx = v1.NewContextWithTrigger(dctx, trigger)
  	disagreements, localOnly := durableDisagreements(dctx, durableFrom(base), workflow, inputs, durableRuntime,
  		durableUnanswered, outputs, runErr, sensitive)
  	result.Failures = append(result.Failures, disagreements...)
  	durableSkipped = localOnly
  }
  if durableSkipped != "" {
  	result.Warnings = append(result.Warnings, &v1.Diagnostic{Field: durableFailureField, Message: "local only: " + durableSkipped})
  }
  ```
- `if runErr == nil { result.Warnings = unusedStubWarnings(stubs) }` must become
  `result.Warnings = append(result.Warnings, unusedStubWarnings(stubs)...)`, or it
  overwrites the local-only note.
- New helper at the end of the file:
  ```go
  // freshCaseRegistry is [caseRegistry] over stubs bound anew from the case, for a
  // second run of it: the first run has spent whatever its stubs count.
  func freshCaseRegistry(test *Test, workflow *v1.Workflow, boundaries map[string]*v1.Workflow, unanswered *unstubbedTasks) (*v1.Registry, error) {
  	compiled, err := compileStubs(test.Stubs)
  	if err != nil {
  		return nil, err
  	}
  	stubs, err := bindStubs(compiled, workflow)
  	if err != nil {
  		return nil, err
  	}
  	for name, callee := range boundaries {
  		if stub, ok := stubs[name]; ok {
  			stub.callee = callee
  		}
  	}

  	return caseRegistry(stubs, v1.SensitiveInputNames(workflow), workflow, unanswered)
  }
  ```

## 2. `cmd/flow/test.go`

- imports: add `"context"` and `".../v1/flowtest/durable"`.
- Flag next to `--mutate`: `cmd.Flags().String("driver", driverLocal, ...)` (`local`
  default, or `both`: also run each passing case on the durable interpreter,
  in-process, with a Continue-As-New between every pair of steps; cases with
  signals, faults, a trigger delivery, plugins or unregistered stub tasks stay
  local and say so).
- After `mutateOpts, err := mutateOptions(...)`: `durableRunner, err := driverOption(cmd, budget, fuzzOpts, mutateOpts)`.
- `--list` conflicts table: `{"--driver", durableRunner != nil}`.
- `RunOptions{... Durable: durableRunner, ...}`.
- `driverOption(cmd, budget, fuzz, mutate) (flowtest.DurableRunner, error)`: reads
  `--driver`; `local` returns nil; `both` is refused beside `--debug`, `--seeds`/`--seed`
  (or `budget.Schedules > 0`), `--fuzz`/`--fuzz-seed` (or `fuzz.Runs > 0 || fuzz.Pinned`)
  and `--mutate`/`--mutant`; it returns a func calling `durable.Run(ctx, wf, inputs, runtime)`
  and adapting `*durable.Result` to `flowtest.DurableResult`.

## 3. `pkg/flowstate/v1/taskruntime.go`

Add, above `TaskStepRefFromContext`:
```go
// ContextWithTaskStep names the step a task is about to run for on a runtime that
// was handed in without one, and leaves ctx alone when it carries no runtime or
// already names a step.
func ContextWithTaskStep(ctx context.Context, stepID string) context.Context {
	runtime, ok := ctx.Value(secretRuntimeKey{}).(TaskRuntime)
	if !ok || runtime.Step.Step != "" || stepID == "" {
		return ctx
	}

	return ContextWithSecretStep(ctx, runtime.Step.Workflow, runtime.Step.Run, stepID)
}
```

## 4. `pkg/flowstate/v1/engine/activities.go`

In `observeTask`, immediately before `return v1.ObserveTaskAttempt(...)`:
`ctx = v1.ContextWithTaskStep(ctx, stepID)`. It must live here (durable only): doing
it in `v1.ObserveTaskAttempt` also stamps the local driver's compensations and breaks
`TestRunCompensatedNamesWhatWasUndone`. Note the durable `undo` activity already
carries the undone step's id on its authorized entry points, so a `step:` stub
answering a compensation is a pre-existing local/durable divergence this proof can
surface.

## 5. Still to do

Docs: `docs/TESTING.md` (what `--driver both` proves and does not: Continue-As-New
state, not signals or faults) and `docs/reference/cli.md` (the flag; check how it is
generated). Run `make fmt`, the flowtest and cmd/flow tests, and
`go run ./cmd/flow test --driver both examples/`; add `--driver both` over
`examples/` to the ordinary CI job; delete this file.
