package embed

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// RunOptions configures one [RunLocal] call: the arguments a workflow
// receives, the custom tasks and worker-side authority available to it, and
// what it may reach outside this process.
//
// # The zero value is fail closed
//
// A zero RunOptions runs a workflow that has no `inputs:` requirements, no
// custom tasks, real wall-clock time, no way to deliver a signal, denies
// every ${secret(...)} reference, and lets the http task reach the public
// internet under exactly the deny-by-default egress policy `flow run local`
// enforces with no flags — internal address ranges refused, every redirect
// hop re-checked, the response body capped — because a nil EgressPolicy does
// not mean "unrestricted", it means "the same posture as an unconfigured CLI
// invocation". See [RunOptions.EgressPolicy] and [RunOptions.Secrets] for
// each field's own zero-value behavior; nothing here becomes more permissive
// by being left unset.
type RunOptions struct {
	// Inputs binds the workflow's own `inputs:` declarations, exactly as
	// `flow run local --input`/`--input-json` does — checked and defaulted
	// by [v1.RunWithInputs] itself via [v1.BindRunInputs], so an undeclared
	// input or a missing required one is refused the same way regardless of
	// which submit boundary a caller used.
	Inputs map[string]any

	// Tasks is the custom tasks this run may execute, in addition to
	// the tasks Flowstate ships with. Left nil, only those built-in tasks run;
	// tasks installed globally for validation or plugins are not execution
	// capabilities for this run.
	//
	// Reading this set does not require [Tasks.Install] to have been called,
	// and does not require it to still be installed if it was — see
	// [Tasks]'s doc for why compiling and running ask two different
	// registries this field, and Install, each answer separately.
	Tasks *Tasks

	// Clock is how the run tells time — what a wait's `now` binding reads,
	// and what a `sleep:` or `wait_for_signal:` timeout counts against. Nil
	// uses [v1.RealClock], real wall-clock time; an embedder testing a
	// workload with a long `sleep:` can supply a [v1.VirtualClock] the same
	// way `flow test` does.
	Clock v1.Clock

	// Signals delivers signals to a `wait_for_signal:` step. Nil means
	// nothing can: a workload that waits fails immediately with
	// [v1.ErrNoSignalWaiter] rather than blocking forever with no
	// explanation, the same fail-closed choice the local driver already
	// makes for every caller. Supply a [v1.NewLocalSignals] and call
	// Deliver on it from another goroutine to script one.
	Signals v1.SignalWaiter

	// EgressPolicy governs what the built-in http task may reach.
	//
	// Nil is not "no policy", and it is not "whatever this process happens
	// to have registered for `http` right now" either — it is always
	// exactly [v1.DefaultEgressPolicy]: internal address ranges denied,
	// loopback denied unless [v1.AllowLoopbackEgressEnv] is set in this
	// process's environment, every redirect hop re-checked, and the
	// response body bounded. RunLocal registers that policy's http task
	// explicitly for the nil case rather than copying whatever
	// [v1.DefaultRegistry] currently holds for `http` — see [RunLocal]'s
	// doc for why an ambient default would not actually be a constant one.
	// Setting EgressPolicy replaces that policy entirely for this run — the
	// same replace-not-merge semantics [netpolicy] gives every other caller
	// of it — it does not add rules on top of the default.
	//
	// This governs only this [RunLocal] call, not the process: unlike
	// `flow run local --egress-policy`, which mutates [v1.DefaultRegistry]
	// (see cmd/flow/egress.go's own comment on why that mutation is the
	// mechanism), RunLocal builds a run-scoped task registry and installs
	// this policy's http task into that instead — see [RunLocal]'s doc.
	// Concurrent RunLocal calls with different policies are therefore safe
	// against each other.
	EgressPolicy *netpolicy.Policy

	// Secrets is the worker-side authority a run needs to resolve
	// ${secret(...)} references or authorize JIT credential exchange. Nil
	// resolves and authorizes nothing — see [Secrets]'s doc for the exact
	// fail-closed posture, which matches `flow run local` invoked with none
	// of its own secret or identity-broker flags.
	Secrets *Secrets
}

// RunLocal compiles [RunOptions] into a run and executes workflow in this
// process, returning what [v1.RunWithInputs] returns.
//
// This is [v1.RunWithInputs] underneath, the same submit boundary `flow run
// local` uses — argument binding, submission-size bounds, and everything
// this package does not reimplement all come from there, so a Go caller gets
// driver parity with the durable driver for free, the same way the CLI does.
//
// # The task registry this run actually executes against
//
// Every call builds a fresh [v1.Registry], seeded only with the task
// definitions Flowstate ships with, and then layers the http task
// (opts.EgressPolicy's, or [v1.DefaultEgressPolicy]'s when nil) and
// opts.Tasks on top, before installing the result on the run's context with
// [v1.NewContextWithRegistry]. Three consequences follow:
//
//   - opts.Tasks does not need [Tasks.Install] to have been called, or to
//     still be installed, for RunLocal to execute it — only [flowfile.Validate]
//     reads the global registry, and [Compile] does not call it. RunLocal
//     runs an opts.Tasks task even when that same set was never installed
//     and so would be reported "unknown task" by [flowfile.Validate]. That
//     divergence is deliberate, not an oversight: validation and execution
//     are answering different questions, on purpose, the same way they
//     already do for [v1.LookupTask] versus [v1.LookupTaskIn] — see
//     [Tasks]'s doc.
//   - opts.EgressPolicy being nil does *not* mean "whatever the copy above
//     happened to carry over for `http`". The http entry is always
//     re-registered explicitly, from opts.EgressPolicy or from
//     [v1.DefaultEgressPolicy] — never left as the copied value — so the
//     documented zero-value posture holds even when something else in the
//     process (`cmd/flow/egress.go`'s own mutation of [v1.DefaultRegistry],
//     or another embedder's [Tasks.Install] overriding `http`) has changed
//     what [v1.DefaultRegistry] itself would have answered.
//   - Tasks added to [v1.DefaultRegistry] for validation or by a plugin host
//     are not executable unless they are also present in opts.Tasks. An
//     install is metadata visibility, not an ambient execution grant.
//   - Two concurrent RunLocal calls with different opts.Tasks or
//     opts.EgressPolicy never interfere with each other: each builds and
//     discards its own registry, and nothing about running one mutates
//     [v1.DefaultRegistry] or any other RunLocal call's registry.
func RunLocal(ctx context.Context, workflow *Workflow, opts RunOptions) (*v1.Workflow_StepOutputs, error) {
	if workflow == nil {
		return nil, fmt.Errorf("flowstate/embed: RunLocal: workflow is nil")
	}

	registry := v1.NewBuiltinRegistry()

	// The http task is always (re-)registered explicitly, never left as
	// whatever the copy above happened to carry over. Reading it off
	// [v1.DefaultRegistry] for the nil case would make RunOptions's zero
	// value an *ambient* posture — whatever the process happens to have
	// registered for `http` at the moment this call runs, which
	// `cmd/flow/egress.go`'s own mutation of that same registry, or another
	// embedder's [Tasks.Install] overriding `http`, can silently change out
	// from under a caller who wrote nothing at all. [RunOptions.EgressPolicy]
	// promises a *constant* default instead: [v1.DefaultEgressPolicy]
	// unless opts.EgressPolicy names something else.
	egressPolicy := opts.EgressPolicy
	if egressPolicy == nil {
		egressPolicy = v1.DefaultEgressPolicy()
	}
	if err := registry.Register(v1.HTTPTaskDef(egressPolicy)); err != nil {
		return nil, fmt.Errorf("flowstate/embed: RunLocal: registering the http task for the given egress policy: %w", err)
	}

	if opts.Tasks != nil {
		for _, def := range opts.Tasks.defs() {
			if err := registry.Register(def); err != nil {
				// [Tasks.Register] already validated this definition; a
				// failure here means this package's own bookkeeping
				// disagreed with what it accepted.
				return nil, fmt.Errorf("flowstate/embed: RunLocal: registering task %q: %w", def.Name, err)
			}
		}
	}
	ctx = v1.NewContextWithRegistry(ctx, registry)

	if opts.Clock != nil {
		ctx = v1.NewContextWithClock(ctx, opts.Clock)
	}
	if opts.Signals != nil {
		ctx = v1.NewContextWithSignalWaiter(ctx, opts.Signals)
	}

	if opts.Secrets != nil {
		if err := opts.Secrets.Identity.Validate(); err != nil {
			return nil, fmt.Errorf("flowstate/embed: RunLocal: %w", err)
		}

		var targets []string
		if opts.Secrets.Broker != nil {
			targets = opts.Secrets.Broker.Targets()
		}
		// Refused before the run starts, the same as `flow run local
		// --auth-policy` does once any policy is configured at all: a
		// workflow naming a credential target this Secrets set does not
		// federate is a mistake worth reporting once, up front, rather than
		// at whichever step happens to reach it first.
		if err := v1.ValidateCredentialTargets(workflow, targets); err != nil {
			return nil, fmt.Errorf("flowstate/embed: RunLocal: %w", err)
		}

		ctx = v1.ContextWithTaskRuntime(ctx, v1.TaskRuntime{
			Store:    opts.Secrets.Store,
			Policy:   opts.Secrets.Policy,
			Broker:   opts.Secrets.Broker,
			Identity: opts.Secrets.Identity,
			Step:     auth.StepRef{Workflow: workflow.GetName(), Run: uuid.NewString()},
		})
	}
	// Left unchanged otherwise: no [v1.TaskRuntime] on the context at all is
	// exactly what [v1.ResolveSecret] and [v1.AuthorizeCredential] treat as
	// "not configured on this worker" and refuse — see [Secrets]'s doc.

	return v1.RunWithInputs(ctx, workflow, v1.NewNamedValues(opts.Inputs))
}

// StepOutput returns the named output of a step from what [RunLocal]
// returned, as a plain Go value, with ok reporting whether it found one.
//
// ok is false, with a nil value, whenever the step named did not run or ran
// but produced no output of that name. Both are typos or logic errors an
// author should see, not a silent zero value read out of a raw map index.
// ok is also false for outputs, present in outputs, that RunLocal itself
// reports as nil.
//
// A found value is converted with [v1.LiteralToGo], the same conversion
// `flow test` uses to compare a stub's canned output against a `.test.yaml`
// expectation, so a list or a map comes back as the Go slice or map an
// author expects rather than a CEL literal they would otherwise have to
// unwrap by hand. A found output whose value is not a literal, such as a
// secret reference reflected back unresolved, is reported as ok=false rather
// than as an error, matching this accessor's "found or not" contract.
func StepOutput(outputs *v1.Workflow_StepOutputs, step, name string) (any, bool) {
	if outputs == nil {
		return nil, false
	}

	values := outputs.GetStepValues()[step]
	if values == nil {
		return nil, false
	}

	value := values.GetNamedValues()[name]
	if value == nil {
		return nil, false
	}

	literal := value.GetLiteral()
	if literal == nil {
		return nil, false
	}

	native, err := v1.LiteralToGo(literal)
	if err != nil {
		return nil, false
	}

	return native, true
}

// StepOutputString is [StepOutput] for the common case of reading a string
// output, so a caller does not need a type assertion for the facade's
// flagship example. ok is false under every condition [StepOutput] itself
// reports false for, and also when the output exists but is not a string.
func StepOutputString(outputs *v1.Workflow_StepOutputs, step, name string) (string, bool) {
	value, ok := StepOutput(outputs, step, name)
	if !ok {
		return "", false
	}

	str, ok := value.(string)
	if !ok {
		return "", false
	}

	return str, true
}
