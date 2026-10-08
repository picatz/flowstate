package flowtest

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The local driver runs a case in one uninterrupted pass, which is the one
// thing a durable run never does: it suspends, serializes its state and resumes
// somewhere else. A case that passes locally therefore says nothing about the
// state a run carries across a Continue-As-New (#1598).
//
// A [DurableRunner] closes that gap. After a case passes locally, its workflow
// runs again on the durable interpreter with a Continue-As-New forced between
// every pair of steps, against freshly bound stubs, and the two runs must
// agree. The interpreter lives behind a func type because it carries the
// Temporal SDK's test environment, which the language server and every other
// importer of this package have no use for; `flow test` supplies it.

// DurableResult is what a durable run produced.
type DurableResult struct {
	// Outputs is the last segment's step outputs: the steps a continued run
	// retains, not every step it ran.
	Outputs *v1.Workflow_StepOutputs

	// Segments is how many workflow executions the run took.
	Segments int
}

// A DurableRunner runs wf with inputs on the durable interpreter, one step per
// segment. ctx reaches every task, which is how the case's stubs do; runtime is
// the secret access the case grants.
type DurableRunner func(ctx context.Context, wf *v1.Workflow, inputs map[string]*v1.Value, runtime v1.TaskRuntime) (DurableResult, error)

// durableFailureField names a disagreement between the drivers in a report.
const durableFailureField = "driver"

type durableKey struct{}

// contextWithDurable asks the cases run under ctx to be proved on runner too.
func contextWithDurable(ctx context.Context, runner DurableRunner) context.Context {
	if runner == nil {
		return ctx
	}

	return context.WithValue(ctx, durableKey{}, runner)
}

func durableFrom(ctx context.Context) DurableRunner {
	runner, _ := ctx.Value(durableKey{}).(DurableRunner)

	return runner
}

// durableIneligible says why a case cannot be run on the durable driver, or
// the empty string when it can. The reason is reported, never swallowed: a
// green that silently skipped the proof would read as one that passed it.
func durableIneligible(test *Test, workflow *v1.Workflow, stubs map[string]*stubbedTask) string {
	switch {
	case len(test.Signals) > 0:
		return "it scripts signals, which the durable driver does not yet receive"
	case len(test.Faults) > 0:
		return "it injects faults, which only the local driver's scheduler can"
	case test.Trigger != nil:
		return "it replays a trigger delivery"
	}
	if pins, err := v1.PinnedPlugins(workflow); err != nil || len(pins) > 0 {
		return "the workflow requires plugins, which a durable worker takes from a deployment's selection"
	}
	for _, name := range slices.Sorted(maps.Keys(stubs)) {
		if _, known := v1.LookupTask(name); !known {
			return fmt.Sprintf("it stubs %q, a task this build does not register, which the durable worker refuses", name)
		}
	}

	return ""
}

// durableDisagreements runs the case's workflow durably under ctx, whose
// registry carries a fresh set of the case's stubs, and reports where it
// differs from the local run that just passed: whether it finished, and, for
// every value the continued run kept, what that step produced.
//
// The comparison is the part both drivers can honestly be asked for. A
// continued run retains only the step outputs later steps read, so the steps
// it dropped are not evidence either way; the ones it kept must equal the
// local run's, because a value that changed across a seam is the bug this
// exists to find.
func durableDisagreements(ctx context.Context, runner DurableRunner, workflow *v1.Workflow, inputs map[string]*v1.Value, runtime v1.TaskRuntime,
	unanswered *unstubbedTasks, local *v1.Workflow_StepOutputs, localErr error, sensitive sensitiveInputs) (failures []*v1.Diagnostic, localOnly string) {
	got, err := runner(ctx, workflow, inputs, runtime)

	failure := func(step, format string, args ...any) *v1.Diagnostic {
		return &v1.Diagnostic{
			Field:   durableFailureField,
			Step:    step,
			Message: redactedErrorText(fmt.Sprintf(format, args...), sensitive),
		}
	}

	switch {
	case ctx.Err() != nil:
		return []*v1.Diagnostic{failure("", "the durable run was cancelled: %v", ctx.Err())}, ""
	case err != nil && localErr == nil:
		if unanswered.any() {
			// The workflow did not fail; a stub could not answer. A `where:` that
			// reads a loop binding, say, has none to read in an activity, which
			// is a limit of the stub and not a disagreement about the workflow.
			return nil, "a stub could not answer the durable driver's invocation, so the proof is not made"
		}

		return []*v1.Diagnostic{failure("", "the run passed locally but failed after %d segment(s) on the durable driver: %v", got.Segments, err)}, ""
	case err == nil && localErr != nil:
		return []*v1.Diagnostic{failure("", "the run failed locally (%v) but finished on the durable driver", localErr)}, ""
	case err != nil:
		// Both failed. The text differs by design (the durable driver wraps an
		// activity's failure), so only that they agree it failed is a claim.
		return nil, ""
	}

	var out []*v1.Diagnostic
	kept := got.Outputs.GetStepValues()
	for _, id := range slices.Sorted(maps.Keys(kept)) {
		want, ran := local.GetStepValues()[id]
		if !ran {
			out = append(out, failure(id, "step %q ran on the durable driver (%d segments) and not locally", id, got.Segments))

			continue
		}
		for _, name := range slices.Sorted(maps.Keys(kept[id].GetNamedValues())) {
			durableValue := kept[id].GetNamedValues()[name]
			localValue, present := want.GetNamedValues()[name]
			switch {
			case !present:
				out = append(out, failure(id, "step %q produced %q on the durable driver and not locally", id, name))
			case !sameValue(localValue, durableValue):
				out = append(out, failure(id, "%s.%s changed once the run continued as new between steps:\n  local:   %s\n  durable: %s",
					id, name, oneLine(localValue), oneLine(durableValue)))
			}
		}
	}

	return out, ""
}

// sameValue is equality of two values by what they mean: a map's entries are
// unordered, so two encodings of one map are one value, which byte equality of
// the messages would call two.
func sameValue(a, b *v1.Value) bool {
	if a.GetLiteral() == nil || b.GetLiteral() == nil {
		return proto.Equal(a, b)
	}
	left, errLeft := v1.LiteralToGo(a.GetLiteral())
	right, errRight := v1.LiteralToGo(b.GetLiteral())
	if errLeft != nil || errRight != nil {
		return proto.Equal(a, b)
	}

	return reflect.DeepEqual(left, right)
}

func oneLine(m proto.Message) string {
	return strings.Join(strings.Fields(fmt.Sprint(m)), " ")
}

// any reports whether a task was invoked that no stub answered.
func (u *unstubbedTasks) any() bool {
	u.mu.Lock()
	defer u.mu.Unlock()

	return len(u.seen) > 0
}
