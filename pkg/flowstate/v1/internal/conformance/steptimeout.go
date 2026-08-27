package conformance

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
)

// What a step's `timeout:` is by the time a task can see it, asked of both
// drivers at once.
//
// #1130 was a host-side bug — `plugin.Plugin.callContext` stacked its own
// thirty second [plugin.DefaultCallTimeout] beneath whatever deadline the
// caller carried, so a plugin task under a ten minute `timeout:` still died at
// thirty seconds. Its fix is to trust the caller's deadline, which makes the
// fix's correctness rest entirely on a claim about the *drivers*: that a step's
// `timeout:` has become a context deadline by the time the call reaches a
// plugin, on both of them.
//
// That claim was true and untested, and it is exactly the shape CLAUDE.md's
// "both execution drivers must agree" section is about — one meaning, written
// down twice. The durable driver's version is the activity's StartToClose,
// which Temporal turns into the deadline on the context an activity runs
// under; the local driver's is `runStepAttempt`'s own [context.WithTimeout]
// (eval.go), which exists at all only because a hung task once hung a local
// run forever while production failed it after two minutes. Two independent
// mechanisms for one promise, and the host now depends on both keeping it: a
// driver that dispatched a task on a deadline-less context would not fail
// here, it would silently fall back to the host's thirty second backstop —
// #1130 again, on one driver only, which is the disagreement that is hardest
// to see because both halves still work.
//
// The fixture stands in for a plugin process rather than launching one, for
// the reason [PluginIdentityTaskDef] gives beside it: what is under test is
// the driver's dispatch, and the deadline is decided before the plugin
// protocol's wire is reached. `pkg/flowstate/v1/plugin`'s own
// TestCallContextKeepsALongerCallerDeadline and
// TestACallOutlivesCallTimeoutWhenItsCallerAllowedTheTime carry the other half
// — what the host does with the deadline it is handed — one process boundary
// further out.

// StepTimeoutTaskName is the name [StepTimeoutTaskDef] registers under. Dotted
// like a plugin task's, because a plugin task is the caller this case is about.
const StepTimeoutTaskName = "test.step_timeout"

// StepTimeoutBudget is the `timeout:` [StepTimeoutWorkflow] declares.
//
// The value is pinned between the two bounds it has to be told apart from, and
// both sides of that are load-bearing. Longer than [plugin.DefaultCallTimeout]
// (thirty seconds), because that gap is the whole subject. Shorter than
// [v1.DefaultStartToCloseTimeout] (two minutes), which is what a step declaring
// no `timeout:` gets on either driver — so a driver that ignored the policy
// entirely and fell back to its default would report a deadline *further* out
// than this budget, and `within_step_budget` below says so. A budget longer
// than the default could not tell the two apart.
//
// Nothing waits it out: the fixture reads the deadline and returns.
const StepTimeoutBudget = 90 * time.Second

// StepTimeoutTaskDef is a [v1.TaskDef] whose Fn reports what the deadline on
// its context looked like on arrival, in the terms the question is asked in
// rather than as a duration.
//
// Booleans, deliberately. The exact remaining time differs between two runs of
// one driver, let alone between two drivers, so an output holding it could
// never be compared; what both drivers have to agree on is whether a plugin
// call made from here would still be bounded by the author's `timeout:` or by
// the host's backstop.
func StepTimeoutTaskDef() v1.TaskDef {
	return v1.TaskDef{
		Name:    StepTimeoutTaskName,
		Summary: "test fixture reporting the deadline its driver dispatched it with",
		Fn: func(ctx context.Context, _ map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
			deadline, present := ctx.Deadline()
			remaining := time.Until(deadline)

			return &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
				"has_deadline": v1.NewLiteral(present),

				// The claim #1130's fix rests on: a plugin call made from this
				// context would keep this deadline, and this deadline is
				// further out than the bound that used to replace it.
				"beyond_host_call_timeout": v1.NewLiteral(present && remaining > plugin.DefaultCallTimeout),

				// And it is this step's own budget rather than something
				// larger — a driver ignoring `timeout:` and falling back to
				// [v1.DefaultStartToCloseTimeout] would satisfy the line above
				// and still be discarding what the author wrote.
				"within_step_budget": v1.NewLiteral(present && remaining <= StepTimeoutBudget),
			}}, nil
		},
	}
}

// StepTimeoutWorkflow builds the one-step workflow both drivers run: a plugin-
// shaped task under a `timeout:` longer than the host's own call bound, the way
// an author writes one for a task that legitimately takes minutes.
func StepTimeoutWorkflow(workflowName, stepID string) *v1.Workflow {
	return &v1.Workflow{
		Name:    workflowName,
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id:     stepID,
			Kind:   &v1.Node_Task{Task: &v1.Task{Name: StepTimeoutTaskName}},
			Policy: &v1.StepPolicy{Timeout: durationpb.New(StepTimeoutBudget)},
		}},
	}
}

// AssertStepTimeoutReachedTheTask is the shared assertion over what
// [StepTimeoutTaskDef] reported, so that both drivers are held to one wording
// of the claim rather than to two.
func AssertStepTimeoutReachedTheTask(t *testing.T, driver string, outputs *v1.Node_Outputs) {
	t.Helper()

	values := outputs.GetNamedValues()

	if !values["has_deadline"].GetLiteral().GetBoolValue() {
		t.Fatalf("%s dispatched a step declaring a %s timeout: on a context with no deadline at all; "+
			"a plugin task run this way falls back to the host's %s backstop however long the author "+
			"allowed (#1130)", driver, StepTimeoutBudget, plugin.DefaultCallTimeout)
	}

	if !values["beyond_host_call_timeout"].GetLiteral().GetBoolValue() {
		t.Errorf("%s left the step less than the host's %s call bound against a %s timeout:, so the "+
			"host's backstop would still be what ends a plugin call",
			driver, plugin.DefaultCallTimeout, StepTimeoutBudget)
	}

	if !values["within_step_budget"].GetLiteral().GetBoolValue() {
		t.Errorf("%s left the step more than its declared %s timeout:, so the deadline it dispatched "+
			"with is not the one the author wrote", driver, StepTimeoutBudget)
	}
}
