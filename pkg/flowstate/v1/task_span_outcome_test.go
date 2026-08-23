package flowstatev1_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// The two arms of the local driver's task span that a successful run never
// reaches, mirroring what `engine/tracing_test.go` already pins for the durable
// driver: a failure marks the span with a classification and never a message,
// and a dispatch the deployment's task-shape policy refuses still leaves a span
// behind.
//
// They are here rather than in `internal/conformance` because the durable half
// of each already exists and is asserted where the durable driver's own
// scaffolding lives — a Temporal test environment with a policy installed is
// not something a shared case can build without dragging the engine into the
// package every other driver-agnostic case avoids. What is shared is the code
// being tested: both drivers reach [v1.RecordTaskOutcome] and neither has its
// own spelling of it.

// TestLocalFailedTaskSpanCarriesTheClassificationNotTheMessage is the local
// half of [conformance.AssertTraceContainment]. The durable half is
// `engine.TestFailedTaskSpanCarriesTheClassificationNotTheMessage`, and the two
// run the same fixture through the same assertion.
func TestLocalFailedTaskSpanCarriesTheClassificationNotTheMessage(t *testing.T) {
	conformance.RegisterTraceContainmentTask(t)

	recorder := conformance.RecordSpans(t)
	authority := conformance.TraceContainmentAuthority()
	ctx := v1.ContextWithTaskRuntime(t.Context(), v1.TaskRuntime{
		Store:    authority.Store(t),
		Policy:   authority.Policy(t),
		Identity: authority.Identity,
	})

	_, err := v1.RunWithInputs(ctx, conformance.TraceContainmentWorkflow(), conformance.TraceContainmentInputs())
	require.Error(t, err, "the task fails on purpose")

	var described bool
	for _, stub := range tracetest.SpanStubsFromReadOnlySpans(recorder.Ended()) {
		if stub.Name != v1.TaskSpanName(conformance.ContainmentTaskName) {
			continue
		}

		require.Equal(t, "Error", stub.Status.Code.String(), "a failed task must mark its span")
		require.NotEmpty(t, stub.Status.Description, "a status with no description says nothing")
		// Both sides lowered, not just the haystack: the fixture's material
		// carries capitals, so lowering one side alone is an assertion that
		// cannot fail — which is how it read before this test ran the shared
		// fixture rather than an all-lowercase constant of its own.
		require.NotContains(t, strings.ToLower(stub.Status.Description),
			strings.ToLower(conformance.ContainmentFailureMessage),
			"the task's own error message reached the span status")
		require.Empty(t, stub.Events, "no exception event, because an exception event carries the message")

		described = true
	}
	require.True(t, described, "no span covered the failing task")

	// The run span is the local driver's alone: the durable driver opens none,
	// because Temporal's interceptor already covers that seam.
	conformance.AssertTraceContainment(t, recorder,
		v1.RunSpanName(conformance.ContainmentWorkflowName),
		v1.TaskSpanName(conformance.ContainmentTaskName))
}

// TestLocalDeniedDispatchStillOpensASpan pins the arm that is easiest to lose.
//
// A refused dispatch is the outcome an operator most wants to find in a trace,
// and the local driver checks the policy above its retry loop — outside the
// span the loop opens — so without a span of its own a denial would be the one
// thing a local run recorded nothing about while the durable driver recorded a
// span with an error status (`engine.checkTaskDispatchPolicy` writes it there).
func TestLocalDeniedDispatchStillOpensASpan(t *testing.T) {
	denying, err := v1.TaskPolicyConfig{Deny: []string{"true"}}.Policy()
	require.NoError(t, err)

	recorder := conformance.RecordSpans(t)

	ctx := v1.NewContextWithTaskPolicy(t.Context(), denying)
	_, err = v1.Run(ctx, oneStep("log"))
	require.Error(t, err, "the policy denies every task")

	var described bool
	for _, stub := range tracetest.SpanStubsFromReadOnlySpans(recorder.Ended()) {
		if stub.Name != v1.TaskSpanName("log") {
			continue
		}

		require.Equal(t, "Error", stub.Status.Code.String(),
			"a denied dispatch left a span that reads as a success")

		described = true
	}
	require.True(t, described, "a denied dispatch left no span at all")
}

// oneStep is a workflow of a single task step with no inputs.
//
// One attempt, declared: a task that fails with no `retry:` gets
// [v1.DefaultRetryAttempts] under either driver, and the backoff between five
// of them is fifteen seconds this test would spend proving something the retry
// tests already own. What is under test here is what one span says about one
// failure.
func oneStep(taskName string) *v1.Workflow {
	return &v1.Workflow{
		Name:    "one-step",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id:     "only",
			Kind:   &v1.Node_Task{Task: &v1.Task{Name: taskName}},
			Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{MaxAttempts: 1}},
		}},
	}
}
