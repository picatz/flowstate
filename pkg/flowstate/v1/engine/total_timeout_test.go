package engine_test

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestTotalTimeoutEndsTheStepDurable is the second of the two driver callers
// [conformance.TotalTimeoutTaskDef] asks for: the durable driver turns a step's
// `total_timeout:` into the activity's ScheduleToCloseTimeout, which Temporal
// enforces server-side across every attempt and every backoff between them —
// the same promise TestTotalTimeoutEndsTheStepLocal makes of the local driver's
// own caused context budget, reached by an entirely different mechanism (#920).
//
// Registered on [v1.DefaultRegistry] rather than a context-scoped one, because
// the activity executing a step runs in a context Temporal hands it and cannot
// see a registry installed on the workflow's — see registerPluginIdentityTask
// in plugin_identity_test.go for the whole of that boundary.
func TestTotalTimeoutEndsTheStepDurable(t *testing.T) {
	var attempts atomic.Int64

	require.NoError(t, v1.DefaultRegistry().Register(conformance.TotalTimeoutTaskDef(&attempts)))

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	engine.Register(env)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{
		Workflow: conformance.TotalTimeoutWorkflow("total-timeout-durable", "poll"),
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError(),
		"the budget expiring is an ordinary step failure `continue_on_error:` tolerates, not a failure of the run")

	var out v1.Workflow_StepOutputs
	require.NoError(t, env.GetWorkflowResult(&out))

	conformance.AssertTotalTimeoutEndedTheStep(t, "the durable driver", out.GetStepValues()["poll"], attempts.Load())
	conformance.AssertTotalTimeoutSuppressesWidening(t, "the durable driver")
}
