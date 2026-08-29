package engine_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestStepTimeoutReachesTheTaskDurable is the second of the two driver callers
// [conformance.StepTimeoutTaskDef] asks for: the durable driver turns a step's
// `timeout:` into the activity's StartToClose, which Temporal turns into the
// deadline on the context the task runs under — the same promise
// TestStepTimeoutReachesTheTaskLocal makes of the local driver's own
// per-attempt bound, and the one `plugin.Plugin.callContext` now depends on
// rather than replacing (#1130).
//
// Registered on [v1.DefaultRegistry] rather than a context-scoped one, because
// the activity executing a step runs in a context Temporal hands it and cannot
// see a registry installed on the workflow's — see registerPluginIdentityTask
// in plugin_identity_test.go for the whole of that boundary.
func TestStepTimeoutReachesTheTaskDurable(t *testing.T) {
	require.NoError(t, v1.DefaultRegistry().Register(conformance.StepTimeoutTaskDef()))

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	engine.Register(env)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{
		Workflow: conformance.StepTimeoutWorkflow("step-timeout-durable", "call"),
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var out v1.Workflow_StepOutputs
	require.NoError(t, env.GetWorkflowResult(&out))

	conformance.AssertStepTimeoutReachedTheTask(t, "the durable driver", out.GetStepValues()["call"])
}
