package engine_test

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
)

// TestRunAddressShapeDurable checks the durable half of the shared assertion in
// [tests.AssertRunAddressShape]: a durable run reports the workflow id it is
// addressed by — the id `flow signal` and `flow get` take — and a run id that
// identifies which execution of it this is. The local half is
// [flowstatev1_test.TestRunAddressShapeLocal].
//
// The expected values are Temporal's own test-environment defaults rather than
// anything this repository chooses, which is the point: the durable driver reads
// the address from the substrate instead of inventing one.
func TestRunAddressShapeDurable(t *testing.T) {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(engine.Run)
	env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{
		Workflow: tests.RunAddressWorkflow(),
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var outputs v1.Workflow_StepOutputs
	require.NoError(t, env.GetWorkflowResult(&outputs))

	// "default-test-workflow-id" and "default-test-run-id" are the test
	// environment's own constants. The run id is the *current* execution's here
	// because the test environment leaves FirstRunID unset and engine.runAddress
	// falls back — see its doc, and TestRunAddressPrefersFirstRunID below for the
	// half of that rule the test environment cannot exercise.
	tests.AssertRunAddressShape(t, &outputs, "default-test-workflow-id", "default-test-run-id")
}

// TestRunAddressPrefersFirstRunID pins the choice engine.runAddress exists to
// make: given both, the address reports the *first* run id of the
// continued-execution chain and not the current execution's.
//
// This is the rule a workload that suspends depends on and the test environment
// cannot show, because it never populates FirstRunID at all. Without it, a run
// that continued as new — which this engine does on its own step budget, with
// nothing in the file to say when — would hand out one callback address before
// it suspended and a different one after.
func TestRunAddressPrefersFirstRunID(t *testing.T) {
	require.Equal(t, "first", engine.RunAddressFrom("wf", "first", "current").GetRunId())
	require.Equal(t, "wf", engine.RunAddressFrom("wf", "first", "current").GetWorkflowId())

	// And the fallback, for the one case where the substrate does not offer a
	// first run id: the current execution's, which is the correct answer for a
	// run that has not continued as new — the two are the same value then.
	require.Equal(t, "current", engine.RunAddressFrom("wf", "", "current").GetRunId())
}
