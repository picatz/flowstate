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

// TestRunIdentityShapeDurable checks the durable half of #206's second gap:
// the identity established when the run was requested (server.go:349) must be
// the one an expression reads under `run.identity`, unchanged, and `run.local`
// must read false — a server sits in front of every durable run, even one
// whose identity provider is unconfigured. The local half of this shared
// assertion is [flowstatev1_test.TestRunIdentityShapeLocal].
func TestRunIdentityShapeDurable(t *testing.T) {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(engine.Run)
	env.OnActivity(engine.Task, mock.Anything, mock.Anything).Return(engine.Task)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{
		Workflow: tests.RunIdentityWorkflow(),
		Identity: &v1.WorkloadIdentity{
			Subject:   "release-requester@example.com",
			Issuer:    "flowstate:test",
			Namespace: "team-a",
		},
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var outputs v1.Workflow_StepOutputs
	require.NoError(t, env.GetWorkflowResult(&outputs))

	tests.AssertRunIdentityShape(t, &outputs, false, "release-requester@example.com")
}

// TestRunIdentityShapePredatesTheField checks invariant 10's own direction: a
// run whose RunState carries no identity at all — every run started before
// this field existed — must read absent rather than broken. `run.identity`
// answers with every field empty, and `run.local` answers false: nothing
// authenticated a run like this any differently than it authenticates one
// today with no identity provider configured, and the two must not be told
// apart by a field that predates one of them.
func TestRunIdentityShapePredatesTheField(t *testing.T) {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(engine.Run)
	env.OnActivity(engine.Task, mock.Anything, mock.Anything).Return(engine.Task)

	// No Identity at all — the wire shape of a RunState written before this
	// field existed, decoded by a build that now reads it.
	env.ExecuteWorkflow(engine.Run, &v1.RunState{
		Workflow: tests.RunIdentityWorkflow(),
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var outputs v1.Workflow_StepOutputs
	require.NoError(t, env.GetWorkflowResult(&outputs))

	tests.AssertRunIdentityShape(t, &outputs, false, "")
}
