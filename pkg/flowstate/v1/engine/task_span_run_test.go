package engine_test

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestRunWorkflowTaskSpans runs [conformance.AssertTaskSpans] against the
// durable driver — the same case [flowstatev1_test.TestRunWorkflowTaskSpans]
// runs against the local one.
//
// This side has always opened these spans; what the shared case adds is that
// the *other* side now opens the same ones, named the same way, in the same
// relation to each other. The two are compared through one expectation rather
// than two, which is the only arrangement in which "the drivers agree about
// traces" is a thing a test can fail on.
func TestRunWorkflowTaskSpans(t *testing.T) {
	recorder := conformance.RecordSpans(t)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(engine.Run)
	env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
	env.OnActivity(engine.TaskWithPrev, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskWithPrev)
	env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: conformance.TaskSpanWorkflow()})
	require.True(t, env.IsWorkflowCompleted())

	err := env.GetWorkflowError()

	var outputs *v1.Workflow_StepOutputs
	if err == nil {
		outputs = &v1.Workflow_StepOutputs{}
		require.NoError(t, env.GetWorkflowResult(outputs))
	}

	conformance.AssertTaskSpans(t, recorder, outputs, err)
}
