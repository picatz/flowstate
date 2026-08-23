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

// TestRunWorkflowHTTPSpan runs [conformance.AssertHTTPSpan] against the durable
// driver — the same case [flowstatev1_test.TestRunWorkflowHTTPSpan] runs against
// the local one.
//
// Here the request goes out from inside an activity, so the span the peer is
// told about is opened on the activity's context rather than on the caller's.
// The local driver reaches the same round tripper by a different route, which is
// the whole point of comparing answers rather than plumbing.
func TestRunWorkflowHTTPSpan(t *testing.T) {
	recorder := conformance.RecordSpans(t)
	server := conformance.NewTracedHTTPServer(t)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(engine.Run)
	env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
	env.OnActivity(engine.TaskWithPrev, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskWithPrev)
	env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{
		Workflow: conformance.HTTPSpanWorkflow(server.URL),
	})
	require.True(t, env.IsWorkflowCompleted())

	err := env.GetWorkflowError()

	var outputs *v1.Workflow_StepOutputs
	if err == nil {
		outputs = &v1.Workflow_StepOutputs{}
		require.NoError(t, env.GetWorkflowResult(outputs))
	}

	conformance.AssertHTTPSpan(t, server, recorder, outputs, err)
}
