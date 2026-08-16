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

// TestRunWorkflowEgressIdentity runs [conformance.EgressIdentityCases] against the
// durable driver — the same cases [flowstatev1_test.TestRunWorkflowEgressIdentity]
// runs against the local one. Two verified callers, which is what makes this
// set able to see a disagreement at all: #295 was exactly a disagreement here,
// and the surface had no both-drivers set to catch it.
//
// The identity arrives on the run's own state and workflow.go copies it into
// the scope each task is dispatched in; the http task renders it for the
// egress policy from there. The local driver reaches the same field by a
// different route, which is the whole point of comparing answers rather than
// plumbing.
func TestRunWorkflowEgressIdentity(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)

	for _, tc := range conformance.EgressIdentityCases() {
		t.Run(tc.Name, func(t *testing.T) {
			conformance.InstallEgressIdentityPolicy(t)

			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskWithPrev, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskWithPrev)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{
				Workflow: conformance.EgressIdentityWorkflow(baseURL),
				Identity: tc.Identity,
			})
			require.True(t, env.IsWorkflowCompleted())

			err := env.GetWorkflowError()

			var outputs *v1.Workflow_StepOutputs
			if err == nil {
				outputs = &v1.Workflow_StepOutputs{}
				require.NoError(t, env.GetWorkflowResult(outputs))
			}

			conformance.AssertEgressIdentityOutcome(t, tc, outputs, err)
		})
	}
}
