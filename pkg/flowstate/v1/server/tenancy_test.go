package server_test

import (
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// A workflow id is not a capability.
//
// Ids appear in logs, dashboards, support tickets and URLs, so any RPC that acts
// on a run because a caller named one correctly is an RPC that acts on any run
// whose id has leaked. These tests check both directions, because a check that
// denies everything passes a test that only tries the thing that should work —
// and a check that allows everything passes a test that only tries the thing that
// should not.

// tenantFixture is a dev server, a worker, and two servers standing in for two
// tenants over the same Temporal client.
type tenantFixture struct {
	teamA *server.FlowstateServer
	teamB *server.FlowstateServer
}

// newTenantFixture starts everything needed to run and address real workloads.
func newTenantFixture(t *testing.T) *tenantFixture {
	t.Helper()

	devServer, err := testsuite.StartDevServer(t.Context(), testsuite.DevServerOptions{
		ClientOptions: &client.Options{Logger: &testingLogger{t: t}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = devServer.Stop() })

	w := worker.New(devServer.Client(), engine.RunTaskQueueName, worker.Options{})
	w.RegisterWorkflow(engine.Run)
	w.RegisterActivity(engine.Task)
	w.RegisterActivity(engine.TaskWithPrev)
	w.RegisterActivity(engine.TaskInScope)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	// Two tenants, one cluster. Without an authenticator in front, a caller's
	// namespace is the one the server was configured with, which is what a
	// single-tenant deployment looks like — so two such servers are two tenants
	// as far as the authorization logic is concerned, and that is the logic under
	// test.
	return &tenantFixture{
		teamA: server.New(devServer.Client(), server.WithNamespace("team-a")),
		teamB: server.New(devServer.Client(), server.WithNamespace("team-b")),
	}
}

// gatedWorkflow is a workload that waits for an approval, which is what makes it
// still addressable while the test asks questions about it.
func gatedWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name: "gated",
		Steps: []*v1.Node{
			{
				Id: "request",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "echo",
					Inputs: map[string]*v1.Value{"message": v1.NewLiteral("requesting approval")},
				}},
			},
			{
				Id: "approval",
				Kind: &v1.Node_Wait{Wait: &v1.Wait{
					Kind:    &v1.Wait_Signal{Signal: &v1.Signal{Name: "deploy-approved"}},
					Timeout: durationpb.New(2 * time.Minute),
				}},
			},
			{
				Id:        "deploy",
				Condition: v1.NewExpr("approval.approved"),
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "echo",
					Inputs: map[string]*v1.Value{"message": v1.NewLiteral("deploying")},
				}},
			},
		},
	}
}

// TestAnotherTenantCannotAddressARun is the negative direction, and the one worth
// having: a caller in one tenant must not be able to reach another's run even
// knowing its id exactly.
func TestAnotherTenantCannotAddressARun(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: gatedWorkflow(),
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()
	require.NotEmpty(t, workflowID)

	t.Run("cannot read it", func(t *testing.T) {
		_, err := fixture.teamB.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: workflowID,
		}))
		require.Error(t, err, "another tenant read a run by id")

		// Not found rather than permission denied: denied would confirm that a
		// run with this id exists somewhere, which is the one fact a caller in
		// the wrong tenant should not learn.
		require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})

	t.Run("cannot signal it", func(t *testing.T) {
		_, err := fixture.teamB.Signal(t.Context(), connect.NewRequest(&v1.SignalRequest{
			WorkflowId: workflowID,
			Name:       "deploy-approved",
			Payload: &v1.Node_Outputs{
				NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)},
			},
		}))
		require.Error(t, err, "another tenant unblocked a run it cannot see")
		require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})

	t.Run("its owner still can", func(t *testing.T) {
		// The positive direction, in the same test as the negative one. A check
		// that refused everyone would pass the two subtests above.
		resp, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: workflowID,
		}))
		require.NoError(t, err, "a run's own tenant could not read it")
		require.Equal(t, workflowID, resp.Msg.GetWorkflowId())
	})
}

// TestRunWithNoSuchIdIsNotFound checks that a caller cannot tell a run in another
// tenant from a run that never existed.
func TestRunWithNoSuchIdIsNotFound(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	_, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
		WorkflowId: "flowstate-workflow-does-not-exist",
	}))
	require.Error(t, err)
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

// TestApprovalGateEndToEnd runs the thing the feature exists for, against a real
// Temporal server: a workload blocks on a human approval, the approval arrives
// over the RPC, and the gated step runs.
func TestApprovalGateEndToEnd(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: gatedWorkflow(),
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()

	// The run is waiting, so it is still running — which is the observable
	// difference between a durable wait and a step that blocks a worker.
	require.Eventually(t, func() bool {
		resp, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: workflowID,
		}))
		return err == nil && resp.Msg.GetStatus() == v1.RunResponse_STATUS_RUNNING
	}, 30*time.Second, 100*time.Millisecond, "the run never reached a running state")

	// The approval, as `flow signal` would send it.
	_, err = fixture.teamA.Signal(t.Context(), connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID,
		Name:       "deploy-approved",
		Payload: &v1.Node_Outputs{
			NamedValues: map[string]*v1.Value{
				"approved": v1.NewLiteral(true),
				"by":       v1.NewLiteral("someone@example.com"),
			},
		},
	}))
	require.NoError(t, err)

	var final *connect.Response[v1.GetResponse]
	require.Eventually(t, func() bool {
		resp, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: workflowID,
		}))
		if err != nil || resp.Msg.GetStatus() != v1.RunResponse_STATUS_COMPLETED {
			return false
		}
		final = resp
		return true
	}, 60*time.Second, 200*time.Millisecond, "the run did not complete after being approved")

	outputs := final.Msg.GetOutputs().GetStepValues()

	require.NotNil(t, outputs["approval"], "the gate recorded no outputs")
	require.True(t, outputs["approval"].GetNamedValues()["approved"].GetLiteral().GetBoolValue(),
		"what the approver sent did not reach the workload")

	require.NotNil(t, outputs["deploy"], "the gated step did not run after approval")
}

// TestSignalRejectsAMalformedRequest checks that validation runs before anything
// is addressed, so a bad request is a bad request rather than a lookup.
func TestSignalRejectsAMalformedRequest(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	tests := []struct {
		name    string
		request *v1.SignalRequest
	}{
		{
			name:    "no workflow id",
			request: &v1.SignalRequest{Name: "deploy-approved"},
		},
		{
			name:    "no signal name",
			request: &v1.SignalRequest{WorkflowId: "flowstate-workflow-x"},
		},
		{
			name:    "a signal name that is not one",
			request: &v1.SignalRequest{WorkflowId: "flowstate-workflow-x", Name: "not a name!"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := fixture.teamA.Signal(t.Context(), connect.NewRequest(test.request))
			require.Error(t, err)
			require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
		})
	}
}
