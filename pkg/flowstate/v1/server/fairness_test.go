package server_test

import (
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// One task queue serves every tenant, so scheduling is where a multi-tenant
// deployment fails first. Without a fairness key the queue is
// first-come-first-served, and a tenant is one large workload away from everyone
// else's work sitting behind theirs — not deliberately, which is what makes it
// likely. A five-thousand-iteration loop is an ordinary thing to write.

// fairnessKeyOf reads the key Temporal recorded for a run.
func fairnessKeyOf(t *testing.T, temporal client.Client, workflowID string) string {
	t.Helper()

	description, err := temporal.DescribeWorkflowExecution(t.Context(), workflowID, "")
	require.NoError(t, err)

	return description.GetWorkflowExecutionInfo().GetPriority().GetFairnessKey()
}

// TestARunIsScheduledUnderItsOwnTenant covers the property, and the fact that it
// comes from the right place.
//
// The key is taken from the authenticated identity and never from the request —
// the same rule the Temporal namespace already follows. A workload that could name
// its own scheduling bucket would mean the first thing anybody writes is the one
// that puts them in a bucket of their own.
func TestARunIsScheduledUnderItsOwnTenant(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)

	spec := &v1.Workflow{
		Name:  "fairness",
		Steps: []*v1.Node{bulky("only", 8)},
	}

	for _, tenant := range []string{"team-a", "team-b"} {
		t.Run(tenant, func(t *testing.T) {
			flowstate := mustNew(t, temporal, server.WithNamespace(tenant))

			response, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{Workflow: spec}))
			require.NoError(t, err)

			require.Equal(t, tenant, fairnessKeyOf(t, temporal, response.Msg.GetWorkflowId()),
				"the run is not scheduled under its own tenant, so one tenant's work can crowd out another's")
		})
	}
}

// TestAnUntenantedRunCarriesNoFairnessKey keeps the zero-configuration path
// unchanged.
//
// Where there is one tenant there is nothing to be fair between, and the empty key
// is Temporal's own default rather than a special case invented here.
func TestAnUntenantedRunCarriesNoFairnessKey(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	flowstate := mustNew(t, temporal)

	response, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: &v1.Workflow{Name: "untenanted", Steps: []*v1.Node{bulky("only", 8)}},
	}))
	require.NoError(t, err)

	require.Empty(t, fairnessKeyOf(t, temporal, response.Msg.GetWorkflowId()))
}

// TestAFairnessKeySurvivesContinueAsNew is the one that would have been found late.
//
// Fairness matters most for exactly the workloads that suspend — a long loop is
// both the thing that crowds a queue and the thing that continues as new — so a
// key that were dropped at the first suspension would protect precisely the runs
// that never needed it. Temporal carries priority to the new run on its own; this
// asserts that rather than trusting it, because it is not a property this code
// sets and would break silently.
func TestAFairnessKeySurvivesContinueAsNew(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)

	flowstate := mustNew(t, temporal, server.WithNamespace("team-a"),
		// One step per run, so the workload suspends between each.
		server.WithMaxStepsPerRun(1))

	response, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: &v1.Workflow{
			Name:  "suspends",
			Steps: []*v1.Node{bulky("a", 8), bulky("b", 8), bulky("c", 8)},
		},
	}))
	require.NoError(t, err)

	workflowID := response.Msg.GetWorkflowId()
	first := response.Msg.GetRunId()

	require.Eventually(t, func() bool {
		description, err := temporal.DescribeWorkflowExecution(t.Context(), workflowID, "")
		if err != nil {
			return false
		}
		return description.GetWorkflowExecutionInfo().GetExecution().GetRunId() != first
	}, 60*time.Second, 200*time.Millisecond, "the run never continued as new")

	require.Equal(t, "team-a", fairnessKeyOf(t, temporal, workflowID),
		"the fairness key was dropped at continue-as-new, so a long workload loses the "+
			"protection exactly when it starts needing it")
}
