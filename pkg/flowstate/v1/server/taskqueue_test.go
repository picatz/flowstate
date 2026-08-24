package server_test

import (
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// taskQueueOf reads the queue Temporal recorded a run on.
//
// Read back from the server rather than asserted against the options this
// package built, because what a run is actually addressed to is the only thing a
// worker fleet can be pointed at — and an options struct that says the right
// thing while the submission says another is exactly the failure a routing test
// exists to catch.
func taskQueueOf(t *testing.T, temporal client.Client, workflowID string) string {
	t.Helper()

	description, err := temporal.DescribeWorkflowExecution(t.Context(), workflowID, "")
	require.NoError(t, err)

	return description.GetExecutionConfig().GetTaskQueue().GetName()
}

// TestRunsGoToTheSharedQueueWhenNothingIsRouted is the "nothing existing moves"
// assertion at the level that matters — a real submission against a real
// Temporal, read back — rather than at the level of the function that composes
// the name.
//
// Byte-identical, and spelled out as a literal rather than as the constant, so
// that renaming the constant fails here instead of quietly moving every existing
// deployment's fleet onto a queue nothing is polling.
func TestRunsGoToTheSharedQueueWhenNothingIsRouted(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)

	spec := &v1.Workflow{Name: "routing", Steps: []*v1.Node{bulky("only", 1)}}

	for _, tenant := range []string{"", "team-a", "team-b"} {
		flowstate := mustNew(t, temporal, server.WithNamespace(tenant))

		response, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{Workflow: spec}))
		require.NoError(t, err)

		require.Equal(t, "flowstate-run-task-queue", taskQueueOf(t, temporal, response.Msg.GetWorkflowId()),
			"a deployment that routes nothing must submit every tenant's runs exactly where it always did")
		require.Equal(t, engine.RunTaskQueueName, taskQueueOf(t, temporal, response.Msg.GetWorkflowId()))
	}
}

// TestRunsGoToTheirOwnTenantsQueueWhenRouted is the positive half, and it is
// only half: it says routing works, not that anything is contained. The
// containment is the worker's refusal — see
// engine_test.TestWorkerForOneTenantRefusesAnotherTenantsRun — and the fact that
// no two tenants can be made to name one queue, which
// engine_test.TestTaskQueueNamesCannotBeForged asserts over the whole cross
// product.
//
// What this does add is that the queue comes from the *authenticated* tenant.
// The request names no namespace and cannot: a workload able to name its own
// queue could name the fleet that executes it, which is the same rule the
// namespace memo and the fairness key already follow.
func TestRunsGoToTheirOwnTenantsQueueWhenRouted(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)

	spec := &v1.Workflow{Name: "routing", Steps: []*v1.Node{bulky("only", 1)}}
	queues := engine.TaskQueues{Prefix: "flowstate-run"}

	for tenant, expected := range map[string]string{
		"team-a": "flowstate-run_team-a",
		"team-b": "flowstate-run_team-b",
		// The default tenant of an untenanted deployment gets a queue too, and
		// it is not the one a tenant literally named "default" would get.
		"":        "flowstate-run__default",
		"default": "flowstate-run_default",
	} {
		flowstate := mustNew(t, temporal, server.WithNamespace(tenant), server.WithTaskQueues(queues))

		response, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{Workflow: spec}))
		require.NoError(t, err)

		require.Equal(t, expected, taskQueueOf(t, temporal, response.Msg.GetWorkflowId()),
			"tenant %q was not routed to its own queue", tenant)
	}
}

// TestRoutingRefusesATenantItCannotPlace is the fail-closed direction at the
// submission boundary.
//
// A recorded namespace outside the grammar is a real case — server.go's own
// comment notes an identity can carry one that predates auth.ValidateNamespace —
// and the routed deployment's answer to it must not be "the queue everybody else
// uses". That fallback is precisely what temporalclient.Pool.For refuses to make
// for the Temporal namespace, for the same reason: a refusal is a
// misconfiguration someone fixes, a fallback is a tenancy breach nobody notices.
//
// The ungrammatical tenant arrives on the *caller*, which is the only way it can
// now: [server.WithNamespace] validates what a deployment configures (see
// TestNewRefusesANamespaceOutsideTheGrammar), so the deployment's own fallback
// can no longer be one. An identity carrying a namespace from before that
// grammar existed still can, and is exactly what this refusal is for — which is
// also why writing this test through the option was always the weaker version of
// it: it proved the handler refuses a value only a misconfigured process could
// hold, rather than one a request can actually arrive with.
func TestRoutingRefusesATenantItCannotPlace(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)

	spec := &v1.Workflow{Name: "routing", Steps: []*v1.Node{bulky("only", 1)}}

	ctx := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Subject:   "someone@example.com",
		Issuer:    "https://issuer.example.com",
		Namespace: "Prod Team",
	})

	flowstate := mustNew(t, temporal,
		server.WithTaskQueues(engine.TaskQueues{Prefix: "flowstate-run"}))

	_, err := flowstate.Run(ctx, connect.NewRequest(&v1.RunRequest{Workflow: spec}))
	require.Error(t, err)
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	require.ErrorContains(t, err, "cannot be placed on one")

	// The same tenant on an unrouted deployment still starts, because the
	// unconfigured path never looks at the namespace at all. Asserted here and
	// not only in the engine's unit test, because "the default path did not
	// change" is a claim about this handler.
	unrouted := mustNew(t, temporal)
	response, err := unrouted.Run(ctx, connect.NewRequest(&v1.RunRequest{Workflow: spec}))
	require.NoError(t, err)
	require.Equal(t, engine.RunTaskQueueName, taskQueueOf(t, temporal, response.Msg.GetWorkflowId()))
}
