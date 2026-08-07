package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// TestWorkerTaskQueueDefaultIsUnchanged is the command-line half of "nothing
// existing moves": a worker started the way every existing deployment starts one
// polls exactly the queue it always polled.
func TestWorkerTaskQueueDefaultIsUnchanged(t *testing.T) {
	queue, err := workerTaskQueue(temporalFlags{taskQueue: engine.RunTaskQueueName})
	require.NoError(t, err)
	require.Equal(t, "flowstate-run-task-queue", queue)

	// And an explicit --task-queue with no tenancy still wins, which is how a
	// deployment that named its queues before any of this existed keeps working.
	queue, err = workerTaskQueue(temporalFlags{taskQueue: "mine", taskQueueExplicit: true})
	require.NoError(t, err)
	require.Equal(t, "mine", queue)
}

// TestWorkerTaskQueueDerivesTheTenantsQueue checks the worker composes the same
// name the server does — from the same function, which is the only way the two
// can be relied on to agree. A worker that spelled it differently would poll a
// queue nothing submits to and simply do nothing, forever, reporting nothing.
func TestWorkerTaskQueueDerivesTheTenantsQueue(t *testing.T) {
	for tenant, expected := range map[string]string{
		"team-a":  "flowstate-run_team-a",
		"":        "flowstate-run__default",
		"default": "flowstate-run_default",
	} {
		queue, err := workerTaskQueue(temporalFlags{
			taskQueue:       engine.RunTaskQueueName,
			taskQueuePrefix: "flowstate-run",
			tenant:          tenant,
			tenantSet:       true,
		})
		require.NoError(t, err, "tenant %q", tenant)
		require.Equal(t, expected, queue)

		// The server's own answer for the same tenant, byte for byte.
		fromServer, err := engine.TaskQueues{Prefix: "flowstate-run"}.For(tenant)
		require.NoError(t, err)
		require.Equal(t, fromServer, queue)
	}
}

// TestWorkerRefusesATenantOnTheSharedQueue is the refusal that keeps `--tenant`
// from being a weapon pointed at everybody else.
//
// A tenant-restricted worker fails every run that is not its own. On the shared
// queue — where an unrouted deployment submits every tenant's runs — it would
// race the general fleet for those runs and fail the ones it won, turning a flag
// meant to contain one misconfiguration into an outage for every other tenant.
// So the combination is refused at startup rather than documented as a thing not
// to do.
func TestWorkerRefusesATenantOnTheSharedQueue(t *testing.T) {
	_, err := workerTaskQueue(temporalFlags{
		taskQueue: engine.RunTaskQueueName,
		tenant:    "team-a",
		tenantSet: true,
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "needs a task queue of its own")
	require.ErrorContains(t, err, "--task-queue-prefix")
	require.ErrorContains(t, err, engine.RunTaskQueueName)
}

// TestWorkerRefusesAPrefixWithNoTenant is the mirror image: the queue a prefix
// composes is a function of the tenant, so half the pair addresses nothing.
// Refused rather than defaulted, for the reason DeploymentOptions refuses half a
// worker version — dropping silently to the other posture is the worst of the
// available answers, because the operator asked for something and got neither
// it nor a word about it.
func TestWorkerRefusesAPrefixWithNoTenant(t *testing.T) {
	_, err := workerTaskQueue(temporalFlags{
		taskQueue:       engine.RunTaskQueueName,
		taskQueuePrefix: "flowstate-run",
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "--tenant")
}

// TestWorkerRefusesATenantItCouldNeverBeSent checks the worker validates its own
// tenant against the one grammar a namespace is admitted by, so a fleet cannot
// be started for a tenant no caller could ever authenticate as — which would
// poll a queue nothing submits to and look exactly like a working worker.
func TestWorkerRefusesATenantItCouldNeverBeSent(t *testing.T) {
	for _, tenant := range []string{"Prod Team", "team_a", "-leading", ".."} {
		_, err := workerTaskQueue(temporalFlags{
			taskQueue:       engine.RunTaskQueueName,
			taskQueuePrefix: "flowstate-run",
			tenant:          tenant,
			tenantSet:       true,
		})
		require.Error(t, err, "tenant %q", tenant)
		require.ErrorContains(t, err, "--tenant is not a namespace")
	}

	require.NoError(t, auth.ValidateNamespace("team-a"))
}

// TestWorkerRefusesAnUnusableTaskQueuePrefix checks the prefix is validated when
// the command line is read, not when a run arrives — there being no run to tell.
func TestWorkerRefusesAnUnusableTaskQueuePrefix(t *testing.T) {
	_, err := workerTaskQueue(temporalFlags{
		taskQueue:       engine.RunTaskQueueName,
		taskQueuePrefix: "flowstate_run",
		tenant:          "team-a",
		tenantSet:       true,
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "must be spelled like a namespace")
}

// TestRoutableTenantsIncludesTheDefault checks the mapping-completeness report
// covers the tenant nobody wrote a name for.
//
// A trust policy with a Default places every namespace it has no entry for,
// including the empty one an unauthenticated caller belongs to. That tenant has
// a queue like any other and is the one most easily left without a fleet,
// because it never appears in the mapping an operator is reading.
func TestRoutableTenantsIncludesTheDefault(t *testing.T) {
	require.Nil(t, routableTenants(nil))

	require.Equal(t, []string{"team-a", "team-b"}, routableTenants(&auth.Tenancy{
		Temporal: map[string]string{"team-b": "ns-b", "team-a": "ns-a"},
	}))

	require.Equal(t, []string{"", "team-a"}, routableTenants(&auth.Tenancy{
		Temporal: map[string]string{"team-a": "ns-a"},
		Default:  "shared",
	}))
}
