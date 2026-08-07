package engine_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
)

// tenantWorker builds a test environment standing in for a worker started with
// `flow worker --tenant <namespace>`: the interpreter registered exactly as
// [engine.Register] registers it, behind exactly the interceptor
// cmd/flow/main.go installs.
func tenantWorker(t *testing.T, tenant string) *testsuite.TestWorkflowEnvironment {
	t.Helper()

	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{
		Interceptors: []interceptor.WorkerInterceptor{engine.TenantInterceptor(tenant)},
	})
	env.RegisterWorkflow(engine.Run)
	env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task).Maybe()

	return env
}

func runFor(namespace string) *v1.RunState {
	return &v1.RunState{
		Workflow: tests.RunIdentityWorkflow(),
		Identity: &v1.WorkloadIdentity{
			Subject:   "release-requester@example.com",
			Issuer:    "flowstate:test",
			Namespace: namespace,
		},
	}
}

// TestWorkerForOneTenantRefusesAnotherTenantsRun is the negative direction the
// house rule asks for, and the point of `flow worker --tenant`.
//
// The functionality test wearing a security test's clothes would be "a worker
// for team-a runs team-a's work" — which is [TestWorkerForOneTenantRunsItsOwn]
// below, kept only as a control. What this asserts is the *refusal*: team-a's
// run reaching team-b's worker does not execute, and does not execute in a way
// that leaves the run wedged either. It fails, terminally, with a reason.
//
// A run that reached the wrong fleet and simply ran would be undetectable
// afterwards: every later request about it is authorized against its own
// recorded tenant and answers correctly, so nothing anywhere would report that
// team-b's secrets, egress policy and plugins had been the ones executing it.
func TestWorkerForOneTenantRefusesAnotherTenantsRun(t *testing.T) {
	env := tenantWorker(t, "team-b")

	env.ExecuteWorkflow(engine.Run, runFor("team-a"))

	require.True(t, env.IsWorkflowCompleted())

	err := env.GetWorkflowError()
	require.Error(t, err, "a run belonging to another tenant must be refused, not executed")
	require.Contains(t, err.Error(), "this worker executes one tenant's workloads only")
	require.Contains(t, err.Error(), `this run belongs to namespace "team-a"`)
	require.Contains(t, err.Error(), "see flow worker --tenant")

	// Never the worker's own tenant: this failure lands in team-a's run history,
	// and naming team-b there would disclose the deployment's tenancy to a
	// tenant that has no business knowing it — the same rule
	// FlowstateServer.clientFor follows when it refuses.
	require.NotContains(t, err.Error(), "team-b")

	// Terminal, not retried: see [engine.TenantInterceptor] on why a
	// misconfiguration that eventually succeeds is worse than one that fails.
	var app *temporal.ApplicationError
	require.ErrorAs(t, err, &app)
	require.True(t, app.NonRetryable())
	require.Equal(t, v1.ErrorKindPolicyDenied.String(), app.Type())
}

// TestWorkerForTheDefaultTenantRefusesANamedTenantsRun probes the direction the
// env secrets provider's bug was found in: not two named tenants, but the
// *default* tenant against a named one.
//
// That pairing is where the separator ambiguity lived — the default tenant and
// namespace "team" both reaching one variable — and it is the pairing a
// deployment reaches by accident, because the default tenant is the one nobody
// wrote a name for. Both directions are asserted, because "A cannot reach B" and
// "B cannot reach A" are different claims.
func TestWorkerForTheDefaultTenantRefusesANamedTenantsRun(t *testing.T) {
	t.Run("default worker, named tenant's run", func(t *testing.T) {
		env := tenantWorker(t, "")

		env.ExecuteWorkflow(engine.Run, runFor("team"))

		require.True(t, env.IsWorkflowCompleted())
		require.ErrorContains(t, env.GetWorkflowError(), `this run belongs to namespace "team"`)
	})

	t.Run("named worker, default tenant's run", func(t *testing.T) {
		env := tenantWorker(t, "team")

		env.ExecuteWorkflow(engine.Run, runFor(""))

		require.True(t, env.IsWorkflowCompleted())
		err := env.GetWorkflowError()
		require.ErrorContains(t, err, `this run belongs to namespace ""`)
		require.NotContains(t, err.Error(), `namespace "team"`)
	})
}

// TestWorkerForOneTenantRefusesARunWithNoTenant is the fail-closed arm: a run
// whose identity was never established carries no namespace, and a worker
// restricted to one tenant cannot tell whose work it is. It is refused rather
// than treated as the default tenant's, unless the worker is the default
// tenant's — which the second half asserts, because that run *is* the default
// tenant's and refusing it would break an untenanted deployment.
func TestWorkerForOneTenantRefusesARunWithNoTenant(t *testing.T) {
	env := tenantWorker(t, "team-a")

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: tests.RunIdentityWorkflow()})

	require.True(t, env.IsWorkflowCompleted())
	require.ErrorContains(t, env.GetWorkflowError(), "this worker executes one tenant's workloads only")

	def := tenantWorker(t, "")
	def.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: tests.RunIdentityWorkflow()})
	require.True(t, def.IsWorkflowCompleted())
	require.NoError(t, def.GetWorkflowError())
}

// TestWorkerForOneTenantRunsItsOwn is the control, and only the control. It
// proves the guard is not simply refusing everything, which is the failure mode
// that would make every assertion above pass for the wrong reason.
func TestWorkerForOneTenantRunsItsOwn(t *testing.T) {
	env := tenantWorker(t, "team-a")

	env.ExecuteWorkflow(engine.Run, runFor("team-a"))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var outputs v1.Workflow_StepOutputs
	require.NoError(t, env.GetWorkflowResult(&outputs))
	tests.AssertRunIdentityShape(t, &outputs, false, "release-requester@example.com")
}

// TestUnrestrictedWorkerIsUnchanged is the other half of "nothing existing
// moves": a worker that declares no tenant installs no interceptor and runs
// every tenant's work, exactly as it did before this existed.
func TestUnrestrictedWorkerIsUnchanged(t *testing.T) {
	for _, namespace := range []string{"", "team-a", "team-b"} {
		suite := &testsuite.WorkflowTestSuite{}
		env := suite.NewTestWorkflowEnvironment()
		env.RegisterWorkflow(engine.Run)
		env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task).Maybe()

		env.ExecuteWorkflow(engine.Run, runFor(namespace))

		require.True(t, env.IsWorkflowCompleted())
		require.NoError(t, env.GetWorkflowError(), "namespace %q", namespace)
	}
}

// TestTenantRefusalNamesTheFlag keeps the diagnostic honest: a refusal that says
// only "refused" leaves an operator with a failed run and nowhere to look.
func TestTenantRefusalNamesTheFlag(t *testing.T) {
	env := tenantWorker(t, "team-b")
	env.ExecuteWorkflow(engine.Run, runFor("team-a"))

	message := env.GetWorkflowError().Error()
	require.True(t, strings.Contains(message, "routing misconfiguration"),
		"the refusal must say what kind of problem this is, got: %s", message)
}
