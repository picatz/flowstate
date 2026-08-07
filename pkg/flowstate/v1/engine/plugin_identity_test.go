package engine_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
)

// registerPluginIdentityTask puts [tests.PluginIdentityTaskDef] where the
// durable driver's activities can actually find it.
//
// The activity that executes a step runs in a fresh context.Context Temporal
// hands it, not one derived from the workflow's — the same boundary #235's fix
// crosses with plugin.NewContextWithIdentity rather than relying on anything
// carrying over automatically. A registry has the identical problem: a task
// registered only on a context-scoped [v1.Registry] is invisible to the
// activity side, which is why [plugin.Host.Register]'s own doc says a host
// registered into anything but [v1.DefaultRegistry] "holds tasks nothing will
// ever look up." Registering here, at package init through a sync.Once,
// follows the one convention this repository already has for it —
// pkg/flowstate/v1/plugin/reachable_test.go's global registration is a
// one-way door for the same reason and the same fix.
//
// needsScope selects which unauthorized entry point the workflow schedules —
// see [tests.PluginIdentityTaskDef]'s doc for why both are worth exercising
// separately. Register overwrites rather than erroring on a name already
// present, so reusing the one global name across these tests — run
// sequentially, none opting into t.Parallel — is safe.
func registerPluginIdentityTask(t *testing.T, needsScope bool) {
	t.Helper()
	require.NoError(t, v1.DefaultRegistry().Register(tests.PluginIdentityTaskDef(needsScope)))
}

// runPluginIdentityDurable installs identity the way the durable driver
// actually does it in production — as [v1.RunState.Identity], read by
// engine/runtime.go's taskActivities.context and engine/activities.go's Task
// and TaskInScope — and runs [tests.PluginIdentityStep] through [engine.Run]
// on a Temporal test environment.
func runPluginIdentityDurable(t *testing.T, needsScope bool, identity *v1.WorkloadIdentity) (subject, namespace string, present bool) {
	t.Helper()
	registerPluginIdentityTask(t, needsScope)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	engine.Register(env)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{
		Workflow: tests.PluginIdentityStep("plugin-identity-durable", "call"),
		Identity: identity,
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var out v1.Workflow_StepOutputs
	require.NoError(t, env.GetWorkflowResult(&out))

	values := out.GetStepValues()["call"].GetNamedValues()
	return values["subject"].GetLiteral().GetStringValue(),
		values["namespace"].GetLiteral().GetStringValue(),
		values["present"].GetLiteral().GetBoolValue()
}

// TestPluginTaskObservesCallerDurable is the second of #235's two driver
// callers: a plugin task run under the durable driver observes the run's
// authenticated identity, on both of the entry points a plugin task with no
// secret authority can reach — TaskInScope, via Scope.Identity, and the plain
// Task activity, via the identity parameter #187 threads into it.
// TestPluginTaskObservesCallerLocal in plugin_identity_local_test.go is the
// first driver caller.
func TestPluginTaskObservesCallerDurable(t *testing.T) {
	for _, needsScope := range []bool{true, false} {
		t.Run(map[bool]string{true: "TaskInScope", false: "Task"}[needsScope], func(t *testing.T) {
			subject, namespace, present := runPluginIdentityDurable(t, needsScope, &v1.WorkloadIdentity{
				Subject: "svc-reader", Issuer: "https://issuer.example", Namespace: "team-a",
			})

			require.True(t, present, "the plugin task's context carried no identity at all")
			require.Equal(t, "svc-reader", subject)
			require.Equal(t, "team-a", namespace)
		})
	}
}

// TestPluginTaskCallerNotStickyAcrossRunsDurable is the tenancy direction on
// the durable driver: two runs of the identical registered TaskDef, back to
// back, under two different identities, must not leak one run's caller into
// the other's. taskActivities.context and Task/TaskInScope build the context
// fresh from each activity invocation's own RunState-derived identity
// argument rather than from anything captured once at plugin load or worker
// registration, which is the closure-at-load trap #235's fix guidance names —
// this is what proves this driver does not fall into it either.
func TestPluginTaskCallerNotStickyAcrossRunsDurable(t *testing.T) {
	firstSubject, firstNamespace, _ := runPluginIdentityDurable(t, true, &v1.WorkloadIdentity{
		Subject: "svc-a", Issuer: "https://issuer.example", Namespace: "team-a",
	})
	secondSubject, secondNamespace, _ := runPluginIdentityDurable(t, true, &v1.WorkloadIdentity{
		Subject: "svc-b", Issuer: "https://issuer.example", Namespace: "team-b",
	})

	require.Equal(t, "svc-a", firstSubject)
	require.Equal(t, "team-a", firstNamespace)
	require.Equal(t, "svc-b", secondSubject)
	require.Equal(t, "team-b", secondNamespace)
	require.NotEqual(t, firstNamespace, secondNamespace,
		"two runs with different identities must each see their own namespace, not a value stuck from the first run")
}

// TestPluginTaskCallerExplicitlyEmptyDurable is the negative shape: a
// [v1.RunState] with no Identity set still reaches the plugin task with an
// explicit, present, empty caller rather than a crash — matching
// [v1.ProtoWorkloadIdentity]'s rule for the local driver's own unestablished
// case in plugin_identity_local_test.go, and what a plugin-side
// sdk.CallerFromContext is written to expect.
func TestPluginTaskCallerExplicitlyEmptyDurable(t *testing.T) {
	subject, namespace, present := runPluginIdentityDurable(t, true, nil)

	require.True(t, present, "a run with no identity must still cross as an explicit empty caller, not as no context value at all")
	require.Empty(t, subject)
	require.Empty(t, namespace)
}
