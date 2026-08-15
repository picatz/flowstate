package engine_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// The traversal for #656, rather than the step.
//
// `pkg/flowstate/v1`'s own registrymiss_test.go asserts what
// [v1.TaskNeedsAuthority] *returns* for a task no registry has. That is
// necessary and it is not sufficient: the defect is not a wrong boolean, it is a
// dispatch that scheduled the wrong activity because of one — and a helper-level
// assertion cannot see a dispatch. This file drives the real workflow through
// [executor.dispatch]'s four-way split and asserts which activity was entered.
//
// # Why this path is reachable rather than hypothetical
//
// [v1.TaskNeedsAuthority] reads [v1.LookupTask], which is [v1.DefaultRegistry] —
// what the *workflow worker's* process has — while the activity that then runs
// the task resolves it through [v1.LookupTaskIn] on the activity worker. Those
// are the same process only by deployment convention. Plugin tasks are registered
// into [v1.DefaultRegistry] by a worker's own plugin host (`cmd/flow`'s
// startPlugins) from its own `--plugin-dir`, so a fleet whose workers do not all
// carry the same plugins has workflow workers that miss names their activity
// workers have. Such a worker picks the arm here, and the arm it picked was the
// one that installs no identity: see runtime.go on `ContextWithTaskRuntime`,
// which only the authorized arms install.
//
// The step never reaches a *leak* today, because the plain arm has no secret
// runtime and the resolution fails there. That is the layout being safe rather
// than the code being safe, which is the whole of #656: the routing decision
// itself must not fail open, because the thing catching it downstream is not
// catching it on purpose.

// unregisteredDispatchTask is a name no registry in this build holds — shaped
// like a plugin task's, because that is the real case.
const unregisteredDispatchTask = "acme.provision"

// activityRecorder records which activity type names a run entered.
type activityRecorder struct {
	mu    sync.Mutex
	names []string
}

func (r *activityRecorder) watch(env *testsuite.TestWorkflowEnvironment) {
	env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.names = append(r.names, info.ActivityType.Name)
	})
}

func (r *activityRecorder) entered() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.names...)
}

// TestDispatchOfAnUnknownTaskHoldingASecretTakesTheAuthorizedArm is the negative
// direction at the traversal: the task named here is registered nowhere, so the
// helper's registry lookup misses, and the invocation visibly carries a secret
// reference regardless. Before #656 this scheduled `TaskInScope`/`Task` — the
// arms with no authority to resolve it.
func TestDispatchOfAnUnknownTaskHoldingASecretTakesTheAuthorizedArm(t *testing.T) {
	_, found := v1.LookupTask(unregisteredDispatchTask)
	require.False(t, found,
		"this case is about a registry miss; a build registering %q tests the hit path instead",
		unregisteredDispatchTask)

	recorder := &activityRecorder{}

	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	engine.Register(env, engine.TaskRuntimeConfig{})
	recorder.watch(env)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{
		Workflow: &v1.Workflow{
			Name:    "dispatch-unknown-task",
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{
				{
					Id: "call",
					Kind: &v1.Node_Task{Task: &v1.Task{
						Name: unregisteredDispatchTask,
						Inputs: map[string]*v1.Value{
							"url":    v1.NewLiteral("https://api.example.com/things"),
							"bearer": {Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "fixture-secret", Name: "API_TOKEN"}}},
						},
					}},
				},
			},
		},
		Identity: &v1.WorkloadIdentity{
			Subject:   "someone@example.com",
			Issuer:    "flowstate:test",
			Namespace: "platform",
		},
	})
	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError(),
		"nothing registers this task, so the run fails whichever arm it took — which is "+
			"exactly why the arm has to be asserted rather than the outcome")

	entered := recorder.entered()
	assert.Contains(t, entered, "TaskAuthorized",
		"a step holding a secret reference was not dispatched to the identity-aware "+
			"activity, because a lookup of its task name missed: %v", entered)
	assert.NotContains(t, entered, "Task",
		"it took the arm that installs no task runtime, where the reference could only "+
			"ever fail — the routing decision itself failed open: %v", entered)
	assert.NotContains(t, entered, "TaskInScope",
		"and not the scope-carrying unauthorized arm either: %v", entered)
}

// TestDispatchOfATaskCarryingNoReferenceStaysOnThePlainArm is the pair of
// directions that keep the case above from being a different bug.
//
// #656's answer for a miss is the registry-independent sweep and nothing more, so
// two things have to hold at the dispatch: a *registered* ordinary task still
// takes the plain arm (nothing about the fix leaked into the hit path), and an
// *unknown* task carrying nothing to resolve does too. The second is the one worth
// driving through a real run rather than asserting at the helper: routing it to
// the identity-aware arm would grant a task nothing can describe the run's
// identity and credential runtime, and would replace the permanent
// [v1.ErrorKindUnknownTask] both drivers promise with whatever that arm answers.
func TestDispatchOfATaskCarryingNoReferenceStaysOnThePlainArm(t *testing.T) {
	run := func(t *testing.T, task *v1.Task) []string {
		t.Helper()

		recorder := &activityRecorder{}

		suite := &testsuite.WorkflowTestSuite{}
		env := suite.NewTestWorkflowEnvironment()
		engine.Register(env, engine.TaskRuntimeConfig{})
		recorder.watch(env)

		env.ExecuteWorkflow(engine.Run, &v1.RunState{
			Workflow: &v1.Workflow{
				Name:    "dispatch-no-reference",
				Profile: v1.CurrentProfile,
				Steps:   []*v1.Node{{Id: "report", Kind: &v1.Node_Task{Task: task}}},
			},
			Identity: &v1.WorkloadIdentity{
				Subject:   "someone@example.com",
				Issuer:    "flowstate:test",
				Namespace: "platform",
			},
		})
		require.True(t, env.IsWorkflowCompleted())

		return recorder.entered()
	}

	t.Run("a registered ordinary task", func(t *testing.T) {
		entered := run(t, &v1.Task{
			Name:   "log",
			Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hello")},
		})

		assert.Contains(t, entered, "Task",
			"an ordinary registered task must stay on the activity name replay "+
				"compatibility depends on: %v", entered)
		assert.NotContains(t, entered, "TaskAuthorized",
			"the miss answer leaked into the hit path, which is a different bug in the "+
				"same function: %v", entered)
	})

	t.Run("an unknown task with nothing to resolve", func(t *testing.T) {
		entered := run(t, &v1.Task{
			Name:   unregisteredDispatchTask,
			Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hello")},
		})

		assert.Contains(t, entered, "Task",
			"nothing in this invocation asked for a credential, so nothing justifies "+
				"handing it the run's identity and credential runtime: %v", entered)
		assert.NotContains(t, entered, "TaskAuthorized",
			"and the permanent unknown-task classification both drivers promise depends "+
				"on this arm being the one a worker certainly has: %v", entered)
	})
}
