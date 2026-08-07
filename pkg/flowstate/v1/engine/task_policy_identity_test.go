package engine_test

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// TestTaskPolicyIdentityNamespaceDenial checks #187 slice 1's motivating
// case named in the issue's own design record — "codex denied outside the
// platform team" — against a *real* attested identity, which only the
// durable driver can carry through to a task dispatch (see
// [tests.TaskPolicyCases]' own doc for why the shared, both-drivers case set
// deliberately stays on `task` alone: the local driver's [v1.Scope.identity]
// is always empty, by design, for every local run — [flowstatev1_test
// .TestRunIdentityShapeLocal] pins that same fact for `run.identity`).
//
// This is the durable-only half of that coverage, mirroring how
// TestRunIdentityShapeDurable is the durable-only half of #215's shared
// identity assertion: a scope's identity comes from RunState.Identity,
// carried across Continue-As-New, and reaches [engine.TaskInScope] as the
// scope the task activity actually receives.
func TestTaskPolicyIdentityNamespaceDenial(t *testing.T) {
	policy, err := v1.TaskPolicyConfig{
		Deny: []string{`task == "http" && identity.namespace != "platform"`},
	}.Policy()
	require.NoError(t, err)

	v1.SetDefaultTaskPolicy(policy)
	t.Cleanup(func() { v1.SetDefaultTaskPolicy(nil) })

	// "http" rather than a plugin task like "codex.exec": it is registered in
	// this build and needs previous outputs ([v1.TaskNeedsPrevOutputs]),
	// which is what makes the engine schedule it through [engine.TaskInScope]
	// — the activity entry point that actually carries the run's [v1.Scope],
	// and therefore its real identity, rather than [engine.Task], which
	// predates scopes and never carries one (see [engine.Task]'s own policy
	// check). Pointed at a loopback address so a dispatch the policy lets
	// through still fails, deterministically and without a network — but
	// fails at egress policy, inside the task, which is the "reached the
	// task" signal this case actually needs to distinguish from "refused
	// before dispatch."
	workflow := func() *v1.Workflow {
		return &v1.Workflow{
			Name:    "task-policy-identity-namespace",
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{
				{
					Id: "run-codex",
					Kind: &v1.Node_Task{Task: &v1.Task{
						Name: "http",
						Inputs: map[string]*v1.Value{
							"url": v1.NewLiteral("http://127.0.0.1:1/"),
						},
					}},
				},
			},
		}
	}

	run := func(t *testing.T, namespace string) error {
		t.Helper()

		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestWorkflowEnvironment()
		env.RegisterWorkflow(engine.Run)
		// codex.exec is a plugin task this build does not register; a
		// dispatch that reaches the activity at all fails as "unknown task"
		// rather than as a policy denial, which is exactly what tells the
		// two failure modes apart below — a policy denial must never let the
		// activity run at all, per the design record's "the plugin never
		// participates in its own policing" and invariant 7's echo (no
		// credential resolves for a denied call; here, no activity dispatches
		// for one either).
		env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
		env.OnActivity(engine.TaskWithPrev, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskWithPrev)
		env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)

		env.ExecuteWorkflow(engine.Run, &v1.RunState{
			Workflow: workflow(),
			Identity: &v1.WorkloadIdentity{
				Subject:   "someone@example.com",
				Issuer:    "flowstate:test",
				Namespace: namespace,
			},
		})
		require.True(t, env.IsWorkflowCompleted())

		return env.GetWorkflowError()
	}

	t.Run("outside platform namespace is denied by the policy, never reaching the task", func(t *testing.T) {
		err := run(t, "some-other-team")
		require.Error(t, err)
		require.Contains(t, err.Error(), "http")
		require.Contains(t, err.Error(), "task-shape policy")
		require.NotContains(t, err.Error(), "egress",
			"a policy denial must fire before the task ever runs — an egress "+
				"denial here would mean the http task's own Fn was reached, "+
				"which means the task-shape check did not run first")
	})

	t.Run("inside platform namespace reaches the task (and only fails at egress, proving the policy let it through)", func(t *testing.T) {
		err := run(t, "platform")
		require.Error(t, err, "127.0.0.1 is denied by the default egress policy, so the dispatch still fails")
		require.Contains(t, err.Error(), "egress",
			"a platform caller must clear the task-shape policy and reach the "+
				"task itself, not be refused by the policy")
		require.NotContains(t, err.Error(), "task-shape policy")
	})
}
