package engine_test

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
)

// Review of PR #228 found #187 slice 1's first cut checked three of what
// turned out to be five task-executing activity entry points: [engine.Task],
// [engine.TaskWithPrev], and [engine.TaskInScope] carried the check, but
// [taskActivities.TaskAuthorized] and [taskActivities.TaskInScopeAuthorized]
// (runtime.go) — the two arms [v1.TaskNeedsAuthority] selects — did not. That
// is exactly backwards: those two are the tasks that resolve secrets and act
// under the run's own identity, which is precisely the shape a deployment's
// task-shape policy exists to gate. This file is the two directions of
// coverage that gap needed: an authority-carrying task denied on the
// authorized path (here, and via a compensation, which reaches the identical
// arms — see [executor.dispatch]'s "four activities... two axes"), and the
// negative direction proving the fix discriminates rather than denying
// everything.

// authorityTask returns a task whose `bearer` input holds a secret
// reference, which is what [v1.TaskNeedsAuthority] scans for — the shape
// that selects [taskActivities.TaskAuthorized] /
// [taskActivities.TaskInScopeAuthorized] in [executor.dispatch] rather than
// [engine.Task] / [engine.TaskInScope].
func authorityTask(baseURL string) *v1.Task {
	return &v1.Task{
		Name: "http",
		Inputs: map[string]*v1.Value{
			"url":    v1.NewLiteral(baseURL + "/status/200"),
			"bearer": {Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "fixture-secret", Name: "API_TOKEN"}}},
		},
	}
}

// newAuthorizedTestEnv builds a test environment through [engine.Register] —
// rather than registering each activity by hand, as the other task-policy
// test files in this package do — because these cases need the real
// [taskActivities.TaskAuthorized] / [taskActivities.TaskInScopeAuthorized]
// activities, which only [engine.Register] wires up (they close over the
// worker's [engine.TaskRuntimeConfig] and cannot be named directly the way
// [engine.Task]/[engine.TaskInScope] can — see versioning.go's own
// registration list).
//
// The config is deliberately empty: no secret store, no policy, no broker.
// See [authorityTask]'s doc for what that buys these cases.
func newAuthorizedTestEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()

	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	engine.Register(env, engine.TaskRuntimeConfig{})
	return env
}

// TestTaskPolicyDeniesAuthorityCarryingTaskDurably is the durable-only
// authority-path counterpart to
// [TestTaskPolicyIdentityNamespaceDenial]: a deny rule over `identity.namespace`
// refuses an authority-carrying task on the authorized activity arm, before
// the task ever reaches [v1.ResolveSecret] — no credential is resolved for a
// denied call, the deployment-side echo of invariant 7 the design record for
// #187 states.
func TestTaskPolicyDeniesAuthorityCarryingTaskDurably(t *testing.T) {
	policy, err := v1.TaskPolicyConfig{
		Deny: []string{`task == "http" && identity.namespace != "platform"`},
	}.Policy()
	require.NoError(t, err)
	v1.SetDefaultTaskPolicy(policy)
	t.Cleanup(func() { v1.SetDefaultTaskPolicy(nil) })

	baseURL := tests.NewHTTPServer(t)

	workflow := func() *v1.Workflow {
		return &v1.Workflow{
			Name:    "task-policy-authority",
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{
				{Id: "authorized-call", Kind: &v1.Node_Task{Task: authorityTask(baseURL)}},
			},
		}
	}

	run := func(t *testing.T, namespace string) error {
		t.Helper()

		env := newAuthorizedTestEnv(t)
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

	t.Run("outside platform namespace is denied before any secret resolves", func(t *testing.T) {
		err := run(t, "some-other-team")
		require.Error(t, err)
		require.Contains(t, err.Error(), "task-shape policy")
		require.NotContains(t, err.Error(), "secret access is not configured",
			"a policy denial must fire before the task ever runs — reaching the "+
				"unconfigured-secret-store failure would mean the authorized "+
				"activity's check did not run first")
	})

	t.Run("inside platform namespace reaches the task (and only fails on the unconfigured secret store)", func(t *testing.T) {
		err := run(t, "platform")
		require.Error(t, err, "no secret store is configured in this test, so the dispatch still fails")
		require.Contains(t, err.Error(), "secret access is not configured")
		require.NotContains(t, err.Error(), "task-shape policy")
	})
}

// TestTaskPolicyDeniesAuthorityCarryingCompensationDurably is the
// compensation half of the same gap: a step's `undo:` dispatches through the
// identical four arms [executor.dispatch] uses for an ordinary step (see its
// own "a compensation goes through the same four arms" comment), so an
// authority-carrying compensation must be governed by the same task-shape
// policy an authority-carrying forward step is.
//
// The forward step uses `log` — no authority, always dispatches through
// [engine.Task] — so it succeeds regardless of the policy under test here and
// registers its compensation; a later step fails outright, which is what
// triggers that compensation. The compensation itself is the
// authority-carrying task, dispatched through
// [taskActivities.TaskInScopeAuthorized] (the http task needs previous
// outputs, so it takes the scope-carrying arm) — the one this PR's review
// found uncovered.
func TestTaskPolicyDeniesAuthorityCarryingCompensationDurably(t *testing.T) {
	policy, err := v1.TaskPolicyConfig{
		Deny: []string{`task == "http" && identity.namespace != "platform"`},
	}.Policy()
	require.NoError(t, err)
	v1.SetDefaultTaskPolicy(policy)
	t.Cleanup(func() { v1.SetDefaultTaskPolicy(nil) })

	baseURL := tests.NewHTTPServer(t)

	workflow := func() *v1.Workflow {
		return &v1.Workflow{
			Name:    "task-policy-authority-undo",
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{
				{
					Id: "provision",
					Kind: &v1.Node_Task{Task: &v1.Task{
						Name:   "log",
						Inputs: map[string]*v1.Value{"message": v1.NewLiteral("provisioned")},
					}},
					Undo: &v1.Compensation{Task: authorityTask(baseURL)},
				},
				{
					Id: "boom",
					Kind: &v1.Node_Task{Task: &v1.Task{
						Name:   "http",
						Inputs: map[string]*v1.Value{"url": v1.NewLiteral(baseURL + "/status/500")},
					}},
				},
			},
		}
	}

	run := func(t *testing.T, namespace string) error {
		t.Helper()

		env := newAuthorizedTestEnv(t)
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

	t.Run("the compensation is denied before any secret resolves, outside the platform namespace", func(t *testing.T) {
		err := run(t, "some-other-team")
		require.Error(t, err, "boom fails the run regardless, which is what triggers the compensation")
		require.NotContains(t, err.Error(), "secret access is not configured",
			"the compensation must be refused by the task-shape policy before it "+
				"ever tries to resolve its bearer secret")
	})

	t.Run("the compensation reaches the task inside the platform namespace", func(t *testing.T) {
		err := run(t, "platform")
		require.Error(t, err, "boom still fails the run, and the compensation still has no secret store")
		require.Contains(t, err.Error(), "secret access is not configured",
			"a platform caller's compensation must clear the task-shape policy "+
				"and reach the task itself")
	})
}

// TestTaskPolicyDeniesScopedCompensationByIdentityDurably covers the
// non-authority scope-carrying dispatch arm. A compensation's inputs are already
// resolved when it is registered, but TaskInScope still needs the run identity
// in that scope to enforce identity-based deployment policy without failing open.
func TestTaskPolicyDeniesScopedCompensationByIdentityDurably(t *testing.T) {
	policy, err := v1.TaskPolicyConfig{
		Deny: []string{`task == "http" && identity.namespace == "blocked-tenant"`},
	}.Policy()
	require.NoError(t, err)
	v1.SetDefaultTaskPolicy(policy)
	t.Cleanup(func() { v1.SetDefaultTaskPolicy(nil) })

	baseURL := tests.NewHTTPServer(t)
	workflow := &v1.Workflow{
		Name:    "task-policy-scoped-undo-identity",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{
			{
				Id: "provision",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "log",
					Inputs: map[string]*v1.Value{"message": v1.NewLiteral("provisioned")},
				}},
				Undo: &v1.Compensation{Task: &v1.Task{
					Name:   "http",
					Inputs: map[string]*v1.Value{"url": v1.NewLiteral(baseURL + "/status/200")},
				}},
			},
			{
				Id: "boom",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name: "unknown-task",
				}},
			},
		},
	}

	run := func(t *testing.T, namespace string) error {
		t.Helper()
		env := newAuthorizedTestEnv(t)
		env.ExecuteWorkflow(engine.Run, &v1.RunState{
			Workflow: workflow,
			Identity: &v1.WorkloadIdentity{Namespace: namespace},
		})
		require.True(t, env.IsWorkflowCompleted())
		return env.GetWorkflowError()
	}

	t.Run("blocked tenant compensation is denied", func(t *testing.T) {
		err := run(t, "blocked-tenant")
		require.Error(t, err)
		require.Contains(t, err.Error(), "task-shape policy")
	})

	t.Run("another tenant compensation reaches the task", func(t *testing.T) {
		err := run(t, "another-tenant")
		require.Error(t, err, "the later unknown task still fails the workflow")
		require.NotContains(t, err.Error(), "task-shape policy")
	})
}

// TestTaskPolicyIdentityMatchesOnPlainTaskActivity closes #187's second
// review finding: the plain [engine.Task] activity (no scope, no authority —
// the arm a task like `log` dispatches through) used to pass a hard-coded
// nil identity to [v1.CheckTaskPolicy], so an identity-based deny rule could
// never match on that path (fail-open for a deny rule) and an identity-based
// allow rule could never match either (a false denial for every legitimate
// caller). [executor.dispatch] now threads `e.identity` — the run's real
// attested identity, the same value [taskActivities.TaskAuthorized] already
// received — through to [engine.Task] as a parameter.
func TestTaskPolicyIdentityMatchesOnPlainTaskActivity(t *testing.T) {
	policy, err := v1.TaskPolicyConfig{
		Deny: []string{`task == "log" && identity.namespace == "blocked-tenant"`},
	}.Policy()
	require.NoError(t, err)
	v1.SetDefaultTaskPolicy(policy)
	t.Cleanup(func() { v1.SetDefaultTaskPolicy(nil) })

	workflow := &v1.Workflow{
		Name:    "task-policy-plain-task-identity",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{
			{
				Id: "report",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "log",
					Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hello")},
				}},
			},
		},
	}

	run := func(t *testing.T, namespace string) error {
		t.Helper()

		suite := &testsuite.WorkflowTestSuite{}
		env := suite.NewTestWorkflowEnvironment()
		env.RegisterWorkflow(engine.Run)
		env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
		env.OnActivity(engine.TaskWithPrev, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskWithPrev)
		env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)

		env.ExecuteWorkflow(engine.Run, &v1.RunState{
			Workflow: workflow,
			Identity: &v1.WorkloadIdentity{
				Subject:   "someone@example.com",
				Issuer:    "flowstate:test",
				Namespace: namespace,
			},
		})
		require.True(t, env.IsWorkflowCompleted())
		return env.GetWorkflowError()
	}

	t.Run("the blocked tenant is denied on the plain Task activity path", func(t *testing.T) {
		err := run(t, "blocked-tenant")
		require.Error(t, err, "an identity-based deny rule must reach the plain Task activity, "+
			"not fail open because identity was passed as nil")
		require.Contains(t, err.Error(), "task-shape policy")
		require.Contains(t, err.Error(), "log")
	})

	t.Run("a distinct tenant's identical task still passes", func(t *testing.T) {
		err := run(t, "some-other-tenant")
		require.NoError(t, err, "a deny rule naming one tenant must not deny a different one — "+
			"the negative direction proving the rule discriminates by identity rather than "+
			"denying regardless of what identity.namespace actually is")
	})
}
