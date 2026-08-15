package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
)

// TestTaskPolicyDeniesScopedCompensationByIdentityLocally is the local driver's
// half of engine.TestTaskPolicyDeniesScopedCompensationByIdentityDurably, and it
// is the same claim rather than a smaller one: a compensation is dispatched
// under the run's own identity, so a task-shape rule keyed on
// `identity.namespace` decides it the same way in both drivers.
//
// Without the identity in the compensation's scope the rule below matches
// nothing here — `identity.namespace` reads empty — so the undo runs, while the
// durable driver refuses it. That is the divergence #295 named: a local run
// answering differently from production about a policy an author is rehearsing
// is worse than no rehearsal, because it is a wrong answer delivered
// confidently.
//
// Both directions are asserted. The blocked tenant's compensation is refused;
// another tenant's reaches the task, and the run still fails only for the
// unrelated reason that drove the unwind in the first place. Asserting only the
// first would pass against a build that denies every compensation.
func TestTaskPolicyDeniesScopedCompensationByIdentityLocally(t *testing.T) {
	policy, err := v1.TaskPolicyConfig{
		Deny: []string{`task == "http" && identity.namespace == "blocked-tenant"`},
	}.Policy()
	require.NoError(t, err)
	v1.SetDefaultTaskPolicy(policy)
	t.Cleanup(func() { v1.SetDefaultTaskPolicy(nil) })

	baseURL := tests.NewHTTPServer(t)
	workflow := &v1.Workflow{
		Name:    "task-policy-scoped-undo-identity-local",
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
		ctx := v1.NewContextWithRehearsalIdentity(t.Context(), &v1.WorkloadIdentity{
			Namespace: namespace,
			Subject:   "rehearsal@example.com",
			Issuer:    "flowstate:test",
		})
		_, err := v1.Run(ctx, workflow)

		return err
	}

	t.Run("blocked tenant compensation is denied", func(t *testing.T) {
		err := run(t, "blocked-tenant")
		require.Error(t, err)
		require.Contains(t, err.Error(), "task-shape policy")
	})

	t.Run("another tenant compensation reaches the task", func(t *testing.T) {
		err := run(t, "another-tenant")
		require.Error(t, err, "the later unknown task still fails the run")
		require.NotContains(t, err.Error(), "task-shape policy")
	})
}
