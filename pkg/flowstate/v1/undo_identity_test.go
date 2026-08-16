package flowstatev1_test

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
)

// TestTaskPolicyDeniesScopedCompensationByIdentityLocally is the local driver's
// half of engine.TestTaskPolicyDeniesScopedCompensationByIdentityDurably. Both
// run the same case from [tests.UndoIdentityWorkflow] with the same policy and
// the same assertions, because the claim being made is that the two drivers
// answer identically — which two separately-built copies stop proving the
// moment either one drifts.
//
// Without the run identity in the compensation's scope, `identity.namespace`
// reads empty here, the rule matches nothing, and the undo runs while the
// durable driver refuses it: a rehearsal permitting what production denies,
// which is the divergence #295 named.
func TestTaskPolicyDeniesScopedCompensationByIdentityLocally(t *testing.T) {
	v1.SetDefaultTaskPolicy(tests.UndoIdentityPolicy(t))
	t.Cleanup(func() { v1.SetDefaultTaskPolicy(nil) })

	workflow := tests.UndoIdentityWorkflow(tests.NewHTTPServer(t))

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
		tests.AssertUndoIdentityDenied(t, run(t, tests.UndoIdentityBlockedNamespace))
	})

	t.Run("another tenant compensation reaches the task", func(t *testing.T) {
		tests.AssertUndoIdentityReached(t, run(t, tests.UndoIdentityAllowedNamespace))
	})
}
