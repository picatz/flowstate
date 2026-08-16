package flowstatev1_test

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestTaskPolicyDeniesScopedCompensationByIdentityLocally is the local driver's
// half of engine.TestTaskPolicyDeniesScopedCompensationByIdentityDurably. Both
// run the same case from [conformance.UndoIdentityWorkflow] with the same policy and
// the same assertions, because the claim being made is that the two drivers
// answer identically — which two separately-built copies stop proving the
// moment either one drifts.
//
// Without the run identity in the compensation's scope, `identity.namespace`
// reads empty here, the rule matches nothing, and the undo runs while the
// durable driver refuses it: a rehearsal permitting what production denies,
// which is the divergence #295 named.
func TestTaskPolicyDeniesScopedCompensationByIdentityLocally(t *testing.T) {
	v1.SetDefaultTaskPolicy(conformance.UndoIdentityPolicy(t))
	t.Cleanup(func() { v1.SetDefaultTaskPolicy(nil) })

	workflow := conformance.UndoIdentityWorkflow(conformance.NewHTTPServer(t))

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
		conformance.AssertUndoIdentityDenied(t, run(t, conformance.UndoIdentityBlockedNamespace))
	})

	t.Run("another tenant compensation reaches the task", func(t *testing.T) {
		conformance.AssertUndoIdentityReached(t, run(t, conformance.UndoIdentityAllowedNamespace))
	})
}
