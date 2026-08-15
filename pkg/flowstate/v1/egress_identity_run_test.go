package flowstatev1_test

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
)

// TestRunWorkflowEgressIdentity runs [tests.EgressIdentityCases] against the
// local driver. The same cases run against the durable driver in the engine
// package (TestRunWorkflowEgressIdentity there) — two verified callers, which
// is what invariant 3 asks a shared case set to have.
//
// The identity reaches the policy here by the route `flow run local --as-*`
// uses, and reaches it durably from the run's own state. Before #295 the local
// route did not exist, so an identity-scoped egress rule refused every local
// request and admitted the production one — the same file, the same policy,
// opposite answers.
//
// No t.Parallel: [tests.InstallEgressIdentityPolicy] swaps the process-wide
// http task registration, the same posture every other registry-swapping test
// in this package takes.
func TestRunWorkflowEgressIdentity(t *testing.T) {
	baseURL := tests.NewHTTPServer(t)

	for _, tc := range tests.EgressIdentityCases() {
		t.Run(tc.Name, func(t *testing.T) {
			tests.InstallEgressIdentityPolicy(t)

			ctx := v1.NewContextWithRehearsalIdentity(t.Context(), tc.Identity)

			out, err := v1.Run(ctx, tests.EgressIdentityWorkflow(baseURL))

			tests.AssertEgressIdentityOutcome(t, tc, out, err)
		})
	}
}
