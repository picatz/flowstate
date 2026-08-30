package conformance

import (
	"strings"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A compensation is dispatched under the run's own identity, and both drivers
// have to agree about that or a rehearsal stops predicting production.
//
// The scenario lives here rather than in either driver's package because the
// claim is precisely that the two answers match: two hand-built copies, one per
// driver, can drift in workflow shape, policy text or assertion while both
// suites stay green, and a pair of tests that no longer ask the same question
// has stopped proving the thing it was written for. `ZeroValueCases` had one
// caller for months and proved half of what it was for; this is the same hazard
// caught earlier.
//
// The rule denies `http` for one namespace only, so the run's identity is the
// entire difference between the two outcomes. A rule denying every compensation
// would pass against a driver that lost the identity altogether.
const (
	// UndoIdentityDenyRule is the task-shape rule both drivers install.
	UndoIdentityDenyRule = `task == "http" && identity.namespace == "blocked-tenant"`

	// UndoIdentityBlockedNamespace is refused by [UndoIdentityDenyRule].
	UndoIdentityBlockedNamespace = "blocked-tenant"

	// UndoIdentityAllowedNamespace is not, and is what makes the assertion a
	// comparison rather than a one-sided check.
	UndoIdentityAllowedNamespace = "another-tenant"

	// undoIdentityDenialText is the substring a task-shape denial carries; see
	// [v1.ErrTaskPolicyDenied].
	undoIdentityDenialText = "task-shape policy"
)

// UndoIdentityWorkflow returns a workflow whose first step registers a
// scope-carrying compensation (`http` needs previous outputs, so it dispatches
// through the scoped arm) and whose second step fails, forcing the unwind.
//
// The failure is an invalid invocation of the known log task rather than a
// failing http call, so the run fails for a reason that has nothing to do with
// the policy under test. An unknown task cannot drive this unwind: capability
// admission correctly refuses it before the first step can register its undo.
func UndoIdentityWorkflow(httpBaseURL string) *v1.Workflow {
	return &v1.Workflow{
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
					Inputs: map[string]*v1.Value{"url": v1.NewLiteral(httpBaseURL + "/status/200")},
				}},
			},
			{
				Id:   "boom",
				Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}},
			},
		},
	}
}

// UndoIdentityPolicy returns the policy both drivers install for this case.
func UndoIdentityPolicy(tb testing.TB) *v1.TaskPolicy {
	tb.Helper()

	policy, err := v1.TaskPolicyConfig{Deny: []string{UndoIdentityDenyRule}}.Policy()
	if err != nil {
		tb.Fatalf("building the undo-identity task policy: %v", err)
	}

	return policy
}

// AssertUndoIdentityDenied checks that the blocked tenant's compensation was
// refused by the task-shape policy.
func AssertUndoIdentityDenied(tb testing.TB, err error) {
	tb.Helper()

	if err == nil {
		tb.Fatal("running with the blocked tenant's identity: got no error, want the compensation refused")
	}
	if !strings.Contains(err.Error(), undoIdentityDenialText) {
		tb.Fatalf("running with the blocked tenant's identity: got %q, want it to name the %s",
			err, undoIdentityDenialText)
	}
}

// AssertUndoIdentityReached checks the other direction: another tenant's
// compensation is not refused, and the run still fails for the unrelated
// reason that drove the unwind.
//
// Both halves matter. Without the second, a driver that refused every
// compensation would satisfy the first assertion just as well.
func AssertUndoIdentityReached(tb testing.TB, err error) {
	tb.Helper()

	if err == nil {
		tb.Fatal("running with a permitted identity: got no error, want the run to fail on invalid task inputs")
	}
	if strings.Contains(err.Error(), undoIdentityDenialText) {
		tb.Fatalf("running with a permitted identity: got %q, want the compensation to reach the task", err)
	}
}
