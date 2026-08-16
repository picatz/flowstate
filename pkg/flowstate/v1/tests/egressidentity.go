package tests

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// Shared cases for #240's identity-scoped egress policy, run against both
// execution drivers — `flowstatev1_test.TestRunWorkflowEgressIdentity`
// locally and `engine.TestRunWorkflowEgressIdentity` durably.
//
// # Why this set exists rather than only the unit test that was here
//
// `Test_httpTask_egressIdentity` already proved the rule mechanism and the
// bridge from a [v1.Scope] to it, by calling the task function with a scope it
// built by hand. What it could not see is the question invariant 3 asks: does
// a *run* reach that task function with the identity it was started with, on
// both drivers? It did not — the local driver never populated the scope's
// identity, so every identity-scoped egress rule declined to match on a laptop
// and matched in production (#295). A hand-built scope is a test of the task;
// a run is a test of the driver, and only the second of those could fail.
//
// So this is the same shape [TaskPolicyCases] takes, for the other policy
// surface that reads the same field, and it is deliberately a separate set
// rather than a field on [TaskPolicyCase]: the two surfaces are configured by
// different mechanisms — a process-wide policy for task shape, a re-registered
// http task for egress — and folding them together would make every case
// carry the other's setup.

// EgressIdentityCase pairs a workflow that egresses with the identity the run
// acts as and whether the deployment's egress policy admits it.
type EgressIdentityCase struct {
	// Name identifies the case in test output.
	Name string

	// Identity is who the run acts as, or nil for a run whose starter named
	// nobody. Each driver carries it by its own route — see
	// [TaskPolicyCase.Identity], which states the same split.
	Identity *v1.WorkloadIdentity

	// Denied is true when the egress policy must refuse the request.
	Denied bool
}

// EgressIdentityAllowRule is the one rule every [EgressIdentityCase] runs
// under: a tenant allowlist naming a single namespace.
//
// One rule shared by every case, rather than one per case, because what the
// cases discriminate is the *identity* — a set where each case brought its own
// rule could not tell a policy that discriminates from a set of policies that
// each happen to answer correctly for their own case.
const EgressIdentityAllowRule = `identity.namespace == "team-a"`

// EgressIdentityWorkflow returns a one-step workflow that egresses to the
// loopback test server, for a case to run under [EgressIdentityAllowRule].
func EgressIdentityWorkflow(httpBaseURL string) *v1.Workflow {
	return &v1.Workflow{
		Name:    "egress-identity",
		Profile: v1.CurrentProfile,
		Steps:   []*v1.Node{echoes("reach", httpBaseURL, `"hello"`)},
	}
}

// EgressIdentityExpectedOutputs is what a permitted [EgressIdentityWorkflow]
// run produces.
func EgressIdentityExpectedOutputs() *v1.Workflow_StepOutputs {
	return &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"reach": said("hello"),
	}}
}

// InstallEgressIdentityPolicy registers an http task enforcing
// [EgressIdentityAllowRule] for the duration of the test, restoring whatever
// was registered before.
//
// It permits loopback for the same reason [allowLoopback] does — the test
// server is loopback and the shipped default correctly refuses it — and it is
// exported because the two drivers' test packages both need it, which is the
// same reason every other helper in this package is exported.
func InstallEgressIdentityPolicy(tb testing.TB) {
	tb.Helper()

	policy, err := netpolicy.New(
		netpolicy.WithAllowLoopback(),
		netpolicy.WithAllowRules(EgressIdentityAllowRule),
	)
	if err != nil {
		tb.Fatalf("building the identity-scoped egress policy: %v", err)
	}

	registry := v1.DefaultRegistry()
	original, existed := registry.Lookup("http")
	if err := registry.Register(v1.HTTPTaskDef(policy)); err != nil {
		tb.Fatalf("registering the identity-scoped http task: %v", err)
	}
	tb.Cleanup(func() {
		if existed {
			_ = registry.Register(original)
		}
	})
}

// EgressIdentityCases returns the shared cases both drivers must agree on.
//
// The negative direction is the point, per CLAUDE.md: an allowlist that admits
// its own tenant proves nothing on its own, because a driver that lost the
// identity entirely would fail it in one direction and a driver that ignored
// the rule entirely would pass it. The set is therefore three answers to one
// rule — the tenant it names, a tenant it does not, and a run naming nobody —
// and a driver has to get all three right.
func EgressIdentityCases() []EgressIdentityCase {
	return []EgressIdentityCase{
		{
			// #295's reproduction on this surface: the run acts as the tenant
			// the rule admits, so the request goes out. This is what a local
			// run refused while production allowed it.
			Name:     "the admitted tenant egresses",
			Identity: &v1.WorkloadIdentity{Namespace: "team-a", Subject: "spiffe://acme/team-a"},
		},
		{
			// The boundary: a different tenant, the same rule, refused.
			Name:     "another tenant is refused the same host",
			Identity: &v1.WorkloadIdentity{Namespace: "team-b", Subject: "spiffe://acme/team-b"},
			Denied:   true,
		},
		{
			// And the fail-closed reading kept explicit: a run whose starter
			// named nobody matches no allow rule and is refused. Carrying a
			// rehearsal identity is what a local run does when it has one to
			// carry, never a blanket exemption — this case is what fails if
			// it ever becomes one.
			Name:   "a run that names nobody is refused",
			Denied: true,
		},
	}
}

// AssertEgressIdentityOutcome checks a driver's answer for one case against
// what the policy requires, given what that driver's run produced.
//
// A denial is asserted by text rather than by errors.As, because the durable
// driver's errors round-trip through Temporal's failure conversion and arrive
// as a different type — the choice `engine.TestRunWorkflowTaskPolicy` already
// makes, for the identical reason. What both drivers must produce is a failure
// that names the policy as the refuser, so that is what is compared.
//
// A permitted case asserts the run's outputs and not merely its success: a
// request the policy admits has to have actually reached the peer and come
// back, and "no error" is also what a driver that skipped the step entirely
// would report.
func AssertEgressIdentityOutcome(tb testing.TB, c EgressIdentityCase, outputs *v1.Workflow_StepOutputs, err error) {
	tb.Helper()

	if c.Denied {
		if err == nil {
			tb.Fatalf("the egress policy must refuse this request, and the run succeeded")
		}
		if !containsAny(err.Error(), "egress", "policy", "denied") {
			tb.Fatalf("the failure must read as an egress policy refusal, got: %v", err)
		}
		return
	}

	if err != nil {
		tb.Fatalf("the egress policy admits this identity, and the run failed: %v", err)
	}
	if want := EgressIdentityExpectedOutputs(); !proto.Equal(want, outputs) {
		tb.Fatalf("the admitted run produced %v, want %v", outputs, want)
	}
}

// containsAny reports whether text contains any of the substrings, used to
// keep [AssertEgressIdentityOutcome] from pinning one driver's exact wording
// while still refusing a failure that is not about policy at all.
func containsAny(text string, substrings ...string) bool {
	for _, substring := range substrings {
		if strings.Contains(text, substring) {
			return true
		}
	}
	return false
}
