package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// gateWorkflow is a minimal workflow with one `wait_for_signal:`, used
// throughout this file so [v1.CheckSignalPolicies] has something to check a
// declared policy's signal name against.
func gateWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name: "gate",
		Steps: []*v1.Node{
			{
				Id: "approval",
				Kind: &v1.Node_Wait{Wait: &v1.Wait{
					Kind:    &v1.Wait_Signal{Signal: &v1.Signal{Name: "deploy-approved"}},
					Timeout: durationpb.New(0),
				}},
			},
		},
	}
}

// policyAllows reports whether identity satisfies policy, as an enforcement
// point asks it: through [v1.SignalPolicyCheck], with nothing known about a
// starter or the run's inputs.
func policyAllows(t *testing.T, policy *v1.SignalPolicy, identity *v1.WorkloadIdentity) bool {
	t.Helper()

	return v1.SignalPolicyCheck(t.Context(), policy, identity, nil, false, nil) == nil
}

func TestCheckSignalPoliciesAcceptsNoPolicyAtAll(t *testing.T) {
	require.NoError(t, v1.CheckSignalPolicies(gateWorkflow()))
}

func TestCheckSignalPoliciesAcceptsAWellFormedPolicy(t *testing.T) {
	wf := gateWorkflow()
	wf.Signals = map[string]*v1.SignalPolicy{
		"deploy-approved": {Allow: `sender.identity.principal == "` + v1.QualifiedSubject("https://issuer.example.com", "release-manager@example.com") + `"`},
	}
	require.NoError(t, v1.CheckSignalPolicies(wf))
}

// TestCheckSignalPoliciesRefusesAnUndeclaredName is the misspelling case: a
// policy for a signal name nothing waits for is refused, because that is
// almost always the wrong name typed twice rather than the same name once.
func TestCheckSignalPoliciesRefusesAnUndeclaredName(t *testing.T) {
	wf := gateWorkflow()
	wf.Signals = map[string]*v1.SignalPolicy{
		"deploy-aproved": {Allow: `sender.identity.principal == "` + v1.QualifiedSubject("https://issuer.example.com", "release-manager@example.com") + `"`},
	}
	err := v1.CheckSignalPolicies(wf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no `wait_for_signal:`")
}

// TestCheckSignalPoliciesRefusesAPolicyWithNoPredicate checks the cross-field
// fact protovalidate's per-field rules cannot see on their own: a policy with
// no predicate authorizes nobody, which is indistinguishable from a typo.
func TestCheckSignalPoliciesRefusesAPolicyWithNoPredicate(t *testing.T) {
	wf := gateWorkflow()
	wf.Signals = map[string]*v1.SignalPolicy{
		"deploy-approved": {},
	}
	err := v1.CheckSignalPolicies(wf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "authorizes nobody")
}

// TestABareSubjectPredicateNeverAdmitsAQualifiedSender restates #215's lesson
// for signal policy: a principal is "<issuer>#<subject>", so a predicate
// comparing it with a bare subject can never match a real sender, whichever
// issuer minted the subject.
func TestABareSubjectPredicateNeverAdmitsAQualifiedSender(t *testing.T) {
	policy := &v1.SignalPolicy{Allow: `sender.identity.principal == "release-manager@example.com"`}

	require.False(t, policyAllows(t, policy, &v1.WorkloadIdentity{Principal: &v1.Principal{Issuer: "https://issuer.example.com", Subject: "release-manager@example.com"}}))
}

func TestQualifiedSubjectAndLooksLikeQualifiedSubject(t *testing.T) {
	require.Equal(t, "https://issuer.example.com#sub@example.com",
		v1.QualifiedSubject("https://issuer.example.com", "sub@example.com"))

	require.True(t, v1.LooksLikeQualifiedSubject("https://issuer.example.com#sub@example.com"))
	require.False(t, v1.LooksLikeQualifiedSubject("sub@example.com"), "no '#' at all")
	require.False(t, v1.LooksLikeQualifiedSubject("#sub@example.com"), "empty issuer before '#'")
	require.False(t, v1.LooksLikeQualifiedSubject("https://issuer.example.com#"), "empty subject after '#'")
	require.False(t, v1.LooksLikeQualifiedSubject("mesh#x#y"), "more than one '#' is ambiguous")
}

// TestSignalPolicyPredicateClausesAreAnded checks that a predicate naming both
// a principal and a claim requires both: an intersection, not either alone.
func TestSignalPolicyPredicateClausesAreAnded(t *testing.T) {
	policy := &v1.SignalPolicy{Allow: `sender.identity.principal == "` + v1.QualifiedSubject("https://issuer.example.com", "release-manager@example.com") + `" && sender.identity.claims["team"] == "release-managers"`}

	// Matches the principal, but not the claim: refused.
	require.False(t, policyAllows(t, policy, &v1.WorkloadIdentity{Principal: &v1.Principal{Issuer: "https://issuer.example.com", Subject: "release-manager@example.com", Claims: v1.StringClaimValues(map[string]string{"team": "some-other-team"})}}))

	// Matches both: allowed.
	require.True(t, policyAllows(t, policy, &v1.WorkloadIdentity{Principal: &v1.Principal{Issuer: "https://issuer.example.com", Subject: "release-manager@example.com", Claims: v1.StringClaimValues(map[string]string{"team": "release-managers"})}}))
}

// TestSignalPolicyPredicateAlternativesAreOred checks that `||` makes
// alternatives: satisfying any one of them is enough.
func TestSignalPolicyPredicateAlternativesAreOred(t *testing.T) {
	policy := &v1.SignalPolicy{Allow: `sender.identity.principal == "` + v1.QualifiedSubject("https://issuer.example.com", "alice@example.com") + `" || sender.identity.principal == "` + v1.QualifiedSubject("https://issuer.example.com", "bob@example.com") + `"`}

	require.True(t, policyAllows(t, policy, &v1.WorkloadIdentity{Principal: &v1.Principal{Issuer: "https://issuer.example.com", Subject: "alice@example.com"}}))
	require.True(t, policyAllows(t, policy, &v1.WorkloadIdentity{Principal: &v1.Principal{Issuer: "https://issuer.example.com", Subject: "bob@example.com"}}))
	require.False(t, policyAllows(t, policy, &v1.WorkloadIdentity{Principal: &v1.Principal{Issuer: "https://issuer.example.com", Subject: "carol@example.com"}}))
}

// TestSignalPolicyNamespacePredicate checks the namespace form independently of
// principal and claims.
func TestSignalPolicyNamespacePredicate(t *testing.T) {
	policy := &v1.SignalPolicy{Allow: `sender.identity.namespace == "release-managers-ns"`}

	require.True(t, policyAllows(t, policy, &v1.WorkloadIdentity{Principal: &v1.Principal{Namespace: "release-managers-ns"}}))
	require.False(t, policyAllows(t, policy, &v1.WorkloadIdentity{Principal: &v1.Principal{Namespace: "team-a"}}))
}

// TestCheckSignalPoliciesRefusesAnInputsPredicateWithNothingNarrowing is the
// narrowing rule at the policy level: whoever starts a run chooses its inputs,
// so a predicate over them alone would let the starter name their own approver.
func TestCheckSignalPoliciesRefusesAnInputsPredicateWithNothingNarrowing(t *testing.T) {
	wf := gateWorkflow()
	wf.Signals = map[string]*v1.SignalPolicy{
		"deploy-approved": {Allow: `sender.identity.principal == "https://issuer.example.com#" + inputs.expected_approver`},
	}
	err := v1.CheckSignalPolicies(wf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "name themselves as their own approver",
		"the diagnostic must say what is wrong")
	require.Contains(t, err.Error(), "sender.identity.claims",
		"the diagnostic must say what to do, not only that something is wrong")
}

// TestCheckSignalPoliciesAllowsANarrowedInputsPredicate is the positive
// direction the refusal above needs to mean anything. Without it, a check that
// refused every predicate reading inputs would satisfy it and still break the
// feature outright.
func TestCheckSignalPoliciesAllowsANarrowedInputsPredicate(t *testing.T) {
	for name, expression := range map[string]string{
		"narrowed by a comparison with the starter": `sender.identity.principal == "https://issuer.example.com#" + inputs.expected_approver && sender.identity.principal != run.identity.principal`,
		"narrowed by claims":                        `sender.identity.principal == "https://issuer.example.com#" + inputs.expected_approver && sender.identity.claims["role"] == "release-manager"`,
	} {
		t.Run(name, func(t *testing.T) {
			wf := gateWorkflow()
			wf.Signals = map[string]*v1.SignalPolicy{"deploy-approved": {Allow: expression}}
			require.NoError(t, v1.CheckSignalPolicies(wf))
		})
	}
}

// TestCheckSignalPolicyShapeRefusesAMemoPolicyWithNoPredicate is the decoded
// side: a policy read back off a run's memo that decodes to no predicate (a
// run frozen by a release that still recorded the retired rule list, whose
// field numbers are reserved) is refused, never read as "no policy".
func TestCheckSignalPolicyShapeRefusesAMemoPolicyWithNoPredicate(t *testing.T) {
	err := v1.CheckSignalPolicyShape(map[string]*v1.SignalPolicy{"deploy-approved": {}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "authorizes nobody")
}

// TestSignalPolicyClosedPrincipals pins what the quorum check may count: only
// predicates whose admitted principals can be enumerated exactly are closed.
func TestSignalPolicyClosedPrincipals(t *testing.T) {
	const a, b = "https://i#a", "https://i#b"

	closed := map[string][]string{
		`sender.identity.principal == "` + a + `"`:                                                                                     {a},
		`"` + a + `" == sender.identity.principal`:                                                                                     {a},
		`sender.identity.principal in ["` + a + `", "` + b + `"]`:                                                                      {a, b},
		`sender.identity.principal == "` + a + `" || sender.identity.principal == "` + b + `"`:                                         {a, b},
		`sender.identity.principal == "` + a + `" && sender.identity.claims["team"] == "x"`:                                            {a},
		`(sender.identity.principal == "` + a + `" || sender.identity.principal == "` + b + `") && sender.identity.claims["t"] == "x"`: {a, b},
		`sender.identity.principal == "` + a + `" && sender.identity.principal != run.identity.principal`:                              {a},
	}
	for expression, want := range closed {
		got, ok := v1.SignalPolicyClosedPrincipals(&v1.SignalPolicy{Allow: expression})
		require.True(t, ok, expression)
		require.Equal(t, want, got, expression)
	}

	for _, expression := range []string{
		`sender.identity.claims["team"] == "x"`,
		`sender.identity.namespace == "n"`,
		`sender.identity.principal == "` + a + `" || sender.identity.claims["team"] == "x"`,
		`sender.identity.principal == "x#" + inputs.who && sender.identity.claims["t"] == "x"`,
		`!(sender.identity.principal == "` + a + `")`,
		`sender.identity.principal.startsWith("https://i#")`,
		`not a valid ((expression`,
		``,
	} {
		_, ok := v1.SignalPolicyClosedPrincipals(&v1.SignalPolicy{Allow: expression})
		require.False(t, ok, expression)
	}

	_, ok := v1.SignalPolicyClosedPrincipals(nil)
	require.False(t, ok, "no policy admits any authenticated caller, so nothing is closed")
}
