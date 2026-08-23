package flowstatev1_test

import (
	"context"
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

func TestCheckSignalPoliciesAcceptsNoPolicyAtAll(t *testing.T) {
	require.NoError(t, v1.CheckSignalPolicies(gateWorkflow()))
}

func TestCheckSignalPoliciesAcceptsAWellFormedPolicy(t *testing.T) {
	wf := gateWorkflow()
	wf.Signals = map[string]*v1.SignalPolicy{
		"deploy-approved": {Allow: []*v1.SignalPolicyRule{
			{Subject: v1.QualifiedSubject("https://issuer.example.com", "release-manager@example.com")},
		}},
	}
	require.NoError(t, v1.CheckSignalPolicies(wf))
}

// TestCheckSignalPoliciesRefusesAnUndeclaredName is the misspelling case: a
// policy for a signal name nothing waits for is refused, because that is
// almost always the wrong name typed twice rather than the same name once.
func TestCheckSignalPoliciesRefusesAnUndeclaredName(t *testing.T) {
	wf := gateWorkflow()
	wf.Signals = map[string]*v1.SignalPolicy{
		"deploy-aproved": {Allow: []*v1.SignalPolicyRule{ // misspelled, on purpose
			{Subject: v1.QualifiedSubject("https://issuer.example.com", "release-manager@example.com")},
		}},
	}
	err := v1.CheckSignalPolicies(wf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no `wait_for_signal:`")
}

// TestCheckSignalPoliciesRefusesARuleThatMatchesEverySender checks the
// cross-field fact protovalidate's per-field rules cannot see on their own:
// a rule with nothing set on it authorizes every sender, which defeats the
// point of writing a policy at all.
func TestCheckSignalPoliciesRefusesARuleThatMatchesEverySender(t *testing.T) {
	wf := gateWorkflow()
	wf.Signals = map[string]*v1.SignalPolicy{
		"deploy-approved": {Allow: []*v1.SignalPolicyRule{{}}},
	}
	err := v1.CheckSignalPolicies(wf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "matches every sender")
}

// TestCheckSignalPoliciesRefusesAnUnqualifiedSubject restates #215's lesson
// for signal policy: a subject with no issuer is ambiguous across identity
// providers and is refused rather than silently matching any issuer's
// version of that subject.
func TestCheckSignalPoliciesRefusesAnUnqualifiedSubject(t *testing.T) {
	wf := gateWorkflow()
	wf.Signals = map[string]*v1.SignalPolicy{
		"deploy-approved": {Allow: []*v1.SignalPolicyRule{
			{Subject: "release-manager@example.com"}, // no "issuer#" prefix
		}},
	}
	err := v1.CheckSignalPolicies(wf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "issuer")
}

func TestQualifiedSubjectAndLooksLikeQualifiedSubject(t *testing.T) {
	require.Equal(t, "https://issuer.example.com#sub@example.com",
		v1.QualifiedSubject("https://issuer.example.com", "sub@example.com"))

	require.True(t, v1.LooksLikeQualifiedSubject("https://issuer.example.com#sub@example.com"))
	require.False(t, v1.LooksLikeQualifiedSubject("sub@example.com"), "no '#' at all")
	require.False(t, v1.LooksLikeQualifiedSubject("#sub@example.com"), "empty issuer before '#'")
	require.False(t, v1.LooksLikeQualifiedSubject("https://issuer.example.com#"), "empty subject after '#'")
}

// TestSignalPolicyAllowsRuleIsAnAndOfItsSetFields checks that a rule naming
// both a subject and a claim requires both — an intersection within one
// rule, not either alone.
func TestSignalPolicyAllowsRuleIsAnAndOfItsSetFields(t *testing.T) {
	policy := &v1.SignalPolicy{Allow: []*v1.SignalPolicyRule{{
		Subject: v1.QualifiedSubject("https://issuer.example.com", "release-manager@example.com"),
		Claims:  map[string]string{"team": "release-managers"},
	}}}

	// Matches subject, but not the claim: refused.
	require.False(t, v1.SignalPolicyAllows(policy, &v1.WorkloadIdentity{
		Issuer:  "https://issuer.example.com",
		Subject: "release-manager@example.com",
		Claims:  map[string]string{"team": "some-other-team"},
	}))

	// Matches both: allowed.
	require.True(t, v1.SignalPolicyAllows(policy, &v1.WorkloadIdentity{
		Issuer:  "https://issuer.example.com",
		Subject: "release-manager@example.com",
		Claims:  map[string]string{"team": "release-managers"},
	}))
}

// TestSignalPolicyAllowsRulesAreAlternatives checks that multiple rules in
// one `allow:` list are OR'd: satisfying any one of them is enough.
func TestSignalPolicyAllowsRulesAreAlternatives(t *testing.T) {
	policy := &v1.SignalPolicy{Allow: []*v1.SignalPolicyRule{
		{Subject: v1.QualifiedSubject("https://issuer.example.com", "alice@example.com")},
		{Subject: v1.QualifiedSubject("https://issuer.example.com", "bob@example.com")},
	}}

	require.True(t, v1.SignalPolicyAllows(policy, &v1.WorkloadIdentity{
		Issuer: "https://issuer.example.com", Subject: "alice@example.com",
	}))
	require.True(t, v1.SignalPolicyAllows(policy, &v1.WorkloadIdentity{
		Issuer: "https://issuer.example.com", Subject: "bob@example.com",
	}))
	require.False(t, v1.SignalPolicyAllows(policy, &v1.WorkloadIdentity{
		Issuer: "https://issuer.example.com", Subject: "carol@example.com",
	}))
}

// TestSignalPolicyAllowsNamespaceRule checks the namespace form
// independently of subject and claims.
func TestSignalPolicyAllowsNamespaceRule(t *testing.T) {
	policy := &v1.SignalPolicy{Allow: []*v1.SignalPolicyRule{{Namespace: "release-managers-ns"}}}

	require.True(t, v1.SignalPolicyAllows(policy, &v1.WorkloadIdentity{Namespace: "release-managers-ns"}))
	require.False(t, v1.SignalPolicyAllows(policy, &v1.WorkloadIdentity{Namespace: "team-a"}))
}

// TestCheckSignalPolicyShapeAllowsUnresolvedSubjectWhenDeclared checks the
// declare-time side of CheckSignalPolicyShape's two-caller split: a rule's
// subject_from is expected to still be an unresolved expression before
// BindRunInputs and resolution have run, and that is not refused.
func TestCheckSignalPolicyShapeAllowsUnresolvedSubjectWhenDeclared(t *testing.T) {
	policies := map[string]*v1.SignalPolicy{
		"deploy-approved": {Allow: []*v1.SignalPolicyRule{
			{SubjectFrom: v1.NewExpr("inputs.expected_approver"), Claims: map[string]string{"role": "release-manager"}},
		}},
	}
	require.NoError(t, v1.CheckSignalPolicyShape(policies, false))
}

func TestCheckSignalPoliciesRefusesUnnarrowedSubjectFrom(t *testing.T) {
	wf := gateWorkflow()
	wf.Signals = map[string]*v1.SignalPolicy{
		"deploy-approved": {Allow: []*v1.SignalPolicyRule{
			{SubjectFrom: v1.NewExpr("inputs.expected_approver")},
		}},
	}
	err := v1.CheckSignalPolicies(wf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "add claims: to the rule, or distinct_from_starter: true",
		"the diagnostic must say what to do, not only that something is wrong")
}

// TestCheckSignalPoliciesRefusesSubjectFromNarrowedOnlyByNamespace is the case
// a namespace-counts-as-narrowing rule would let through, and it is the whole
// of the hazard rather than an edge of it.
//
// A namespace on a rule is compared against the sender's own namespace, and
// every sender that reaches that comparison is already in the run's namespace
// — `FlowstateServer.Signal` reaches `authorizeSignal` only through
// `authorizeRun`, which refuses anyone else. The run's namespace is the
// starter's. So `subject_from` plus `namespace:` authorizes exactly the same
// senders as `subject_from` alone: the starter, having named themselves
// through the run's inputs, matches both halves. Accepting it would leave the
// file reading as though the gate had been narrowed while it had not been,
// which is worse than refusing it.
func TestCheckSignalPoliciesRefusesSubjectFromNarrowedOnlyByNamespace(t *testing.T) {
	wf := gateWorkflow()
	wf.Signals = map[string]*v1.SignalPolicy{
		"deploy-approved": {Allow: []*v1.SignalPolicyRule{{
			SubjectFrom: v1.NewExpr("inputs.expected_approver"),
			Namespace:   "release-managers-ns",
		}}},
	}
	err := v1.CheckSignalPolicies(wf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "A namespace: does not narrow this",
		"the diagnostic must say why the constraint the author reached for is not one")
}

// TestCheckSignalPoliciesAllowsNarrowedSubjectFrom is the positive direction the
// refusals above need to mean anything. Without it, a rule that refused
// every subject_from — rather than only the unnarrowed ones — would satisfy
// all of them and still break the feature outright.
func TestCheckSignalPoliciesAllowsNarrowedSubjectFrom(t *testing.T) {
	t.Run("narrowed by distinct_from_starter", func(t *testing.T) {
		wf := gateWorkflow()
		wf.Signals = map[string]*v1.SignalPolicy{
			"deploy-approved": {
				Allow: []*v1.SignalPolicyRule{{
					SubjectFrom: v1.NewExpr("inputs.expected_approver"),
				}},
				DistinctFromStarter: true,
			},
		}
		require.NoError(t, v1.CheckSignalPolicies(wf))
	})

	t.Run("narrowed by claims", func(t *testing.T) {
		wf := gateWorkflow()
		wf.Signals = map[string]*v1.SignalPolicy{
			"deploy-approved": {Allow: []*v1.SignalPolicyRule{{
				SubjectFrom: v1.NewExpr("inputs.expected_approver"),
				Claims:      map[string]string{"role": "release-manager"},
			}}},
		}
		require.NoError(t, v1.CheckSignalPolicies(wf))
	})
}

func TestCheckSignalPoliciesRefusesSubjectAndSubjectFrom(t *testing.T) {
	wf := gateWorkflow()
	wf.Signals = map[string]*v1.SignalPolicy{
		"deploy-approved": {Allow: []*v1.SignalPolicyRule{{
			Subject:     v1.QualifiedSubject("https://issuer.example.com", "release-manager@example.com"),
			SubjectFrom: v1.NewExpr("inputs.expected_approver"),
			Claims:      map[string]string{"role": "release-manager"},
		}}},
	}
	err := v1.CheckSignalPolicies(wf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "both subject and subject_from")
}

// TestCheckSignalPolicyShapeRefusesUnresolvedSubjectWhenResolved is the
// negative direction: a policy decoded back off a run's memo must never
// still carry a rule's subject_from, because resolution has already run
// before anything reaches a memo. A populated subject_from at this point is
// corruption, not an authoring-time fact, and is refused rather than
// silently evaluated on a future signal delivery.
func TestCheckSignalPolicyShapeRefusesUnresolvedSubjectWhenResolved(t *testing.T) {
	policies := map[string]*v1.SignalPolicy{
		"deploy-approved": {Allow: []*v1.SignalPolicyRule{
			{SubjectFrom: v1.NewExpr("inputs.expected_approver"), Claims: map[string]string{"role": "release-manager"}},
		}},
	}
	err := v1.CheckSignalPolicyShape(policies, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unresolved expression")
}

// TestResolveSignalPolicySubjectsResolvesAgainstBoundInputs is the positive
// case: a rule's subject_from, referencing an input the run was bound with,
// resolves to that input's value and subject_from is cleared.
func TestResolveSignalPolicySubjectsResolvesAgainstBoundInputs(t *testing.T) {
	approver := v1.QualifiedSubject("https://issuer.example.com", "release-manager@example.com")

	wf := gateWorkflow()
	wf.Signals = map[string]*v1.SignalPolicy{
		"deploy-approved": {Allow: []*v1.SignalPolicyRule{
			{SubjectFrom: v1.NewExpr("inputs.expected_approver"), Claims: map[string]string{"role": "release-manager"}},
		}},
	}
	inputs := map[string]*v1.Value{
		"expected_approver": v1.NewLiteral(approver),
	}

	resolved, err := v1.ResolveSignalPolicySubjects(context.Background(), wf, inputs)
	require.NoError(t, err)

	rule := resolved["deploy-approved"].GetAllow()[0]
	require.Equal(t, approver, rule.GetSubject())
	require.Nil(t, rule.GetSubjectFrom())
	require.Equal(t, map[string]string{"role": "release-manager"}, rule.GetClaims(),
		"resolution replaces the expression and carries every other field of the rule through untouched")

	// The original, still-declared policy is untouched.
	require.NotNil(t, wf.GetSignals()["deploy-approved"].GetAllow()[0].GetSubjectFrom())
}

// TestResolveSignalPolicySubjectsRefusesAMalformedResult checks that a
// resolved subject is held to the same "<issuer>#<subject>" shape a literal
// subject is: an interpolated field that resolves to something else is
// refused rather than frozen into a policy no sender could ever satisfy
// correctly (or, worse, one that matches more than intended).
func TestResolveSignalPolicySubjectsRefusesAMalformedResult(t *testing.T) {
	wf := gateWorkflow()
	wf.Signals = map[string]*v1.SignalPolicy{
		"deploy-approved": {Allow: []*v1.SignalPolicyRule{
			{SubjectFrom: v1.NewExpr("inputs.expected_approver"), Claims: map[string]string{"role": "release-manager"}},
		}},
	}
	inputs := map[string]*v1.Value{
		"expected_approver": v1.NewLiteral("no-hash-here"),
	}

	_, err := v1.ResolveSignalPolicySubjects(context.Background(), wf, inputs)
	require.Error(t, err)
	require.Contains(t, err.Error(), "issuer")
}
