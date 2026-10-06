package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// `debug: allow: ${...}` and `triggers: manual: allow: ${...}` are the same
// predicate `signals:` takes, decided by the same evaluator. The signal
// stanza's behaviour is pinned in signalpredicate_test.go; this file pins that
// the other two reach the evaluator, with their own scope, and fail closed.

func debugPredicate(expression string) *v1.SignalPolicy {
	return &v1.SignalPolicy{Allow: expression}
}

func TestADebugPredicateIsDecidedByTheSharedEvaluator(t *testing.T) {
	t.Parallel()

	sre := debugIdentity("https://idp.example", "sre-1", map[string]string{"team": "sre"})
	other := debugIdentity("https://idp.example", "dev-1", map[string]string{"team": "dev"})
	policy := debugPredicate(`sender.identity.claims.team == "sre"`)

	require.NoError(t, v1.CheckDebugPolicy(policy), "a predicate is a usable debug policy")
	require.NoError(t, v1.DebugPolicyCheck(t.Context(), policy, sre, nil, false, nil))

	err := v1.DebugPolicyCheck(t.Context(), policy, other, nil, false, nil)
	require.Error(t, err, "a caller the predicate does not admit was admitted to debug")
	assert.NotContains(t, err.Error(), "declares no `debug:` policy",
		"a predicate is a policy; the refusal must not read as though none existed")
	assert.Contains(t, err.Error(), "debug policy", "the refusal names its stanza, not `signal`")
}

func TestADebugPredicateFailsClosed(t *testing.T) {
	t.Parallel()

	caller := debugIdentity("https://idp.example", "sre-1", map[string]string{"team": "sre"})
	starter := debugIdentity("https://idp.example", "starter", nil)

	for name, test := range map[string]struct {
		expression string
		starter    *v1.WorkloadIdentity
		hasStarter bool
		inputs     map[string]*v1.Value
	}{
		"a non-bool result":       {expression: `sender.identity.principal`},
		"a missing claim errors":  {expression: `sender.identity.claims.nope == "x"`},
		"unknown starter is read": {expression: `sender.identity.principal != run.identity.principal`},
		"inputs are unbound":      {expression: `inputs.who == "x" && sender.identity.claims.team == "sre"`},
		"does not compile":        {expression: `sender.identity.claims.team ==`},
		"outside the scope":       {expression: `steps.a.ok == true`},
		"inputs alone":            {expression: `inputs.who == "https://idp.example#sre-1"`, inputs: map[string]*v1.Value{"who": v1.NewLiteral("https://idp.example#sre-1")}},
		"the starter is the caller": {
			expression: `sender.identity.principal != run.identity.principal`, starter: caller, hasStarter: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			require.Error(t, v1.DebugPolicyCheck(t.Context(), debugPredicate(test.expression), caller, test.starter,
				test.hasStarter, test.inputs), "this must deny")
		})
	}

	// The positive twin of the starter case, so the denial above is the
	// predicate's and not an error in the harness.
	require.NoError(t, v1.DebugPolicyCheck(t.Context(),
		debugPredicate(`sender.identity.principal != run.identity.principal`), caller, starter, true, nil))
	require.NoError(t, v1.DebugPolicyCheck(t.Context(),
		debugPredicate(`inputs.who == "x" && sender.identity.claims.team == "sre"`), caller, nil, false,
		map[string]*v1.Value{"who": v1.NewLiteral("x")}))
}

func TestADebugPredicateReadingInputsNeedsSomethingTheStarterCannotReach(t *testing.T) {
	t.Parallel()

	err := v1.CheckDebugPolicy(debugPredicate(`sender.identity.principal == "a#" + inputs.who`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot reach")

	require.NoError(t, v1.CheckDebugPolicy(
		debugPredicate(`sender.identity.principal == "a#" + inputs.who && sender.identity.claims.team == "sre"`)))
}

func TestADebugPredicateTellsTwoIssuersApart(t *testing.T) {
	t.Parallel()

	policy := debugPredicate(`sender.identity.principal == "https://a.example.com#sre-1"`)
	a := debugIdentity("https://a.example.com", "sre-1", nil)
	b := debugIdentity("https://b.example.com", "sre-1", nil)

	require.NoError(t, v1.DebugPolicyCheck(t.Context(), policy, a, nil, false, nil))
	require.Error(t, v1.DebugPolicyCheck(t.Context(), policy, b, nil, false, nil),
		"the same subject from another issuer was admitted")
}

func TestADebugPredicateComparingWithTheStarterRefusesTheStarter(t *testing.T) {
	t.Parallel()

	starter := debugIdentity("https://idp.example", "starter", map[string]string{"team": "sre"})
	other := debugIdentity("https://idp.example", "sre-2", map[string]string{"team": "sre"})
	policy := &v1.SignalPolicy{Allow: `sender.identity.claims.team == "sre" && sender.identity.principal != run.identity.principal`}

	require.NoError(t, v1.DebugPolicyCheck(t.Context(), policy, other, starter, true, nil))
	require.Error(t, v1.DebugPolicyCheck(t.Context(), policy, starter, starter, true, nil))
}

// manual

func manualPredicateWorkflow(expression string) *v1.Workflow {
	return &v1.Workflow{
		Name:     "manual-allow",
		Triggers: &v1.Triggers{Manual: &v1.ManualTrigger{Allow: expression}},
	}
}

func manualCaller(issuer, subject string, claims map[string]string) *v1.WorkloadIdentity {
	return &v1.WorkloadIdentity{Issuer: issuer, Subject: subject, Namespace: "team-a", Claims: claims}
}

func TestAManualAllowPredicateIsDecidedByTheSharedEvaluator(t *testing.T) {
	t.Parallel()

	ops := manualCaller("https://idp.example", "ops", map[string]string{"team": "ops"})
	dev := manualCaller("https://idp.example", "dev", map[string]string{"team": "dev"})
	wf := manualPredicateWorkflow(`sender.identity.claims.team == "ops"`)

	require.NoError(t, v1.CheckManualStart(t.Context(), wf, ops, v1.QualifiedSubject(ops.GetIssuer(), ops.GetSubject()), "", nil))

	err := v1.CheckManualStart(t.Context(), wf, dev, v1.QualifiedSubject(dev.GetIssuer(), dev.GetSubject()), "", nil)
	require.Error(t, err, "a caller the predicate does not admit was allowed to start the workload")
	assert.Contains(t, err.Error(), "manual start")
}

func TestAManualAllowPredicateFailsClosed(t *testing.T) {
	t.Parallel()

	caller := manualCaller("https://idp.example", "ops", map[string]string{"team": "ops"})
	principal := v1.QualifiedSubject(caller.GetIssuer(), caller.GetSubject())

	for name, test := range map[string]struct {
		expression string
		caller     *v1.WorkloadIdentity
		principal  string
		inputs     map[string]*v1.Value
	}{
		"a non-bool result":            {expression: `sender.identity.principal`, caller: caller, principal: principal},
		"a missing claim errors":       {expression: `sender.identity.claims.nope == "x"`, caller: caller, principal: principal},
		"does not compile":             {expression: `sender.identity.claims.team ==`, caller: caller, principal: principal},
		"reads the run":                {expression: `run.identity.principal == "x"`, caller: caller, principal: principal},
		"reads inputs only":            {expression: `inputs.who == "x"`, caller: caller, principal: principal, inputs: map[string]*v1.Value{"who": v1.NewLiteral("x")}},
		"inputs are unbound":           {expression: `inputs.who == "x" && sender.identity.claims.team == "ops"`, caller: caller, principal: principal},
		"an unauthenticated caller":    {expression: `sender.identity.principal != "x"`, caller: &v1.WorkloadIdentity{}, principal: ""},
		"a predicate that always lies": {expression: `sender.identity.claims.team == "dev"`, caller: caller, principal: principal},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			require.Error(t, v1.CheckManualStart(t.Context(), manualPredicateWorkflow(test.expression), test.caller,
				test.principal, "", test.inputs), "this must refuse the start")
		})
	}

	// The positive twins, so a refusal above is the predicate's and not the harness's.
	require.NoError(t, v1.CheckManualStart(t.Context(),
		manualPredicateWorkflow(`inputs.who == "x" && sender.identity.claims.team == "ops"`), caller, principal, "",
		map[string]*v1.Value{"who": v1.NewLiteral("x")}))
	require.NoError(t, v1.CheckManualStart(t.Context(),
		manualPredicateWorkflow(`sender.identity.principal != "x"`), caller, principal, "", nil))
}

func TestAManualPredicateReadingTheRunIsRefusedAtCompileTime(t *testing.T) {
	t.Parallel()

	err := v1.CheckManualAllowExpr(`sender.identity.principal != run.identity.principal`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "run")

	// The same text is a valid `signals:` predicate: the scope is what differs.
	require.NoError(t, v1.CheckSignalPolicyExpr(`sender.identity.principal != run.identity.principal`))

	// And the narrowing rule: with no starter to compare against, the claims are
	// the only thing the caller's inputs cannot reach.
	err = v1.CheckManualAllowExpr(`inputs.who == sender.identity.subject`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sender.identity.claims")
	require.NoError(t, v1.CheckManualAllowExpr(`inputs.who == sender.identity.subject && sender.identity.claims.team == "ops"`))
}

func TestAManualAllowPredicateContradictsDenied(t *testing.T) {
	t.Parallel()

	require.Error(t, v1.CheckManualTrigger(&v1.ManualTrigger{Denied: true, Allow: `sender.identity.claims.team == "ops"`}),
		"`denied` contradicts a predicate: a refusal that also says who may start is two sentences that cannot both be true")
}

func TestAManualAllowPredicateTellsTwoIssuersApart(t *testing.T) {
	t.Parallel()

	wf := manualPredicateWorkflow(`sender.identity.principal in ["https://a.example.com#ops"]`)
	a := manualCaller("https://a.example.com", "ops", nil)
	b := manualCaller("https://b.example.com", "ops", nil)

	require.NoError(t, v1.CheckManualStart(t.Context(), wf, a, "https://a.example.com#ops", "", nil))
	require.Error(t, v1.CheckManualStart(t.Context(), wf, b, "https://b.example.com#ops", "", nil),
		"the same subject from another issuer was allowed to start the workload")
}

func TestAManualAllowPredicateIsAskedBeforeTheReasonAndDoesNotReplaceIt(t *testing.T) {
	t.Parallel()

	caller := manualCaller("https://idp.example", "ops", map[string]string{"team": "ops"})
	wf := &v1.Workflow{Name: "reasoned", Triggers: &v1.Triggers{Manual: &v1.ManualTrigger{
		Allow: `sender.identity.claims.team == "ops"`, RequireReason: true,
	}}}

	require.Error(t, v1.CheckManualStart(t.Context(), wf, caller, "https://idp.example#ops", "  ", nil),
		"an admitted caller still owes the reason")
	require.NoError(t, v1.CheckManualStart(t.Context(), wf, caller, "https://idp.example#ops", "rotating a key", nil))
}
