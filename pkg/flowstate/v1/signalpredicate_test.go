package flowstatev1_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func predicatePolicy(expression string) *v1.SignalPolicy {
	return &v1.SignalPolicy{AllowExpr: expression}
}

func TestSignalPolicyPredicateIsTypeCheckedAgainstTheClosedScope(t *testing.T) {
	t.Parallel()

	for _, ok := range []string{
		`sender.identity.principal == "https://i#s"`,
		`sender.identity.subject == "s" && sender.identity.issuer == "i" && sender.identity.namespace == "n"`,
		`sender.identity.claims["team"] == "x"`,
		`has(sender.identity.claims.team) && sender.identity.claims.team == "x"`,
		`sender.identity.principal != run.identity.principal`,
		`sender.identity.principal in ["a#b", "c#d"]`,
		`sender.identity.principal == "a#" + inputs.who && sender.identity.claims["team"] == "x"`,
		`sender.identity.principal == "a#" + inputs.who && sender.identity.principal != run.identity.principal`,
	} {
		assert.NoError(t, v1.CheckSignalPolicyExpr(ok), ok)
	}

	for expression, want := range map[string]string{
		`sender.identity.bogus == "x"`:                `bogus`,
		`sender.local == true`:                        `local`,
		`steps.build.ok == true`:                      `steps`,
		`vars.x == "y"`:                               `vars`,
		`secrets.token == "x"`:                        `secrets`,
		`now > timestamp("2026-01-01T00:00:00Z")`:     `now`,
		`run.workflow_id == "x"`:                      `workflow_id`,
		`run.identity.deployment == "x"`:              `deployment`,
		`sender.identity.principal`:                   `want bool`,
		`"allowed"`:                                   `want bool`,
		`sender.identity.principal ==`:                `Syntax error`,
		``:                                            `empty`,
		`   `:                                         `empty`,
		`inputs.x == "y"`:                             `inputs`,
		`inputs.items.exists(i, i == "a")`:            `inputs`,
		`[1].exists(sender, sender == 1) && inputs.x`: `inputs`,
	} {
		err := v1.CheckSignalPolicyExpr(expression)
		require.Error(t, err, expression)
		assert.Contains(t, err.Error(), want, expression)
	}
}

func TestSignalPolicyNarrowingIsSyntacticAndNotFooledByShadowing(t *testing.T) {
	t.Parallel()

	// A comprehension variable spelled like the root is the author's local, and
	// reading it must not count as reading the starter or the sender's claims.
	for _, expression := range []string{
		`inputs.who == "x" && [1].exists(run, run == 1)`,
		`inputs.who == "x" && [1].exists(sender, sender == 1)`,
		`inputs.who == "x" && has(sender.identity.claims)`,
		`inputs.who == "x" && sender.identity.principal == "a#b"`,
	} {
		err := v1.CheckSignalPolicyExpr(expression)
		require.Error(t, err, expression)
		assert.Contains(t, err.Error(), "name themselves as their own approver", expression)
	}
}

func TestSignalPolicyExprReportsWhichPerRunValuesItReads(t *testing.T) {
	t.Parallel()

	reads := func(expression string) v1.SignalPolicyReads {
		return v1.SignalPolicyExprReads(map[string]*v1.SignalPolicy{"a": predicatePolicy(expression)})
	}

	assert.Equal(t, v1.SignalPolicyReads{}, reads(`sender.identity.principal == "a#b"`))
	assert.Equal(t, v1.SignalPolicyReads{Run: true}, reads(`sender.identity.principal != run.identity.principal`))
	assert.Equal(t, v1.SignalPolicyReads{Inputs: true},
		reads(`sender.identity.claims["x"] == inputs.x`))
	assert.Equal(t, v1.SignalPolicyReads{Inputs: true, Run: true},
		reads(`inputs.x == "a" && sender.identity.principal != run.identity.principal`))
	assert.Equal(t, v1.SignalPolicyReads{}, v1.SignalPolicyExprReads(map[string]*v1.SignalPolicy{
		"rules": {Allow: []*v1.SignalPolicyRule{{Namespace: "x"}}},
	}), "a rule-list policy reads nothing per-run")
}

func TestSignalPolicyRefusalNeverQuotesAnInputOrAClaim(t *testing.T) {
	t.Parallel()

	// cel-go's conversion errors quote the operand ("parsing \"...\""), and an
	// operand here can be an input or a claim.
	const secret = "TOPSECRETVALUE"

	byInput := predicatePolicy(`int(inputs.n) == 1 && sender.identity.claims["x"] == "y"`)
	err := v1.SignalPolicyCheck(t.Context(), byInput,
		&v1.WorkloadIdentity{Claims: map[string]string{"x": "y"}}, nil, false,
		map[string]*v1.Value{"n": v1.NewLiteral(secret)})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), secret, "an input's value reached the refusal")

	byClaim := predicatePolicy(`int(sender.identity.claims["n"]) == 1`)
	err = v1.SignalPolicyCheck(t.Context(), byClaim,
		&v1.WorkloadIdentity{Claims: map[string]string{"n": secret}}, nil, false, nil)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), secret, "a claim's value reached the refusal")

	// A predicate that is itself refused at compile names the author's own
	// expression and nothing a run supplied.
	err = v1.SignalPolicyCheck(t.Context(), predicatePolicy(`inputs.n == "`+"x"+`"`),
		&v1.WorkloadIdentity{}, nil, false, map[string]*v1.Value{"n": v1.NewLiteral(secret)})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), secret)
}

func TestSignalPolicyPredicateDeniesWhenTheContextIsCancelled(t *testing.T) {
	t.Parallel()

	cancelled, stop := context.WithCancel(t.Context())
	stop()

	err := v1.SignalPolicyCheck(cancelled,
		predicatePolicy(`sender.identity.principal == "a#b"`),
		&v1.WorkloadIdentity{Issuer: "a", Subject: "b"}, nil, false, nil)
	// Whether the interpreter notices before a trivial predicate finishes is
	// not the contract; that it never errors into an allow is.
	if err != nil {
		assert.NotContains(t, err.Error(), "a#b")
	}
}

func TestSignalPolicyCheckSetsBothMechanismsRefusedAndNeitherRefused(t *testing.T) {
	t.Parallel()

	both := &v1.SignalPolicy{
		Allow:     []*v1.SignalPolicyRule{{Namespace: "n"}},
		AllowExpr: `sender.identity.namespace == "n"`,
	}
	sender := &v1.WorkloadIdentity{Namespace: "n"}

	require.Error(t, v1.SignalPolicyCheck(t.Context(), both, sender, nil, false, nil),
		"a policy with both mechanisms was answered by one of them")
	require.Error(t, v1.CheckPolicyShape(`signals["x"]`, both, false))
	require.Error(t, v1.CheckPolicyShape(`signals["x"]`, &v1.SignalPolicy{}, false),
		"a policy with neither authorizes nobody and is refused, not read as open")

	only := predicatePolicy(`sender.identity.namespace == "n"`)
	require.NoError(t, v1.CheckPolicyShape(`signals["x"]`, only, false))
	require.NoError(t, v1.CheckPolicyShape(`signals["x"]`, only, true), "the decoded side checks the same policy")
	require.NoError(t, v1.SignalPolicyCheck(t.Context(), only, sender, nil, false, nil))
	require.Error(t, v1.SignalPolicyCheck(t.Context(), only, &v1.WorkloadIdentity{Namespace: "m"}, nil, false, nil))

	// The rule list is unchanged by any of this.
	rules := &v1.SignalPolicy{Allow: []*v1.SignalPolicyRule{{Namespace: "n"}}}
	require.NoError(t, v1.SignalPolicyCheck(t.Context(), rules, sender, nil, false, nil))
	require.Error(t, v1.SignalPolicyCheck(t.Context(), rules, &v1.WorkloadIdentity{Namespace: "m"}, nil, false, nil))
}

func TestSignalPolicyShapeRefusesAnUnusablePredicateWithTheStanzaNamed(t *testing.T) {
	t.Parallel()

	err := v1.CheckSignalPolicyShape(map[string]*v1.SignalPolicy{
		"deploy-approved": predicatePolicy(`inputs.who == "x"`),
	}, true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `signals["deploy-approved"].allow`)
	assert.Contains(t, err.Error(), "cannot reach")
}

func TestDebugStanzaDoesNotAcceptAnAllowPredicateYet(t *testing.T) {
	t.Parallel()

	policy := predicatePolicy(`sender.identity.claims["team"] == "sre"`)
	caller := &v1.WorkloadIdentity{Claims: map[string]string{"team": "sre"}}

	require.Error(t, v1.CheckDebugPolicy(policy, false))
	require.Error(t, v1.DebugPolicyCheck(policy, caller, nil, false),
		"a predicate in `debug:` must read as nobody, never as the caller it would admit")
}

func TestWebhookBridgeIsNotRefusedForAPredicatePolicy(t *testing.T) {
	t.Parallel()

	wf := &v1.Workflow{
		Name: "bridge",
		Signals: map[string]*v1.SignalPolicy{
			"stage-approved": predicatePolicy(`sender.identity.issuer == "` + v1.WebhookPrincipalIssuer + `"`),
		},
	}

	require.NoError(t, v1.CheckWebhookSignalPolicy(wf, "slack", &v1.WebhookTrigger_Signal{Name: "stage-approved"}))
}
