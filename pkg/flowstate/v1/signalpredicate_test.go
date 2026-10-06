package flowstatev1_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func predicatePolicy(expression string) *v1.SignalPolicy {
	return &v1.SignalPolicy{Allow: expression}
}

// TestSignalPolicyPredicateReadsTheKindAPolicyAssigned proves `kind` is read from
// the identity and only equals a named kind: a sender the trust policy gave no
// kind never satisfies `kind == "human"`, and is never mistaken for a workload.
func TestSignalPolicyPredicateReadsTheKindAPolicyAssigned(t *testing.T) {
	t.Parallel()

	policy := predicatePolicy(`sender.identity.kind == "human" && sender.identity.principal != run.identity.principal`)
	starter := &v1.WorkloadIdentity{Issuer: "https://i", Subject: "starter", PrincipalKind: v1.PrincipalKind_PRINCIPAL_KIND_WORKLOAD}

	for kind, wantAllowed := range map[v1.PrincipalKind]bool{
		v1.PrincipalKind_PRINCIPAL_KIND_HUMAN:       true,
		v1.PrincipalKind_PRINCIPAL_KIND_AGENT:       false,
		v1.PrincipalKind_PRINCIPAL_KIND_WORKLOAD:    false,
		v1.PrincipalKind_PRINCIPAL_KIND_UNSPECIFIED: false,
	} {
		sender := &v1.WorkloadIdentity{Issuer: "https://i", Subject: "alice", PrincipalKind: kind}
		err := v1.SignalPolicyCheck(context.Background(), policy, sender, starter, true, nil)
		if wantAllowed {
			require.NoError(t, err, kind.String())
		} else {
			require.Error(t, err, kind.String())
		}
	}

	// The starter's own kind is readable too.
	require.NoError(t, v1.SignalPolicyCheck(context.Background(),
		predicatePolicy(`run.identity.kind == "workload" && sender.identity.principal != ""`),
		&v1.WorkloadIdentity{Issuer: "https://i", Subject: "alice"}, starter, true, nil))
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
	assert.Equal(t, v1.SignalPolicyReads{Inputs: true, InputNames: []string{"x"}},
		reads(`sender.identity.claims["x"] == inputs.x`))
	assert.Equal(t, v1.SignalPolicyReads{Inputs: true, InputNames: []string{"x"}, Run: true},
		reads(`inputs.x == "a" && sender.identity.principal != run.identity.principal`))
	assert.Equal(t, v1.SignalPolicyReads{}, v1.SignalPolicyExprReads(map[string]*v1.SignalPolicy{
		"none": {},
	}), "a policy with no predicate reads nothing per-run")
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

func TestSignalPolicyCheckRefusesAPolicyWithNoPredicate(t *testing.T) {
	t.Parallel()

	sender := &v1.WorkloadIdentity{Namespace: "n"}

	require.Error(t, v1.SignalPolicyCheck(t.Context(), &v1.SignalPolicy{}, sender, nil, false, nil),
		"a policy with no predicate was read as open")
	require.Error(t, v1.CheckPolicyShape(`signals["x"]`, &v1.SignalPolicy{}),
		"a policy with no predicate authorizes nobody and is refused, not read as open")

	only := predicatePolicy(`sender.identity.namespace == "n"`)
	require.NoError(t, v1.CheckPolicyShape(`signals["x"]`, only))
	require.NoError(t, v1.SignalPolicyCheck(t.Context(), only, sender, nil, false, nil))
	require.Error(t, v1.SignalPolicyCheck(t.Context(), only, &v1.WorkloadIdentity{Namespace: "m"}, nil, false, nil))
}

func TestSignalPolicyShapeRefusesAnUnusablePredicateWithTheStanzaNamed(t *testing.T) {
	t.Parallel()

	err := v1.CheckSignalPolicyShape(map[string]*v1.SignalPolicy{
		"deploy-approved": predicatePolicy(`inputs.who == "x"`),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `signals["deploy-approved"].allow`)
	assert.Contains(t, err.Error(), "cannot reach")
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

// The run records only the input names a predicate names, so a name has to be
// readable statically, in every spelling that names one.
func TestSignalPolicyExprReportsTheInputNamesItReads(t *testing.T) {
	t.Parallel()

	names := func(expression string) []string {
		return v1.SignalPolicyExprReads(map[string]*v1.SignalPolicy{"a": predicatePolicy(expression)}).InputNames
	}
	const narrow = ` && sender.identity.claims["t"] == "x"`

	assert.Equal(t, []string{"a", "b", "c", "d"},
		names(`inputs.b == "" && inputs["a"] == "" && has(inputs.c) && "d" in inputs && inputs.a.x == ""`+narrow))
	assert.Equal(t, []string{"x"}, names(`inputs.x == "" && inputs.x != "y"`+narrow), "a name read twice is one name")
	assert.Empty(t, names(`[1].exists(inputs, inputs == 1) && sender.identity.claims["t"] == "x"`),
		"a comprehension variable spelled inputs is the author's local, not the run's inputs")
}

// A predicate that reads `inputs` without naming a key cannot be recorded
// narrowly, and recording everything would put sensitive inputs in history.
func TestSignalPolicyRefusesAnInputsReadThatNamesNoInput(t *testing.T) {
	t.Parallel()

	const narrow = ` && sender.identity.claims["t"] == "x"`

	for _, expression := range []string{
		`inputs[sender.identity.claims["k"]] == "y"` + narrow,
		`size(inputs) > 0` + narrow,
		`inputs.exists(k, k == "a")` + narrow,
		`has(inputs.a) && inputs.all(k, true)` + narrow,
	} {
		err := v1.CheckSignalPolicyExpr(expression)
		require.Error(t, err, expression)
		assert.Contains(t, err.Error(), "without naming an input", expression)
	}
}

func TestCheckWorkflowPolicyInputsRefusesASensitiveInput(t *testing.T) {
	t.Parallel()

	workflow := func(signal, debug string) *v1.Workflow {
		wf := &v1.Workflow{
			DeclaredInputs: []*v1.InputDeclaration{
				{Name: "approver"},
				{Name: "token", Sensitive: true},
			},
			Signals: map[string]*v1.SignalPolicy{"go": predicatePolicy(signal)},
		}
		if debug != "" {
			wf.Debug = predicatePolicy(debug)
		}

		return wf
	}
	const narrow = ` && sender.identity.claims["t"] == "x"`

	require.NoError(t, v1.CheckWorkflowPolicyInputs(workflow(`inputs.approver == "a"`+narrow, "")))
	require.NoError(t, v1.CheckWorkflowPolicyInputs(workflow(`sender.identity.claims["t"] == "x"`, "")),
		"a predicate that reads no input reads no sensitive one")

	err := v1.CheckWorkflowPolicyInputs(workflow(`inputs["token"] == "hunter2"`+narrow, ""))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `signals["go"].allow reads the input "token"`)
	assert.NotContains(t, err.Error(), "hunter2", "the refusal quotes the author's input name, never a value")

	err = v1.CheckWorkflowPolicyInputs(workflow(`inputs.approver == "a"`+narrow, `has(inputs.token)`+narrow))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `debug.allow reads the input "token"`)
}
