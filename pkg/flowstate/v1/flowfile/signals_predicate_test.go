package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `signals: <name>: allow: ${...}` — the one predicate that replaces the rule
// list. The engine's own behavior is pinned in signalpredicate_test.go and the
// shared conformance table; this file is the grammar: what an author writes,
// what it compiles to, and what is refused in the editor with a position.

func predicateSource(allow string) string {
	return `edition: v2026.4
name: deploy-gate
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      timeout: 24h
signals:
  deploy-approved:
    allow: ` + allow + `
`
}

const releasePredicate = `${sender.identity.claims.team == "release-managers" && sender.identity.principal != run.identity.principal}`

func TestParsingAnAllowPredicate(t *testing.T) {
	t.Parallel()

	workflow, positions, err := flowfile.Parse([]byte(predicateSource(`'` + releasePredicate + `'`)))
	require.NoError(t, err)

	policy := workflow.GetSignals()["deploy-approved"]
	require.NotNil(t, policy)
	assert.Empty(t, policy.GetAllow(), "a predicate is not a rule list")
	assert.Equal(t,
		`sender.identity.claims.team == "release-managers" && sender.identity.principal != run.identity.principal`,
		policy.GetAllowExpr(), "the stored source is the expression without its fence")

	_, ok := positions.At("signals.deploy-approved.allow")
	assert.True(t, ok)
	_, ok = positions.ExprAt("signals.deploy-approved.allow")
	assert.True(t, ok, "the editor underlines the expression, not its fence")

	diagnostics, err := flowfile.ValidateSource([]byte(predicateSource(`'` + releasePredicate + `'`)))
	require.NoError(t, err)
	require.Empty(t, diagnostics)
}

func TestAnAllowPredicateMayBeABlockScalar(t *testing.T) {
	t.Parallel()

	source := predicateSource(`>-
      ${ (sender.identity.principal == "https://issuer.example.com#" + "lead@example.com"
          && sender.identity.claims.team == "release-managers")
         && sender.identity.principal != run.identity.principal }`)

	workflow, err := flowfile.Unmarshal([]byte(source))
	require.NoError(t, err)

	expression := workflow.GetSignals()["deploy-approved"].GetAllowExpr()
	assert.Contains(t, expression, `sender.identity.claims.team == "release-managers"`)
	assert.NotContains(t, expression, "\n\n")

	diagnostics, err := flowfile.ValidateSource([]byte(source))
	require.NoError(t, err)
	require.Empty(t, diagnostics)
}

func TestMarshalIsTheInverseForAnAllowPredicate(t *testing.T) {
	t.Parallel()

	for _, allow := range []string{
		`'` + releasePredicate + `'`,
		`'${sender.identity.principal in ["https://a.example.com#x", "https://b.example.com#y"]}'`,
		`'${sender.identity.claims["k: v"] == "a # b"}'`,
	} {
		workflow, err := flowfile.Unmarshal([]byte(predicateSource(allow)))
		require.NoError(t, err, allow)

		written, err := flowfile.Marshal(workflow)
		require.NoError(t, err, allow)

		again, err := flowfile.Unmarshal(written)
		require.NoError(t, err, string(written))

		assert.Equal(t,
			workflow.GetSignals()["deploy-approved"].GetAllowExpr(),
			again.GetSignals()["deploy-approved"].GetAllowExpr(), "the predicate changed across a round trip:\n%s", written)
		assert.Empty(t, again.GetSignals()["deploy-approved"].GetAllow())

		again2, err := flowfile.Marshal(again)
		require.NoError(t, err)
		assert.Equal(t, string(written), string(again2), "a second Marshal is not byte-identical")
	}
}

func TestMarshalRefusesAPolicyHoldingBothMechanismsRatherThanDroppingOne(t *testing.T) {
	t.Parallel()

	workflow, err := flowfile.Unmarshal([]byte(predicateSource(`'` + releasePredicate + `'`)))
	require.NoError(t, err)
	workflow.GetSignals()["deploy-approved"].Allow = []*v1.SignalPolicyRule{{Namespace: "n"}}

	_, err = flowfile.Marshal(workflow)
	require.Error(t, err)
}

func TestAnAllowPredicateBesideDistinctFromStarterRoundTrips(t *testing.T) {
	t.Parallel()

	source := strings.Replace(predicateSource(`'${sender.identity.claims.team == "x"}'`),
		"    allow:", "    distinct_from_starter: true\n    allow:", 1)
	workflow, err := flowfile.Unmarshal([]byte(source))
	require.NoError(t, err)
	assert.True(t, workflow.GetSignals()["deploy-approved"].GetDistinctFromStarter())

	written, err := flowfile.Marshal(workflow)
	require.NoError(t, err)
	assert.Contains(t, string(written), "distinct_from_starter: true")
}

func TestAnAllowPredicateIsRefusedInTheEditorWithAPosition(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		allow string
		want  string
	}{
		"reads only inputs": {
			`'${sender.identity.principal == "https://issuer.example.com#" + inputs.approver}'`,
			"name themselves as their own approver",
		},
		"an unknown field":         {`'${sender.identity.bogus == "x"}'`, "bogus"},
		"a root outside the scope": {`'${steps.build.ok == true}'`, "steps"},
		"not a bool":               {`'${sender.identity.principal}'`, "want bool"},
	} {
		source := predicateSource(tc.allow)
		if name == "reads only inputs" {
			source = strings.Replace(source, "steps:", "inputs:\n  approver:\n    type: string\nsteps:", 1)
		}

		diagnostics, err := flowfile.ValidateSource([]byte(source))
		require.NoError(t, err, name)
		require.NotEmpty(t, diagnostics, name)

		var found bool
		for _, d := range diagnostics {
			if d.Field == "signals.deploy-approved.allow" && strings.Contains(d.Message, tc.want) {
				found = true
				assert.NotZero(t, d.Line, "%s: the diagnostic carries no position", name)
			}
		}
		assert.True(t, found, "%s: no diagnostic naming %q on the predicate: %v", name, tc.want, diagnostics)
	}
}

func TestAnAllowPredicateThatDoesNotParseIsRefusedAtTheSyntaxError(t *testing.T) {
	t.Parallel()

	_, _, err := flowfile.Parse([]byte(predicateSource(`'${sender.identity.principal ==}'`)))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is not a valid expression")
}

func TestAnAllowStringThatIsNotAnExpressionIsRefused(t *testing.T) {
	t.Parallel()

	_, _, err := flowfile.Parse([]byte(predicateSource(`'sender.identity.claims.team == "x"'`)))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a `${...}` expression")
}

func TestARuleListStillParsesAndRoundTripsUnchanged(t *testing.T) {
	t.Parallel()

	workflow, err := flowfile.Unmarshal([]byte(signaledSource))
	require.NoError(t, err)

	policy := workflow.GetSignals()["deploy-approved"]
	assert.Len(t, policy.GetAllow(), 2)
	assert.Empty(t, policy.GetAllowExpr())
}

func TestAnAllowPredicatePolicyEndToEnd(t *testing.T) {
	t.Parallel()

	workflow, err := flowfile.Unmarshal([]byte(predicateSource(`'${sender.identity.principal in ["https://a.example.com#alice", "https://b.example.com#bob"]}'`)))
	require.NoError(t, err)
	policy := workflow.GetSignals()["deploy-approved"]

	check := func(issuer, subject string) error {
		return v1.SignalPolicyCheck(t.Context(), policy,
			&v1.WorkloadIdentity{Issuer: issuer, Subject: subject}, nil, false, nil)
	}

	require.NoError(t, check("https://a.example.com", "alice"))
	require.NoError(t, check("https://b.example.com", "bob"))
	require.Error(t, check("https://b.example.com", "alice"), "alice from the other issuer was admitted")
	require.Error(t, check("https://a.example.com", "bob"))
}
