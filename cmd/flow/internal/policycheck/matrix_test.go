package policycheck_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/policycheck"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

func TestParseMatrixReadsARow(t *testing.T) {
	t.Parallel()

	matrix, err := policycheck.ParseMatrix([]byte(`
identities:
  - name: sre-lead
    principal:
      subject: sre-lead@example.com
      issuer: https://issuer.example.com
      namespace: prod
      claims: {team: release-managers}
    starter: {principal: {subject: dev@example.com, issuer: https://issuer.example.com}}
    inputs: {approver: sre-lead@example.com}
    expect_by_gate: {signals.approve: admitted, debug: refused}
  - name: nobody
    starter: {}
    expect: refused
  - name: unspecified
`))
	require.NoError(t, err)
	require.Len(t, matrix.Identities, 3)

	lead := matrix.Identities[0]
	require.Equal(t, "sre-lead", lead.Name)
	require.Equal(t, "sre-lead@example.com", lead.Subject)
	require.Equal(t, "prod", lead.Namespace)
	require.Equal(t, map[string]string{"team": "release-managers"}, lead.Claims)
	require.Equal(t, "dev@example.com", lead.Starter.Subject)
	require.Equal(t, "sre-lead@example.com", lead.Inputs["approver"])

	want, asserted := lead.Expect.For(signal("approve"))
	require.True(t, asserted)
	require.Equal(t, policycheck.OutcomeAdmitted, want)
	_, asserted = lead.Expect.For(signal("crash"))
	require.False(t, asserted, "a gate the map does not name asserts nothing")

	nobody := matrix.Identities[1]
	require.NotNil(t, nobody.Starter, "`starter: {}` is a known empty starter, not an unknown one")
	require.Equal(t, policycheck.OutcomeRefused, nobody.Expect.All)

	require.Nil(t, matrix.Identities[2].Starter, "an absent starter is unknown")
}

func TestParseMatrixRefusals(t *testing.T) {
	t.Parallel()

	many := "identities:\n" + strings.Repeat("  - name: x\n", 1)
	for i := range policycheck.MaxMatrixRows + 1 {
		many += fmt.Sprintf("  - name: n%d\n", i)
	}

	tests := []struct {
		name string
		doc  string
		want string
	}{
		{"empty", "identities: []", "must contain at least 1"},
		{"a misspelled key is not ignored", "identities:\n  - name: a\n    expct: admitted", "expct"},
		{"an unknown top-level key", "identities:\n  - name: a\nrows: []", "rows"},
		{"expect is one of two words", "identities:\n  - name: a\n    expect: allowed", "must be in list"},
		{"a per-gate expect is one of two words", "identities:\n  - name: a\n    expect_by_gate: {debug: maybe}", "must be in list"},
		{"no name", "identities:\n  - principal: {subject: a, issuer: b}", "identities[0].name: must be at least 1"},
		{"a name is not a control sequence", "identities:\n  - name: \"a\\u001b[31m\"", "does not match regex"},
		{"a name is not a C1 control sequence", "identities:\n  - name: \"a\\u009b31m\"", "does not match regex"},
		{"a long name", "identities:\n  - name: " + strings.Repeat("a", policycheck.MaxRowNameRunes+1), "must be at most 64"},
		{"a duplicate name", "identities:\n  - name: a\n  - name: a", "listed twice"},
		{"a subject without an issuer", "identities:\n  - name: a\n    principal: {subject: s}", `identity "a" names a subject or an issuer without the other`},
		{"an issuer without a subject", "identities:\n  - name: a\n    principal: {issuer: i}", "without the other"},
		{"a half-specified starter", "identities:\n  - name: a\n    starter: {principal: {subject: s}}", `identity "a" starter names a subject`},
		{"an empty claim value", "identities:\n  - name: a\n    principal: {claims: {team: \"\"}}", "empty value"},
		{"too many rows", many, "no more than 256"},
		{"too large", "identities:\n  - name: a\n" + strings.Repeat("#", policycheck.MaxMatrixBytes), "byte limit"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := policycheck.ParseMatrix([]byte(tt.doc))
			require.ErrorContains(t, err, tt.want)
		})
	}
}

// The matrix refuses a malformed identity with the sentence a test file's
// identity is refused with, because it asks the same function.
func TestAMatrixIdentityIsHeldToTheTestFileRule(t *testing.T) {
	t.Parallel()

	_, err := policycheck.ParseMatrix([]byte("identities:\n  - name: a\n    principal: {subject: s}\n"))
	require.Error(t, err)

	want := (&flowtest.ScriptedIdentity{Subject: "s"}).Check(`identity "a"`)
	require.Error(t, want)
	require.Equal(t, want.Error(), err.Error())
}

func TestCheckGatesRefusesAnExpectationNothingDecides(t *testing.T) {
	t.Parallel()

	matrix, err := policycheck.ParseMatrix([]byte("identities:\n  - name: a\n    expect_by_gate: {debug: refused}\n"))
	require.NoError(t, err)

	require.NoError(t, matrix.CheckGates([]policycheck.Gate{debugGate}))

	err = matrix.CheckGates([]policycheck.Gate{signal("approve")})
	require.ErrorContains(t, err, `"debug"`)
	require.ErrorContains(t, err, "signals.approve", "it names what is checked")
}

func TestExpectationAndResult(t *testing.T) {
	t.Parallel()

	admitted := policycheck.Decision{Gate: signal("a"), Admitted: true}
	refused := policycheck.Decision{Gate: debugGate, Reason: "no"}

	tests := []struct {
		name   string
		expect policycheck.Expectation
		wrong  []policycheck.Decision
	}{
		{"nothing asserted", policycheck.Expectation{}, nil},
		{"all, held", policycheck.Expectation{All: policycheck.OutcomeAdmitted}, []policycheck.Decision{refused}},
		{"all, the other way", policycheck.Expectation{All: policycheck.OutcomeRefused}, []policycheck.Decision{admitted}},
		{"per gate wins over all", policycheck.Expectation{
			All:    policycheck.OutcomeRefused,
			ByGate: map[string]policycheck.Outcome{"signals.a": policycheck.OutcomeAdmitted},
		}, nil},
		{"per gate, contradicted", policycheck.Expectation{
			ByGate: map[string]policycheck.Outcome{"debug": policycheck.OutcomeAdmitted},
		}, []policycheck.Decision{refused}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := policycheck.Result{Decisions: []policycheck.Decision{admitted, refused}, Expect: tt.expect}

			require.Equal(t, tt.wrong, result.Mismatches())
			require.Equal(t, len(tt.wrong) == 0, result.Matches())
		})
	}
}

func TestRenderingAnswersEachGateOnceAndMarksAMismatch(t *testing.T) {
	t.Parallel()

	gates := []policycheck.Gate{signal("approve"), debugGate}

	results := []policycheck.Result{
		{Name: "lead", Expect: policycheck.Expectation{All: policycheck.OutcomeAdmitted}, Decisions: []policycheck.Decision{
			{Gate: signal("approve"), Admitted: true},
			{Gate: debugGate, Reason: "the sender does not satisfy this debug policy's allow predicate"},
		}},
		{Name: "other", Decisions: []policycheck.Decision{
			{Gate: signal("approve"), Reason: "the sender does not satisfy this signal's allow predicate"},
			{Gate: debugGate, Reason: "the sender does not satisfy this debug policy's allow predicate"},
		}},
	}

	var table bytes.Buffer
	require.NoError(t, policycheck.WriteTable(&table, gates, results))

	out := table.String()
	require.Contains(t, out, "identity")
	require.Contains(t, out, "signals.approve")
	require.Contains(t, out, "refused (expected admitted)", "a contradicted expectation is marked in its cell")
	require.Equal(t, 1, strings.Count(out, "debug refuses with:"),
		"the engine's sentence is written once per gate, not once per row")

	var lines bytes.Buffer
	require.NoError(t, policycheck.WriteLines(&lines, results[0]))
	require.Contains(t, lines.String(), "signals.approve")
	require.Contains(t, lines.String(), "admitted")
	require.Contains(t, lines.String(), "refused (expected admitted): the sender does not satisfy this debug policy")

	report := policycheck.NewReport(gates, results)
	require.False(t, report.Matches)
	require.Equal(t, []string{"signals.approve", "debug"}, report.Gates)
	require.Equal(t, policycheck.OutcomeRefused, report.Results[0].Decisions[1].Outcome)
	require.NotNil(t, report.Results[0].Decisions[1].Matches)
	require.False(t, *report.Results[0].Decisions[1].Matches)
	require.Nil(t, report.Results[1].Decisions[0].Matches, "no expectation, no verdict")
}

// TestMatrixCarriesAKindToItsRows proves a row's `kind:` and its starter's reach
// the identities a predicate reads, and that a misspelled kind is refused when the
// matrix is read rather than recording none.
func TestMatrixCarriesAKindToItsRows(t *testing.T) {
	t.Parallel()

	matrix, err := policycheck.ParseMatrix([]byte(`identities:
  - name: a person
    principal: {subject: alice, issuer: https://issuer.example.com, kind: human}
    starter: {principal: {subject: ci, issuer: https://issuer.example.com, kind: workload}}
`))
	require.NoError(t, err)
	require.Equal(t, "human", matrix.Identities[0].Kind)
	require.Equal(t, "workload", matrix.Identities[0].Starter.Kind)

	_, err = policycheck.ParseMatrix([]byte("identities:\n  - name: typo\n    principal: {kind: humen}\n"))
	require.Error(t, err)
}

// A scripted identity reads strings, so what a row's principal can say beyond
// them is refused rather than dropped: a check that ignored a list claim or
// granted actions would answer for an identity other than the one written.
func TestMatrixRefusesWhatAScriptedIdentityCannotCarry(t *testing.T) {
	for name, tc := range map[string]struct{ yaml, want string }{
		"a list claim":   {"principal: {claims: {groups: [sre]}}", `claim "groups" is not a string`},
		"an object":      {"principal: {claims: {slack: {user: U1}}}", `claim "slack" is not a string`},
		"a number":       {"principal: {claims: {level: 3}}", `claim "level" is not a string`},
		"actions":        {"principal: {actions: [read]}", "`actions`"},
		"an issuer key":  {"principal: {issuer_entry: ci}", "`issuer_entry`"},
		"on the starter": {"starter: {principal: {claims: {groups: [sre]}}}", `claim "groups" is not a string`},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := policycheck.ParseMatrix([]byte("identities:\n  - name: a\n    " + tc.yaml + "\n"))
			require.ErrorContains(t, err, tc.want)
		})
	}
}
