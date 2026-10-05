package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `debug: allow: ${...}` and `triggers: manual: allow: ${...}`: the grammar
// `signals:` already has, accepted beside the old forms. The engine's behaviour
// is pinned in pkg/flowstate/v1 and the server's; this file is what an author
// writes, what it compiles to, and what the editor refuses with a position.

func debugSource(allow string) string {
	return `edition: v2026.4
name: dbg
steps:
  - id: s
    sleep: 1s
debug:
  allow: ` + allow + `
`
}

func manualSource(manual string) string {
	return `edition: v2026.4
name: manual-gate
inputs:
  who:
    type: string
    default: nobody
triggers:
  - manual:
` + manual + `
steps:
  - id: s
    sleep: 1s
`
}

func TestDebugAcceptsAnAllowPredicateBesideTheRuleList(t *testing.T) {
	t.Parallel()

	workflow, err := flowfile.Unmarshal([]byte(debugSource(`'${sender.identity.claims.team == "sre"}'`)))
	require.NoError(t, err)
	assert.Equal(t, `sender.identity.claims.team == "sre"`, workflow.GetDebug().GetAllowExpr())
	assert.Empty(t, workflow.GetDebug().GetAllow())

	diagnostics, err := flowfile.ValidateSource([]byte(debugSource(`'${sender.identity.claims.team == "sre"}'`)))
	require.NoError(t, err)
	require.Empty(t, diagnostics)

	// The rule list is untouched.
	listed, err := flowfile.Unmarshal([]byte("edition: v2026.4\nname: dbg\nsteps:\n  - id: s\n    sleep: 1s\ndebug:\n  allow:\n    - claims: {team: sre}\n"))
	require.NoError(t, err)
	assert.Len(t, listed.GetDebug().GetAllow(), 1)
	assert.Empty(t, listed.GetDebug().GetAllowExpr())

	// Round trip, byte-stable.
	written, err := flowfile.Marshal(workflow)
	require.NoError(t, err)
	again, err := flowfile.Unmarshal(written)
	require.NoError(t, err)
	assert.Equal(t, workflow.GetDebug().GetAllowExpr(), again.GetDebug().GetAllowExpr())
	rewritten, err := flowfile.Marshal(again)
	require.NoError(t, err)
	assert.Equal(t, string(written), string(rewritten))
}

func TestADebugPredicateIsRefusedInTheEditorWithAPosition(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct{ allow, want string }{
		"inputs alone":     {`'${inputs.who == "x"}'`, "cannot reach"},
		"outside scope":    {`'${steps.s.ok == true}'`, "steps"},
		"not a bool":       {`'${sender.identity.principal}'`, "want bool"},
		"an unknown field": {`'${sender.identity.bogus == "x"}'`, "bogus"},
	} {
		source := strings.Replace(debugSource(tc.allow), "steps:", "inputs:\n  who:\n    type: string\n    default: x\nsteps:", 1)

		diagnostics, err := flowfile.ValidateSource([]byte(source))
		require.NoError(t, err, name)

		var found bool
		for _, d := range diagnostics {
			if d.Field == "debug.allow" && strings.Contains(d.Message, tc.want) {
				found = true
				assert.NotZero(t, d.Line, "%s: no position", name)
			}
		}
		assert.True(t, found, "%s: no diagnostic naming %q on debug.allow: %v", name, tc.want, diagnostics)
	}

	_, _, err := flowfile.Parse([]byte(debugSource(`'sender.identity.claims.team == "x"'`)))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a `${...}` expression")
}

func TestManualAcceptsAnAllowPredicateBesideItsOldForms(t *testing.T) {
	t.Parallel()

	workflow, _, err := flowfile.Parse([]byte(manualSource(
		`      allow: '${sender.identity.principal in ["https://issuer.example.com#ops@example.com"]}'`)))
	require.NoError(t, err)

	manual := workflow.GetTriggers().GetManual()
	require.NotNil(t, manual)
	assert.Equal(t, `sender.identity.principal in ["https://issuer.example.com#ops@example.com"]`, manual.GetAllowExpr())
	assert.Empty(t, manual.GetAllowedPrincipals())

	diagnostics, err := flowfile.ValidateSource([]byte(manualSource(
		`      allow: '${sender.identity.principal in ["https://issuer.example.com#ops@example.com"]}'`)))
	require.NoError(t, err)
	require.Empty(t, diagnostics)

	// The predicate and require_reason compose.
	both, err := flowfile.Unmarshal([]byte(manualSource("      require_reason: true\n      allow: '${sender.identity.claims.team == \"ops\"}'")))
	require.NoError(t, err)
	assert.True(t, both.GetTriggers().GetManual().GetRequireReason())
	assert.NotEmpty(t, both.GetTriggers().GetManual().GetAllowExpr())

	// allowed_principals alone is unchanged.
	old, err := flowfile.Unmarshal([]byte(manualSource("      allowed_principals: https://issuer.example.com#ops@example.com")))
	require.NoError(t, err)
	assert.Len(t, old.GetTriggers().GetManual().GetAllowedPrincipals(), 1)
	assert.Empty(t, old.GetTriggers().GetManual().GetAllowExpr())
}

func TestManualAllowRoundTripsAndMarshalRefusesBothMechanisms(t *testing.T) {
	t.Parallel()

	workflow, err := flowfile.Unmarshal([]byte(manualSource(
		"      require_reason: true\n      allow: '${sender.identity.claims.team == \"ops\"}'")))
	require.NoError(t, err)

	written, err := flowfile.Marshal(workflow)
	require.NoError(t, err)
	again, err := flowfile.Unmarshal(written)
	require.NoError(t, err, string(written))
	assert.Equal(t, workflow.GetTriggers().GetManual().GetAllowExpr(), again.GetTriggers().GetManual().GetAllowExpr())
	assert.True(t, again.GetTriggers().GetManual().GetRequireReason())
	rewritten, err := flowfile.Marshal(again)
	require.NoError(t, err)
	assert.Equal(t, string(written), string(rewritten), "a second Marshal is not byte-identical")

	// Marshal refuses to delete one of two mechanisms from an author's block.
	workflow.GetTriggers().GetManual().AllowedPrincipals = []string{"https://issuer.example.com#ops@example.com"}
	_, err = flowfile.Marshal(workflow)
	require.Error(t, err)
}

func TestAManualBlockWritingBothMechanismsIsRefusedInTheEditor(t *testing.T) {
	t.Parallel()

	source := manualSource("      allowed_principals: https://issuer.example.com#ops@example.com\n" +
		"      allow: '${sender.identity.claims.team == \"ops\"}'")

	workflow, err := flowfile.Unmarshal([]byte(source))
	require.NoError(t, err, "the two keys parse; refusing the pair is validation's, with a position")
	require.Error(t, v1.CheckManualTrigger(workflow.GetTriggers().GetManual()))

	diagnostics, err := flowfile.ValidateSource([]byte(source))
	require.NoError(t, err)
	require.NotEmpty(t, diagnostics)
	assert.Contains(t, diagnostics[0].Message, "one or the other")
}

func TestAManualPredicateIsRefusedInTheEditorWithAPosition(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct{ allow, want string }{
		"reads the run":     {`'${sender.identity.principal != run.identity.principal}'`, "run"},
		"inputs alone":      {`'${inputs.who == "x"}'`, "sender.identity.claims"},
		"outside the scope": {`'${steps.s.ok == true}'`, "steps"},
		"not a bool":        {`'${sender.identity.principal}'`, "want bool"},
	} {
		diagnostics, err := flowfile.ValidateSource([]byte(manualSource("      allow: " + tc.allow)))
		require.NoError(t, err, name)

		var found bool
		for _, d := range diagnostics {
			if strings.HasPrefix(d.Field, "triggers.manual") && strings.Contains(d.Message, tc.want) {
				found = true
				assert.NotZero(t, d.Line, "%s: no position", name)
			}
		}
		assert.True(t, found, "%s: no diagnostic naming %q on the manual predicate: %v", name, tc.want, diagnostics)
	}

	for name, allow := range map[string]string{
		"a bare string":    `'sender.identity.claims.team == "x"'`,
		"a list":           `[a, b]`,
		"a syntax error":   `'${sender.identity.principal ==}'`,
		"an empty fence":   `'${}'`,
		"an empty fence 2": `'${  }'`,
	} {
		_, _, err := flowfile.Parse([]byte(manualSource("      allow: " + allow)))
		require.Error(t, err, name, "an `allow:` that is not one predicate must not compile to a block that narrows nothing")
	}
}

func TestADeniedManualBlockCannotAlsoWriteAPredicate(t *testing.T) {
	t.Parallel()

	require.Error(t, v1.CheckManualTrigger(&v1.ManualTrigger{Denied: true, AllowExpr: `sender.identity.claims.team == "ops"`}))
}
