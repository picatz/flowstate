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
// `signals:` already has, the one grammar every who-may-act stanza uses. The engine's behaviour
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

func TestDebugAcceptsAnAllowPredicate(t *testing.T) {
	t.Parallel()

	workflow, err := flowfile.Unmarshal([]byte(debugSource(`'${sender.identity.claims.team == "sre"}'`)))
	require.NoError(t, err)
	assert.Equal(t, `sender.identity.claims.team == "sre"`, workflow.GetDebug().GetAllow())

	diagnostics, err := flowfile.ValidateSource([]byte(debugSource(`'${sender.identity.claims.team == "sre"}'`)))
	require.NoError(t, err)
	require.Empty(t, diagnostics)

	// The retired rule list is refused, with the way out and no echoed value.
	_, err = flowfile.Unmarshal([]byte("edition: v2026.4\nname: dbg\nsteps:\n  - id: s\n    sleep: 1s\ndebug:\n  allow:\n    - claims: {team: secret-team}\n"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "flow fix")
	assert.NotContains(t, err.Error(), "secret-team")

	// Round trip, byte-stable.
	written, err := flowfile.Marshal(workflow)
	require.NoError(t, err)
	again, err := flowfile.Unmarshal(written)
	require.NoError(t, err)
	assert.Equal(t, workflow.GetDebug().GetAllow(), again.GetDebug().GetAllow())
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

func TestManualAcceptsAnAllowPredicate(t *testing.T) {
	t.Parallel()

	workflow, _, err := flowfile.Parse([]byte(manualSource(
		`      allow: '${sender.identity.principal in ["https://issuer.example.com#ops@example.com"]}'`)))
	require.NoError(t, err)

	manual := workflow.GetTriggers().GetManual()
	require.NotNil(t, manual)
	assert.Equal(t, `sender.identity.principal in ["https://issuer.example.com#ops@example.com"]`, manual.GetAllow())

	diagnostics, err := flowfile.ValidateSource([]byte(manualSource(
		`      allow: '${sender.identity.principal in ["https://issuer.example.com#ops@example.com"]}'`)))
	require.NoError(t, err)
	require.Empty(t, diagnostics)

	// The predicate and require_reason compose.
	both, err := flowfile.Unmarshal([]byte(manualSource("      require_reason: true\n      allow: '${sender.identity.claims.team == \"ops\"}'")))
	require.NoError(t, err)
	assert.True(t, both.GetTriggers().GetManual().GetRequireReason())
	assert.NotEmpty(t, both.GetTriggers().GetManual().GetAllow())
}

func TestManualAllowedPrincipalsIsRefusedWithTheWayOut(t *testing.T) {
	t.Parallel()

	_, err := flowfile.Unmarshal([]byte(manualSource("      allowed_principals: https://issuer.example.com#secret-operator")))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "allowed_principals")
	assert.Contains(t, err.Error(), "flow fix")
	assert.NotContains(t, err.Error(), "secret-operator", "a refusal names the key, never the value")

	_, err = flowfile.ValidateSource([]byte(manualSource("      allowed_principals: https://issuer.example.com#secret-operator")))
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "secret-operator")
	assert.Regexp(t, `^\d+:\d+: `, err.Error(), "the refusal carries a position")
}

func TestManualAllowRoundTrips(t *testing.T) {
	t.Parallel()

	workflow, err := flowfile.Unmarshal([]byte(manualSource(
		"      require_reason: true\n      allow: '${sender.identity.claims.team == \"ops\"}'")))
	require.NoError(t, err)

	written, err := flowfile.Marshal(workflow)
	require.NoError(t, err)
	again, err := flowfile.Unmarshal(written)
	require.NoError(t, err, string(written))
	assert.Equal(t, workflow.GetTriggers().GetManual().GetAllow(), again.GetTriggers().GetManual().GetAllow())
	assert.True(t, again.GetTriggers().GetManual().GetRequireReason())
	rewritten, err := flowfile.Marshal(again)
	require.NoError(t, err)
	assert.Equal(t, string(written), string(rewritten), "a second Marshal is not byte-identical")
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

	require.Error(t, v1.CheckManualTrigger(&v1.ManualTrigger{Denied: true, Allow: `sender.identity.claims.team == "ops"`}))
}
