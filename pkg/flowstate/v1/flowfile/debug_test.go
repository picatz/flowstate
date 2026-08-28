package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// picatz/flowstate#928 stage 2's grammar: `debug:` declares who may pause a
// durable run at a step boundary under a lease. It shares `signals:`'s
// grammar entirely — see flowfile/signals.go, and [v1.Workflow.Debug] for why
// its zero case denies where its neighbour's allows.
const debuggableSource = `edition: v2026.3
name: deploy-gate
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      timeout: 24h
signals:
  deploy-approved:
    allow:
      - claims:
          team: release-managers
debug:
  allow:
    - subject: "https://issuer.example.com#sre-1@example.com"
    - claims:
        team: sre
  distinct_from_starter: true
`

// TestParsingADebugStanza pins what the stanza compiles to, and that it lands
// on positions a diagnostic can point at.
func TestParsingADebugStanza(t *testing.T) {
	t.Parallel()

	workflow, positions, err := flowfile.Parse([]byte(debuggableSource))
	require.NoError(t, err)

	policy := workflow.GetDebug()
	require.NotNil(t, policy, "the `debug:` stanza did not compile to anything")
	require.Len(t, policy.GetAllow(), 2)

	assert.Equal(t, "https://issuer.example.com#sre-1@example.com", policy.GetAllow()[0].GetSubject())
	assert.Equal(t, map[string]string{"team": "sre"}, policy.GetAllow()[1].GetClaims())
	assert.True(t, policy.GetDistinctFromStarter(),
		"separation of duties is expressible for debugging exactly as it is for signals")

	// The two stanzas are separate answers: who may approve is not who may
	// debug, and the file says both.
	require.NotNil(t, workflow.GetSignals()["deploy-approved"])
	assert.NotEqual(t,
		workflow.GetSignals()["deploy-approved"].GetAllow()[0].GetClaims(),
		policy.GetAllow()[1].GetClaims())

	for _, path := range []string{
		"debug", "debug.allow", "debug.allow[0]", "debug.allow[0].subject",
		"debug.allow[1]", "debug.allow[1].claims",
	} {
		_, ok := positions.At(path)
		assert.True(t, ok, "no recorded position for %q", path)
	}
}

// TestADebugStanzaValidates is the property every diagnostic test below has to
// be distinguishable from.
func TestADebugStanzaValidates(t *testing.T) {
	t.Parallel()

	diagnostics, err := flowfile.ValidateSource([]byte(debuggableSource))
	require.NoError(t, err)
	require.Empty(t, diagnostics, "a well-formed debug policy reported a diagnostic")
}

// TestMarshalIsTheInverseForDebug is the `flow fmt` guard: a key the parser
// reads and the writer does not know about is a command that silently deletes
// an author's policy.
func TestMarshalIsTheInverseForDebug(t *testing.T) {
	t.Parallel()

	workflow, err := flowfile.Unmarshal([]byte(debuggableSource))
	require.NoError(t, err)
	require.NotNil(t, workflow.GetDebug(), "the fixture has no debug policy to lose")

	written, err := flowfile.Marshal(workflow)
	require.NoError(t, err)
	require.Contains(t, string(written), "debug:",
		"the stanza was dropped on the way out")

	again, err := flowfile.Unmarshal(written)
	require.NoError(t, err)

	original, roundTripped := workflow.GetDebug(), again.GetDebug()
	require.NotNil(t, roundTripped, "the debug policy vanished across Marshal/Unmarshal")
	require.Len(t, roundTripped.GetAllow(), len(original.GetAllow()))

	for i, rule := range original.GetAllow() {
		assert.Equal(t, rule.GetSubject(), roundTripped.GetAllow()[i].GetSubject())
		assert.Equal(t, rule.GetNamespace(), roundTripped.GetAllow()[i].GetNamespace())
		assert.Equal(t, rule.GetClaims(), roundTripped.GetAllow()[i].GetClaims())
	}
	assert.Equal(t, original.GetDistinctFromStarter(), roundTripped.GetDistinctFromStarter())

	// Byte-identical on a second pass, so `flow fmt` is idempotent for this key
	// too rather than only reversible.
	twice, err := flowfile.Marshal(again)
	require.NoError(t, err)
	assert.Equal(t, string(written), string(twice))
}

// TestADebugRuleWithNothingSetIsRefused: the shape checks a signal policy gets
// apply here, reported against `debug:` rather than against a stanza the author
// did not write.
func TestADebugRuleWithNothingSetIsRefused(t *testing.T) {
	t.Parallel()

	diagnostics, err := flowfile.ValidateSource([]byte(`edition: v2026.3
name: wide-open
steps:
  - id: work
    log:
      message: hello
debug:
  allow:
    - namespace: ""
`))
	require.NoError(t, err)
	require.NotEmpty(t, diagnostics, "a rule that matches every sender was accepted")

	assert.Equal(t, "debug.allow[0]", diagnostics[0].Field)
	assert.Contains(t, diagnostics[0].Message, "matches every sender")
}

// TestADebugSubjectMustBeIssuerQualified: a subject is unique only within its
// issuer, and the rule that says so is the same one `signals:` follows —
// refused by the compiler, with a line and a column, because sharing the
// grammar means sharing the diagnostics rather than only the message shape.
func TestADebugSubjectMustBeIssuerQualified(t *testing.T) {
	t.Parallel()

	_, err := flowfile.ValidateSource([]byte(`edition: v2026.3
name: bare-subject
steps:
  - id: work
    log:
      message: hello
debug:
  allow:
    - subject: sre-1@example.com
`))
	require.Error(t, err, "a bare subject was accepted")

	assert.Contains(t, err.Error(), "debug.allow[0].subject",
		"the fault is reported at the path in the author's own file")
	assert.Contains(t, err.Error(), "issuer")
	assert.NotContains(t, err.Error(), "signals",
		"an author reading a fault about `debug:` is not told about a stanza they did not write")
}

// TestTheNarrowingCheckAppliesToDebugToo is the security half of sharing the
// grammar: a rule whose subject comes from the run's own inputs lets whoever
// started the run name themselves, and a debug policy is exactly where that
// would matter most.
func TestTheNarrowingCheckAppliesToDebugToo(t *testing.T) {
	t.Parallel()

	unnarrowed := `edition: v2026.3
name: self-debug
inputs:
  debugger:
    type: string
    required: true
steps:
  - id: work
    log:
      message: hello
debug:
  allow:
    - subject: ${inputs.debugger}
`

	diagnostics, err := flowfile.ValidateSource([]byte(unnarrowed))
	require.NoError(t, err)
	require.NotEmpty(t, diagnostics, "a caller could name themselves as the caller allowed to pause their own run")
	assert.Equal(t, "debug.allow[0].subject", diagnostics[0].Field)
	assert.Contains(t, diagnostics[0].Message, "narrows")

	// And the same rule accepted once something the run's inputs cannot reach
	// is beside it, so the diagnostic above is about the narrowing rather than
	// about expressions being refused here at all.
	narrowed := strings.Replace(unnarrowed,
		"    - subject: ${inputs.debugger}\n",
		"    - subject: ${inputs.debugger}\n      claims:\n        team: sre\n", 1)

	diagnostics, err = flowfile.ValidateSource([]byte(narrowed))
	require.NoError(t, err)
	assert.Empty(t, diagnostics, "a narrowed interpolation is legal for `debug:` exactly as it is for `signals:`")
}

// TestAWaitOnAReservedNameIsRefused is the collision the engine's reservation
// exists to prevent, reported where an author meets it.
func TestAWaitOnAReservedNameIsRefused(t *testing.T) {
	t.Parallel()

	diagnostics, err := flowfile.ValidateSource([]byte(`edition: v2026.3
name: collides
steps:
  - id: gate
    wait_for_signal:
      name: ` + v1.DebugPauseSignal + `
      timeout: 1h
`))
	require.NoError(t, err)
	require.NotEmpty(t, diagnostics, "a wait a pause ask would answer was accepted")
	assert.Contains(t, diagnostics[0].Message, v1.ReservedSignalPrefix)

	// The positive direction: an ordinary name on the same shape is fine, so
	// the refusal is about the reservation rather than about waits.
	diagnostics, err = flowfile.ValidateSource([]byte(`edition: v2026.3
name: ordinary
steps:
  - id: gate
    wait_for_signal:
      name: deploy-approved
      timeout: 1h
`))
	require.NoError(t, err)
	assert.Empty(t, diagnostics)
}

// TestASignalPolicyOnAReservedNameIsRefused: who may debug is `debug:`, and a
// policy smuggled in under a reserved signal name would be a second spelling
// governing a channel `signals:` does not own.
func TestASignalPolicyOnAReservedNameIsRefused(t *testing.T) {
	t.Parallel()

	diagnostics, err := flowfile.ValidateSource([]byte(`edition: v2026.3
name: smuggled
steps:
  - id: work
    log:
      message: hello
signals:
  ` + v1.DebugPauseSignal + `:
    allow:
      - claims:
          team: sre
`))
	require.NoError(t, err)
	require.NotEmpty(t, diagnostics)

	var found bool
	for _, d := range diagnostics {
		if strings.Contains(d.Message, v1.ReservedSignalPrefix) {
			found = true
		}
	}
	assert.True(t, found,
		"a `signals:` policy under a reserved name was accepted; diagnostics were %v", diagnostics)
}

// TestAnAbsentDebugStanzaRoundTripsAsAbsent: a workflow that is not debuggable
// must not gain an empty stanza by being written out, which would make `flow
// fmt` change what a file means.
func TestAnAbsentDebugStanzaRoundTripsAsAbsent(t *testing.T) {
	t.Parallel()

	workflow, err := flowfile.Unmarshal([]byte(`edition: v2026.3
name: plain
steps:
  - id: work
    log:
      message: hello
`))
	require.NoError(t, err)
	require.Nil(t, workflow.GetDebug())

	written, err := flowfile.Marshal(workflow)
	require.NoError(t, err)
	assert.NotContains(t, string(written), "debug:",
		"a workflow nobody may debug gained a stanza saying so")

	again, err := flowfile.Unmarshal(written)
	require.NoError(t, err)
	assert.Nil(t, again.GetDebug(),
		"absent has to survive the round trip, because absent is what denies")
}
