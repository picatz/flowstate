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
const debuggableSource = `edition: v2026.4
name: deploy-gate
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      timeout: 24h
signals:
  deploy-approved:
    allow: ${sender.identity.claims.team == "release-managers"}
debug:
  allow: >-
    ${(sender.identity.principal == "https://issuer.example.com#sre-1@example.com" ||
    sender.identity.claims.team == "sre") &&
    sender.identity.principal != run.identity.principal}
`

// TestParsingADebugStanza pins what the stanza compiles to, and that it lands
// on positions a diagnostic can point at.
func TestParsingADebugStanza(t *testing.T) {
	t.Parallel()

	workflow, positions, err := flowfile.Parse([]byte(debuggableSource))
	require.NoError(t, err)

	policy := workflow.GetDebug()
	require.NotNil(t, policy, "the `debug:` stanza did not compile to anything")
	assert.Contains(t, policy.GetAllow(), "https://issuer.example.com#sre-1@example.com")
	assert.Contains(t, policy.GetAllow(), `sender.identity.claims.team == "sre"`)
	assert.Contains(t, policy.GetAllow(), "sender.identity.principal != run.identity.principal",
		"separation of duties is expressible for debugging exactly as it is for signals")

	// The two stanzas are separate answers: who may approve is not who may
	// debug, and the file says both.
	require.NotNil(t, workflow.GetSignals()["deploy-approved"])
	assert.NotEqual(t, workflow.GetSignals()["deploy-approved"].GetAllow(), policy.GetAllow())

	for _, path := range []string{
		"debug", "debug.allow", "signals.deploy-approved.allow",
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
	assert.Equal(t, original.GetAllow(), roundTripped.GetAllow())

	// Byte-identical on a second pass, so `flow fmt` is idempotent for this key
	// too rather than only reversible.
	twice, err := flowfile.Marshal(again)
	require.NoError(t, err)
	assert.Equal(t, string(written), string(twice))
}

// TestADebugStanzaStillWritingTheRetiredFormsIsRefusedWithTheWayOut: the old
// rule list and `distinct_from_starter:` are refused at parse, with a sentence
// that says to run `flow fix` and names keys, never the values an author wrote.
func TestADebugStanzaStillWritingTheRetiredFormsIsRefusedWithTheWayOut(t *testing.T) {
	t.Parallel()

	for name, stanza := range map[string]string{
		"a rule list":           "debug:\n  allow:\n    - subject: secret-subject@example.com\n",
		"distinct_from_starter": "debug:\n  allow: ${sender.identity.claims.team == \"secret-team\"}\n  distinct_from_starter: true\n",
	} {
		source := "edition: v2026.4\nname: old\nsteps:\n  - id: work\n    log:\n      message: hello\n" + stanza

		_, err := flowfile.Unmarshal([]byte(source))
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), "flow fix", name)
		assert.NotContains(t, err.Error(), "secret-", "%s: a refusal names the key, never the value", name)

		_, err = flowfile.ValidateSource([]byte(source))
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), "flow fix", name)
		assert.Regexp(t, `^\d+:\d+: `, err.Error(), name)
	}
}

// TestADebugStanzaWithNoAllowNamesTheClosedDefault: the shared grammar refuses
// a policy with no `allow:` in both stanzas, but the remedy differs. Removing a
// signal's policy opens the signal; removing `debug:` closes debugging, so the
// sentence a `signals:` entry gets would tell a `debug:` author the opposite of
// what removing it does.
func TestADebugStanzaWithNoAllowNamesTheClosedDefault(t *testing.T) {
	t.Parallel()

	_, err := flowfile.ValidateSource([]byte(`edition: v2026.4
name: no-allow
steps:
  - id: work
    log:
      message: hello
debug: {}
`))
	require.Error(t, err, "a debug policy with no allow predicate was accepted")

	assert.Contains(t, err.Error(), "authorizes nobody")
	assert.Contains(t, err.Error(), "not debuggable at all")
	assert.NotContains(t, err.Error(), "any authenticated caller",
		"an absent `debug:` denies every pause ask, so removing it opens nothing")

	// The neighbouring stanza keeps its own remedy, which is true there.
	_, err = flowfile.ValidateSource([]byte(`edition: v2026.4
name: no-allow
steps:
  - id: approval
    wait_for_signal:
      name: go
signals:
  go: {}
`))
	require.Error(t, err, "a signal policy with no allow list was accepted")
	assert.Contains(t, err.Error(), "any authenticated caller in the run's tenant may deliver it")
}

// TestTheNarrowingCheckAppliesToDebugToo is the security half of sharing the
// grammar: a predicate whose principal comes from the run's own inputs lets whoever
// started the run name themselves, and a debug policy is exactly where that
// would matter most.
func TestTheNarrowingCheckAppliesToDebugToo(t *testing.T) {
	t.Parallel()

	unnarrowed := `edition: v2026.4
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
  allow: ${sender.identity.principal == "https://issuer.example.com#" + inputs.debugger}
`

	diagnostics, err := flowfile.ValidateSource([]byte(unnarrowed))
	require.NoError(t, err)
	require.NotEmpty(t, diagnostics, "a caller could name themselves as the caller allowed to pause their own run")
	assert.Equal(t, "debug.allow", diagnostics[0].Field)
	assert.Contains(t, diagnostics[0].Message, "sender.identity.claims")

	// And the same rule accepted once something the run's inputs cannot reach
	// is beside it, so the diagnostic above is about the narrowing rather than
	// about expressions being refused here at all.
	narrowed := strings.Replace(unnarrowed,
		`"https://issuer.example.com#" + inputs.debugger}`,
		`"https://issuer.example.com#" + inputs.debugger && sender.identity.claims.team == "sre"}`, 1)

	diagnostics, err = flowfile.ValidateSource([]byte(narrowed))
	require.NoError(t, err)
	assert.Empty(t, diagnostics, "a narrowed interpolation is legal for `debug:` exactly as it is for `signals:`")
}

// TestAWaitOnAReservedNameIsRefused is the collision the engine's reservation
// exists to prevent, reported where an author meets it.
func TestAWaitOnAReservedNameIsRefused(t *testing.T) {
	t.Parallel()

	diagnostics, err := flowfile.ValidateSource([]byte(`edition: v2026.4
name: collides
steps:
  - id: gate
    wait_for_signal:
      name: ` + v1.DebugSignal + `
      timeout: 1h
`))
	require.NoError(t, err)
	require.NotEmpty(t, diagnostics, "a wait a pause ask would answer was accepted")
	assert.Contains(t, diagnostics[0].Message, v1.ReservedSignalPrefix)

	// And it points at the wait rather than at the file. A diagnostic that names
	// `steps` sends an author to the whole block; this one underlines the key
	// they wrote, which is the standard this package sets.
	assert.Equal(t, "gate", diagnostics[0].Step,
		"the diagnostic does not say which step waits on the reserved channel")
	assert.Equal(t, "wait_for_signal.name", diagnostics[0].Field,
		"the diagnostic does not say which key is at fault, so it cannot underline one")
	assert.Positive(t, diagnostics[0].Line,
		"the diagnostic has no position, so an editor has nowhere to put it")

	// The batch spelling declares the same channel and gets the same refusal
	// under its own key — a name reported for one spelling and not the other
	// would be a gate that is refused or not depending on how it was written.
	batch, err := flowfile.ValidateSource([]byte(`edition: v2026.4
name: collides-in-a-batch
steps:
  - id: gate
    wait_for_signals:
      name: ` + v1.DebugSignal + `
      max_batch: 2
      timeout: 1h
`))
	require.NoError(t, err)
	require.NotEmpty(t, batch, "a batch wait on a reserved channel was accepted")
	assert.Equal(t, "wait_for_signals.name", batch[0].Field)
	assert.Equal(t, "gate", batch[0].Step)

	// The positive direction: an ordinary name on the same shape is fine, so
	// the refusal is about the reservation rather than about waits.
	diagnostics, err = flowfile.ValidateSource([]byte(`edition: v2026.4
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

	diagnostics, err := flowfile.ValidateSource([]byte(`edition: v2026.4
name: smuggled
steps:
  - id: work
    log:
      message: hello
signals:
  ` + v1.DebugSignal + `:
    allow: ${sender.identity.claims.team == "sre"}
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

	workflow, err := flowfile.Unmarshal([]byte(`edition: v2026.4
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
