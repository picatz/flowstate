package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// #206 gap 1's grammar: `signals:` declares, per signal name, who may
// deliver it, as one `allow: ${...}` predicate. See
// [pkg/flowstate/v1/signalpolicy.go] for the enforcement this describes and
// [pkg/flowstate/v1/server/lifecycle.go] for where it is checked. The
// predicate-specific grammar is pinned in signals_predicate_test.go; this file
// is the block around it.
const signaledSource = `edition: v2026.4
name: deploy-gate
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      timeout: 24h
signals:
  deploy-approved:
    allow: >-
      ${sender.identity.principal == "https://issuer.example.com#release-manager@example.com" ||
      sender.identity.claims.team == "release-managers"}
`

// TestParsingASignalsBlock pins what the block compiles to.
func TestParsingASignalsBlock(t *testing.T) {
	t.Parallel()

	workflow, positions, err := flowfile.Parse([]byte(signaledSource))
	require.NoError(t, err)

	policy := workflow.GetSignals()["deploy-approved"]
	require.NotNil(t, policy)
	assert.Contains(t, policy.GetAllow(), `sender.identity.principal == "https://issuer.example.com#release-manager@example.com"`)
	assert.Contains(t, policy.GetAllow(), `sender.identity.claims.team == "release-managers"`)

	for _, path := range []string{
		"signals", "signals.deploy-approved", "signals.deploy-approved.allow",
	} {
		_, ok := positions.At(path)
		assert.True(t, ok, "no recorded position for %q", path)
	}
}

// TestASignalsBlockValidates checks that a well-formed policy passes
// [flowfile.Validate] cleanly — the property every other diagnostic test in
// this file depends on being distinguishable from.
func TestASignalsBlockValidates(t *testing.T) {
	t.Parallel()

	diagnostics, err := flowfile.ValidateSource([]byte(signaledSource))
	require.NoError(t, err)
	require.Empty(t, diagnostics, "a well-formed signal policy reported a diagnostic")
}

// TestMarshalIsTheInverseForSignals is the guard against `flow fmt` silently
// deleting an author's signal policy — the identical hazard
// [TestMarshalIsTheInverseForTriggers] guards for `triggers:`.
func TestMarshalIsTheInverseForSignals(t *testing.T) {
	t.Parallel()

	workflow, err := flowfile.Unmarshal([]byte(signaledSource))
	require.NoError(t, err)

	written, err := flowfile.Marshal(workflow)
	require.NoError(t, err)

	again, err := flowfile.Unmarshal(written)
	require.NoError(t, err)

	assert.Equal(t, len(workflow.GetSignals()), len(again.GetSignals()))
	for name, policy := range workflow.GetSignals() {
		roundTripped := again.GetSignals()[name]
		require.NotNil(t, roundTripped, "signal %q vanished across Marshal/Unmarshal", name)
		assert.Equal(t, policy.GetAllow(), roundTripped.GetAllow())
	}
}

// TestMarshalWritesSignalsInSortedOrder checks that two Marshal calls on the
// same workflow produce byte-identical output, which requires an order that
// does not depend on Go's randomized map iteration.
func TestMarshalWritesSignalsInSortedOrder(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.4
name: multi-gate
steps:
  - id: a
    wait_for_signal:
      name: zzz-last
      timeout: 1h
  - id: b
    wait_for_signal:
      name: aaa-first
      timeout: 1h
signals:
  zzz-last:
    allow: ${sender.identity.principal == "https://issuer.example.com#z@example.com"}
  aaa-first:
    allow: ${sender.identity.principal == "https://issuer.example.com#a@example.com"}
`
	workflow, err := flowfile.Unmarshal([]byte(src))
	require.NoError(t, err)

	first, err := flowfile.Marshal(workflow)
	require.NoError(t, err)
	second, err := flowfile.Marshal(workflow)
	require.NoError(t, err)

	require.Equal(t, string(first), string(second), "Marshal was not deterministic across repeated calls")

	// aaa-first sorts before zzz-last regardless of declaration order.
	assert.Less(t,
		strings.Index(string(first), "aaa-first"),
		strings.Index(string(first), "zzz-last"),
		"signals were not written in sorted order")
}

// TestSignalPolicyForAnUndeclaredNameIsMisspelled checks the diagnostic that
// exists because a policy nothing waits for is almost always a typo of the
// name a `wait_for_signal:` actually uses.
func TestSignalPolicyForAnUndeclaredNameIsMisspelled(t *testing.T) {
	t.Parallel()

	source := strings.Replace(signaledSource, "deploy-approved:\n    allow:", "deploy-aproved:\n    allow:", 1)

	diagnostics, err := flowfile.ValidateSource([]byte(source))
	require.NoError(t, err)
	require.NotEmpty(t, diagnostics)
	assert.Contains(t, diagnostics[0].Message, "no `wait_for_signal:`")
}

// TestSignalPolicyWithNoPredicateIsRefused checks the diagnostic for a policy
// that authorizes nobody, which is indistinguishable from a typo.
func TestSignalPolicyWithNoPredicateIsRefused(t *testing.T) {
	t.Parallel()

	source := `edition: v2026.4
name: deploy-gate
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      timeout: 24h
signals:
  deploy-approved: {}
`
	_, _, err := flowfile.Parse([]byte(source))
	require.Error(t, err, "a policy with no predicate was accepted silently")
	assert.Contains(t, err.Error(), "authorizes nobody")
}

// TestSignalPolicyPredicateComparingABareSubjectCompilesButNeverMatches
// restates #215's lesson for signal policy at the file level: the compiler
// does not guess what a string compared with a principal was meant to be, so
// the enforcement test pins that a bare subject never admits a qualified
// sender (see signalpolicy_test.go).
func TestSignalPolicyPredicateComparingABareSubjectCompilesButNeverMatches(t *testing.T) {
	t.Parallel()

	source := strings.Replace(signaledSource,
		`"https://issuer.example.com#release-manager@example.com"`,
		`"release-manager@example.com"`, 1)

	workflow, err := flowfile.Unmarshal([]byte(source))
	require.NoError(t, err)

	policy := &v1.SignalPolicy{Allow: workflow.GetSignals()["deploy-approved"].GetAllow()}
	require.Error(t, v1.SignalPolicyCheck(t.Context(), policy,
		&v1.WorkloadIdentity{Issuer: "https://issuer.example.com", Subject: "release-manager@example.com"}, nil, false, nil),
		"a bare subject admitted a qualified sender")
}

// TestAnEmptySignalsBlockDoesNotRoundTrip mirrors the `triggers:` and
// `inputs:` rule: writing nothing under `signals:` is refused rather than
// compiling to a silent no-op that Marshal would then be unable to tell
// apart from the block being absent.
func TestAnEmptySignalsBlockDoesNotRoundTrip(t *testing.T) {
	t.Parallel()

	source := `edition: v2026.4
name: deploy-gate
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      timeout: 24h
signals: {}
`
	workflow, err := flowfile.Unmarshal([]byte(source))
	require.NoError(t, err)
	assert.Nil(t, workflow.GetSignals(), "an empty `signals:` block compiled to something rather than nothing")
}

// #207 slice 1's per-run predicates: a predicate may read the run's inputs, so
// long as something the starter cannot choose narrows it.

// perRunSignaledSource is a predicate over the run's inputs, narrowed by a
// comparison with the starter — one of the two shapes the narrowing check
// accepts.
const perRunSignaledSource = `edition: v2026.4
name: deploy-gate
inputs:
  expected_approver:
    type: string
    required: true
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      timeout: 24h
signals:
  deploy-approved:
    allow: >-
      ${sender.identity.principal == "https://issuer.example.com#" + inputs.expected_approver &&
      sender.identity.principal != run.identity.principal}
`

// TestASignalPredicateReadingInputsValidatesWhenNarrowed checks that the
// well-formed case passes [flowfile.Validate] cleanly, the property the
// refusals below depend on being able to tell apart from a predicate that
// lacks the constraint.
func TestASignalPredicateReadingInputsValidatesWhenNarrowed(t *testing.T) {
	t.Parallel()

	diagnostics, err := flowfile.ValidateSource([]byte(perRunSignaledSource))
	require.NoError(t, err)
	require.Empty(t, diagnostics, "a properly narrowed per-run predicate reported a diagnostic")

	workflow, positions, err := flowfile.Parse([]byte(perRunSignaledSource))
	require.NoError(t, err)
	assert.Contains(t, workflow.GetSignals()["deploy-approved"].GetAllow(), "inputs.expected_approver")

	_, ok := positions.At("signals.deploy-approved.allow")
	assert.True(t, ok)
}

// TestNarrowingCheckRefusesAPredicateReadingOnlyInputs is #207's narrowing
// check, the negative direction: a predicate that reads the run's inputs and
// nothing the starter cannot choose is refused — a caller must not be able to
// choose their own authorization constraint by choosing what they submit — and
// the diagnostic is positioned at the `allow:` the author wrote.
func TestNarrowingCheckRefusesAPredicateReadingOnlyInputs(t *testing.T) {
	t.Parallel()

	source := `edition: v2026.4
name: deploy-gate
inputs:
  expected_approver:
    type: string
    required: true
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      timeout: 24h
signals:
  deploy-approved:
    allow: ${sender.identity.principal == "https://issuer.example.com#" + inputs.expected_approver}
`
	diagnostics, err := flowfile.ValidateSource([]byte(source))
	require.NoError(t, err)
	require.Len(t, diagnostics, 1)

	d := diagnostics[0]
	assert.Contains(t, d.Message, "name themselves as their own approver")
	assert.Contains(t, d.Message, "sender.identity.claims")
	assert.NotZero(t, d.Column, "the narrowing diagnostic carried no source position")
	// The diagnostic points at the line `allow:` is written on.
	assert.Equal(t, 14, d.Line, "the narrowing diagnostic did not point at the allow: line")
}

// TestNarrowingCheckRefusesAPredicateNarrowedOnlyByNamespace is the case an
// author is most likely to write believing it is safe.
//
// A namespace is compared against the sender's own namespace, and every sender
// that can reach the check is already in the run's namespace — the server
// refuses anyone else before a policy is consulted at all. So it narrows
// nothing, and the predicate authorizes exactly what the unnarrowed one does.
func TestNarrowingCheckRefusesAPredicateNarrowedOnlyByNamespace(t *testing.T) {
	t.Parallel()

	source := `edition: v2026.4
name: deploy-gate
inputs:
  expected_approver:
    type: string
    required: true
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      timeout: 24h
signals:
  deploy-approved:
    allow: ${sender.identity.principal == "https://issuer.example.com#" + inputs.expected_approver && sender.identity.namespace == "release-managers-ns"}
`
	diagnostics, err := flowfile.ValidateSource([]byte(source))
	require.NoError(t, err)
	require.Len(t, diagnostics, 1,
		"a namespace comparison was accepted as narrowing a predicate over inputs, which it does not")
	assert.NotZero(t, diagnostics[0].Line)
}

// TestNarrowingCheckAllowsAPredicateReadingInputsWithClaims checks the
// constraint the narrowing check accepts: claims are attested on the sender's
// own token, which the run's inputs cannot reach.
func TestNarrowingCheckAllowsAPredicateReadingInputsWithClaims(t *testing.T) {
	t.Parallel()

	source := `edition: v2026.4
name: deploy-gate
inputs:
  expected_approver:
    type: string
    required: true
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      timeout: 24h
signals:
  deploy-approved:
    allow: ${sender.identity.principal == "https://issuer.example.com#" + inputs.expected_approver && sender.identity.claims.team == "release-managers"}
`
	diagnostics, err := flowfile.ValidateSource([]byte(source))
	require.NoError(t, err)
	require.Empty(t, diagnostics, "a predicate over inputs narrowed by claims reported a diagnostic")
}

// TestMarshalIsTheInverseForAPerRunPredicate is [TestMarshalIsTheInverseForSignals]
// for the per-run shape: the expression survives byte for byte, and a second
// Marshal is identical to the first.
func TestMarshalIsTheInverseForAPerRunPredicate(t *testing.T) {
	t.Parallel()

	workflow, err := flowfile.Unmarshal([]byte(perRunSignaledSource))
	require.NoError(t, err)

	written, err := flowfile.Marshal(workflow)
	require.NoError(t, err)
	assert.Contains(t, string(written), "inputs.expected_approver", "Marshal dropped the expression")
	assert.Contains(t, string(written), "run.identity.principal", "Marshal dropped the starter comparison")

	again, err := flowfile.Unmarshal(written)
	require.NoError(t, err)
	assert.Equal(t,
		workflow.GetSignals()["deploy-approved"].GetAllow(),
		again.GetSignals()["deploy-approved"].GetAllow())

	again2, err := flowfile.Marshal(again)
	require.NoError(t, err)
	assert.Equal(t, string(written), string(again2))
}

// TestSignalPolicyEndToEnd closes the loop between "what an author writes" and
// "what the server checks it against".
func TestSignalPolicyEndToEnd(t *testing.T) {
	t.Parallel()

	workflow, err := flowfile.Unmarshal([]byte(signaledSource))
	require.NoError(t, err)

	policy := workflow.GetSignals()["deploy-approved"]
	check := func(identity *v1.WorkloadIdentity) bool {
		return v1.SignalPolicyCheck(t.Context(), policy, identity, nil, false, nil) == nil
	}

	assert.True(t, check(&v1.WorkloadIdentity{
		Issuer:  "https://issuer.example.com",
		Subject: "release-manager@example.com",
	}), "the declared subject was refused")

	assert.True(t, check(&v1.WorkloadIdentity{
		Issuer:  "https://issuer.example.com",
		Subject: "whoever@example.com",
		Claims:  map[string]string{"team": "release-managers"},
	}), "the declared claim was refused")

	assert.False(t, check(&v1.WorkloadIdentity{
		Issuer:  "https://issuer.example.com",
		Subject: "some-other-engineer@example.com",
	}), "an undeclared sender was authorized")
}
