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
// deliver it. See [pkg/flowstate/v1/signalpolicy.go] for the enforcement
// this describes and [pkg/flowstate/v1/server/lifecycle.go] for where it is
// checked.
const signaledSource = `edition: v2026.2
name: deploy-gate
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      timeout: 24h
signals:
  deploy-approved:
    allow:
      - subject: "https://issuer.example.com#release-manager@example.com"
      - claims:
          team: release-managers
`

// TestParsingASignalsBlock pins what the block compiles to.
func TestParsingASignalsBlock(t *testing.T) {
	t.Parallel()

	workflow, positions, err := flowfile.Parse([]byte(signaledSource))
	require.NoError(t, err)

	policy := workflow.GetSignals()["deploy-approved"]
	require.NotNil(t, policy)
	require.Len(t, policy.GetAllow(), 2)

	assert.Equal(t, "https://issuer.example.com#release-manager@example.com", policy.GetAllow()[0].GetSubject())
	assert.Empty(t, policy.GetAllow()[0].GetClaims())

	assert.Empty(t, policy.GetAllow()[1].GetSubject())
	assert.Equal(t, map[string]string{"team": "release-managers"}, policy.GetAllow()[1].GetClaims())

	for _, path := range []string{
		"signals", "signals.deploy-approved", "signals.deploy-approved.allow",
		"signals.deploy-approved.allow[0]", "signals.deploy-approved.allow[0].subject",
		"signals.deploy-approved.allow[1]", "signals.deploy-approved.allow[1].claims",
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
		require.Len(t, roundTripped.GetAllow(), len(policy.GetAllow()))
		for i, rule := range policy.GetAllow() {
			assert.Equal(t, rule.GetSubject(), roundTripped.GetAllow()[i].GetSubject())
			assert.Equal(t, rule.GetNamespace(), roundTripped.GetAllow()[i].GetNamespace())
			assert.Equal(t, rule.GetClaims(), roundTripped.GetAllow()[i].GetClaims())
		}
	}
}

// TestMarshalWritesSignalsInSortedOrder checks that two Marshal calls on the
// same workflow produce byte-identical output, which requires an order that
// does not depend on Go's randomized map iteration.
func TestMarshalWritesSignalsInSortedOrder(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.2
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
    allow:
      - subject: "https://issuer.example.com#z@example.com"
  aaa-first:
    allow:
      - subject: "https://issuer.example.com#a@example.com"
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

// TestSignalPolicyRuleWithNothingSetIsRefused checks the diagnostic for a
// rule that would authorize every sender — almost certainly not what an
// author meant when they wrote a rule at all.
func TestSignalPolicyRuleWithNothingSetIsRefused(t *testing.T) {
	t.Parallel()

	source := `edition: v2026.2
name: deploy-gate
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      timeout: 24h
signals:
  deploy-approved:
    allow:
      - {}
`
	_, _, err := flowfile.Parse([]byte(source))
	require.Error(t, err, "an empty rule under `allow:` was accepted silently")
	assert.Contains(t, err.Error(), "match every sender")
}

// TestSignalPolicySubjectMustBeIssuerQualified checks #215's lesson,
// restated for signal policy: a bare subject with no issuer is refused with
// an explanation, not merely rejected by protovalidate's generic pattern
// message.
func TestSignalPolicySubjectMustBeIssuerQualified(t *testing.T) {
	t.Parallel()

	source := strings.Replace(signaledSource,
		`subject: "https://issuer.example.com#release-manager@example.com"`,
		`subject: "release-manager@example.com"`, 1)

	_, _, err := flowfile.Parse([]byte(source))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "issuer")
}

// TestAnEmptySignalsBlockDoesNotRoundTrip mirrors the `triggers:` and
// `inputs:` rule: writing nothing under `signals:` is refused rather than
// compiling to a silent no-op that Marshal would then be unable to tell
// apart from the block being absent.
func TestAnEmptySignalsBlockDoesNotRoundTrip(t *testing.T) {
	t.Parallel()

	source := `edition: v2026.2
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

// TestSignalPolicyAllowsAndDeniesEndToEnd exercises [v1.SignalPolicyAllows]
// against the exact shape this file's grammar compiles to, closing the loop
// between "what an author writes" and "what the server checks it against".
func TestSignalPolicyAllowsAndDeniesEndToEnd(t *testing.T) {
	t.Parallel()

	workflow, err := flowfile.Unmarshal([]byte(signaledSource))
	require.NoError(t, err)

	policy := workflow.GetSignals()["deploy-approved"]

	allowed := &v1.WorkloadIdentity{
		Issuer:  "https://issuer.example.com",
		Subject: "release-manager@example.com",
	}
	assert.True(t, v1.SignalPolicyAllows(policy, allowed), "the declared subject was refused")

	byClaim := &v1.WorkloadIdentity{
		Issuer:  "https://issuer.example.com",
		Subject: "whoever@example.com",
		Claims:  map[string]string{"team": "release-managers"},
	}
	assert.True(t, v1.SignalPolicyAllows(policy, byClaim), "the declared claim was refused")

	denied := &v1.WorkloadIdentity{
		Issuer:  "https://issuer.example.com",
		Subject: "some-other-engineer@example.com",
	}
	assert.False(t, v1.SignalPolicyAllows(policy, denied), "an undeclared sender was authorized")
}
