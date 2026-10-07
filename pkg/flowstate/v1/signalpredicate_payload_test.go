package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The delivery verdicts for `payload` are the shared conformance table's
// (internal/conformance, run through the server and the local driver); these are
// the compile-time rules and the shapes that table cannot state.

func TestSignalPolicyPayloadMustBeNarrowedByWhatTheSenderCannotChoose(t *testing.T) {
	t.Parallel()

	for _, expression := range []string{
		`payload.decision == "approve"`,
		`payload.decision == "approve" && [1].exists(run, run == 1)`,
		`payload.decision == "approve" && has(sender.identity.claims)`,
		`payload.decision == "approve" && sender.identity.principal == "a#b"`,
		`payload.decision == "approve" && inputs.who == "x" && sender.identity.principal == "a#b"`,
	} {
		err := v1.CheckSignalPolicyExpr(expression)
		require.Error(t, err, expression)
		assert.Contains(t, err.Error(), "reads `payload` but nothing alongside it", expression)
	}

	for _, expression := range []string{
		`payload.decision == "approve" && sender.identity.claims.team == "x"`,
		`payload.decision == "reject" || (payload.decision == "approve" && sender.identity.claims.team == "x")`,
		`payload.decision == "approve" && sender.identity.principal != run.identity.principal`,
	} {
		require.NoError(t, v1.CheckSignalPolicyExpr(expression), expression)
	}
}

func TestSignalPolicyPayloadIsReadButNothingIsRecordedForIt(t *testing.T) {
	t.Parallel()

	reads := v1.SignalPolicyExprReads(map[string]*v1.SignalPolicy{"a": predicatePolicy(
		`payload.decision == "approve" && sender.identity.claims.team == "x"`)})

	assert.Equal(t, v1.SignalPolicyReads{Claims: true, Payload: true}, reads,
		"payload is tracked, and is not an input or the starter: the run records nothing for it")
}

func TestSignalPolicyPayloadIsOutOfScopeWhereThereIsNoDelivery(t *testing.T) {
	t.Parallel()

	manual := `payload.decision == "approve" && sender.identity.claims.team == "x"`
	_, err := v1.CompileManualAllowPredicate(manual)
	require.Error(t, err, "a manual start has no delivery, so `payload` is an undeclared name there")

	err = v1.CheckDebugPolicy(&v1.SignalPolicy{Allow: manual})
	require.Error(t, err, "a debug lease carries no payload to branch on")
	assert.Contains(t, err.Error(), "payload")
}

func TestSignalPolicyUnboundPayloadDeniesAndIsNotAnEmptyOne(t *testing.T) {
	t.Parallel()

	sender := &v1.WorkloadIdentity{Issuer: "https://i", Subject: "s", Claims: map[string]string{"team": "x"}}

	// `!has(payload.decision)` would be true over an empty payload; with none
	// offered it errors, so a caller with no delivery never reads absence as a
	// verdict.
	policy := predicatePolicy(`!has(payload.decision) && sender.identity.claims.team == "x"`)

	require.Error(t, v1.SignalPolicyCheck(t.Context(), policy, sender, nil, false, nil, nil))
	require.NoError(t, v1.SignalPolicyCheck(t.Context(), policy, sender, nil, false, nil, v1.BoundSignalPayload(nil)))
}

func TestSignalPolicyRefusalNeverQuotesAPayloadValue(t *testing.T) {
	t.Parallel()

	const secret = "TOPSECRETPAYLOAD"

	sender := &v1.WorkloadIdentity{Issuer: "https://i", Subject: "s", Claims: map[string]string{"team": "x"}}
	payload := &v1.Node_Outputs{NamedValues: map[string]*v1.Value{"n": v1.NewLiteral(secret)}}

	// An evaluation error (int() of a non-numeric string quotes its operand in
	// cel-go) and a plain mismatch: neither may carry the value.
	for _, expression := range []string{
		`int(payload.n) == 1 && sender.identity.claims.team == "x"`,
		`payload.n == "other" && sender.identity.claims.team == "x"`,
		`payload.missing == "y" && sender.identity.claims.team == "x"`,
	} {
		err := v1.SignalPolicyCheck(t.Context(), predicatePolicy(expression), sender, nil, false, nil, payload)
		require.Error(t, err, expression)
		assert.NotContains(t, err.Error(), secret, expression)
	}
}
