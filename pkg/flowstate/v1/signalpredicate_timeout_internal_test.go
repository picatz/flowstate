package flowstatev1

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The deadline, not the caller's context and not the cost bound, is what
// denies here: the context is live and the predicate is within its cost, so
// only the timeout wired into the evaluation can refuse it.
func TestSignalPolicyPredicateDeniesWhenItsOwnDeadlineExpires(t *testing.T) {
	t.Parallel()

	values := make([]any, 60)
	for i := range values {
		values[i] = i
	}
	inputs := map[string]*Value{"items": NewLiteralList(values...)}
	src := `inputs.items.all(a, inputs.items.all(b, a >= 0)) && sender.identity.claims["team"] == "release"`
	approver := &WorkloadIdentity{Principal: &Principal{Claims: StringClaimValues(map[string]string{"team": "release"})}}

	require.NoError(t, signalPolicyExprAllows(context.Background(), "signal", src, approver, nil, false, inputs),
		"the control: the same predicate admits under the normal timeout")

	require.Error(t, signalPolicyExprAllowsWithin(context.Background(), time.Nanosecond, src, approver, nil, false, inputs),
		"an evaluation past its own deadline admitted")
}

// The same proof for the other two stanzas: `debug:` rides the signal loop
// (label "debug policy") and `manual:` the same loop in its own scope, so one
// deadline bounds all three.
func TestDebugAndManualPredicatesDenyWhenTheirOwnDeadlineExpires(t *testing.T) {
	t.Parallel()

	values := make([]any, 60)
	for i := range values {
		values[i] = i
	}
	inputs := map[string]*Value{"items": NewLiteralList(values...)}
	src := `inputs.items.all(a, inputs.items.all(b, a >= 0)) && sender.identity.claims["team"] == "release"`
	caller := &WorkloadIdentity{Principal: &Principal{Claims: StringClaimValues(map[string]string{"team": "release"})}}

	for name, manual := range map[string]bool{"debug policy": false, "manual start": true} {
		require.NoError(t, allowPredicateAllowsWithin(context.Background(), SignalPolicyExprTimeout, name, manual, src,
			caller, nil, false, inputs), "the control: %s admits under the normal timeout", name)

		require.Error(t, allowPredicateAllowsWithin(context.Background(), time.Nanosecond, name, manual, src,
			caller, nil, false, inputs), "%s: an evaluation past its own deadline admitted", name)
	}

	require.NoError(t, manualAllowExprAllows(context.Background(), src, caller, inputs))
}
