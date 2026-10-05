package flowstatev1_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func TestSignalPolicyInputsAreUnboundWhenTheCallerHoldsNone(t *testing.T) {
	t.Parallel()

	approver := &v1.WorkloadIdentity{Claims: map[string]string{"team": "release"}}
	narrow := ` && sender.identity.claims["team"] == "release"`

	// Over an empty scope each of these would read as true. Unbound, each is an
	// evaluation error, and an error denies.
	for _, expression := range []string{
		`!has(inputs.x)` + narrow,
		`"x" in inputs` + narrow,
		`!("x" in inputs)` + narrow,
		`inputs.size() == 0` + narrow,
	} {
		require.Error(t,
			v1.SignalPolicyCheck(t.Context(), predicatePolicy(expression), approver, nil, false, nil),
			"%s was evaluated over an empty scope", expression)
	}

	// Inputs that were recorded and are empty are a real answer: only a nil map
	// means nothing was recorded.
	require.NoError(t, v1.SignalPolicyCheck(t.Context(), predicatePolicy(`inputs.size() == 0`+narrow),
		approver, nil, false, map[string]*v1.Value{}))
}

func TestSignalPolicyPredicateDeniesWhenItsDeadlineHasPassed(t *testing.T) {
	t.Parallel()

	values := make([]any, 60)
	for i := range values {
		values[i] = i
	}
	inputs := map[string]*v1.Value{"items": v1.NewLiteralList(values...)}
	policy := predicatePolicy(`inputs.items.all(a, inputs.items.all(b, a >= 0)) && sender.identity.claims["team"] == "release"`)
	approver := &v1.WorkloadIdentity{Claims: map[string]string{"team": "release"}}

	require.NoError(t, v1.SignalPolicyCheck(t.Context(), policy, approver, nil, false, inputs),
		"the control: this predicate is within its cost bound and admits")

	expired, cancel := context.WithDeadline(t.Context(), time.Unix(0, 0))
	defer cancel()
	require.Error(t, v1.SignalPolicyCheck(expired, policy, approver, nil, false, inputs),
		"an evaluation past its deadline admitted")

	assert.Equal(t, time.Second, v1.SignalPolicyExprTimeout)
}
