package flowstatev1_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func TestSignalPolicyInputsAreUnboundWhenTheCallerHoldsNone(t *testing.T) {
	t.Parallel()

	approver := &v1.WorkloadIdentity{Principal: &v1.Principal{Claims: v1.StringClaimValues(map[string]string{"team": "release"})}}
	narrow := ` && sender.identity.claims["team"] == "release"`

	// Over an empty scope each of these would read as true. Unbound, each is an
	// evaluation error, and an error denies.
	for _, expression := range []string{
		`!has(inputs.x)` + narrow,
		`"x" in inputs` + narrow,
		`!("x" in inputs)` + narrow,
	} {
		require.Error(t,
			v1.SignalPolicyCheck(t.Context(), predicatePolicy(expression), approver, nil, false, nil),
			"%s was evaluated over an empty scope", expression)
	}

	// Inputs that were recorded and are empty are a real answer: only a nil map
	// means nothing was recorded.
	require.NoError(t, v1.SignalPolicyCheck(t.Context(), predicatePolicy(`!has(inputs.x)`+narrow),
		approver, nil, false, map[string]*v1.Value{}))
}

func TestSignalPolicyTimeoutConstantIsOneSecond(t *testing.T) {
	t.Parallel()

	assert.Equal(t, time.Second, v1.SignalPolicyExprTimeout)
}
