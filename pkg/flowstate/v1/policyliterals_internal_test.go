package flowstatev1

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPolicyEnvironmentRefusesAnUnparseableLiteral is #1856: the environment
// carries the shared literal validators, so a regex that cannot compile is
// refused when the rule loads rather than on every request.
func TestPolicyEnvironmentRefusesAnUnparseableLiteral(t *testing.T) {
	env, err := newTaskPolicyEnv()
	require.NoError(t, err)

	_, issues := env.Compile(`'x'.matches('(')`)
	require.Error(t, issues.Err())
}

func TestAllowPolicyEnvironmentRefusesAnUnparseableLiteral(t *testing.T) {
	for _, withRun := range []bool{true, false} {
		env, err := allowPolicyEnv(withRun)
		require.NoError(t, err)

		_, issues := env.Compile(`'x'.matches('(')`)
		require.Error(t, issues.Err(), "withRun=%v", withRun)
	}
}
