package execpolicy

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPolicyEnvironmentRefusesAnUnparseableLiteral is #1856: the environment
// carries the shared literal validators, so a regex that cannot compile is
// refused when the rule loads rather than on every request.
func TestPolicyEnvironmentRefusesAnUnparseableLiteral(t *testing.T) {
	env, err := newRuleEnv()
	require.NoError(t, err)

	_, issues := env.Compile(`'x'.matches('(')`)
	require.Error(t, issues.Err())
}
