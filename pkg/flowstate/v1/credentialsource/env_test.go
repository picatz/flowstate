package credentialsource_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/credentialsource"
)

func TestEnvSource_ReadsTheNamedVariable(t *testing.T) {
	t.Setenv("MY_CUSTOM_TOKEN_VAR", "  env-token  \n")

	source := credentialsource.NewEnvSource("MY_CUSTOM_TOKEN_VAR")

	token, err := source.Token(t.Context())
	require.NoError(t, err)
	raw, ok := token.Bearer()
	require.True(t, ok)
	assert.Equal(t, "env-token", raw)
}

// TestEnvSource_Unset_FailsClosed is the negative direction: an explicitly
// named env source with nothing in its variable is an error, unlike the
// CLI's implicit default chain where an unset FLOWSTATE_TOKEN legitimately
// means anonymous. Naming a source is asking for a credential.
func TestEnvSource_Unset_FailsClosed(t *testing.T) {
	t.Setenv("MY_CUSTOM_TOKEN_VAR", "")

	source := credentialsource.NewEnvSource("MY_CUSTOM_TOKEN_VAR")

	token, err := source.Token(t.Context())
	require.Error(t, err)
	assert.ErrorIs(t, err, credentialsource.ErrSourceUnusable)
	assert.True(t, token.IsZero())
}

func TestEnvSource_WhitespaceOnly_FailsClosed(t *testing.T) {
	t.Setenv("MY_CUSTOM_TOKEN_VAR", "   \t\n  ")

	source := credentialsource.NewEnvSource("MY_CUSTOM_TOKEN_VAR")

	_, err := source.Token(t.Context())
	require.Error(t, err)
	assert.ErrorIs(t, err, credentialsource.ErrSourceUnusable)
}
