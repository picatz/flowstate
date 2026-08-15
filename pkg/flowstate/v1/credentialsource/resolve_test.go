package credentialsource_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/credentialsource"
)

// TestResolve_UnknownName_FailsClosed is the fail-closed contract [Resolve]
// exists to hold: a name nobody wrote a Source for must not build one that
// quietly does nothing.
func TestResolve_UnknownName_FailsClosed(t *testing.T) {
	_, err := credentialsource.Resolve("totally-made-up", credentialsource.Config{})
	require.Error(t, err)
	assert.ErrorIs(t, err, credentialsource.ErrUnknownSource)
}

// TestResolve_RoadmapNames_FailClosed asserts that a name this package's own
// documentation promises (gitlab, terraform-cloud) but has not built yet
// fails the same way an outright typo does, rather than silently resolving
// to something that looks like it works.
func TestResolve_RoadmapNames_FailClosed(t *testing.T) {
	for _, name := range []string{"gitlab", "terraform-cloud"} {
		t.Run(name, func(t *testing.T) {
			_, err := credentialsource.Resolve(name, credentialsource.Config{})
			require.Error(t, err)
			assert.ErrorIs(t, err, credentialsource.ErrUnknownSource)
		})
	}
}

func TestResolve_GitHubActions_RequiresAudience(t *testing.T) {
	_, err := credentialsource.Resolve(credentialsource.SourceGitHubActions, credentialsource.Config{})
	require.Error(t, err)
}

func TestResolve_GitHubActions_BuildsWithAudience(t *testing.T) {
	source, err := credentialsource.Resolve(credentialsource.SourceGitHubActions, credentialsource.Config{
		Audience: "flowstate",
	})
	require.NoError(t, err)
	assert.Equal(t, credentialsource.SourceGitHubActions, source.Name())
}

func TestResolve_File_RequiresPath(t *testing.T) {
	_, err := credentialsource.Resolve(credentialsource.SourceFile, credentialsource.Config{})
	require.Error(t, err)
}

func TestResolve_Env_DefaultsVariable(t *testing.T) {
	t.Setenv("FLOWSTATE_TOKEN", "resolved-env-token")

	source, err := credentialsource.Resolve(credentialsource.SourceEnv, credentialsource.Config{})
	require.NoError(t, err)

	token, err := source.Token(t.Context())
	require.NoError(t, err)
	raw, ok := token.Bearer()
	require.True(t, ok)
	assert.Equal(t, "resolved-env-token", raw)
}
