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

// TestResolve_CISources_Build asserts that every source this package
// documents resolves to one that names itself — the two CI sources included,
// which until picatz/flowstate#559's second half were refused by name.
//
// They build without an audience, unlike github-actions: neither platform
// mints on demand, so an audience is a check on the token the job was already
// given rather than a parameter of a request. What is not checkable at
// construction is checked at [credentialsource.Source.Token], where the
// environment is.
func TestResolve_CISources_Build(t *testing.T) {
	for _, name := range []string{credentialsource.SourceGitLab, credentialsource.SourceTerraformCloud} {
		t.Run(name, func(t *testing.T) {
			source, err := credentialsource.Resolve(name, credentialsource.Config{})
			require.NoError(t, err)
			assert.Equal(t, name, source.Name())
		})
	}
}

// TestResolve_CISources_RefuseRatherThanGoAnonymous is the fail-closed half:
// a source named explicitly, in an environment that has none of what it
// needs, refuses. It never returns the zero token with a nil error, which a
// caller could not tell apart from "anonymous is fine here".
func TestResolve_CISources_RefuseRatherThanGoAnonymous(t *testing.T) {
	for _, name := range []string{credentialsource.SourceGitLab, credentialsource.SourceTerraformCloud} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("GITLAB_CI", "")
			t.Setenv("TFC_RUN_ID", "")
			t.Setenv(credentialsource.DefaultGitLabIDTokenEnvVar, "")
			t.Setenv(credentialsource.DefaultTerraformCloudTokenEnvVar, "")

			source, err := credentialsource.Resolve(name, credentialsource.Config{
				Audience: "https://flowstate.example.com",
			})
			require.NoError(t, err)

			token, err := source.Token(t.Context())
			require.Error(t, err)
			assert.ErrorIs(t, err, credentialsource.ErrSourceUnusable)
			assert.True(t, token.IsZero())
		})
	}
}

// TestResolve_GitLab_EnvVarNamesTheIDToken covers Config.EnvVar reaching the
// gitlab source, which is the only way a job whose `id_tokens:` key is not
// the documented default can be read at all.
func TestResolve_GitLab_EnvVarNamesTheIDToken(t *testing.T) {
	t.Setenv("GITLAB_CI", "true")
	t.Setenv("SOME_OTHER_ID_TOKEN", "")

	source, err := credentialsource.Resolve(credentialsource.SourceGitLab, credentialsource.Config{
		EnvVar: "SOME_OTHER_ID_TOKEN",
	})
	require.NoError(t, err)

	_, err = source.Token(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SOME_OTHER_ID_TOKEN")
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
