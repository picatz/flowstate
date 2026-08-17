package secretstest_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets/secretstest"
)

// TestVerifyNamespaceIsolation_EnvProvider runs the conformance kit against the
// env provider the way its own author would, in place of hand-rolling the
// negative-direction assertions [secrets] already carries for the same
// provider — this is the kit those assertions motivated.
func TestVerifyNamespaceIsolation_EnvProvider(t *testing.T) {
	t.Setenv("TEAM_A_SECRET_API_KEY", "team-a-value")
	t.Setenv("TEAM_B_SECRET_API_KEY", "team-b-value")

	provider, err := secrets.NewEnvProvider(secrets.WithEnvNamespaces(map[string]string{
		"team-a": "TEAM_A_SECRET_",
		"team-b": "TEAM_B_SECRET_",
	}))
	require.NoError(t, err)

	secretstest.VerifyNamespaceIsolation(t, provider, []secretstest.NamespaceFixture{
		{Namespace: "team-a", Ref: secrets.NewRef("env", "API_KEY"), Value: "team-a-value"},
		{Namespace: "team-b", Ref: secrets.NewRef("env", "API_KEY"), Value: "team-b-value"},
	})
}

// TestVerifyNamespaceIsolation_FileProvider runs the same kit against the file
// provider.
func TestVerifyNamespaceIsolation_FileProvider(t *testing.T) {
	dir := t.TempDir()

	for _, entry := range []struct{ path, value string }{
		{"team-a/api-key", "team-a-value"},
		{"team-b/api-key", "team-b-value"},
	} {
		full := filepath.Join(dir, entry.path)
		require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o700))
		require.NoError(t, os.WriteFile(full, []byte(entry.value), 0o600))
	}

	provider, err := secrets.NewFileProvider(dir, secrets.WithFileNamespaced())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })

	secretstest.VerifyNamespaceIsolation(t, provider, []secretstest.NamespaceFixture{
		{Namespace: "team-a", Ref: secrets.NewRef("file", "api-key"), Value: "team-a-value"},
		{Namespace: "team-b", Ref: secrets.NewRef("file", "api-key"), Value: "team-b-value"},
	})
}
