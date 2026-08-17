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
		{
			Namespace: "team-a",
			Ref:       secrets.NewRef("env", "API_KEY"),
			Value:     "team-a-value",
			// "TEAM_A_API_KEY" is exactly what a requester with no namespace
			// mapping (reading under the bare DefaultEnvPrefix, i.e. the
			// empty namespace) would have to name in order to land on
			// $FLOWSTATE_SECRET_TEAM_A_API_KEY under the naive
			// prefix+NAMESPACE+"_"+name scheme CLAUDE.md documents — the
			// same string a *correctly* fixed provider derives for
			// "team-a"+"API_KEY" only because it is told to, not because it
			// concatenates. "A_API_KEY" is the same collision from the other
			// side: what a namespace literally called "team" would have to
			// name under that scheme to land on the same variable. Neither
			// collision is reachable from "team-b" — only from the specific
			// requester namespace named below, which is why each is bound
			// to one.
			Collisions: []secretstest.Collision{
				{FromNamespace: "", Ref: secrets.NewRef("env", "TEAM_A_API_KEY")},
				{FromNamespace: "team", Ref: secrets.NewRef("env", "A_API_KEY")},
			},
		},
		{
			Namespace: "team-b",
			Ref:       secrets.NewRef("env", "API_KEY"),
			Value:     "team-b-value",
			Collisions: []secretstest.Collision{
				{FromNamespace: "", Ref: secrets.NewRef("env", "TEAM_B_API_KEY")},
				{FromNamespace: "team", Ref: secrets.NewRef("env", "B_API_KEY")},
			},
		},
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
		{
			Namespace: "team-a",
			Ref:       secrets.NewRef("file", "api-key"),
			Value:     "team-a-value",
			// A path-segment analogue of the env collision: a reference that
			// climbs out of the requester's own namespace directory and back
			// into team-a's, the way a provider that joined the namespace and
			// name with plain string concatenation — rather than a real path
			// segment plus os.Root confinement — would resolve it. The only
			// requester that can even attempt this climb is team-b, the other
			// namespace this fixture list defines, so the collision is bound
			// to it explicitly.
			Collisions: []secretstest.Collision{
				{FromNamespace: "team-b", Ref: secrets.NewRef("file", "../team-a/api-key")},
			},
		},
		{
			Namespace: "team-b",
			Ref:       secrets.NewRef("file", "api-key"),
			Value:     "team-b-value",
			Collisions: []secretstest.Collision{
				{FromNamespace: "team-a", Ref: secrets.NewRef("file", "../team-b/api-key")},
			},
		},
	})
}
