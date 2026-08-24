package secrets_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets/secretstest"
)

// The shipped providers, run through the isolation kit that was built for
// providers — and, until #934's audit, run through by none. secrets.go cites
// [secretstest.VerifyNamespaceIsolation] as the check an implementation
// should pass, and env.go and file.go are this module's own two historical
// namespace leaks; a kit with zero callers outside its own tests is the
// ZeroValueCases shape wearing a package boundary. The collision entries
// below are the two historical derivations, spelled the way the bugs
// actually spelled them, so a regression toward either concatenation scheme
// lands exactly where it used to.

// TestEnvProviderNamespaceIsolation configures the env provider the way a
// multi-tenant operator does — a base prefix for the default tenant and an
// explicit, disjoint prefix per namespace — and probes every direction.
func TestEnvProviderNamespaceIsolation(t *testing.T) {
	t.Setenv("FLOWSTATE_SECRET_API_KEY", "default-value")
	t.Setenv("FLOWSTATE_TEAM_A_API_KEY", "team-a-value")
	t.Setenv("FLOWSTATE_TEAM_B_API_KEY", "team-b-value")

	provider, err := secrets.NewEnvProvider(
		secrets.WithEnvPrefix("FLOWSTATE_SECRET_"),
		secrets.WithEnvNamespaces(map[string]string{
			"team-a": "FLOWSTATE_TEAM_A_",
			"team-b": "FLOWSTATE_TEAM_B_",
		}),
	)
	require.NoError(t, err)

	secretstest.VerifyNamespaceIsolation(t, provider, []secretstest.NamespaceFixture{
		{
			Namespace: "",
			Ref:       secrets.NewRef("env", "API_KEY"),
			Value:     "default-value",
		},
		{
			Namespace: "team-a",
			Ref:       secrets.NewRef("env", "API_KEY"),
			Value:     "team-a-value",
			// The historical env leak, both spellings (CLAUDE.md's tenancy
			// postmortem): under prefix+NAMESPACE+"_"+name derivation, the
			// default tenant naming "TEAM_A_API_KEY" and namespace "team"
			// naming "A_API_KEY" both landed on team-a's variable. Today the
			// first derives a different variable and the second is an
			// unmapped namespace's refusal — and if the derivation ever
			// regresses, these are the requests that reach team-a's value.
			Collisions: []secretstest.Collision{
				{FromNamespace: "", Ref: secrets.NewRef("env", "TEAM_A_API_KEY")},
				{FromNamespace: "team", Ref: secrets.NewRef("env", "A_API_KEY")},
			},
		},
		{
			Namespace: "team-b",
			Ref:       secrets.NewRef("env", "API_KEY"),
			Value:     "team-b-value",
		},
	}, secretstest.WithUnconfiguredNamespace("never-mapped-tenant"))
}

// TestFileProviderNamespaceIsolation lays out the namespaced directory the
// provider documents — one segment per tenant, `_default` included — and
// probes every direction, the slash-carrying reference of the historical
// file leak among them.
func TestFileProviderNamespaceIsolation(t *testing.T) {
	dir := t.TempDir()
	for tenant, value := range map[string]string{
		secrets.DefaultNamespaceDir: "default-value",
		"team-a":                    "team-a-value",
		"team-b":                    "team-b-value",
	} {
		require.NoError(t, os.MkdirAll(filepath.Join(dir, tenant), 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(dir, tenant, "api-key"), []byte(value), 0o600))
	}

	provider, err := secrets.NewFileProvider(dir, secrets.WithFileNamespaced())
	require.NoError(t, err)
	t.Cleanup(func() { _ = provider.Close() })

	secretstest.VerifyNamespaceIsolation(t, provider, []secretstest.NamespaceFixture{
		{
			Namespace: "",
			Ref:       secrets.NewRef("file", "api-key"),
			Value:     "default-value",
		},
		{
			Namespace: "team-a",
			Ref:       secrets.NewRef("file", "api-key"),
			Value:     "team-a-value",
			// The historical file leak: a reference may contain a slash, so
			// before every tenant had a segment the default tenant could read
			// team-a's file by naming "team-a/api-key" — and a traversal
			// spelling reaches for the same file from a sibling's segment.
			// Under the namespaced layout the first reads
			// _default/team-a/api-key (absent) and the second is cleaned or
			// refused; a regression in either direction lands on the value.
			Collisions: []secretstest.Collision{
				{FromNamespace: "", Ref: secrets.NewRef("file", "team-a/api-key")},
				{FromNamespace: "team-b", Ref: secrets.NewRef("file", "../team-a/api-key")},
			},
		},
		{
			Namespace: "team-b",
			Ref:       secrets.NewRef("file", "api-key"),
			Value:     "team-b-value",
		},
	}, secretstest.WithUnconfiguredNamespace("never-configured-tenant"))
}
