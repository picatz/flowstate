package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	"github.com/stretchr/testify/require"
)

func TestRunLocalResolvesAuthorizedBearer(t *testing.T) {
	const material = "local-secret-material"
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		called = true
		require.Equal(t, "Bearer "+material, req.Header.Get("Authorization"))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	policy := filepath.Join(t.TempDir(), "auth.yaml")
	require.NoError(t, os.WriteFile(policy, []byte(`issuers:
  - name: local
    issuer: https://issuer.example
    audiences: [flowstate]
    algorithms: [RS256]
secrets:
  allow:
    - 'true'
`), 0o600))
	t.Setenv("PAYMENTS_SECRET_API_TOKEN", material)
	egress := filepath.Join(t.TempDir(), "egress.yaml")
	require.NoError(t, os.WriteFile(egress, []byte("egress:\n  allow_loopback: true\n"), 0o600))

	workflow := fmt.Sprintf(`edition: v2026.2
name: secret-http
steps:
  - id: fetch
    http:
      url: %s
      bearer: ${secret('env:API_TOKEN')}
`, server.URL)
	stdout, stderr, err := runLocal(t, workflow,
		"--secret-env", "API_TOKEN",
		"--secret-env-namespace", "payments=PAYMENTS_SECRET_",
		"--secret-require-namespace", "--as-namespace", "payments",
		"--auth-policy", policy, "--egress-policy", egress, "--output", "json")
	require.NoError(t, err, stderr)
	require.True(t, called)
	require.NotContains(t, stdout, material)
	require.NotContains(t, stderr, material)
}

func TestLocalPolicyIdentityIsExplicitAndValidated(t *testing.T) {
	root := newRootCommand()
	local, _, err := root.Find([]string{"run", "local"})
	require.NoError(t, err)
	for name, value := range map[string]string{
		"as-subject": "repo:acme/payments", "as-issuer": "https://issuer.example",
		"as-namespace": "payments", "as-deployment": "staging",
	} {
		require.NoError(t, local.Flags().Set(name, value))
	}
	require.NoError(t, local.Flags().Set("as-claim", "repository=acme/payments"))

	identity, err := localWorkloadIdentity(local)
	require.NoError(t, err)
	require.Equal(t, "repo:acme/payments", identity.Subject)
	require.Equal(t, "payments", identity.Namespace)
	require.Equal(t, "staging", identity.Deployment)
	require.Equal(t, "acme/payments", identity.Claims["repository"])

	require.NoError(t, local.Flags().Set("as-claim", "broken"))
	_, err = localWorkloadIdentity(local)
	require.ErrorContains(t, err, "want NAME=VALUE")
}

// TestAsClaimCannotOverrideRunMode checks the negative direction of #243's
// third gap: --as-claim can set an arbitrary claim name, so it must not be
// able to set "run_mode" — the claim that says whether an assertion came from
// `flow run local` or a server-attested run. This reuses the same
// reserved-claim collision check identity.Validate() already applies to
// "namespace", "sub", and every other claim an Issuer sets itself; there is no
// separate check written for "run_mode".
func TestAsClaimCannotOverrideRunMode(t *testing.T) {
	root := newRootCommand()
	local, _, err := root.Find([]string{"run", "local"})
	require.NoError(t, err)
	for name, value := range map[string]string{
		"as-subject": "repo:acme/payments", "as-issuer": "https://issuer.example",
		"as-namespace": "acme", "as-deployment": "prod",
	} {
		require.NoError(t, local.Flags().Set(name, value))
	}

	// An operator trying to make a local rehearsal claim to be server-attested.
	require.NoError(t, local.Flags().Set("as-claim", "run_mode=server"))

	_, err = localWorkloadIdentity(local)
	require.Error(t, err, "--as-claim must not be able to set the run_mode claim")
	require.ErrorContains(t, err, "run_mode")
}

func TestNamespacedSecretConfigurationIsTenantScoped(t *testing.T) {
	root := newRootCommand()
	local, _, err := root.Find([]string{"run", "local"})
	require.NoError(t, err)
	require.NoError(t, local.Flags().Set("secret-env", "API_TOKEN"))
	require.NoError(t, local.Flags().Set("secret-env-namespace", "team-a=TEAM_A_SECRET_"))
	require.NoError(t, local.Flags().Set("secret-require-namespace", "true"))
	t.Setenv("TEAM_A_SECRET_API_TOKEN", "team-a-value")

	registry, configured, closeProviders, err := secretRegistry(local)
	require.NoError(t, err)
	defer closeProviders()
	require.True(t, configured)
	store, err := newSecretStore(local, registry)
	require.NoError(t, err)

	_, err = store.For(nil)
	require.ErrorIs(t, err, secrets.ErrNamespace)
	resolver, err := store.For(secrets.Namespace("team-a"))
	require.NoError(t, err)
	secret, err := resolver.Resolve(t.Context(), secrets.NewRef("env", "API_TOKEN"))
	require.NoError(t, err)
	require.Equal(t, "team-a-value", secret.Reveal())

	other, err := store.For(secrets.Namespace("team-b"))
	require.NoError(t, err)
	_, err = other.Resolve(t.Context(), secrets.NewRef("env", "API_TOKEN"))
	require.ErrorIs(t, err, secrets.ErrNamespace)
}

func TestSecretProviderWithoutPolicyFailsClosed(t *testing.T) {
	_, _, err := runLocal(t, narratingWorkflow, "--secret-env", "API_TOKEN")
	require.ErrorContains(t, err, "no access policy")
}
