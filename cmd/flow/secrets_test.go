package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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

// TestSecretRegistryDefaultsRegisterNothing pins the fail-closed baseline every
// backend below is checked against: with no secret flags at all, a worker resolves
// nothing, and every reference — env, file, keychain, op, command, or vault — fails
// as an unknown scheme rather than resolving empty.
func TestSecretRegistryDefaultsRegisterNothing(t *testing.T) {
	root := newRootCommand()
	local, _, err := root.Find([]string{"run", "local"})
	require.NoError(t, err)

	registry, configured, closeProviders, err := secretRegistry(local)
	require.NoError(t, err)
	defer closeProviders()
	require.False(t, configured)
	require.Empty(t, registry.Schemes())
}

// TestSecretRegistryWiresCommandProvider is the CLI-to-provider path for the
// command: escape hatch, exercised for real: printf is on every CI runner, so this
// is not a stub standing in for the wiring — it is the wiring, run end to end.
func TestSecretRegistryWiresCommandProvider(t *testing.T) {
	root := newRootCommand()
	local, _, err := root.Find([]string{"run", "local"})
	require.NoError(t, err)
	require.NoError(t, local.Flags().Set("secret-command", "printf"))
	require.NoError(t, local.Flags().Set("secret-command", "resolved-%s"))
	require.NoError(t, local.Flags().Set("secret-command", "{{name}}"))

	registry, configured, closeProviders, err := secretRegistry(local)
	require.NoError(t, err)
	defer closeProviders()
	require.True(t, configured)
	require.Equal(t, []string{"command"}, registry.Schemes())

	store, err := newSecretStore(local, registry)
	require.NoError(t, err)
	resolver, err := store.For(nil)
	require.NoError(t, err)

	secret, err := resolver.Resolve(t.Context(), secrets.NewRef("command", "github-token"))
	require.NoError(t, err)
	require.Equal(t, "resolved-github-token", secret.Reveal())
}

// TestSecretRegistryCommandProviderMisconfigurationFailsAtStartup checks that a
// command: backend pointed at a nonexistent executable refuses to construct, rather
// than failing the first workflow that needs a secret.
func TestSecretRegistryCommandProviderMisconfigurationFailsAtStartup(t *testing.T) {
	root := newRootCommand()
	local, _, err := root.Find([]string{"run", "local"})
	require.NoError(t, err)
	require.NoError(t, local.Flags().Set(
		"secret-command", "flowstate-secrets-command-that-does-not-exist-anywhere"))

	_, _, closeProviders, err := secretRegistry(local)
	defer closeProviders()
	require.ErrorContains(t, err, "not installed or not on PATH")
}

// TestSecretRegistryKeychainOnNonDarwinFailsWithAClearMessage covers the platform
// handling CLAUDE.md calls out explicitly: the keychain provider is macOS-only, and
// --secret-keychain on any other platform must say so rather than fail on whatever
// generic "tool missing" message the underlying constructor produces. This is the
// path every non-macOS CI runner exercises.
func TestSecretRegistryKeychainOnNonDarwinFailsWithAClearMessage(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.Skip("this pins the non-macOS message; macOS is covered by the provider's own tests")
	}

	root := newRootCommand()
	local, _, err := root.Find([]string{"run", "local"})
	require.NoError(t, err)
	require.NoError(t, local.Flags().Set("secret-keychain", "true"))

	_, _, closeProviders, err := secretRegistry(local)
	defer closeProviders()
	require.ErrorContains(t, err, "only works on macOS")
	require.ErrorContains(t, err, runtime.GOOS)
}

// TestSecretRegistryOnePasswordFailsClosedWithoutTheCLI checks that --secret-op
// refuses to construct when the op CLI is not on PATH, rather than registering a
// scheme that fails on first use.
func TestSecretRegistryOnePasswordFailsClosedWithoutTheCLI(t *testing.T) {
	if _, err := exec.LookPath(secrets.OnePasswordCommand); err == nil {
		t.Skip("the op CLI is installed on this machine; the fail-closed path is untestable here")
	}

	root := newRootCommand()
	local, _, err := root.Find([]string{"run", "local"})
	require.NoError(t, err)
	require.NoError(t, local.Flags().Set("secret-op", "true"))

	_, _, closeProviders, err := secretRegistry(local)
	defer closeProviders()
	require.ErrorContains(t, err, "1Password")
}

// TestSecretRegistryVaultRequiresExactlyOneAuthMethod covers the CLI-level checks
// that keep a vault: backend from being registered half-configured, before the
// provider's own construction ever runs.
func TestSecretRegistryVaultRequiresExactlyOneAuthMethod(t *testing.T) {
	t.Run("neither a token nor a role is an error", func(t *testing.T) {
		root := newRootCommand()
		local, _, err := root.Find([]string{"run", "local"})
		require.NoError(t, err)
		require.NoError(t, local.Flags().Set("secret-vault-addr", "https://vault.example.com:8200"))

		_, _, closeProviders, err := secretRegistry(local)
		defer closeProviders()
		require.ErrorContains(t, err, "no authentication is configured")
	})

	t.Run("both a token file and a kubernetes role is an error", func(t *testing.T) {
		root := newRootCommand()
		local, _, err := root.Find([]string{"run", "local"})
		require.NoError(t, err)

		tokenFile := filepath.Join(t.TempDir(), "token")
		require.NoError(t, os.WriteFile(tokenFile, []byte("s.example"), 0o600))

		require.NoError(t, local.Flags().Set("secret-vault-addr", "https://vault.example.com:8200"))
		require.NoError(t, local.Flags().Set("secret-vault-token-file", tokenFile))
		require.NoError(t, local.Flags().Set("secret-vault-kubernetes-role", "flowstate-worker"))

		_, _, closeProviders, err := secretRegistry(local)
		defer closeProviders()
		require.ErrorContains(t, err, "not both")
	})

	t.Run("a static token from a file registers the scheme", func(t *testing.T) {
		root := newRootCommand()
		local, _, err := root.Find([]string{"run", "local"})
		require.NoError(t, err)

		tokenFile := filepath.Join(t.TempDir(), "token")
		require.NoError(t, os.WriteFile(tokenFile, []byte("s.example-token\n"), 0o600))

		require.NoError(t, local.Flags().Set("secret-vault-addr", "https://vault.example.com:8200"))
		require.NoError(t, local.Flags().Set("secret-vault-token-file", tokenFile))

		registry, configured, closeProviders, err := secretRegistry(local)
		require.NoError(t, err)
		defer closeProviders()
		require.True(t, configured)
		require.Equal(t, []string{"vault"}, registry.Schemes())
	})

	t.Run("a kubernetes role with no projected token fails at construction", func(t *testing.T) {
		// This worker has no service account token mounted, so the vault provider
		// itself must refuse to start rather than fail the first workflow that
		// needs a secret.
		root := newRootCommand()
		local, _, err := root.Find([]string{"run", "local"})
		require.NoError(t, err)

		require.NoError(t, local.Flags().Set("secret-vault-addr", "https://vault.example.com:8200"))
		require.NoError(t, local.Flags().Set("secret-vault-kubernetes-role", "flowstate-worker"))

		_, _, closeProviders, err := secretRegistry(local)
		defer closeProviders()
		require.Error(t, err)
	})

	t.Run("an unreachable vault address is rejected at parse time", func(t *testing.T) {
		root := newRootCommand()
		local, _, err := root.Find([]string{"run", "local"})
		require.NoError(t, err)

		require.NoError(t, local.Flags().Set("secret-vault-addr", "http://vault.example.com"))
		require.NoError(t, local.Flags().Set("secret-vault-token-file", ""))
		t.Setenv("FLOWSTATE_SECRET_VAULT_TOKEN", "s.example-token")

		_, _, closeProviders, err := secretRegistry(local)
		defer closeProviders()
		// Plaintext http is refused except for loopback: this is the provider's own
		// transport rule, reached through the CLI wiring rather than duplicated by
		// it.
		require.ErrorContains(t, err, "cleartext")
	})
}
