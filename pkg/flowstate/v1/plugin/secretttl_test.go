package plugin

import (
	"testing"
	"time"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// A plugin can say how long caching a value is safe, and the engine threw it away.
//
// The comment where it was thrown away said the engine could not honor it —
// "secrets.Provider returns (Secret, error), with nowhere to carry a TTL, so
// secrets.Cache applies its own default regardless" — and then logged a warning
// about that limitation once per plugin. [secrets.NewSecretWithTTL] is the carrier
// the comment says does not exist, and [secrets.Cache] has consulted it the whole
// time, in the package this file's subject already imports.
//
// It survived because nothing asked. There was no test anywhere touching
// `expires_in`, in either direction.
//
// The cache's half of this — a shorter answer shortens the entry, a longer one is
// capped — is covered inside the secrets package by Test_Cache_providerSuppliedTTL,
// which builds the secret in Go and has been green throughout. That is exactly why
// it could not see this: a test of either half cannot see a missing wire between
// them, and the wire is what these two cover.

// TestAPluginsLeaseReachesTheSecret is the wire.
func TestAPluginsLeaseReachesTheSecret(t *testing.T) {
	t.Parallel()

	provider := onlySecretProvider(t)

	leased, err := provider.Resolve(t.Context(), secrets.Request{
		Ref: &flowstatev1.SecretRef{Scheme: provider.Scheme(), Name: "leased"},
	})
	if err != nil {
		t.Fatalf("resolving a leased secret: %v", err)
	}

	if got := leased.TTL(); got != 30*time.Second {
		t.Fatalf("TTL = %v, want %v; the plugin said how long this may be cached and the "+
			"engine did not carry it", got, 30*time.Second)
	}
}

// TestAPluginThatSaysNothingGetsTheOperatorsDefault is the other direction, and the
// one a careless mapping breaks.
//
// Every plugin written before this field existed omits it, and zero is what the
// cache reads as "the provider is not saying". A mapping that turned an absent
// hint into an instantly-expiring entry would make every such plugin resolve on
// every step that names a secret.
func TestAPluginThatSaysNothingGetsTheOperatorsDefault(t *testing.T) {
	t.Parallel()

	provider := onlySecretProvider(t)

	ordinary, err := provider.Resolve(t.Context(), secrets.Request{
		Ref: &flowstatev1.SecretRef{Scheme: provider.Scheme(), Name: "api-key"},
	})
	if err != nil {
		t.Fatalf("resolving an ordinary secret: %v", err)
	}

	if got := ordinary.TTL(); got != 0 {
		t.Fatalf("TTL = %v, want 0; a plugin that said nothing about caching was given a "+
			"lifetime it never asked for", got)
	}
}

// onlySecretProvider starts a plugin and returns the one provider it serves.
func onlySecretProvider(t *testing.T) secrets.Provider {
	t.Helper()

	providers := openHost(t, testConfig(t, pluginDir(t, "ok"))).SecretProviders()
	if len(providers) != 1 {
		t.Fatalf("host provides %d secret providers, want 1", len(providers))
	}

	return providers[0]
}
