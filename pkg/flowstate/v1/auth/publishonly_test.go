package auth_test

import (
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
)

// A server only publishes what its workers sign (picatz/flowstate#2161), so it is
// built from public keys alone. These pin both halves: the publish-only issuer
// serves everything a relying party fetches, and it cannot sign.

// TestPublishOnlyIssuerServesTheKeySetButCannotMint is the property the mode
// exists for: the process serving the key set holds nothing it could sign with.
func TestPublishOnlyIssuerServesTheKeySetButCannotMint(t *testing.T) {
	pair := newKeyPair(t, "2026-08")

	issuer, err := auth.NewIssuer("https://flowstate.test", auth.SigningKey{},
		auth.WithVerifyOnlyKey(pair.id, pair.public))
	require.NoError(t, err)

	assert.Equal(t, "", issuer.ActiveKeyID(), "a publish-only issuer signs with no key")
	assert.Equal(t, []string{pair.id}, publishedKeyIDs(t, issuer))
	assert.Equal(t, []string{"ES256"}, algorithmNames(issuer.Discovery().IDTokenSigningAlgValuesSupported))
	assert.Equal(t, []string{"EC"}, issuer.WorkloadMetadata().KeyTypesSupported)

	server := httptest.NewServer(issuer.Handler())
	t.Cleanup(server.Close)

	for _, path := range []string{auth.DiscoveryPath, auth.WorkloadIssuerMetadataPath, issuer.JWKSPath()} {
		response, err := server.Client().Get(server.URL + path)
		require.NoError(t, err)
		_ = response.Body.Close()
		assert.Equal(t, http.StatusOK, response.StatusCode, path)
	}

	_, err = issuer.Mint(t.Context(), testIdentity(), testStepRef(), "flowstate-test")
	require.ErrorIs(t, err, auth.ErrNoSigningKey, "a publish-only issuer must refuse to mint")

	other := newKeyPair(t, "2026-09")
	require.ErrorIs(t, issuer.Rotate(other.signing), auth.ErrNoSigningKey,
		"rotating would install a signing key in a process that must hold none")
	assert.Equal(t, []string{pair.id}, publishedKeyIDs(t, issuer), "a refused rotation changes nothing")
}

// TestIssuerWithNoKeyAtAllIsStillRefused is the negative direction: publish-only
// is a zero signing key *plus* something to publish, not a way to build an issuer
// that serves an empty key set.
func TestIssuerWithNoKeyAtAllIsStillRefused(t *testing.T) {
	_, err := auth.NewIssuer("https://flowstate.test", auth.SigningKey{})
	require.ErrorIs(t, err, auth.ErrNoSigningKey)
}

// TestPublishOnlyKeysOutliveRetention pins that a publish-only issuer keeps
// every key it was given for as long as it runs. It has no signing key, so each
// one is a key a worker is signing with now; expiring them after key_retention
// would empty the key set a day after start-up while assertions still verify.
// Retention still governs keys a worker-side issuer rotates out.
func TestPublishOnlyKeysOutliveRetention(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	pair := newKeyPair(t, "2026-08")

	issuer, err := auth.NewIssuer("https://flowstate.test", auth.SigningKey{},
		auth.WithIssuerClock(clock.Now),
		auth.WithKeyRetention(time.Hour),
		auth.WithVerifyOnlyKey(pair.id, pair.public))
	require.NoError(t, err)
	require.Equal(t, []string{pair.id}, publishedKeyIDs(t, issuer))

	clock.Advance(100 * time.Hour)

	assert.Equal(t, []string{pair.id}, publishedKeyIDs(t, issuer))
	assert.NotEmpty(t, issuer.WorkloadMetadata().SigningAlgValuesSupported)
}

// TestPublishOnlyPolicyIssuerPublishesWhatAWorkerSigns is the end-to-end check
// the split rests on: the key set a server serves from public keys alone
// verifies an assertion a worker-side issuer holding the matching private key
// minted, under the same key id.
func TestPublishOnlyPolicyIssuerPublishesWhatAWorkerSigns(t *testing.T) {
	const audience = "flowstate-test"

	var (
		clock       = authtest.NewClock(referenceTime)
		restartable = newRestartableIssuer(t)
		pair        = newKeyPair(t, "2026-08")
		stranger    = newKeyPair(t, "2026-08")
		claims      = slices.Sorted(maps.Keys(testIdentity().Claims))
	)

	policy := auth.FederationPolicy{Issuer: restartable.server.URL, DeclaredClaims: claims}

	// The server: public key only.
	server, err := policy.PublishOnlyIssuer(
		auth.WithFederationClock(clock.Now),
		auth.WithFederationVerifyOnlyKey(pair.id, pair.public))
	require.NoError(t, err)
	restartable.mu.Lock()
	restartable.handler = server.Handler()
	restartable.mu.Unlock()

	// The worker: the matching private key, building its issuer from the same
	// policy.
	broker, err := policy.Broker(pair.signing, auth.WithFederationClock(clock.Now))
	require.NoError(t, err)

	verifier := newVerifier(t,
		auth.Policy{Issuers: []auth.TrustedIssuer{{
			Name: "flowstate-self", Issuer: restartable.server.URL, Audiences: []string{audience},
		}}},
		auth.WithClock(clock.Now),
	)

	assertion, err := broker.Issuer().Mint(t.Context(), testIdentity(), testStepRef(), audience)
	require.NoError(t, err)
	require.Equal(t, pair.id, assertion.KeyID)

	_, err = verifier.Verify(t.Context(), assertion.Token())
	require.NoError(t, err, "the server's public-only key set must verify what the worker signed")

	// Negative direction: a different key under the same id is not covered by
	// what the server publishes, so a verifier that is told only the real public
	// half refuses it.
	forged, err := auth.NewIssuer(restartable.server.URL, stranger.signing,
		auth.WithIssuerClock(clock.Now), auth.WithDeclaredClaims(claims...))
	require.NoError(t, err)
	bad, err := forged.Mint(t.Context(), testIdentity(), testStepRef(), audience)
	require.NoError(t, err)

	clock.Advance(time.Minute)
	_, err = verifier.Verify(t.Context(), bad.Token())
	require.Error(t, err, "publishing a public key must not vouch for another key sharing its id")
}

// TestPublishOnlyPolicyIssuerNeedsAKey keeps the policy-level builder from
// serving an empty key set when a deployment forgot to name any key.
func TestPublishOnlyPolicyIssuerNeedsAKey(t *testing.T) {
	_, err := auth.FederationPolicy{Issuer: "https://flowstate.test"}.PublishOnlyIssuer()
	require.ErrorIs(t, err, auth.ErrNoSigningKey)
}

func TestFederationPolicyTargetNamesAreSorted(t *testing.T) {
	policy := auth.FederationPolicy{Targets: []auth.FederationTarget{{Name: "vault"}, {Name: "aws"}}}
	assert.Equal(t, []string{"aws", "vault"}, policy.TargetNames())
	assert.Empty(t, auth.FederationPolicy{}.TargetNames())
}

func algorithmNames[T ~string](algorithms []T) []string {
	names := make([]string, len(algorithms))
	for i, algorithm := range algorithms {
		names[i] = string(algorithm)
	}
	return names
}
