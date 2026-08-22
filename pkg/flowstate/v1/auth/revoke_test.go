package auth_test

import (
	"testing"
	"time"

	"github.com/picatz/jose/pkg/jwa"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
)

// publishedKeyIDs returns the key ids an issuer's key set currently carries,
// which is what a relying party fetching it would see.
func publishedKeyIDs(t *testing.T, issuer *auth.Issuer) []string {
	t.Helper()

	ids := make([]string, 0, len(issuer.KeySet().Keys))
	for _, key := range issuer.KeySet().Keys {
		id, ok := key["kid"].(string)
		require.True(t, ok, "every published key names itself")
		ids = append(ids, id)
	}
	return ids
}

// TestRevokeKeyIsNotRotation is the distinction the method exists to make
// operable: rotating leaves a retired key verifying for its whole retention
// period, and revoking stops it now.
//
// An operator who rotates in response to a suspected compromise has done
// nothing about the assertions already signed with the key they are worried
// about. The first half of this test is that fact, asserted rather than assumed,
// because a verb whose absence is only documented is a verb nobody finds.
func TestRevokeKeyIsNotRotation(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	original := issuer.ActiveKeyID()

	replacement, err := auth.GenerateSigningKey("2026-08", jwa.ES256)
	require.NoError(t, err)
	require.NoError(t, issuer.Rotate(replacement))

	// Rotation alone: the retired key is still published, so everything signed
	// with it still verifies.
	require.Equal(t, "2026-08", issuer.ActiveKeyID())
	require.Contains(t, publishedKeyIDs(t, issuer), original,
		"rotation keeps the retired key published, which is why it is not revocation")

	require.NoError(t, issuer.RevokeKey(original))

	require.NotContains(t, publishedKeyIDs(t, issuer), original,
		"a revoked key is withdrawn from the key set immediately")
	require.Contains(t, publishedKeyIDs(t, issuer), "2026-08",
		"revoking one key must not disturb the active one")

	// The discovery document is built from the same key set, so an algorithm
	// only the revoked key used goes with it.
	require.NotEmpty(t, issuer.Discovery().IDTokenSigningAlgValuesSupported)
}

// TestRevokeKeyRefuses covers the two ways revoking could leave an issuer
// silently broken, or leave an operator believing they had revoked something.
func TestRevokeKeyRefuses(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	t.Run("the active signing key", func(t *testing.T) {
		// Withdrawing the key assertions are currently signed with would
		// publish a set that verifies nothing this issuer is about to mint.
		err := issuer.RevokeKey(issuer.ActiveKeyID())
		require.ErrorIs(t, err, auth.ErrInvalidPolicy)
		require.Contains(t, err.Error(), "rotate to a new key first")
		require.Contains(t, publishedKeyIDs(t, issuer), issuer.ActiveKeyID())
	})

	t.Run("a key this issuer never published", func(t *testing.T) {
		// An error rather than a no-op: "revoked" and "misspelled the key id
		// and revoked nothing" must not look the same during an incident.
		err := issuer.RevokeKey("2019-04")
		require.ErrorIs(t, err, auth.ErrUnknownKey)
	})

	t.Run("no key id at all", func(t *testing.T) {
		require.ErrorIs(t, issuer.RevokeKey(""), auth.ErrInvalidPolicy)
	})

	t.Run("the same key twice", func(t *testing.T) {
		original := issuer.ActiveKeyID()

		replacement, err := auth.GenerateSigningKey("2026-09", jwa.ES256)
		require.NoError(t, err)
		require.NoError(t, issuer.Rotate(replacement))

		require.NoError(t, issuer.RevokeKey(original))
		require.ErrorIs(t, issuer.RevokeKey(original), auth.ErrUnknownKey,
			"a key already gone is unknown, not revoked again")
	})
}

// TestRevokedKeyStopsVerifyingRealAssertions is the negative direction, end to
// end: an assertion that was verifiable a moment ago is not verifiable now, and
// the reason is that the key it names is no longer in the set a relying party
// fetches.
func TestRevokedKeyStopsVerifyingRealAssertions(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, server := newIssuer(t, clock)

	// An assertion signed by the key that is about to be retired and revoked.
	assertion, err := issuer.Mint(t.Context(), testIdentity(), testStepRef(), "https://api.partner.example.com")
	require.NoError(t, err)
	compromised := assertion.KeyID

	verifier := newVerifier(t,
		auth.Policy{
			Issuers: []auth.TrustedIssuer{{
				Name:      "flowstate",
				Issuer:    server.URL,
				Audiences: []string{"https://api.partner.example.com"},
			}},
		},
		auth.WithClock(clock.Now),
		// A short cache and no refresh floor, so that advancing the clock a
		// little is enough to make the verifier refetch. What is asserted then
		// is the key set's contents rather than a cache's age — and the
		// advance is itself the honest part of the model: a relying party
		// stops accepting a revoked key when its own cache next refreshes.
		auth.WithKeyCacheTTL(time.Second),
		auth.WithMinKeyRefreshInterval(0),
	)

	principal, err := verifier.Verify(t.Context(), assertion.Token())
	require.NoError(t, err, "the assertion verifies while its key is published")
	require.Equal(t, assertion.Subject, principal.Subject)

	replacement, err := auth.GenerateSigningKey("2026-08", jwa.ES256)
	require.NoError(t, err)
	require.NoError(t, issuer.Rotate(replacement))

	// Rotation on its own changes nothing for an assertion already signed —
	// which is exactly why the second verb has to exist.
	clock.Advance(2 * time.Second)
	_, err = verifier.Verify(t.Context(), assertion.Token())
	require.NoError(t, err, "rotation does not invalidate what was already signed")

	require.NoError(t, issuer.RevokeKey(compromised))

	clock.Advance(2 * time.Second)
	_, err = verifier.Verify(t.Context(), assertion.Token())
	require.Error(t, err, "a revoked key's assertions must stop verifying")
	require.ErrorIs(t, err, auth.ErrUnknownKey)
}
