package auth_test

import (
	"crypto"
	"errors"
	"testing"
	"time"

	"github.com/picatz/jose/pkg/jwa"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth/signers/vaulttransit"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets/vault"
)

// These tests run the Vault Transit signer through the same issuer, verifier,
// and leak enumeration every other [auth.Signer] goes through. The signer's own
// transport behaviour (formats, bounds, timeouts) is tested beside it; what is
// here is what only an issuer can show: that what Transit signs is what a
// relying party verifies, across a rotation, and that nothing falls back to a
// key this process holds.

const transitKeyName = "flowstate-identity"

func transitBackend(t *testing.T, typ string) *authtest.Transit {
	t.Helper()

	transit := authtest.NewTransit()
	t.Cleanup(func() { _ = transit.Close() })
	transit.CreateKey(transitKeyName, typ)

	return transit
}

func transitConfig(transit *authtest.Transit) vaulttransit.Config {
	return vaulttransit.Config{
		Address:      transit.URL(),
		Key:          transitKeyName,
		EgressPolicy: authtest.EgressPolicy(),
		Vault:        []vault.Option{vault.WithToken(authtest.TransitToken)},
	}
}

func newTransitSigner(t *testing.T, transit *authtest.Transit) (*vaulttransit.Signer, auth.SigningKey) {
	t.Helper()

	signer, err := vaulttransit.New(t.Context(), transitConfig(transit))
	require.NoError(t, err)

	key, err := signer.SigningKey(t.Context())
	require.NoError(t, err)

	return signer, key
}

func transitVerifier(t *testing.T, restartable *restartableIssuer, clock *authtest.Clock, audience string) *auth.OIDCVerifier {
	t.Helper()

	return newVerifier(t,
		auth.Policy{Issuers: []auth.TrustedIssuer{{
			Name:      "flowstate-self",
			Issuer:    restartable.server.URL,
			Audiences: []string{audience},
		}}},
		auth.WithClock(clock.Now),
		auth.WithKeyCacheTTL(time.Minute),
		auth.WithMinKeyRefreshInterval(time.Second),
	)
}

func TestTransitSignerMintsAssertionsTheVerifierAccepts(t *testing.T) {
	const audience = "flowstate-test"

	for _, tc := range []struct {
		typ       string
		algorithm jwa.Algorithm
	}{
		{authtest.TransitECDSAP256, jwa.ES256},
		{authtest.TransitEd25519, jwa.EdDSA},
	} {
		t.Run(tc.typ, func(t *testing.T) {
			var (
				clock       = authtest.NewClock(referenceTime)
				restartable = newRestartableIssuer(t)
				transit     = transitBackend(t, tc.typ)
			)

			signer, key := newTransitSigner(t, transit)
			require.Equal(t, tc.algorithm, key.Algorithm())

			issuer := restartable.start(t, clock, key)
			verifier := transitVerifier(t, restartable, clock, audience)

			assertion, err := issuer.Mint(t.Context(), testIdentity(), testStepRef(), audience)
			require.NoError(t, err)
			require.Equal(t, signer.KeyID(), assertion.KeyID)

			identity, err := verifier.Verify(t.Context(), assertion.Token())
			require.NoError(t, err, "what Transit signed must verify against the key set the issuer publishes")
			require.Equal(t, assertion.Subject, identity.Subject)

			// The published key is the one Transit reported, and nothing local.
			require.Equal(t, []string{signer.KeyID()}, publishedKeyIDs(t, issuer))
		})
	}
}

// TestTransitSignerRotationKeepsThePreviousVersionPublished is the overlap the
// trust policy documents for file keys, carried to a backend rotation: the
// signer serves the new version, the old version stays published, and an
// assertion signed before the rotation verifies after it.
func TestTransitSignerRotationKeepsThePreviousVersionPublished(t *testing.T) {
	const audience = "flowstate-test"

	var (
		clock       = authtest.NewClock(referenceTime)
		restartable = newRestartableIssuer(t)
		transit     = transitBackend(t, authtest.TransitECDSAP256)
	)

	first, firstKey := newTransitSigner(t, transit)
	issuer := restartable.start(t, clock, firstKey)
	verifier := transitVerifier(t, restartable, clock, audience)

	before, err := issuer.Mint(t.Context(), testIdentity(), testStepRef(), audience)
	require.NoError(t, err)
	require.Equal(t, transitKeyName+"-v1", before.KeyID)

	// In-process rotation: the operator rotates in Transit, and the signer for
	// the new version is proved against its own public key before it is installed.
	transit.RotateKey(transitKeyName)

	second, rotated, err := first.Next(t.Context())
	require.NoError(t, err)
	require.True(t, rotated)

	secondKey, err := second.SigningKey(t.Context())
	require.NoError(t, err)
	require.NoError(t, issuer.Rotate(secondKey))

	after, err := issuer.Mint(t.Context(), testIdentity(), testStepRef(), audience)
	require.NoError(t, err)
	require.Equal(t, transitKeyName+"-v2", after.KeyID)
	require.Equal(t, []string{transitKeyName + "-v2", transitKeyName + "-v1"}, publishedKeyIDs(t, issuer))

	clock.Advance(2 * time.Second)

	_, err = verifier.Verify(t.Context(), after.Token())
	require.NoError(t, err)
	_, err = verifier.Verify(t.Context(), before.Token())
	require.NoError(t, err, "an assertion signed before the rotation must still verify")

	// The restart shape, the one an operator actually performs: a new process
	// signs with the latest version and publishes the backend's previous
	// versions for verification only, with no key file anywhere.
	var previous []keyPair
	for _, key := range second.Previous() {
		previous = append(previous, keyPair{id: key.ID, public: key.Key})
	}

	restarted := restartable.start(t, clock, secondKey, previous...)
	require.Equal(t, []string{transitKeyName + "-v2", transitKeyName + "-v1"}, publishedKeyIDs(t, restarted))

	// Withdrawing a version in Transit stops it being published by the next
	// process, which is the operator's way to end the overlap.
	transit.SetMinDecryptionVersion(transitKeyName, 2)
	set, err := vaulttransit.Read(t.Context(), transitConfig(transit))
	require.NoError(t, err)
	require.Empty(t, set.Previous)
}

// TestTransitSignerFailsClosedAtTheIssuer: a backend that refuses is a mint that
// fails. Nothing signs with another key, because this process holds none.
func TestTransitSignerFailsClosedAtTheIssuer(t *testing.T) {
	const audience = "flowstate-test"

	var (
		clock       = authtest.NewClock(referenceTime)
		restartable = newRestartableIssuer(t)
		transit     = transitBackend(t, authtest.TransitECDSAP256)
	)

	_, key := newTransitSigner(t, transit)
	issuer := restartable.start(t, clock, key)

	_, err := issuer.Mint(t.Context(), testIdentity(), testStepRef(), audience)
	require.NoError(t, err)

	transit.RevokeToken()

	assertion, err := issuer.Mint(t.Context(), testIdentity(), testStepRef(), audience)
	require.ErrorIs(t, err, secrets.ErrPermission)
	require.Empty(t, assertion.Token())
	require.NotContains(t, err.Error(), authtest.TransitToken)
	require.NotContains(t, err.Error(), authtest.TransitErrorMarker)
}

// TestTransitSignerWithAWrongPublicKeyIsRefusedByTheIssuerSeam: the check the
// Signer contract describes, through [auth.NewProviderSigningKey] directly with
// the public half the backend reported for a different key.
func TestTransitSignerWithAWrongPublicKeyIsRefusedByTheIssuerSeam(t *testing.T) {
	transit := transitBackend(t, authtest.TransitECDSAP256)
	transit.CreateKey("other", authtest.TransitECDSAP256)

	signer, err := vaulttransit.New(t.Context(), transitConfig(transit))
	require.NoError(t, err)

	_, err = auth.NewProviderSigningKey(t.Context(), signer, transit.PublicKey("other", 1))
	require.ErrorIs(t, err, auth.ErrInvalidPolicy)
	require.ErrorContains(t, err, "does not verify what that signer signs")
}

// TestPublishOnlyIssuerServesWhatTheBackendHolds: a server that only publishes
// reads the public keys from Transit and cannot sign.
func TestPublishOnlyIssuerServesWhatTheBackendHolds(t *testing.T) {
	transit := transitBackend(t, authtest.TransitEd25519)
	transit.RotateKey(transitKeyName)

	set, err := vaulttransit.Read(t.Context(), transitConfig(transit))
	require.NoError(t, err)

	opts := []auth.IssuerOption{auth.WithVerifyOnlyKey(set.Current.ID, set.Current.Key)}
	for _, previous := range set.Previous {
		opts = append(opts, auth.WithVerifyOnlyKey(previous.ID, previous.Key))
	}

	issuer, err := auth.NewIssuer("https://flowstate.example", auth.SigningKey{}, opts...)
	require.NoError(t, err)

	require.Empty(t, issuer.ActiveKeyID())
	require.ElementsMatch(t, []string{transitKeyName + "-v2", transitKeyName + "-v1"}, publishedKeyIDs(t, issuer))

	_, err = issuer.Mint(t.Context(), testIdentity(), testStepRef(), "flowstate-test")
	require.True(t, errors.Is(err, auth.ErrNoSigningKey))
}

// TestTransitSignerPassesTheProviderSignerLeakEnumeration runs the Transit signer
// through the same renderings every provider-backed key is held to, with the
// Vault token as the material that must never appear.
func TestTransitSignerPassesTheProviderSignerLeakEnumeration(t *testing.T) {
	transit := transitBackend(t, authtest.TransitECDSAP256)
	signer, err := vaulttransit.New(t.Context(), transitConfig(transit))
	require.NoError(t, err)

	var public crypto.PublicKey = signer.Public()

	requireSignerDoesNotLeak(t, signer, public, authtest.TransitToken)
}
