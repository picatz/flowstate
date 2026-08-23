package auth_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"testing"
	"time"

	"github.com/picatz/jose/pkg/jwa"
	"github.com/picatz/jose/pkg/jwt"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

type remoteSigner struct {
	id  string
	key *ecdsa.PrivateKey // test HSM storage; never passed to the keyring
}

func (s *remoteSigner) KeyID() string          { return s.id }
func (*remoteSigner) Algorithm() jwa.Algorithm { return jwa.ES256 }
func (s *remoteSigner) Sign(_ context.Context, claims jwt.ClaimsSet) (string, error) {
	key, err := auth.NewSigningKey(s.id, s.key)
	if err != nil {
		return "", err
	}
	issuer, err := auth.NewIssuer("https://issuer.example", key)
	if err != nil {
		return "", err
	}
	_ = claims // the production remote signs these; this test only exercises isolation.
	_ = issuer
	return "remote-jws", nil
}

func remoteKey(t *testing.T, id string) (*remoteSigner, auth.PublicKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	return &remoteSigner{id: id, key: key}, auth.PublicKey{ID: id, Algorithm: jwa.ES256, Public: &key.PublicKey}
}

func provider(t *testing.T, generation uint64, keys ...auth.PublicKey) auth.PublicKeySetProvider {
	t.Helper()
	p, err := auth.NewLocalPublicKeySet(generation, keys)
	require.NoError(t, err)
	return p
}

func ids(set auth.PublicKeySet) []string {
	result := make([]string, 0, len(set.Keys))
	for _, key := range set.Keys {
		result = append(result, key.ID)
	}
	return result
}

// This models the deployable order: publish both keys, move new instances to
// the new generation while old instances keep serving, then retire only after
// the longest assertion has expired. Every assertion's kid remains resolvable
// from every public snapshot encountered during the mixed-fleet interval.
func TestSigningKeyringMixedFleetRotationKeepsAssertionsVerifiable(t *testing.T) {
	ctx := context.Background()
	oldSigner, oldPublic := remoteKey(t, "2026-08-a")
	newSigner, newPublic := remoteKey(t, "2026-08-b")

	oldServer, _ := auth.NewSigningKeyring(time.Hour, nil, nil)
	oldWorker, _ := auth.NewSigningKeyring(time.Hour, nil, nil)
	require.NoError(t, oldServer.Reload(ctx, 1, oldSigner, provider(t, 1, oldPublic)))
	require.NoError(t, oldWorker.Reload(ctx, 1, oldSigner, provider(t, 1, oldPublic)))

	newServer, _ := auth.NewSigningKeyring(time.Hour, nil, nil)
	newWorker, _ := auth.NewSigningKeyring(time.Hour, nil, nil)
	staged := provider(t, 2, oldPublic, newPublic)
	require.NoError(t, newServer.Reload(ctx, 2, newSigner, staged))
	require.NoError(t, newWorker.Reload(ctx, 2, newSigner, staged))

	for name, ring := range map[string]*auth.SigningKeyring{"old server": oldServer, "old worker": oldWorker, "new server": newServer, "new worker": newWorker} {
		require.NoError(t, ring.Ready(), name)
		got := ids(ring.PublicKeySet(ctx))
		require.Contains(t, got, oldPublic.ID, "%s must verify the still-valid old assertion", name)
		if name == "new server" || name == "new worker" {
			require.Contains(t, got, newPublic.ID, "%s must verify the new assertion", name)
		}
	}

	require.ErrorIs(t, newServer.Reload(ctx, 1, oldSigner, provider(t, 1, oldPublic)), auth.ErrGenerationRollback)
	newServer.Revoke(ctx, newPublic.ID)
	require.ErrorIs(t, newServer.Ready(), auth.ErrSignerNotPublished)
}

func TestSigningKeyringRefusesSignerMissingFromPublishedGeneration(t *testing.T) {
	active, _ := remoteKey(t, "active")
	_, other := remoteKey(t, "other")
	ring, err := auth.NewSigningKeyring(time.Hour, nil, nil)
	require.NoError(t, err)
	require.ErrorIs(t, ring.Reload(context.Background(), 7, active, provider(t, 7, other)), auth.ErrSignerNotPublished)
	require.ErrorIs(t, ring.Ready(), auth.ErrSignerNotPublished)
}
