package auth_test

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/picatz/jose/pkg/jwa"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
)

// The rotation an operator actually performs is a restart, and until
// picatz/flowstate#891 nothing carried a key across one.
//
// [auth.Issuer.Rotate] rotates in place, which is the rotation nobody can run:
// no deployment calls it, and a process that starts with one key publishes that
// key and only that key. So these tests never call Rotate. They build an issuer,
// throw it away, and build another one the way a restarted process would — which
// is the boundary the retention window has to cross for it to mean anything.

// keyPair is a signing key and the public half a later process would publish it
// under. [auth.SigningKey] deliberately exposes neither half, so the material is
// generated here and handed to both sides, exactly as `flow` does when it reads
// a PEM off disk.
type keyPair struct {
	id      string
	signing auth.SigningKey
	public  any
}

func newKeyPair(t *testing.T, id string) keyPair {
	t.Helper()

	private, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	signing, err := auth.NewSigningKey(id, private)
	require.NoError(t, err)

	return keyPair{id: id, signing: signing, public: &private.PublicKey}
}

// restartableIssuer serves one issuer URL across several issuers, so a test can
// replace the process's issuer the way a restart does: same identity, same
// published paths, an entirely new [auth.Issuer] behind them.
type restartableIssuer struct {
	server *httptest.Server

	mu      sync.RWMutex
	handler http.Handler
}

func newRestartableIssuer(t *testing.T) *restartableIssuer {
	t.Helper()

	restartable := &restartableIssuer{}
	restartable.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		restartable.mu.RLock()
		current := restartable.handler
		restartable.mu.RUnlock()

		if current == nil {
			http.Error(w, "issuer not ready", http.StatusServiceUnavailable)
			return
		}
		current.ServeHTTP(w, r)
	}))
	t.Cleanup(restartable.server.Close)

	return restartable
}

// start builds an issuer as a process starting with the given keys would, and
// serves it. The first key signs; the rest are published for verification only,
// which is exactly the rule `--identity-key`'s order carries.
func (r *restartableIssuer) start(t *testing.T, clock *authtest.Clock, signing auth.SigningKey, verifyOnly ...keyPair) *auth.Issuer {
	t.Helper()

	opts := []auth.IssuerOption{
		auth.WithIssuerClock(clock.Now),
		auth.WithKeyRetention(time.Hour),
		auth.WithDeclaredClaims(slices.Sorted(maps.Keys(testIdentity().Claims))...),
	}
	for _, key := range verifyOnly {
		opts = append(opts, auth.WithVerifyOnlyKey(key.id, key.public))
	}

	issuer, err := auth.NewIssuer(r.server.URL, signing, opts...)
	require.NoError(t, err)

	r.mu.Lock()
	r.handler = issuer.Handler()
	r.mu.Unlock()

	return issuer
}

// TestVerifyOnlyKeysCarryAssertionsAcrossARestart is the acceptance the issue
// asked for: an assertion minted before a restart verifies after it, for as long
// as it is valid, and the old key leaves the key set when retention lapses.
func TestVerifyOnlyKeysCarryAssertionsAcrossARestart(t *testing.T) {
	const audience = "flowstate-test"

	var (
		clock       = authtest.NewClock(referenceTime)
		restartable = newRestartableIssuer(t)
		old         = newKeyPair(t, "2026-07")
		fresh       = newKeyPair(t, "2026-08")
	)

	verifier := newVerifier(t,
		auth.Policy{
			Issuers: []auth.TrustedIssuer{{
				Name:      "flowstate-self",
				Issuer:    restartable.server.URL,
				Audiences: []string{audience},
			}},
		},
		auth.WithClock(clock.Now),
		auth.WithKeyCacheTTL(time.Minute),
		auth.WithMinKeyRefreshInterval(time.Second),
	)

	before, err := restartable.start(t, clock, old.signing).
		Mint(t.Context(), testIdentity(), testStepRef(), audience)
	require.NoError(t, err)
	require.Equal(t, old.id, before.KeyID)

	// The restart. The new process signs with the new key and publishes the old
	// one for verification only, which is `--identity-key 2026-08.pem
	// --identity-key 2026-07.pem`.
	restarted := restartable.start(t, clock, fresh.signing, old)

	require.Equal(t, fresh.id, restarted.ActiveKeyID())
	require.Equal(t, []string{fresh.id, old.id}, publishedKeyIDs(t, restarted),
		"both keys are published, the signing one first")

	after, err := restarted.Mint(t.Context(), testIdentity(), testStepRef(), audience)
	require.NoError(t, err)
	require.Equal(t, fresh.id, after.KeyID,
		"a verify-only key must never sign; the process signs with the key it was given first")

	// A relying party that has never seen either key: it refetches on the
	// unknown key id, and the set it fetches has to cover both.
	clock.Advance(2 * time.Second)

	_, err = verifier.Verify(t.Context(), after.Token())
	require.NoError(t, err, "an assertion signed after the restart must verify")

	_, err = verifier.Verify(t.Context(), before.Token())
	require.NoError(t, err, "an assertion signed before the restart must still verify")

	// Past the retention window the old key is withdrawn, which is what makes
	// key_retention mean something across the boundary rotation crosses.
	clock.Advance(2 * time.Hour)
	require.Equal(t, []string{fresh.id}, publishedKeyIDs(t, restarted),
		"a verify-only key is dropped once the configured retention has elapsed")

	// And the negative direction, which is the point of the retention rather
	// than a side effect: an assertion signed by the withdrawn key no longer
	// verifies, once the relying party refreshes.
	stale, err := restartable.start(t, clock, old.signing).
		Mint(t.Context(), testIdentity(), testStepRef(), audience)
	require.NoError(t, err)

	restartable.mu.Lock()
	restartable.handler = restarted.Handler()
	restartable.mu.Unlock()

	clock.Advance(2 * time.Minute)

	_, err = verifier.Verify(t.Context(), stale.Token())
	require.Error(t, err, "a key past its retention verifies nothing")
}

// TestUnnamedKeysAreNotPublished is the other negative direction: publication is
// what the operator listed, and nothing else.
func TestUnnamedKeysAreNotPublished(t *testing.T) {
	var (
		clock       = authtest.NewClock(referenceTime)
		restartable = newRestartableIssuer(t)
		named       = newKeyPair(t, "2026-07")
		unnamed     = newKeyPair(t, "2026-06")
		signing     = newKeyPair(t, "2026-08")
	)

	issuer := restartable.start(t, clock, signing.signing, named)

	require.Equal(t, []string{signing.id, named.id}, publishedKeyIDs(t, issuer))
	require.NotContains(t, publishedKeyIDs(t, issuer), unnamed.id,
		"a key nobody named is not published, however recently it signed")
}

// TestVerifyOnlyKeysAreRefusedRatherThanSkipped covers the fail-closed half. A
// key that cannot be published is a start-up error: the alternative is a process
// that comes up serving a key set the operator believes covers a rotation and
// does not, with nothing said.
func TestVerifyOnlyKeysAreRefusedRatherThanSkipped(t *testing.T) {
	var (
		clock   = authtest.NewClock(referenceTime)
		signing = newKeyPair(t, "2026-08")
		other   = newKeyPair(t, "2026-07")
	)

	tests := []struct {
		name    string
		opts    []auth.IssuerOption
		mention string
	}{
		{
			name:    "no id",
			opts:    []auth.IssuerOption{auth.WithVerifyOnlyKey("", other.public)},
			mention: "needs the id",
		},
		{
			name:    "id with whitespace",
			opts:    []auth.IssuerOption{auth.WithVerifyOnlyKey("2026 07", other.public)},
			mention: "whitespace",
		},
		{
			name:    "the active key's id",
			opts:    []auth.IssuerOption{auth.WithVerifyOnlyKey(signing.id, other.public)},
			mention: "active signing key",
		},
		{
			name: "the same id twice",
			opts: []auth.IssuerOption{
				auth.WithVerifyOnlyKey(other.id, other.public),
				auth.WithVerifyOnlyKey(other.id, newKeyPair(t, "ignored").public),
			},
			mention: "given twice",
		},
		{
			name:    "a private key",
			opts:    []auth.IssuerOption{auth.WithVerifyOnlyKey(other.id, mustGenerateEd25519Private(t))},
			mention: "cannot verify assertions",
		},
		{
			name:    "an RSA key below the floor",
			opts:    []auth.IssuerOption{auth.WithVerifyOnlyKey(other.id, mustGenerateSmallRSAPublic(t))},
			mention: "want at least 2048",
		},
		{
			name:    "an unsupported curve",
			opts:    []auth.IssuerOption{auth.WithVerifyOnlyKey(other.id, mustGenerateP384Public(t))},
			mention: "P-256",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := auth.NewIssuer("https://flowstate.example.com", signing.signing,
				append([]auth.IssuerOption{auth.WithIssuerClock(clock.Now)}, tc.opts...)...)

			require.Error(t, err, "a key that cannot be published must refuse start-up")
			assert.ErrorIs(t, err, auth.ErrInvalidPolicy)
			assert.Contains(t, err.Error(), tc.mention)
		})
	}
}

// TestVerifyOnlyKeysCanBeRevokedEarly checks that a key published this way is a
// retired key in every sense, including the one an incident needs: it is
// addressable by [auth.Issuer.RevokeKey], the verb rotation deliberately is not.
func TestVerifyOnlyKeysCanBeRevokedEarly(t *testing.T) {
	var (
		clock       = authtest.NewClock(referenceTime)
		restartable = newRestartableIssuer(t)
		old         = newKeyPair(t, "2026-07")
		fresh       = newKeyPair(t, "2026-08")
	)

	issuer := restartable.start(t, clock, fresh.signing, old)
	require.NoError(t, issuer.RevokeKey(old.id))
	require.Equal(t, []string{fresh.id}, publishedKeyIDs(t, issuer))

	require.ErrorIs(t, issuer.RevokeKey(fresh.id), auth.ErrInvalidPolicy,
		"the signing key is still not revocable, whatever else is published")
}

func mustGenerateEd25519Private(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return private
}

func mustGenerateSmallRSAPublic(t *testing.T) *rsa.PublicKey {
	t.Helper()
	private, err := rsa.GenerateKey(rand.Reader, 1024)
	require.NoError(t, err)
	return &private.PublicKey
}

func mustGenerateP384Public(t *testing.T) *ecdsa.PublicKey {
	t.Helper()
	private, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	require.NoError(t, err)
	return &private.PublicKey
}

// verify the algorithm list keeps naming every published key's algorithm, since
// a relying party reads it out of the discovery document.
func TestDiscoveryNamesEveryPublishedAlgorithm(t *testing.T) {
	var (
		clock       = authtest.NewClock(referenceTime)
		restartable = newRestartableIssuer(t)
		signing     = newKeyPair(t, "2026-08")
	)

	rsaPrivate, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	issuer := restartable.start(t, clock, signing.signing,
		keyPair{id: "2026-07", public: &rsaPrivate.PublicKey})

	document := issuer.Discovery()
	require.ElementsMatch(t, []jwa.Algorithm{jwa.ES256, jwa.RS256}, document.IDTokenSigningAlgValuesSupported)
}

// steppingClock jumps forward on every reading, once a test arms it.
//
// A stopped clock cannot see the defect [TestPublishedAlgorithmsAndKeyTypesDescribeOneKeySet]
// is about, because that defect is two readings of the clock disagreeing. This
// makes the disagreement certain rather than a race a test would have to win.
type steppingClock struct {
	mu   sync.Mutex
	now  time.Time
	step time.Duration
}

// stepBy arms the clock: from here every reading lands this much later than the
// one before it.
func (c *steppingClock) stepBy(step time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.step = step
}

// Now returns the current instant and moves the clock on by the armed step.
func (c *steppingClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now
	c.now = c.now.Add(c.step)

	return now
}

// TestPublishedAlgorithmsAndKeyTypesDescribeOneKeySet pins that the two
// cryptographic fields of the workload document are read from one snapshot of
// the key set.
//
// Answering them separately means two readings of the clock and two acquisitions
// of the lock, which a retained key's retention can expire between — and, in a
// running server, which [auth.Issuer.Rotate] or [auth.Issuer.RevokeKey] can land
// between. Either way the published document names an algorithm whose key type
// it does not name, or the reverse. A relying party caches that for the
// discovery cache lifetime and then refuses assertions signed by a key it was
// told about only half of.
//
// The clock here does deterministically what concurrency would do occasionally:
// the retained RSA key is inside its retention at the first reading and outside
// it at the second.
func TestPublishedAlgorithmsAndKeyTypesDescribeOneKeySet(t *testing.T) {
	var (
		clock   = &steppingClock{now: referenceTime}
		signing = newKeyPair(t, "2026-08")
	)

	rsaPrivate, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	issuer, err := auth.NewIssuer("https://flowstate.test", signing.signing,
		auth.WithIssuerClock(clock.Now),
		auth.WithKeyRetention(time.Hour),
		auth.WithDeclaredClaims(slices.Sorted(maps.Keys(testIdentity().Claims))...),
		auth.WithVerifyOnlyKey("2026-07", &rsaPrivate.PublicKey),
	)
	require.NoError(t, err)

	// The retained key expires an hour after the instant the issuer was built,
	// so a two-hour step straddles it exactly once.
	clock.stepBy(2 * time.Hour)

	metadata := issuer.WorkloadMetadata()

	require.Contains(t, metadata.SigningAlgValuesSupported, jwa.RS256,
		"test setup: the first reading of the clock has to still be inside the retained key's retention")
	require.Contains(t, metadata.KeyTypesSupported, "RSA",
		"the algorithms and the key types were read at different instants, so the published "+
			"document describes two different key sets")
}
