package auth_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/picatz/jose/pkg/jwa"
	"github.com/picatz/jose/pkg/jwt"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// parkedSigner stops inside a signature and waits to be let go, so a test can
// look at what the issuer is holding while a provider is slow.
//
// A barrier rather than a sleep: entered is closed when a signature has really
// begun and release is closed when the test is done looking, so nothing here
// races a duration. Only the first armed call parks — later mints sign
// normally, which is what makes "a stuck signature does not block the mints
// behind it" observable.
type parkedSigner struct {
	*fakeProvider

	mu    sync.Mutex
	armed bool

	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func newParkedSigner(t *testing.T, id string) (*parkedSigner, *ecdsa.PublicKey) {
	t.Helper()

	base, public := newFakeProvider(t, id)
	return &parkedSigner{
		fakeProvider: base,
		entered:      make(chan struct{}),
		release:      make(chan struct{}),
	}, public
}

// arm parks the next signature. Called after [auth.NewProviderSigningKey], whose
// start-up proof of possession is a signature of its own and must not park.
func (p *parkedSigner) arm() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.armed = true
}

func (p *parkedSigner) Sign(ctx context.Context, claims jwt.ClaimsSet) (string, error) {
	p.mu.Lock()
	park := p.armed
	if park {
		p.armed = false
	}
	p.mu.Unlock()

	if park {
		p.once.Do(func() { close(p.entered) })

		select {
		case <-p.release:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}

	return p.fakeProvider.Sign(ctx, claims)
}

// mintResult is one background mint's answer.
type mintResult struct {
	assertion auth.Assertion
	err       error
}

// mintInBackground starts a mint and returns the channel its answer arrives on.
func mintInBackground(ctx context.Context, issuer *auth.Issuer) <-chan mintResult {
	done := make(chan mintResult, 1)
	go func() {
		assertion, err := issuer.Mint(ctx, testIdentity(), testStepRef(), "https://as.example.com")
		done <- mintResult{assertion: assertion, err: err}
	}()
	return done
}

// awaitMint waits for a background mint, failing rather than hanging.
func awaitMint(t *testing.T, done <-chan mintResult) mintResult {
	t.Helper()

	select {
	case result := <-done:
		return result
	case <-time.After(30 * time.Second):
		t.Fatal("the mint never returned")
		return mintResult{}
	}
}

// mustNotBlock runs work and fails if it has not finished promptly, which is
// how "the issuer is not holding a lock across the signature" is asserted
// without the test itself hanging when it is.
func mustNotBlock(t *testing.T, what string, work func() error) {
	t.Helper()

	done := make(chan error, 1)
	go func() { done <- work() }()

	select {
	case err := <-done:
		require.NoError(t, err, what)
	case <-time.After(30 * time.Second):
		t.Fatalf("%s blocked while a signature was in flight; the issuer is holding a lock across the provider call", what)
	}
}

// newParkedIssuer returns an issuer signing through a parked provider, with one
// extra retired key published so that [auth.Issuer.RevokeKey] has something to
// revoke that is not the key a parked mint is signing with.
func newParkedIssuer(t *testing.T, id string, opts ...auth.IssuerOption) (*auth.Issuer, *parkedSigner) {
	t.Helper()

	provider, public := newParkedSigner(t, id)

	key, err := auth.NewProviderSigningKey(t.Context(), provider, public)
	require.NoError(t, err)

	previous, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	issuer, err := auth.NewIssuer("https://flowstate.example", key,
		append([]auth.IssuerOption{
			auth.WithDeclaredClaims("repository"),
			auth.WithVerifyOnlyKey("previous", &previous.PublicKey),
			// Long enough that a parked signature never releases itself: a
			// test about the lock must fail because of the lock, and the
			// default bound expiring underneath it would both hide that and
			// report the wrong reason. Tests about the bound pass their own,
			// which wins by arriving later.
			auth.WithSigningTimeout(time.Hour),
		}, opts...)...)
	require.NoError(t, err)

	provider.arm()

	return issuer, provider
}

// TestMintDoesNotHoldTheIssuerLockAcrossSigning is picatz/flowstate#1055: the
// signature is a round trip to a machine this process does not control, and it
// must not be made while holding the lock the issuer's own verbs need.
//
// Signing under the read lock made a hung provider an outage of everything, not
// only of the mint that was unlucky. [auth.Issuer.Rotate] and
// [auth.Issuer.RevokeKey] are readers' writers, so they waited for the stuck
// signature — and Go's [sync.RWMutex] gives a waiting writer priority over
// readers arriving afterwards, so every *later* mint waited behind that writer
// too. One slow KMS call, and the token path stops for the whole process,
// including the two verbs an operator reaches for to fix it.
//
// So all three are asserted here while one signature is parked: rotation
// completes, revocation completes, and a second mint completes. A barrier, not
// a sleep — the parked signature is genuinely in flight for the whole of it.
//
// The parked mint then succeeds despite the rotation, which is the other half
// of the property: rotating is not revoking, the key it was signing with is
// still published, and an assertion signed by it is still perfectly valid.
func TestMintDoesNotHoldTheIssuerLockAcrossSigning(t *testing.T) {
	issuer, provider := newParkedIssuer(t, "kms-parked")

	parked := mintInBackground(t.Context(), issuer)
	<-provider.entered

	replacement, err := auth.GenerateSigningKey("2026-09", jwa.ES256)
	require.NoError(t, err)

	mustNotBlock(t, "Rotate", func() error { return issuer.Rotate(replacement) })
	mustNotBlock(t, "RevokeKey", func() error { return issuer.RevokeKey("previous") })
	mustNotBlock(t, "a second mint", func() error {
		_, err := issuer.Mint(t.Context(), testIdentity(), testStepRef(), "https://as.example.com")
		return err
	})

	close(provider.release)

	result := awaitMint(t, parked)
	require.NoError(t, result.err,
		"a key rotated out mid-signature is still published, so the assertion signed with it is still valid")
	require.Equal(t, "kms-parked", result.assertion.KeyID)
}

// TestMintDiscardsAnAssertionSignedByAKeyRevokedMidSignature is the window that
// signing under the read lock used to close, closed the other way: the mint
// re-reads the published key set after the signature and refuses to hand back an
// assertion whose key is no longer in it.
//
// Without that re-check, moving the signature out of the lock would trade one
// bug for a worse one. [auth.Issuer.RevokeKey] exists to stop new assertions
// being signed by a key an operator believes is compromised; a relying party
// holding a cached key set would accept one, so returning it would make
// revocation report success while doing the one thing it forbids.
func TestMintDiscardsAnAssertionSignedByAKeyRevokedMidSignature(t *testing.T) {
	issuer, provider := newParkedIssuer(t, "kms-revoked-midflight")

	parked := mintInBackground(t.Context(), issuer)
	<-provider.entered

	// The signing key cannot be revoked while it is active, which is the same
	// order an operator responding to a compromise follows.
	replacement, err := auth.GenerateSigningKey("2026-09", jwa.ES256)
	require.NoError(t, err)
	require.NoError(t, issuer.Rotate(replacement))
	require.NoError(t, issuer.RevokeKey("kms-revoked-midflight"))

	close(provider.release)

	result := awaitMint(t, parked)
	require.ErrorIs(t, result.err, auth.ErrUnknownKey,
		"an assertion signed by a key revoked while it was being signed must never be returned")
	require.True(t, result.assertion.IsZero(), "the refused mint returns no assertion")
	require.Empty(t, result.assertion.Token(), "the refused mint returns no token")
}

// TestSigningTimeoutBoundsTheProviderCall is the bound itself: the resource the
// far side controls is how long it takes to answer, so that is what is bounded.
//
// The mint's own context is not a substitute. A caller with no deadline — an
// activity started with a background context, a CLI invocation — would otherwise
// wait on a stuck provider forever, and the issuer would have accepted an
// unbounded remote call because nobody upstream happened to set a clock.
func TestSigningTimeoutBoundsTheProviderCall(t *testing.T) {
	issuer, provider := newParkedIssuer(t, "kms-hangs", auth.WithSigningTimeout(50*time.Millisecond))

	// Deliberately a context that never expires: the bound under test is the
	// issuer's, and a caller deadline would make this pass either way.
	result := awaitMint(t, mintInBackground(context.Background(), issuer))

	<-provider.entered // the signature really was in flight

	require.Error(t, result.err)
	require.ErrorIs(t, result.err, context.DeadlineExceeded)
	require.Contains(t, result.err.Error(), "kms-hangs")
	require.True(t, result.assertion.IsZero())
}

// TestSigningTimeoutMustBePositive is the fail-closed half: an unbounded remote
// call cannot be configured, not even by leaving a zero value in place.
func TestSigningTimeoutMustBePositive(t *testing.T) {
	key, err := auth.GenerateSigningKey("test-key", jwa.ES256)
	require.NoError(t, err)

	for _, timeout := range []time.Duration{0, -time.Second} {
		_, err := auth.NewIssuer("https://flowstate.example", key, auth.WithSigningTimeout(timeout))
		require.ErrorIs(t, err, auth.ErrInvalidPolicy, "signing timeout %s", timeout)
		require.Contains(t, err.Error(), "signing timeout must be positive")
	}
}

// TestMintUsesTheCallersDeadlineWhenItIsSooner keeps the two clocks in the right
// order: the issuer's bound is a ceiling, not a floor, so a caller who has
// already given up is not made to wait for it.
func TestMintUsesTheCallersDeadlineWhenItIsSooner(t *testing.T) {
	issuer, _ := newParkedIssuer(t, "kms-caller-gives-up", auth.WithSigningTimeout(time.Hour))

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	result := awaitMint(t, mintInBackground(ctx, issuer))

	require.Error(t, result.err)
	require.True(t, errors.Is(result.err, context.DeadlineExceeded),
		"the caller's own deadline still ends the mint")
	require.NotContains(t, result.err.Error(), "did not finish within",
		"the caller's deadline is not reported as the issuer's signing timeout")
}

// TestFederationPolicySigningTimeoutReachesTheIssuer is the option seen from
// where a deployment actually configures one.
//
// A deployment signing through a KMS is configured from a policy file, not by
// calling [auth.NewIssuer] in Go, so a bound reachable only from the Go API is
// a bound most of the deployments that need it cannot set. The other issuer
// knobs — assertion lifetime, key retention — are already spelled there, and
// this is read the same way: a positive value overrides the default, and zero
// leaves it alone.
func TestFederationPolicySigningTimeoutReachesTheIssuer(t *testing.T) {
	provider, public := newParkedSigner(t, "kms-from-policy")

	key, err := auth.NewProviderSigningKey(t.Context(), provider, public)
	require.NoError(t, err)

	broker, err := auth.FederationPolicy{
		Issuer:         "https://flowstate.example",
		SigningTimeout: 50 * time.Millisecond,
	}.Broker(key)
	require.NoError(t, err)

	provider.arm()

	identity := testIdentity()
	identity.Claims = nil // the policy declares none

	// A context that never expires, so the only bound in play is the policy's.
	_, err = broker.Issuer().Mint(context.Background(), identity, testStepRef(), "https://as.example.com")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Contains(t, err.Error(), "did not finish within 50ms",
		"the policy's timeout is the one applied, not the default")
}
