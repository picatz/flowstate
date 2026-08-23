package auth_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/picatz/jose/pkg/header"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/picatz/jose/pkg/jwk"
	"github.com/picatz/jose/pkg/jwt"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// providerContextKey is what [TestProviderSigningKeyIsGivenTheMintRequestContext]
// puts in the context an issuer mints under, so the signer can report whether
// the context it was handed is that one or an unrelated background context.
type providerContextKey struct{}

// fakeProvider stands in for a KMS or an HSM: it holds a private key this
// process is pretending not to have, and signs claims it is sent.
//
// secret is material the boundary exists to contain. It is unexported and
// reachable only through the receiver captured by the Sign method value, which
// is exactly the containment shape [TestProviderSigningKeyDoesNotLeakThroughContainingStructs]
// exercises.
type fakeProvider struct {
	id     string
	kid    string // what Sign actually stamps, when it differs from id
	noKid  bool   // stamp no "kid" header at all
	alg    jwa.Algorithm
	key    *ecdsa.PrivateKey
	secret string

	// sawContextValue records whether the last Sign call was given the context
	// the caller minted under, rather than one manufactured on the way down.
	sawContextValue bool
}

func (p *fakeProvider) KeyID() string { return p.id }

func (p *fakeProvider) Algorithm() jwa.Algorithm { return p.alg }

func (p *fakeProvider) Sign(ctx context.Context, claims jwt.ClaimsSet) (string, error) {
	if ctx == nil {
		return "", fmt.Errorf("provider %q was given a nil context", p.id)
	}
	if _, ok := ctx.Value(providerContextKey{}).(string); ok {
		p.sawContextValue = true
	}

	params := header.Parameters{
		header.Type:      jwt.Type,
		header.Algorithm: jwa.ES256,
	}
	if !p.noKid {
		kid := p.kid
		if kid == "" {
			kid = p.id
		}
		params[header.KeyID] = kid
	}

	token, err := jwt.New(params, claims, p.key)
	if err != nil {
		return "", err
	}
	return token.String(), nil
}

// newFakeProvider returns a provider signing for a freshly generated P-256 key,
// along with that key's public half.
func newFakeProvider(t *testing.T, id string) (*fakeProvider, *ecdsa.PublicKey) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	return &fakeProvider{id: id, alg: jwa.ES256, key: key, secret: providerSecret}, &key.PublicKey
}

// providerSecret is material a provider holds that must never be rendered.
const providerSecret = "SUPERSECRET-PROVIDER-MATERIAL"

// TestProviderSigningKeyMintsAssertionsTheIssuerPublishesTheKeyFor is the whole
// point of the seam: an issuer configured with a key it cannot itself sign for
// mints assertions a relying party can verify from the published key set.
func TestProviderSigningKeyMintsAssertionsTheIssuerPublishesTheKeyFor(t *testing.T) {
	provider, public := newFakeProvider(t, "kms-2026-08")

	key, err := auth.NewProviderSigningKey(t.Context(), provider, public)
	require.NoError(t, err)
	require.Equal(t, "kms-2026-08", key.ID())
	require.Equal(t, jwa.ES256, key.Algorithm())

	issuer, err := auth.NewIssuer("https://flowstate.example", key,
		auth.WithDeclaredClaims("repository"))
	require.NoError(t, err)

	assertion, err := issuer.Mint(t.Context(), testIdentity(), testStepRef(), "https://as.example.com")
	require.NoError(t, err)

	token, err := jwt.Parse(assertion.Token())
	require.NoError(t, err)

	kid, err := token.Header.Get(header.KeyID)
	require.NoError(t, err)
	require.Equal(t, "kms-2026-08", kid,
		"the assertion has to name the key the issuer publishes, or no relying party can select it")

	require.NoError(t, token.VerifySignature([]jwa.Algorithm{jwa.ES256}, map[string]any{"kms-2026-08": public}),
		"an assertion minted through the provider must verify against the published public half")

	var published []string
	for _, entry := range issuer.KeySet().Keys {
		id, _ := entry[jwk.KeyID].(string)
		published = append(published, id)
	}
	require.Contains(t, published, "kms-2026-08")
}

// TestProviderSigningKeyIsGivenTheMintRequestContext is the context threading,
// asserted from the far end: the context a caller mints under is the one the
// remote signer is handed, so a remote signer inherits that request's deadline
// and cancellation rather than running unbounded.
func TestProviderSigningKeyIsGivenTheMintRequestContext(t *testing.T) {
	provider, public := newFakeProvider(t, "kms-context")

	key, err := auth.NewProviderSigningKey(t.Context(), provider, public)
	require.NoError(t, err)

	issuer, err := auth.NewIssuer("https://flowstate.example", key,
		auth.WithDeclaredClaims("repository"))
	require.NoError(t, err)

	provider.sawContextValue = false

	ctx := context.WithValue(t.Context(), providerContextKey{}, "from the mint request")
	_, err = issuer.Mint(ctx, testIdentity(), testStepRef(), "https://as.example.com")
	require.NoError(t, err)

	require.True(t, provider.sawContextValue,
		"the signer was handed a context that is not the one the mint request carried")
}

// TestNewProviderSigningKeyRefusesAnAlgorithmMismatch is the cheap half of the
// pairing check: a signer that says it produces one algorithm, handed a public
// key used with another, is a misconfiguration whichever half is wrong.
func TestNewProviderSigningKeyRefusesAnAlgorithmMismatch(t *testing.T) {
	provider, public := newFakeProvider(t, "kms-mismatch")
	provider.alg = jwa.RS256

	_, err := auth.NewProviderSigningKey(t.Context(), provider, public)
	require.ErrorIs(t, err, auth.ErrInvalidPolicy)
	require.ErrorContains(t, err, "RS256")
	require.ErrorContains(t, err, "ES256")

	// And the other way around, so the refusal is not an artefact of which side
	// happens to be named first.
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	provider.alg = jwa.ES256
	_, err = auth.NewProviderSigningKey(t.Context(), provider, &rsaKey.PublicKey)
	require.ErrorIs(t, err, auth.ErrInvalidPolicy)
}

// TestNewProviderSigningKeyRefusesAPublicKeyTheSignerDoesNotHold is the finding
// the algorithm comparison cannot make: two P-256 keys are unrelated and look
// identical to it. Accepting this pairing gives an issuer that mints
// well-formed assertions every relying party rejects, with nothing in this
// process able to tell.
func TestNewProviderSigningKeyRefusesAPublicKeyTheSignerDoesNotHold(t *testing.T) {
	provider, _ := newFakeProvider(t, "kms-a")
	_, otherPublic := newFakeProvider(t, "kms-b")

	_, err := auth.NewProviderSigningKey(t.Context(), provider, otherPublic)
	require.ErrorIs(t, err, auth.ErrInvalidPolicy)
	require.ErrorContains(t, err, "does not verify what that signer signs")
}

// TestNewProviderSigningKeyRefusesAKidTheSignerDoesNotStamp covers the other
// way the published key and the signed assertion can fail to meet: the
// signature is right and the header does not name the key the set carries.
//
// The kid-less case is the one that would otherwise get through. A signature
// verifies fine against a single key with no "kid" to select it, and
// [keySet.lookupLocked] accepts such a token for exactly as long as the issuer
// publishes one key for the algorithm — so a kid-less signer works in testing
// and stops working the first time an operator publishes a second key, which is
// what every rotation does.
func TestNewProviderSigningKeyRefusesAKidTheSignerDoesNotStamp(t *testing.T) {
	t.Run("a different kid", func(t *testing.T) {
		provider, public := newFakeProvider(t, "kms-declared")
		provider.kid = "kms-actually-stamped"

		_, err := auth.NewProviderSigningKey(t.Context(), provider, public)
		require.ErrorIs(t, err, auth.ErrInvalidPolicy)
		require.ErrorContains(t, err, "stamps kid")
		require.ErrorContains(t, err, "kms-actually-stamped")
	})

	t.Run("no kid at all", func(t *testing.T) {
		provider, public := newFakeProvider(t, "kms-anonymous")
		provider.noKid = true

		_, err := auth.NewProviderSigningKey(t.Context(), provider, public)
		require.ErrorIs(t, err, auth.ErrInvalidPolicy)
		require.ErrorContains(t, err, "stamps no")
	})
}

// TestNewProviderSigningKeyRefusesAnUnusableKeyID holds the provider path to
// the same two rules [auth.NewSigningKey] applies to an id an operator chose.
func TestNewProviderSigningKeyRefusesAnUnusableKeyID(t *testing.T) {
	for name, id := range map[string]string{
		"empty":      "",
		"space":      "kms 2026-08",
		"tab":        "kms\t2026-08",
		"newline":    "kms\n2026-08",
		"carriage":   "kms\r2026-08",
		"only space": " ",
	} {
		t.Run(name, func(t *testing.T) {
			provider, public := newFakeProvider(t, id)

			_, err := auth.NewProviderSigningKey(t.Context(), provider, public)
			require.ErrorIs(t, err, auth.ErrInvalidPolicy)
		})
	}
}

// TestNewProviderSigningKeyRefusesANilSigner keeps the zero configuration from
// producing a key that would fail at the first mint instead of at start-up.
func TestNewProviderSigningKeyRefusesANilSigner(t *testing.T) {
	_, err := auth.NewProviderSigningKey(t.Context(), nil, nil)
	require.ErrorIs(t, err, auth.ErrNoSigningKey)
}

// TestNewProviderSigningKeyRefusesASignerThatCannotSign covers the remote half
// failing: a KMS that denies the call, is unreachable, or answers with
// something that is not a JWS is a deployment that will not work, and it says
// so when the configuration loads rather than at the first mint.
func TestNewProviderSigningKeyRefusesASignerThatCannotSign(t *testing.T) {
	provider, public := newFakeProvider(t, "kms-broken")

	_, err := auth.NewProviderSigningKey(t.Context(), brokenSigner{provider}, public)
	require.ErrorIs(t, err, auth.ErrInvalidPolicy)
	require.ErrorContains(t, err, "proof of possession")

	_, err = auth.NewProviderSigningKey(t.Context(), garbageSigner{provider}, public)
	require.ErrorIs(t, err, auth.ErrInvalidPolicy)
	require.ErrorContains(t, err, "compact JWS")
}

type brokenSigner struct{ *fakeProvider }

func (brokenSigner) Sign(context.Context, jwt.ClaimsSet) (string, error) {
	return "", fmt.Errorf("kms unavailable")
}

type garbageSigner struct{ *fakeProvider }

func (garbageSigner) Sign(context.Context, jwt.ClaimsSet) (string, error) {
	return "not a token", nil
}

// valueSigner is a [auth.Signer] with value receivers, holding its material in
// plain fields.
//
// The leak test below needs this shape rather than the pointer one. fmt prints
// the fields of a struct value it reaches and stops at the address of a
// pointer, so a test that only ever hands over a pointer implementation reports
// containment whether or not the key contains anything — it would pass over a
// SigningKey that held the signer in a plain field, which is the exact defect
// the test exists to refuse.
type valueSigner struct {
	id     string
	secret string
	key    ecdsa.PrivateKey

	// raw is the private scalar in a shape fmt will actually render. The
	// ecdsa.PrivateKey above holds it behind a pointer, which prints as an
	// address whether or not the key is contained, so it cannot tell the two
	// apart on its own.
	raw []byte
}

func (s valueSigner) KeyID() string { return s.id }

func (s valueSigner) Algorithm() jwa.Algorithm { return jwa.ES256 }

func (s valueSigner) Sign(_ context.Context, claims jwt.ClaimsSet) (string, error) {
	token, err := jwt.New(header.Parameters{
		header.Type:      jwt.Type,
		header.Algorithm: jwa.ES256,
		header.KeyID:     s.id,
	}, claims, &s.key)
	if err != nil {
		return "", err
	}
	return token.String(), nil
}

// providerHolder holds a provider-backed key in an unexported field, which is
// the containment shape a String method cannot defend — see leak_test.go for
// why this whole class of test exists.
type providerHolder struct {
	key auth.SigningKey
}

type nestedProviderHolder struct {
	inner providerHolder
}

type exportedProviderHolder struct {
	Key auth.SigningKey
}

// TestProviderSigningKeyDoesNotLeakThroughContainingStructs is leak_test.go's
// claim carried to the key shape this file introduces.
//
// The two shapes differ in a way worth checking rather than assuming.
// [auth.NewSigningKey] stores a closure that captured the private key;
// [auth.NewProviderSigningKey] stores signer.Sign, a *method value*, whose
// receiver is an interface holding whatever the implementer put there — here a
// private key and a marker string. If reflection could reach the bound receiver
// of a method value the way it reaches a struct field, every implementer's
// material would print through any struct that happened to hold the key.
func TestProviderSigningKeyDoesNotLeakThroughContainingStructs(t *testing.T) {
	private, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	raw, err := private.Bytes()
	require.NoError(t, err)

	provider := valueSigner{id: "kms-leak", secret: providerSecret, key: *private, raw: raw}

	key, err := auth.NewProviderSigningKey(t.Context(), provider, &private.PublicKey)
	require.NoError(t, err)

	holder := providerHolder{key: key}

	renderings := map[string]func() string{
		// Directly, where the String and Format methods do apply.
		"key %v":  func() string { return fmt.Sprintf("%v", key) },
		"key %+v": func() string { return fmt.Sprintf("%+v", key) },
		"key %#v": func() string { return fmt.Sprintf("%#v", key) },
		"key %s":  func() string { return fmt.Sprintf("%s", key) },

		// Through an unexported field, where fmt cannot call String and prints
		// the fields it can see instead. %s on a struct with no String method
		// is a vet error, so that vector is caught at build time; %v is not,
		// which is what makes it the dangerous one.
		"holder %v":  func() string { return fmt.Sprintf("%v", holder) },
		"holder %+v": func() string { return fmt.Sprintf("%+v", holder) },
		"holder %#v": func() string { return fmt.Sprintf("%#v", holder) },

		// And through more than one level of containment.
		"nested %v":  func() string { return fmt.Sprintf("%v", nestedProviderHolder{inner: holder}) },
		"nested %+v": func() string { return fmt.Sprintf("%+v", nestedProviderHolder{inner: holder}) },
		"nested %#v": func() string { return fmt.Sprintf("%#v", nestedProviderHolder{inner: holder}) },

		"pointer to holder %v":  func() string { return fmt.Sprintf("%v", &holder) },
		"pointer to holder %+v": func() string { return fmt.Sprintf("%+v", &holder) },

		"slice of holders %v": func() string { return fmt.Sprintf("%v", []providerHolder{holder}) },
		"slice of holders %+v": func() string {
			return fmt.Sprintf("%+v", []providerHolder{holder})
		},
		"slice of holders %#v": func() string {
			return fmt.Sprintf("%#v", []providerHolder{holder})
		},
		"slice of keys %v":  func() string { return fmt.Sprintf("%v", []auth.SigningKey{key}) },
		"slice of keys %+v": func() string { return fmt.Sprintf("%+v", []auth.SigningKey{key}) },
		"slice of keys %#v": func() string { return fmt.Sprintf("%#v", []auth.SigningKey{key}) },
		"map of holders %v": func() string { return fmt.Sprintf("%v", map[string]providerHolder{"a": holder}) },
		"array of holders %v": func() string {
			return fmt.Sprintf("%v", [1]providerHolder{holder})
		},

		// Exported fields, where a String method does apply.
		"exported %v":  func() string { return fmt.Sprintf("%v", exportedProviderHolder{Key: key}) },
		"exported %+v": func() string { return fmt.Sprintf("%+v", exportedProviderHolder{Key: key}) },
		"exported %#v": func() string { return fmt.Sprintf("%#v", exportedProviderHolder{Key: key}) },
	}

	for name, render := range renderings {
		t.Run(name, func(t *testing.T) {
			rendered := render()
			require.NotContains(t, rendered, providerSecret,
				"%s leaked the provider's material:\n%s", name, rendered)
			require.NotContains(t, rendered, fmt.Sprintf("%v", raw),
				"%s leaked the provider's private scalar:\n%s", name, rendered)
		})
	}
}

// oversizedSigner answers correctly until the call after padAfter, and then
// returns a token of whatever size it likes: a provider that regressed, or was
// compromised, some time after the deployment loaded.
type oversizedSigner struct {
	*fakeProvider

	padAfter int

	mu    sync.Mutex
	calls int
}

func (p *oversizedSigner) Sign(ctx context.Context, claims jwt.ClaimsSet) (string, error) {
	raw, err := p.fakeProvider.Sign(ctx, claims)
	if err != nil {
		return "", err
	}

	p.mu.Lock()
	p.calls++
	pad := p.calls > p.padAfter
	p.mu.Unlock()

	if pad {
		raw += strings.Repeat("A", 64<<10)
	}

	return raw, nil
}

// TestProviderSigningKeyBoundsEverySignature is a bound-*placement* test rather
// than a bound test.
//
// The size of a signer's answer is the far side's choice on every call, so a
// check written into the start-up proof bounds a provider that was well behaved
// when the configuration loaded and nothing afterwards — and afterwards is
// where a compromise or a regression lives. Both ends are asserted here for
// that reason: the first call, and a later one.
func TestProviderSigningKeyBoundsEverySignature(t *testing.T) {
	t.Run("at construction", func(t *testing.T) {
		provider, public := newFakeProvider(t, "kms-oversized-at-startup")

		_, err := auth.NewProviderSigningKey(t.Context(), &oversizedSigner{fakeProvider: provider}, public)
		require.ErrorIs(t, err, auth.ErrInvalidPolicy,
			"a provider that cannot be configured is a start-up error")
		require.ErrorIs(t, err, auth.ErrMalformedToken,
			"and the reason it cannot be configured has to survive into the error")
	})

	t.Run("at a later mint", func(t *testing.T) {
		provider, public := newFakeProvider(t, "kms-oversized-later")

		// One good signature — the proof of possession — and then not.
		key, err := auth.NewProviderSigningKey(t.Context(),
			&oversizedSigner{fakeProvider: provider, padAfter: 1}, public)
		require.NoError(t, err)

		issuer, err := auth.NewIssuer("https://flowstate.example", key,
			auth.WithDeclaredClaims("repository"))
		require.NoError(t, err)

		_, err = issuer.Mint(t.Context(), testIdentity(), testStepRef(), "https://as.example.com")
		require.ErrorIs(t, err, auth.ErrMalformedToken,
			"a bound that only covers the start-up proof is one a provider passes once and then ignores")
	})
}

// concurrentSigner refuses to answer until several of its calls are in flight
// at once, which is a claim about the caller rather than about itself: if
// anything between [auth.Issuer.Mint] and here serialized signatures, the
// second call would wait for the first and no call would ever be released.
type concurrentSigner struct {
	*fakeProvider

	want int

	mu       sync.Mutex
	armed    bool
	inFlight int
	together chan struct{}
}

func (p *concurrentSigner) arm() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.armed = true
}

func (p *concurrentSigner) Sign(ctx context.Context, claims jwt.ClaimsSet) (string, error) {
	p.mu.Lock()
	armed := p.armed
	if armed {
		p.inFlight++
		if p.inFlight == p.want {
			close(p.together)
		}
	}
	p.mu.Unlock()

	if armed {
		select {
		case <-p.together:
		case <-time.After(30 * time.Second):
			return "", fmt.Errorf("only saw fewer than %d signatures in flight at once", p.want)
		}
	}

	return p.fakeProvider.Sign(ctx, claims)
}

// TestProviderSigningKeyIsCalledConcurrently pins the contract [auth.Signer]'s
// doc comment states: mints run in parallel, so an implementation must be safe
// for concurrent use.
//
// It is worth asserting rather than asserting in prose alone. Mint holds the
// issuer's *read* lock across the signature, which is what allows the
// concurrency — a later change to a write lock, or any serialization added
// between Mint and the signer, would be invisible to every other test in this
// package and would quietly turn one KMS round trip into the whole process's
// rate limit. Here it deadlocks this test instead.
func TestProviderSigningKeyIsCalledConcurrently(t *testing.T) {
	const mints = 4

	base, public := newFakeProvider(t, "kms-concurrent")
	provider := &concurrentSigner{fakeProvider: base, want: mints, together: make(chan struct{})}

	// Armed only after the start-up proof, which is one call on its own and
	// would otherwise wait for companions that do not exist yet.
	key, err := auth.NewProviderSigningKey(t.Context(), provider, public)
	require.NoError(t, err)
	provider.arm()

	issuer, err := auth.NewIssuer("https://flowstate.example", key,
		auth.WithDeclaredClaims("repository"))
	require.NoError(t, err)

	errs := make(chan error, mints)
	for range mints {
		go func() {
			_, err := issuer.Mint(context.Background(), testIdentity(), testStepRef(), "https://as.example.com")
			errs <- err
		}()
	}

	for range mints {
		require.NoError(t, <-errs,
			"an issuer mints concurrently, so its signer is called concurrently; nothing here may serialize that")
	}
}
