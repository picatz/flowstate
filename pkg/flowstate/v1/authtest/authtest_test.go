package authtest_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/picatz/jose/pkg/jwa"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
)

// referenceTime is the instant these tests start their clocks at, so that what
// they assert does not depend on when they run.
var referenceTime = time.Date(2026, time.July, 25, 12, 0, 0, 0, time.UTC)

// newIssuer starts an issuer that is closed when the test ends.
func newIssuer(t *testing.T, options ...authtest.IssuerOption) *authtest.Issuer {
	t.Helper()

	issuer := authtest.NewIssuer(options...)
	t.Cleanup(func() { _ = issuer.Close() })

	return issuer
}

// verifierFor returns a verifier trusting one issuer for one audience.
func verifierFor(t *testing.T, issuer *authtest.Issuer, clock *authtest.Clock, entry auth.TrustedIssuer) *auth.OIDCVerifier {
	t.Helper()

	if entry.Name == "" {
		entry.Name = "test"
	}
	if entry.Issuer == "" {
		entry.Issuer = issuer.URL()
	}
	if entry.Audiences == nil {
		entry.Audiences = []string{"flowstate"}
	}

	verifier, err := auth.NewOIDCVerifier(
		auth.Policy{Issuers: []auth.TrustedIssuer{entry}},
		auth.WithClock(clock.Now),
	)
	require.NoError(t, err)

	return verifier
}

// TestIssuerMintsVerifiableTokens is the base case, and the one everything else
// here is a variation of: a token this issuer mints verifies against a policy
// naming this issuer, and carries the claims it was given.
func TestIssuerMintsVerifiableTokens(t *testing.T) {
	t.Parallel()

	clock := authtest.NewClock(referenceTime)
	issuer := newIssuer(t, authtest.WithClock(clock.Now))

	verifier := verifierFor(t, issuer, clock, auth.TrustedIssuer{Role: "operator"})

	token := issuer.MintToken(
		map[string]any{"team": "platform"},
		authtest.WithSubject("runner"),
		authtest.WithAudience("flowstate"),
	)

	principal, err := verifier.Verify(t.Context(), token)
	require.NoError(t, err)

	assert.Equal(t, issuer.URL(), principal.Issuer)
	assert.Equal(t, "runner", principal.Subject)
	assert.Equal(t, "operator", principal.Role)
	assert.Equal(t, referenceTime.Unix(), principal.IssuedAt.Unix())
	assert.Equal(t, referenceTime.Add(authtest.DefaultLifetime).Unix(), principal.ExpiresAt.Unix())

	team, ok := principal.StringClaim("team")
	require.True(t, ok)
	assert.Equal(t, "platform", team)
}

// TestMintTokenRequiresAnAudience is the fail-closed rule this package cares
// most about, because a token addressed to nobody is what makes a test pass
// against a policy that is not checking.
func TestMintTokenRequiresAnAudience(t *testing.T) {
	t.Parallel()

	issuer := newIssuer(t)

	assert.Panics(t, func() {
		issuer.MintToken(nil)
	}, "minting with no audience must not be something a test can do by accident")

	assert.Panics(t, func() {
		issuer.MintToken(nil, authtest.Without("aud"))
	}, "there is one spelling for an audience-less token, and it is WithoutAudience")

	assert.NotPanics(t, func() {
		issuer.MintToken(nil, authtest.WithoutAudience())
	})

	// A claims map that already carries an audience has decided.
	assert.NotPanics(t, func() {
		issuer.MintToken(map[string]any{"aud": "flowstate"})
	})
}

// TestMintTokenClaimsWin checks that a claim the caller wrote is minted exactly
// as written. Everything a test proves about a malformed token depends on this
// double not tidying claims up.
func TestMintTokenClaimsWin(t *testing.T) {
	t.Parallel()

	clock := authtest.NewClock(referenceTime)
	issuer := newIssuer(t, authtest.WithClock(clock.Now))

	token := issuer.MintToken(map[string]any{
		"iss": "https://issuer.invalid",
		"sub": 1234,
		"exp": "not a timestamp",
	}, authtest.WithSubject("ignored"), authtest.WithAudience("flowstate"))

	claims := decodeClaims(t, token)
	assert.Equal(t, "https://issuer.invalid", claims["iss"])
	assert.Equal(t, float64(1234), claims["sub"])
	assert.Equal(t, "not a timestamp", claims["exp"])

	// And the map the caller passed is not written into.
	original := map[string]any{"team": "platform"}
	issuer.MintToken(original, authtest.WithAudience("flowstate"))
	assert.Equal(t, map[string]any{"team": "platform"}, original)
}

// TestMintTokenDeliberateInvalidity checks the shapes a policy has to refuse.
// Each is a one-line option here, which is the point: the negative half of a
// trust policy should be no harder to write than the positive half.
func TestMintTokenDeliberateInvalidity(t *testing.T) {
	t.Parallel()

	clock := authtest.NewClock(referenceTime)
	issuer := newIssuer(t, authtest.WithClock(clock.Now))
	verifier := verifierFor(t, issuer, clock, auth.TrustedIssuer{})

	tests := []struct {
		name    string
		options []authtest.TokenOption
		wantErr error
	}{
		{
			name:    "addressed to another service",
			options: []authtest.TokenOption{authtest.WithAudience("some-other-service")},
			wantErr: auth.ErrInvalidAudience,
		},
		{
			name:    "addressed to nobody",
			options: []authtest.TokenOption{authtest.WithoutAudience()},
			wantErr: auth.ErrMissingClaim,
		},
		{
			name:    "already expired",
			options: []authtest.TokenOption{authtest.WithAudience("flowstate"), authtest.Expired()},
			wantErr: auth.ErrTokenExpired,
		},
		{
			name:    "naming a key the issuer does not publish",
			options: []authtest.TokenOption{authtest.WithAudience("flowstate"), authtest.WithKeyID("invented")},
			wantErr: auth.ErrUnknownKey,
		},
		{
			name: "signed by a key the issuer does not publish",
			options: []authtest.TokenOption{
				authtest.WithAudience("flowstate"),
				authtest.SignedBy(authtest.GenerateKey("stolen", jwa.ES256)),
			},
			wantErr: auth.ErrUnknownKey,
		},
		{
			name: "lying about how it was signed",
			options: []authtest.TokenOption{
				authtest.WithAudience("flowstate"),
				authtest.WithAlgorithm(jwa.RS256),
			},
			wantErr: auth.ErrDisallowedAlgorithm,
		},
		{
			name:    "missing a claim the verifier requires",
			options: []authtest.TokenOption{authtest.WithAudience("flowstate"), authtest.Without("iat")},
			wantErr: auth.ErrMissingClaim,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := verifier.Verify(t.Context(), issuer.MintToken(nil, test.options...))
			require.ErrorIs(t, err, test.wantErr)
		})
	}
}

// TestIssuerPathSegment covers the provider whose issuer identifier carries a
// path segment. Discovery lives below the whole identifier, so a relying party
// that appends the well known path to the host cannot reach it at all.
func TestIssuerPathSegment(t *testing.T) {
	t.Parallel()

	clock := authtest.NewClock(referenceTime)
	issuer := newIssuer(t,
		authtest.WithClock(clock.Now),
		authtest.WithIssuerPath("/enterprise"),
		authtest.WithJWKSPath("/.well-known/jwks"),
	)

	require.Contains(t, issuer.URL(), "/enterprise")
	require.Equal(t, issuer.URL()+"/.well-known/openid-configuration", issuer.DiscoveryURL())
	require.NotContains(t, issuer.JWKSURL(), "/enterprise")

	verifier := verifierFor(t, issuer, clock, auth.TrustedIssuer{})

	principal, err := verifier.Verify(t.Context(),
		issuer.MintToken(nil, authtest.WithSubject("runner"), authtest.WithAudience("flowstate")))
	require.NoError(t, err)
	assert.Equal(t, issuer.URL(), principal.Issuer)

	// The document is served below the identifier, which is the part a relying
	// party appending the well known path to the host would miss.
	assert.Equal(t, http.StatusOK, statusOf(t, issuer.DiscoveryURL()))
	assert.Equal(t, http.StatusNotFound, statusOf(t, issuer.URL()[:len(issuer.URL())-len("/enterprise")]+authtest.DiscoveryPath))
}

// TestIssuerSignsEveryAlgorithm checks that a key of each algorithm this
// package generates can actually verify a token. A double that mints tokens
// nothing accepts would fail every test it was used in, for reasons in the
// double rather than in what was being tested.
func TestIssuerSignsEveryAlgorithm(t *testing.T) {
	t.Parallel()

	// ES384 is absent because the auth package deliberately does not admit it,
	// so a token signed with one cannot be verified here. GenerateKey still
	// makes such a key, for a test that is about it being refused.
	for _, algorithm := range []jwa.Algorithm{
		jwa.RS256, jwa.RS384, jwa.RS512,
		jwa.PS256, jwa.PS384, jwa.PS512,
		jwa.ES256, jwa.ES512,
		jwa.EdDSA,
	} {
		t.Run(algorithm, func(t *testing.T) {
			t.Parallel()

			clock := authtest.NewClock(referenceTime)
			key := authtest.GenerateKey("signing", algorithm)
			issuer := newIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))

			verifier := verifierFor(t, issuer, clock, auth.TrustedIssuer{
				Algorithms: []jwa.Algorithm{algorithm},
			})

			principal, err := verifier.Verify(t.Context(),
				issuer.MintToken(nil, authtest.WithSubject("runner"), authtest.WithAudience("flowstate")))
			require.NoError(t, err, "a %s token minted here must verify", algorithm)
			assert.Equal(t, "runner", principal.Subject)

			// A tampered signature must fail, so that the case above cannot be
			// a signature check that never happened.
			//
			// The first character of the signature is altered rather than the
			// last, because the trailing base64url character carries padding
			// bits that decode to nothing: changing it can leave the signature
			// bytes identical.
			token := issuer.MintToken(nil, authtest.WithAudience("flowstate"))
			dot := strings.LastIndex(token, ".")
			replacement := "A"
			if strings.HasPrefix(token[dot+1:], "A") {
				replacement = "B"
			}

			_, err = verifier.Verify(t.Context(), token[:dot+1]+replacement+token[dot+2:])
			assert.Error(t, err, "a tampered %s signature must not verify", algorithm)
		})
	}
}

// TestGenerateKeyRefusesUnsupportedAlgorithms checks that the symmetric
// algorithms and "none" cannot be reached through this package. No issuer's key
// set legitimately contains one, so a key here that could sign with one would
// only ever be used to fake an issuer doing something it cannot do.
func TestGenerateKeyRefusesUnsupportedAlgorithms(t *testing.T) {
	t.Parallel()

	for _, algorithm := range []jwa.Algorithm{jwa.HS256, jwa.None, "ES256K", ""} {
		assert.Panics(t, func() { authtest.GenerateKey("key", algorithm) }, "algorithm %q", algorithm)
	}
}

// TestGenerateKeyReturnsDistinctKeys checks the property a negative test
// depends on: two keys are two keys. A double that shared one underneath would
// make "signed by a key the issuer does not publish" pass by accident.
func TestGenerateKeyReturnsDistinctKeys(t *testing.T) {
	t.Parallel()

	clock := authtest.NewClock(referenceTime)
	published := authtest.GenerateKey("published", jwa.RS256)
	unpublished := authtest.GenerateKey("published", jwa.RS256) // the same id, a different key

	issuer := newIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(published))
	verifier := verifierFor(t, issuer, clock, auth.TrustedIssuer{})

	_, err := verifier.Verify(t.Context(),
		issuer.MintToken(nil, authtest.WithAudience("flowstate"), authtest.SignedBy(unpublished)))
	require.ErrorIs(t, err, auth.ErrInvalidSignature)
}

// TestIssuerRotation checks that replacing the published keys is visible to a
// relying party, and that a request count shows when it went back for them.
func TestIssuerRotation(t *testing.T) {
	t.Parallel()

	var (
		clock  = authtest.NewClock(referenceTime)
		oldKey = authtest.GenerateKey("old", jwa.ES256)
		newKey = authtest.GenerateKey("new", jwa.ES256)
		issuer = newIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(oldKey))
	)

	verifier := verifierFor(t, issuer, clock, auth.TrustedIssuer{})

	_, err := verifier.Verify(t.Context(), issuer.MintToken(nil, authtest.WithAudience("flowstate")))
	require.NoError(t, err)

	_, err = verifier.Verify(t.Context(), issuer.MintToken(nil, authtest.WithAudience("flowstate")))
	require.NoError(t, err)

	require.Equal(t, 1, issuer.Requests().JWKS, "the key set should have been fetched once and cached")

	issuer.SetKeys(oldKey, newKey)
	clock.Advance(2 * time.Minute)

	_, err = verifier.Verify(t.Context(),
		issuer.MintToken(nil, authtest.WithAudience("flowstate"), authtest.SignedBy(newKey)))
	require.NoError(t, err, "a rotated-in key should be picked up")
	require.Equal(t, 2, issuer.Requests().JWKS)
}

// TestIssuerMisbehaviour checks the ways an issuer can fail a relying party.
// Each of these is a deployment's problem to refuse rather than to work around.
func TestIssuerMisbehaviour(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		sabotage func(t *testing.T, issuer *authtest.Issuer)
	}{
		{
			name: "the key set endpoint is down",
			sabotage: func(_ *testing.T, issuer *authtest.Issuer) {
				issuer.SetKeySetResponse(http.StatusInternalServerError, nil)
			},
		},
		{
			name: "the key set is not a key set",
			sabotage: func(_ *testing.T, issuer *authtest.Issuer) {
				issuer.SetKeySetResponse(http.StatusOK, []byte("<html>not a key set</html>"))
			},
		},
		{
			name: "the discovery document claims to be someone else",
			sabotage: func(_ *testing.T, issuer *authtest.Issuer) {
				issuer.SetDiscoveredIssuer("https://issuer.invalid")
			},
		},
		{
			name: "the discovery document is missing",
			sabotage: func(_ *testing.T, issuer *authtest.Issuer) {
				issuer.SetDiscoveryHandler(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusNotFound)
				})
			},
		},
		{
			name: "the key set is redirected to an unprotected host",
			sabotage: func(_ *testing.T, issuer *authtest.Issuer) {
				issuer.RedirectKeySet("http://keys.example.invalid/jwks")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			clock := authtest.NewClock(referenceTime)
			issuer := newIssuer(t, authtest.WithClock(clock.Now))
			verifier := verifierFor(t, issuer, clock, auth.TrustedIssuer{})

			token := issuer.MintToken(nil, authtest.WithAudience("flowstate"))
			test.sabotage(t, issuer)

			_, err := verifier.Verify(t.Context(), token)
			require.ErrorIs(t, err, auth.ErrIssuerUnavailable)
		})
	}
}

// TestIssuerRecovers checks the other direction of the same knob: an issuer put
// back the way it was serves keys again, so a test can show a deployment
// recovering rather than only failing.
func TestIssuerRecovers(t *testing.T) {
	t.Parallel()

	clock := authtest.NewClock(referenceTime)
	issuer := newIssuer(t, authtest.WithClock(clock.Now))
	verifier := verifierFor(t, issuer, clock, auth.TrustedIssuer{})

	issuer.SetKeySetResponse(http.StatusServiceUnavailable, nil)

	token := issuer.MintToken(nil, authtest.WithSubject("runner"), authtest.WithAudience("flowstate"))

	_, err := verifier.Verify(t.Context(), token)
	require.ErrorIs(t, err, auth.ErrIssuerUnavailable)

	issuer.SetKeySetResponse(http.StatusOK, nil)

	// Past the interval a relying party waits before asking again, which is a
	// property of the deployment rather than of the issuer.
	clock.Advance(2 * time.Minute)

	principal, err := verifier.Verify(t.Context(), token)
	require.NoError(t, err)
	assert.Equal(t, "runner", principal.Subject)
}

// TestKeyJWKPublication checks what a key set says about a key, since a relying
// party is entitled to believe it.
func TestKeyJWKPublication(t *testing.T) {
	t.Parallel()

	plain := authtest.GenerateKey("ec", jwa.ES256).JWK()
	assert.Equal(t, "ec", plain["kid"])
	assert.NotContains(t, plain, "alg", "an algorithm is published only when asked for")
	assert.NotContains(t, plain, "use", "a use is published only when asked for")

	declared := authtest.GenerateKey("ec", jwa.ES256,
		authtest.PublishAlgorithm(jwa.ES512),
		authtest.PublishUse("sig"),
	).JWK()
	assert.Equal(t, jwa.ES512, declared["alg"])
	assert.Equal(t, "sig", declared["use"])

	anonymous := authtest.GenerateKey("", jwa.ES256).JWK()
	assert.NotContains(t, anonymous, "kid", "a key with no id is published without one")

	// The map is the caller's to spoil.
	plain["kid"] = "altered"
	assert.Equal(t, "ec", authtest.GenerateKey("ec", jwa.ES256).JWK()["kid"])
}

// TestKeySignRawHeader checks the escape hatch: a header exactly as written,
// with nothing filled in, which is what a token that a relying party must
// refuse on its header alone is built from.
func TestKeySignRawHeader(t *testing.T) {
	t.Parallel()

	clock := authtest.NewClock(referenceTime)
	key := authtest.GenerateKey("ec", jwa.ES256)
	issuer := newIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))
	verifier := verifierFor(t, issuer, clock, auth.TrustedIssuer{})

	claims := map[string]any{
		"iss": issuer.URL(),
		"sub": "runner",
		"aud": "flowstate",
		"iat": referenceTime.Unix(),
		"exp": referenceTime.Add(time.Hour).Unix(),
	}

	// A key id that is not a string cannot name a key, and must not be read as
	// if it did.
	_, err := verifier.Verify(t.Context(), key.Sign(map[string]any{
		"typ": "JWT",
		"alg": jwa.ES256,
		"kid": 42,
	}, claims))
	require.ErrorIs(t, err, auth.ErrMalformedToken)

	// The same header, spelled correctly, verifies. Otherwise the case above
	// would prove nothing about the key id in particular.
	principal, err := verifier.Verify(t.Context(), key.Sign(map[string]any{
		"typ": "JWT",
		"alg": jwa.ES256,
		"kid": "ec",
	}, claims))
	require.NoError(t, err)
	assert.Equal(t, "runner", principal.Subject)
}

// TestClockMovesByHand checks that the clock an issuer mints against is the one
// a test holds, which is what makes a token's age a decision rather than a wait.
func TestClockMovesByHand(t *testing.T) {
	t.Parallel()

	clock := authtest.NewClock(referenceTime)
	issuer := newIssuer(t, authtest.WithClock(clock.Now))
	verifier := verifierFor(t, issuer, clock, auth.TrustedIssuer{MaxTokenAge: 10 * time.Minute})

	token := issuer.MintToken(nil, authtest.WithAudience("flowstate"))

	_, err := verifier.Verify(t.Context(), token)
	require.NoError(t, err)

	clock.Advance(11 * time.Minute)

	_, err = verifier.Verify(t.Context(), token)
	require.ErrorIs(t, err, auth.ErrTokenExpired, "the token should have aged past what the issuer entry allows")
}

// statusOf returns the status an issuer answers a GET with.
func statusOf(t *testing.T, url string) int {
	t.Helper()

	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)

	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())

	return response.StatusCode
}

// decodeClaims returns a token's claims, without verifying it.
func decodeClaims(t *testing.T, token string) map[string]any {
	t.Helper()

	segments := strings.Split(token, ".")
	require.Len(t, segments, 3)

	decoded, err := base64.RawURLEncoding.DecodeString(segments[1])
	require.NoError(t, err)

	var claims map[string]any
	require.NoError(t, json.Unmarshal(decoded, &claims))

	return claims
}

// TestWithAudienceRefusesEmptiness pins the closing of the disguised audience
// hole: a computed slice expanding to nothing, or carrying an empty string,
// must fail at the option rather than mint `aud: []` or `aud: ""` past the
// fail-closed contract.
func TestWithAudienceRefusesEmptiness(t *testing.T) {
	t.Parallel()

	assert.Panics(t, func() {
		var none []string
		authtest.WithAudience(none...)
	}, "an empty expansion counted as the audience having been named")

	assert.Panics(t, func() {
		authtest.WithAudience("")
	}, "an empty audience string minted a token no verifier should accept")

	assert.Panics(t, func() {
		authtest.WithAudience("flowstate", "")
	}, "one empty member hid behind a real one")
}
