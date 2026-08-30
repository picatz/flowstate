package auth_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/picatz/jose/pkg/header"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/picatz/jose/pkg/jwk"
	"github.com/picatz/jose/pkg/jwt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// referenceTime is the instant the test clock starts at, so that token lifetimes
// in these tests are unaffected by when they run.
var referenceTime = time.Date(2026, time.July, 25, 12, 0, 0, 0, time.UTC)

// newVerifier builds a verifier for a policy, failing the test if the policy is
// not usable.
func newVerifier(t *testing.T, policy auth.Policy, opts ...auth.Option) *auth.OIDCVerifier {
	t.Helper()

	verifier, err := auth.NewOIDCVerifier(policy, append([]auth.Option{auth.WithEgressPolicy(authtest.EgressPolicy())}, opts...)...)
	require.NoError(t, err)

	return verifier
}

// newVerifierWithClient builds a verifier whose caller supplies the HTTP client,
// which replaces the egress boundary rather than loosening it — so unlike
// [newVerifier] it names no egress policy, and combining the two is refused.
func newVerifierWithClient(t *testing.T, policy auth.Policy, opts ...auth.Option) *auth.OIDCVerifier {
	t.Helper()

	verifier, err := auth.NewOIDCVerifier(policy, opts...)
	require.NoError(t, err)

	return verifier
}

// TestOIDCVerifierRejects covers the failures that matter: everything a caller
// might present that is not a valid token minted for this deployment by a
// trusted issuer.
func TestOIDCVerifierRejects(t *testing.T) {
	var (
		key         = authtest.GenerateKey("primary", jwa.ES256)
		unpublished = authtest.GenerateKey("unpublished", jwa.ES256)
		clock       = authtest.NewClock(referenceTime)
		issuer      = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))
	)

	verifier := newVerifier(t,
		auth.Policy{
			Issuers: []auth.TrustedIssuer{{
				Name:      "test",
				Issuer:    issuer.URL(),
				Audiences: []string{"flowstate"},
				Role:      "operator",
			}},
		},
		auth.WithClock(clock.Now),
		// Refetch on every unrecognized key id, so that a rejection here is
		// about the token and not about the refresh rate limit.
		auth.WithMinKeyRefreshInterval(0),
	)

	// claims returns the claims of a valid token, for a case to spoil.
	claims := func() map[string]any {
		return issuer.Claims(authtest.WithSubject("workflow-runner"), authtest.WithAudience("flowstate"))
	}

	// mint returns a token of the same shape, missing whatever the options say
	// it is missing.
	mint := func(options ...authtest.TokenOption) string {
		return issuer.MintToken(nil, append([]authtest.TokenOption{
			authtest.WithSubject("workflow-runner"),
			authtest.WithAudience("flowstate"),
		}, options...)...)
	}

	tests := []struct {
		name    string
		token   func(t *testing.T) string
		wantErr error
	}{
		{
			name:    "no token at all",
			token:   func(*testing.T) string { return "" },
			wantErr: auth.ErrNoToken,
		},
		{
			name:    "not a JWT",
			token:   func(*testing.T) string { return "hunter2" },
			wantErr: auth.ErrMalformedToken,
		},
		{
			name: "only two segments",
			token: func(t *testing.T) string {
				return dropSignature(t, issuer.MintToken(claims()))
			},
			wantErr: auth.ErrMalformedToken,
		},
		{
			name: "signature is not base64url",
			token: func(t *testing.T) string {
				return issuer.MintToken(claims()) + "!!"
			},
			wantErr: auth.ErrMalformedToken,
		},
		{
			name: "unsigned token claiming alg none",
			token: func(t *testing.T) string {
				return noneToken(t, claims())
			},
			wantErr: auth.ErrDisallowedAlgorithm,
		},
		{
			name: "HMAC token signed with a shared secret",
			token: func(t *testing.T) string {
				return hmacToken(t, key.ID(), []byte("a-32-byte-long-secret-for-hmac!!"), claims())
			},
			wantErr: auth.ErrDisallowedAlgorithm,
		},
		{
			name: "algorithm confusion: HMAC token signed with the issuer's public key",
			token: func(t *testing.T) string {
				return hmacToken(t, key.ID(), publicKeyBytes(t, key.Public()), claims())
			},
			wantErr: auth.ErrDisallowedAlgorithm,
		},
		{
			name: "algorithm confusion: RSA header over an ECDSA signature",
			token: func(t *testing.T) string {
				return key.Sign(map[string]any{
					header.Type:      jwt.Type,
					header.Algorithm: jwa.RS256,
					header.KeyID:     key.ID(),
				}, claims())
			},
			wantErr: auth.ErrDisallowedAlgorithm,
		},
		{
			name: "expired",
			token: func(t *testing.T) string {
				spoiled := claims()
				spoiled[jwt.ExpirationTime] = referenceTime.Add(-time.Minute).Unix()
				return issuer.MintToken(spoiled)
			},
			wantErr: auth.ErrTokenExpired,
		},
		{
			name: "not valid before a future instant",
			token: func(t *testing.T) string {
				spoiled := claims()
				spoiled[jwt.NotBefore] = referenceTime.Add(time.Hour).Unix()
				return issuer.MintToken(spoiled)
			},
			wantErr: auth.ErrTokenNotYetValid,
		},
		{
			name: "issued in the future",
			token: func(t *testing.T) string {
				spoiled := claims()
				spoiled[jwt.IssuedAt] = referenceTime.Add(time.Hour).Unix()
				return issuer.MintToken(spoiled)
			},
			wantErr: auth.ErrTokenNotYetValid,
		},
		{
			name: "no expiry, so it would never stop working",
			token: func(*testing.T) string {
				return mint(authtest.Without(jwt.ExpirationTime))
			},
			wantErr: auth.ErrMissingClaim,
		},
		{
			name: "no issued-at",
			token: func(*testing.T) string {
				return mint(authtest.Without(jwt.IssuedAt))
			},
			wantErr: auth.ErrMissingClaim,
		},
		{
			name: "no subject",
			token: func(*testing.T) string {
				return mint(authtest.Without(jwt.Subject))
			},
			wantErr: auth.ErrMissingClaim,
		},
		{
			name: "no audience",
			token: func(*testing.T) string {
				return mint(authtest.WithoutAudience())
			},
			wantErr: auth.ErrMissingClaim,
		},
		{
			name: "no issuer",
			token: func(*testing.T) string {
				return mint(authtest.Without(jwt.Issuer))
			},
			wantErr: auth.ErrMissingClaim,
		},
		{
			name: "audience of another service",
			token: func(t *testing.T) string {
				spoiled := claims()
				spoiled[jwt.Audience] = "some-other-service"
				return issuer.MintToken(spoiled)
			},
			wantErr: auth.ErrInvalidAudience,
		},
		{
			name: "audience list without ours",
			token: func(t *testing.T) string {
				spoiled := claims()
				spoiled[jwt.Audience] = []string{"some-other-service", "https://example.com"}
				return issuer.MintToken(spoiled)
			},
			wantErr: auth.ErrInvalidAudience,
		},
		{
			name: "issuer we do not trust",
			token: func(t *testing.T) string {
				spoiled := claims()
				spoiled[jwt.Issuer] = "https://issuer.invalid"
				return issuer.MintToken(spoiled)
			},
			wantErr: auth.ErrUntrustedIssuer,
		},
		{
			name: "issuer that only looks like ours",
			token: func(t *testing.T) string {
				spoiled := claims()
				spoiled[jwt.Issuer] = issuer.URL() + "/"
				return issuer.MintToken(spoiled)
			},
			wantErr: auth.ErrUntrustedIssuer,
		},
		{
			name: "tampered signature",
			token: func(t *testing.T) string {
				return tamperSignature(t, issuer.MintToken(claims()))
			},
			wantErr: auth.ErrInvalidSignature,
		},
		{
			name: "signed by a key the issuer does not publish",
			token: func(t *testing.T) string {
				return issuer.MintToken(claims(), authtest.SignedBy(unpublished))
			},
			wantErr: auth.ErrUnknownKey,
		},
		{
			name: "signed by an unpublished key claiming a published key id",
			token: func(t *testing.T) string {
				return unpublished.Sign(map[string]any{
					header.Type:      jwt.Type,
					header.Algorithm: jwa.ES256,
					header.KeyID:     key.ID(),
				}, claims())
			},
			wantErr: auth.ErrInvalidSignature,
		},
		{
			name: "critical header extension we do not understand",
			token: func(t *testing.T) string {
				return key.Sign(map[string]any{
					header.Type:      jwt.Type,
					header.Algorithm: jwa.ES256,
					header.KeyID:     key.ID(),
					header.Critical:  []string{"https://flowstate.example/must-understand"},
				}, claims())
			},
			wantErr: auth.ErrMalformedToken,
		},
		{
			name: "header type that is not a JWT",
			token: func(t *testing.T) string {
				return key.Sign(map[string]any{
					header.Type:      "JWE",
					header.Algorithm: jwa.ES256,
					header.KeyID:     key.ID(),
				}, claims())
			},
			wantErr: auth.ErrMalformedToken,
		},
		{
			name: "no algorithm in the header",
			token: func(t *testing.T) string {
				return key.Sign(map[string]any{
					header.Type:  jwt.Type,
					header.KeyID: key.ID(),
				}, claims())
			},
			wantErr: auth.ErrMalformedToken,
		},
		{
			name: "algorithm the issuer's policy does not allow",
			token: func(t *testing.T) string {
				return key.Sign(map[string]any{
					header.Type:      jwt.Type,
					header.Algorithm: jwa.ES384,
					header.KeyID:     key.ID(),
				}, claims())
			},
			wantErr: auth.ErrDisallowedAlgorithm,
		},
		{
			name: "expiry that is not a timestamp",
			token: func(t *testing.T) string {
				spoiled := claims()
				spoiled[jwt.ExpirationTime] = referenceTime.Add(time.Hour).Format(time.RFC3339)
				return key.Sign(map[string]any{
					header.Type:      jwt.Type,
					header.Algorithm: jwa.ES256,
					header.KeyID:     key.ID(),
				}, spoiled)
			},
			wantErr: auth.ErrMalformedToken,
		},
		{
			name: "subject that is not a string",
			token: func(t *testing.T) string {
				spoiled := claims()
				spoiled[jwt.Subject] = 1234
				return key.Sign(map[string]any{
					header.Type:      jwt.Type,
					header.Algorithm: jwa.ES256,
					header.KeyID:     key.ID(),
				}, spoiled)
			},
			wantErr: auth.ErrMalformedToken,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			principal, err := verifier.Verify(t.Context(), test.token(t))

			require.Error(t, err)
			require.ErrorIs(t, err, test.wantErr)
			require.True(t, principal.IsZero(), "a rejected token must not yield an identity, got %v", principal)
		})
	}
}

// TestOIDCVerifierAccepts covers the tokens that should work, including every
// signature algorithm and both shapes of the audience claim.
func TestOIDCVerifierAccepts(t *testing.T) {
	tests := []struct {
		name   string
		key    func(t *testing.T) *authtest.Key
		claims func(issuer *authtest.Issuer) map[string]any
		check  func(t *testing.T, principal auth.Principal)
	}{
		{
			name: "RS256",
			key:  func(t *testing.T) *authtest.Key { return authtest.GenerateKey("rsa", jwa.RS256) },
		},
		{
			name: "ES256",
			key:  func(t *testing.T) *authtest.Key { return authtest.GenerateKey("ec", jwa.ES256) },
		},
		{
			name: "ES512",
			key:  func(t *testing.T) *authtest.Key { return authtest.GenerateKey("ec-521", jwa.ES512) },
		},
		{
			name: "EdDSA",
			key:  func(t *testing.T) *authtest.Key { return authtest.GenerateKey("ed25519", jwa.EdDSA) },
		},
		{
			name: "key declaring its own algorithm",
			key: func(t *testing.T) *authtest.Key {
				return authtest.GenerateKey("ec", jwa.ES256, authtest.PublishAlgorithm(jwa.ES256))
			},
		},
		{
			name: "key published for signature use",
			key: func(t *testing.T) *authtest.Key {
				return authtest.GenerateKey("ec", jwa.ES256, authtest.PublishUse("sig"))
			},
		},
		{
			name: "no key id, when the issuer publishes exactly one key",
			key:  func(t *testing.T) *authtest.Key { return authtest.GenerateKey("", jwa.ES256) },
		},
		{
			name: "audience as a list containing ours",
			key:  func(t *testing.T) *authtest.Key { return authtest.GenerateKey("ec", jwa.ES256) },
			claims: func(issuer *authtest.Issuer) map[string]any {
				claims := issuer.Claims(authtest.WithSubject("workflow-runner"), authtest.WithoutAudience())
				claims[jwt.Audience] = []string{"some-other-service", "flowstate"}
				return claims
			},
			check: func(t *testing.T, principal auth.Principal) {
				require.True(t, principal.HasAudience("flowstate"))
				require.True(t, principal.HasAudience("some-other-service"))
			},
		},
		{
			name: "extra claims are carried through",
			key:  func(t *testing.T) *authtest.Key { return authtest.GenerateKey("ec", jwa.ES256) },
			claims: func(issuer *authtest.Issuer) map[string]any {
				claims := issuer.Claims(authtest.WithSubject("workflow-runner"), authtest.WithAudience("flowstate"))
				claims["email"] = "someone@example.com"
				claims["groups"] = []string{"platform", "sre"}
				return claims
			},
			check: func(t *testing.T, principal auth.Principal) {
				email, ok := principal.StringClaim("email")
				require.True(t, ok)
				require.Equal(t, "someone@example.com", email)

				groups, ok := principal.Claim("groups")
				require.True(t, ok)
				require.NotEmpty(t, groups)

				_, ok = principal.StringClaim("groups")
				require.False(t, ok, "a list claim is not a string claim")
			},
		},
		{
			name: "token that is nearly expired",
			key:  func(t *testing.T) *authtest.Key { return authtest.GenerateKey("ec", jwa.ES256) },
			claims: func(issuer *authtest.Issuer) map[string]any {
				claims := issuer.Claims(authtest.WithSubject("workflow-runner"), authtest.WithAudience("flowstate"))
				claims[jwt.ExpirationTime] = referenceTime.Add(time.Second).Unix()
				return claims
			},
		},
		{
			name: "not-before that has passed",
			key:  func(t *testing.T) *authtest.Key { return authtest.GenerateKey("ec", jwa.ES256) },
			claims: func(issuer *authtest.Issuer) map[string]any {
				claims := issuer.Claims(authtest.WithSubject("workflow-runner"), authtest.WithAudience("flowstate"))
				claims[jwt.NotBefore] = referenceTime.Add(-time.Minute).Unix()
				return claims
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				key    = test.key(t)
				clock  = authtest.NewClock(referenceTime)
				issuer = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))
			)

			verifier := newVerifier(t,
				auth.Policy{
					Issuers: []auth.TrustedIssuer{{
						Name:      "test",
						Issuer:    issuer.URL(),
						Audiences: []string{"flowstate"},
						Role:      "operator",
					}},
				},
				auth.WithClock(clock.Now),
			)

			claims := issuer.Claims(authtest.WithSubject("workflow-runner"), authtest.WithAudience("flowstate"))
			if test.claims != nil {
				claims = test.claims(issuer)
			}

			principal, err := verifier.Verify(t.Context(), issuer.MintToken(claims))
			require.NoError(t, err)

			require.Equal(t, issuer.URL(), principal.Issuer)
			require.Equal(t, "test", principal.IssuerName)
			require.Equal(t, "workflow-runner", principal.Subject)
			require.Equal(t, "operator", principal.Role)
			require.Equal(t, issuer.URL()+"#workflow-runner", principal.ID())
			require.False(t, principal.IsZero())
			require.False(t, principal.IsAnonymous())
			require.Equal(t, referenceTime.Unix(), principal.IssuedAt.Unix())
			require.False(t, principal.ExpiresAt.IsZero())

			if test.check != nil {
				test.check(t, principal)
			}
		})
	}
}

// TestOIDCVerifierClockSkew checks that the skew allowance is applied at both
// ends of a token's life, and only up to its configured size.
func TestOIDCVerifierClockSkew(t *testing.T) {
	tests := []struct {
		name      string
		skew      time.Duration
		expiresAt time.Time
		issuedAt  time.Time
		wantErr   error
	}{
		{
			name:      "expiry just inside the skew allowance",
			skew:      30 * time.Second,
			expiresAt: referenceTime.Add(-15 * time.Second),
			issuedAt:  referenceTime.Add(-time.Hour),
		},
		{
			name:      "expiry just outside the skew allowance",
			skew:      30 * time.Second,
			expiresAt: referenceTime.Add(-45 * time.Second),
			issuedAt:  referenceTime.Add(-time.Hour),
			wantErr:   auth.ErrTokenExpired,
		},
		{
			name:      "issued slightly in the future by a fast issuer clock",
			skew:      30 * time.Second,
			expiresAt: referenceTime.Add(time.Hour),
			issuedAt:  referenceTime.Add(15 * time.Second),
		},
		{
			name:      "issued far in the future",
			skew:      30 * time.Second,
			expiresAt: referenceTime.Add(time.Hour),
			issuedAt:  referenceTime.Add(45 * time.Second),
			wantErr:   auth.ErrTokenNotYetValid,
		},
		{
			name:      "no allowance at all",
			skew:      0,
			expiresAt: referenceTime.Add(-time.Second),
			issuedAt:  referenceTime.Add(-time.Hour),
			wantErr:   auth.ErrTokenExpired,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				key    = authtest.GenerateKey("ec", jwa.ES256)
				clock  = authtest.NewClock(referenceTime)
				issuer = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))
			)

			verifier := newVerifier(t,
				auth.Policy{
					Issuers: []auth.TrustedIssuer{{
						Name:      "test",
						Issuer:    issuer.URL(),
						Audiences: []string{"flowstate"},
					}},
				},
				auth.WithClock(clock.Now),
				auth.WithClockSkew(test.skew),
			)

			claims := issuer.Claims(authtest.WithSubject("runner"), authtest.WithAudience("flowstate"))
			claims[jwt.ExpirationTime] = test.expiresAt.Unix()
			claims[jwt.IssuedAt] = test.issuedAt.Unix()

			_, err := verifier.Verify(t.Context(), issuer.MintToken(claims))
			if test.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, test.wantErr)
		})
	}
}

// TestOIDCVerifierKeyRotation checks that a key added to an issuer's key set is
// picked up without restarting, and that a key removed from it stops working
// once the cache expires.
func TestOIDCVerifierKeyRotation(t *testing.T) {
	var (
		oldKey = authtest.GenerateKey("old", jwa.ES256)
		newKey = authtest.GenerateKey("new", jwa.ES256)
		clock  = authtest.NewClock(referenceTime)
		issuer = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(oldKey))
	)

	verifier := newVerifier(t,
		auth.Policy{
			Issuers: []auth.TrustedIssuer{{
				Name:      "test",
				Issuer:    issuer.URL(),
				Audiences: []string{"flowstate"},
			}},
		},
		auth.WithClock(clock.Now),
		auth.WithKeyCacheTTL(15*time.Minute),
		auth.WithMinKeyRefreshInterval(time.Minute),
	)

	claims := func() map[string]any {
		return issuer.Claims(authtest.WithSubject("runner"), authtest.WithAudience("flowstate"))
	}

	// The original key works, and its key set is cached rather than refetched.
	_, err := verifier.Verify(t.Context(), issuer.MintToken(claims(), authtest.SignedBy(oldKey)))
	require.NoError(t, err)

	_, err = verifier.Verify(t.Context(), issuer.MintToken(claims(), authtest.SignedBy(oldKey)))
	require.NoError(t, err)

	jwksRequests := issuer.Requests().JWKS
	require.Equal(t, 1, jwksRequests, "the cached key set should be reused")

	// The issuer rotates: both keys are published for an overlap period.
	issuer.SetKeys(oldKey, newKey)

	// A token signed with the new key names a key id the cache has never seen,
	// which is what triggers a refetch. The rate limit has to be past first.
	clock.Advance(2 * time.Minute)

	principal, err := verifier.Verify(t.Context(), issuer.MintToken(claims(), authtest.SignedBy(newKey)))
	require.NoError(t, err, "a rotated-in key should be picked up automatically")
	require.Equal(t, "runner", principal.Subject)

	jwksRequests = issuer.Requests().JWKS
	require.Equal(t, 2, jwksRequests, "an unknown key id should cause exactly one refetch")

	// Both keys work during the overlap.
	_, err = verifier.Verify(t.Context(), issuer.MintToken(claims(), authtest.SignedBy(oldKey)))
	require.NoError(t, err)

	// The issuer finishes the rotation by withdrawing the old key. Tokens signed
	// with it keep working only until the cached key set expires.
	issuer.SetKeys(newKey)

	_, err = verifier.Verify(t.Context(), issuer.MintToken(claims(), authtest.SignedBy(oldKey)))
	require.NoError(t, err, "the withdrawn key is still cached")

	clock.Advance(16 * time.Minute)

	_, err = verifier.Verify(t.Context(), issuer.MintToken(claims(), authtest.SignedBy(oldKey)))
	require.ErrorIs(t, err, auth.ErrUnknownKey, "a withdrawn key must stop working once the cache expires")

	_, err = verifier.Verify(t.Context(), issuer.MintToken(claims(), authtest.SignedBy(newKey)))
	require.NoError(t, err)
}

// TestOIDCVerifierRefetchRateLimit checks that a caller cannot turn this process
// into a load generator against an issuer by presenting tokens with invented key
// ids.
func TestOIDCVerifierRefetchRateLimit(t *testing.T) {
	var (
		key    = authtest.GenerateKey("primary", jwa.ES256)
		clock  = authtest.NewClock(referenceTime)
		issuer = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))
	)

	verifier := newVerifier(t,
		auth.Policy{
			Issuers: []auth.TrustedIssuer{{
				Name:      "test",
				Issuer:    issuer.URL(),
				Audiences: []string{"flowstate"},
			}},
		},
		auth.WithClock(clock.Now),
		auth.WithMinKeyRefreshInterval(time.Minute),
	)

	// Prime the cache with a legitimate request.
	_, err := verifier.Verify(t.Context(), issuer.MintToken(nil, authtest.WithSubject("runner"), authtest.WithAudience("flowstate")))
	require.NoError(t, err)

	before := issuer.Requests().JWKS
	require.Equal(t, 1, before)

	// A stream of tokens naming key ids that do not exist.
	for range 50 {
		attacker := authtest.GenerateKey("forged", jwa.ES256)
		_, err := verifier.Verify(t.Context(), issuer.MintToken(nil,
			authtest.WithSubject("attacker"), authtest.WithAudience("flowstate"), authtest.SignedBy(attacker)))
		require.ErrorIs(t, err, auth.ErrUnknownKey)
	}

	after := issuer.Requests().JWKS
	require.Equal(t, before, after, "unknown key ids must not reach the issuer again within the refresh interval")

	// Once the interval passes, one more refetch is allowed.
	clock.Advance(2 * time.Minute)

	attacker := authtest.GenerateKey("forged", jwa.ES256)
	_, err = verifier.Verify(t.Context(), issuer.MintToken(nil,
		authtest.WithSubject("attacker"), authtest.WithAudience("flowstate"), authtest.SignedBy(attacker)))
	require.ErrorIs(t, err, auth.ErrUnknownKey)

	later := issuer.Requests().JWKS
	require.Equal(t, after+1, later)
}

// TestOIDCVerifierIssuerUnavailable covers issuers that cannot be used: ones
// that are down, that serve nonsense, or that claim to be someone else.
func TestOIDCVerifierIssuerUnavailable(t *testing.T) {
	tests := []struct {
		name     string
		sabotage func(t *testing.T, issuer *authtest.Issuer)
		wantErr  error
	}{
		{
			name: "key set endpoint is failing",
			sabotage: func(t *testing.T, issuer *authtest.Issuer) {
				issuer.SetKeySetResponse(http.StatusInternalServerError, nil)
			},
			wantErr: auth.ErrIssuerUnavailable,
		},
		{
			name: "key set is not JSON",
			sabotage: func(t *testing.T, issuer *authtest.Issuer) {
				issuer.SetKeySetResponse(http.StatusOK, []byte("<html>not a key set</html>"))
			},
			wantErr: auth.ErrIssuerUnavailable,
		},
		{
			name: "key set is empty",
			sabotage: func(t *testing.T, issuer *authtest.Issuer) {
				issuer.SetKeySetResponse(http.StatusOK, []byte(`{"keys":[]}`))
			},
			wantErr: auth.ErrIssuerUnavailable,
		},
		{
			name: "key set holds nothing usable for signatures",
			sabotage: func(t *testing.T, issuer *authtest.Issuer) {
				issuer.SetKeySetResponse(http.StatusOK, []byte(`{"keys":[{"kty":"oct","kid":"symmetric","k":"c2VjcmV0"}]}`))
			},
			wantErr: auth.ErrIssuerUnavailable,
		},
		{
			name: "key set is unreasonably large",
			sabotage: func(t *testing.T, issuer *authtest.Issuer) {
				// The padding goes inside an otherwise usable key, so that this
				// case can only pass because of the size limit.
				key := authtest.GenerateKey("primary", jwa.ES256).JWK()
				key["x5t#S256"] = strings.Repeat("a", 2<<20)

				body, err := json.Marshal(jwk.Set{Keys: []jwk.Value{key}})
				require.NoError(t, err)
				require.Greater(t, len(body), 1<<20)

				issuer.SetKeySetResponse(http.StatusOK, body)
			},
			wantErr: auth.ErrIssuerUnavailable,
		},
		{
			name: "RSA key is too small to be trusted",
			sabotage: func(t *testing.T, issuer *authtest.Issuer) {
				// A 512-bit modulus, which RFC 7518 forbids for RS256.
				issuer.SetKeySetResponse(http.StatusOK, []byte(`{"keys":[{"kty":"RSA","kid":"weak","n":"1TCB4nCyfmXVKMbCXPMHKzGVzGHLGxHUCbLmiHOWmXKfYMuqzzGKcVLQVvKKmfMGvFLLnKmVjJJKPBLXBnEQnQ","e":"AQAB"}]}`))
			},
			wantErr: auth.ErrIssuerUnavailable,
		},
		{
			name: "discovery document claims a different issuer",
			sabotage: func(t *testing.T, issuer *authtest.Issuer) {
				issuer.SetDiscoveredIssuer("https://issuer.invalid")
			},
			wantErr: auth.ErrIssuerUnavailable,
		},
		{
			name: "discovery document advertises no key set",
			sabotage: func(t *testing.T, issuer *authtest.Issuer) {
				issuer.SetDiscoveryHandler(func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte(`{"issuer":"` + issuer.URL() + `"}`))
				})
			},
			wantErr: auth.ErrIssuerUnavailable,
		},
		{
			name: "discovery document points its key set at an unprotected host",
			sabotage: func(t *testing.T, issuer *authtest.Issuer) {
				issuer.SetDiscoveryHandler(func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte(`{"issuer":"` + issuer.URL() + `","jwks_uri":"http://keys.example.com/jwks"}`))
				})
			},
			wantErr: auth.ErrIssuerUnavailable,
		},
		{
			name: "discovery endpoint is missing",
			sabotage: func(t *testing.T, issuer *authtest.Issuer) {
				issuer.SetDiscoveryHandler(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusNotFound)
				})
			},
			wantErr: auth.ErrIssuerUnavailable,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				key    = authtest.GenerateKey("primary", jwa.ES256)
				clock  = authtest.NewClock(referenceTime)
				issuer = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))
			)

			test.sabotage(t, issuer)

			verifier := newVerifier(t,
				auth.Policy{
					Issuers: []auth.TrustedIssuer{{
						Name:      "test",
						Issuer:    issuer.URL(),
						Audiences: []string{"flowstate"},
					}},
				},
				auth.WithClock(clock.Now),
			)

			token := issuer.MintToken(nil, authtest.WithSubject("runner"), authtest.WithAudience("flowstate"))

			_, err := verifier.Verify(t.Context(), token)
			require.ErrorIs(t, err, test.wantErr)

			// Priming reports the same problem, which is how an operator finds
			// out about it at startup.
			require.Error(t, verifier.Prime(t.Context()))
		})
	}
}

// TestOIDCVerifierPrime checks that priming fetches keys up front, so that a
// healthy issuer is confirmed at startup and the first real request does not pay
// for discovery.
func TestOIDCVerifierPrime(t *testing.T) {
	var (
		key    = authtest.GenerateKey("primary", jwa.ES256)
		clock  = authtest.NewClock(referenceTime)
		issuer = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))
	)

	verifier := newVerifier(t,
		auth.Policy{
			Issuers: []auth.TrustedIssuer{{
				Name:      "test",
				Issuer:    issuer.URL(),
				Audiences: []string{"flowstate"},
			}},
		},
		auth.WithClock(clock.Now),
	)

	require.NoError(t, verifier.Prime(t.Context()))

	requests := issuer.Requests()
	discovery, jwks := requests.Discovery, requests.JWKS
	require.Equal(t, 1, discovery)
	require.Equal(t, 1, jwks)

	// Priming again while the keys are cached does nothing.
	require.NoError(t, verifier.Prime(t.Context()))

	requests = issuer.Requests()
	discovery, jwks = requests.Discovery, requests.JWKS
	require.Equal(t, 1, discovery)
	require.Equal(t, 1, jwks)

	_, err := verifier.Verify(t.Context(), issuer.MintToken(nil, authtest.WithSubject("runner"), authtest.WithAudience("flowstate")))
	require.NoError(t, err)

	jwks = issuer.Requests().JWKS
	require.Equal(t, 1, jwks, "verification should use the primed keys")
}

// TestOIDCVerifierIgnoresMTLSIssuers checks that the certificate-only half of
// a mixed trust policy never becomes a bearer-token issuer or an OIDC discovery
// target, even when its operator-chosen label happens to look like a URL.
//
// The label here is a live test issuer's URL, which is the only arrangement
// that can tell the two failure modes apart: a label that resolves to nothing
// makes Prime error and would look like a pass for the wrong reason. Every
// assertion is therefore a negative one — nothing was fetched, and the token
// that label mints is not trusted.
func TestOIDCVerifierIgnoresMTLSIssuers(t *testing.T) {
	issuer := newTestIssuer(t)
	verifier := newVerifier(t, auth.Policy{Issuers: []auth.TrustedIssuer{{
		Name:         "mesh",
		Kind:         auth.IssuerKindMTLS,
		Issuer:       issuer.URL(),
		ClientCAFile: newTestCA(t, "test-ca").clientCAFile(t),
		SubjectFrom:  auth.SubjectFromURISAN,
	}}})

	// Priming is where the unwanted egress would happen, so it is checked
	// before anything else has a chance to fetch on demand.
	require.NoError(t, verifier.Prime(t.Context()), "priming must not treat an mTLS label as a discovery target")
	require.Zero(t, issuer.Requests().Discovery, "Prime must not fetch discovery for a kind: mtls entry")
	require.Zero(t, issuer.Requests().JWKS, "Prime must not fetch a key set for a kind: mtls entry")

	_, err := verifier.Verify(t.Context(), issuer.MintToken(nil, authtest.WithoutAudience()))
	require.ErrorIs(t, err, auth.ErrUntrustedIssuer, "a kind: mtls label must not admit a bearer token")
	require.Zero(t, issuer.Requests().Discovery, "a rejected issuer must not have been fetched on demand either")
	require.Zero(t, issuer.Requests().JWKS, "a rejected issuer must not have been fetched on demand either")
}

// TestOIDCVerifierConcurrent checks that many simultaneous first requests share
// one key set fetch rather than each starting their own, and that the verifier
// holds up under the race detector.
func TestOIDCVerifierConcurrent(t *testing.T) {
	var (
		key    = authtest.GenerateKey("primary", jwa.ES256)
		clock  = authtest.NewClock(referenceTime)
		issuer = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))
	)

	verifier := newVerifier(t,
		auth.Policy{
			Issuers: []auth.TrustedIssuer{{
				Name:      "test",
				Issuer:    issuer.URL(),
				Audiences: []string{"flowstate"},
				Role:      "operator",
			}},
		},
		auth.WithClock(clock.Now),
	)

	token := issuer.MintToken(nil, authtest.WithSubject("runner"), authtest.WithAudience("flowstate"))

	const callers = 32

	// assert rather than require: require calls t.FailNow, which must only be
	// called from the goroutine running the test.
	var wait sync.WaitGroup
	for range callers {
		wait.Go(func() {
			principal, err := verifier.Verify(t.Context(), token)
			assert.NoError(t, err)
			assert.Equal(t, "operator", principal.Role)
		})
	}
	wait.Wait()

	requests := issuer.Requests()
	discovery, jwks := requests.Discovery, requests.JWKS
	require.Equal(t, 1, discovery, "concurrent first requests should share one discovery")
	require.Equal(t, 1, jwks, "concurrent first requests should share one key set fetch")
}

// TestOIDCVerifierStaticJWKSURL checks that an issuer without a discovery
// document can still be used by naming its key set directly.
func TestOIDCVerifierStaticJWKSURL(t *testing.T) {
	var (
		key    = authtest.GenerateKey("primary", jwa.ES256)
		clock  = authtest.NewClock(referenceTime)
		issuer = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))
	)

	issuer.SetDiscoveryHandler(func(w http.ResponseWriter, r *http.Request) {
		t.Error("discovery should not be attempted when a key set URL is configured")
	})

	verifier := newVerifier(t,
		auth.Policy{
			Issuers: []auth.TrustedIssuer{{
				Name:      "test",
				Issuer:    issuer.URL(),
				Audiences: []string{"flowstate"},
				JWKSURL:   issuer.URL() + "/jwks",
			}},
		},
		auth.WithClock(clock.Now),
	)

	_, err := verifier.Verify(t.Context(), issuer.MintToken(nil, authtest.WithSubject("runner"), authtest.WithAudience("flowstate")))
	require.NoError(t, err)

	requests := issuer.Requests()
	discovery, jwks := requests.Discovery, requests.JWKS
	require.Zero(t, discovery)
	require.Equal(t, 1, jwks)
}

// TestOIDCVerifierAmbiguousKey checks that a token without a key id is refused
// when the issuer publishes several usable keys, rather than the verifier trying
// each key until one happens to work.
func TestOIDCVerifierAmbiguousKey(t *testing.T) {
	var (
		signer = authtest.GenerateKey("", jwa.ES256)
		other  = authtest.GenerateKey("", jwa.ES256)
		clock  = authtest.NewClock(referenceTime)
		issuer = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(signer, other))
	)

	verifier := newVerifier(t,
		auth.Policy{
			Issuers: []auth.TrustedIssuer{{
				Name:      "test",
				Issuer:    issuer.URL(),
				Audiences: []string{"flowstate"},
			}},
		},
		auth.WithClock(clock.Now),
	)

	token := issuer.MintToken(nil, authtest.WithSubject("runner"), authtest.WithAudience("flowstate"))

	_, err := verifier.Verify(t.Context(), token)
	require.ErrorIs(t, err, auth.ErrUnknownKey)

	// The same refusal once the key set is cached, rather than a refetch on every
	// such token.
	_, err = verifier.Verify(t.Context(), token)
	require.ErrorIs(t, err, auth.ErrUnknownKey)

	jwksRequests := issuer.Requests().JWKS
	require.Equal(t, 1, jwksRequests)
}

// TestOIDCVerifierPerIssuerAlgorithms checks that each policy entry's own
// algorithm allowlist is applied, not just the union of them: an entry restricted
// to RS256 must not admit a token signed with an EC key that another entry
// permits.
func TestOIDCVerifierPerIssuerAlgorithms(t *testing.T) {
	var (
		rsaKey = authtest.GenerateKey("rsa", jwa.RS256)
		ecKey  = authtest.GenerateKey("ec", jwa.ES256)
		clock  = authtest.NewClock(referenceTime)
		issuer = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(rsaKey, ecKey))
	)

	verifier := newVerifier(t,
		auth.Policy{
			Issuers: []auth.TrustedIssuer{
				{
					Name:       "rsa-only",
					Issuer:     issuer.URL(),
					Audiences:  []string{"flowstate"},
					Algorithms: []jwa.Algorithm{jwa.RS256},
					Role:       "rsa-caller",
				},
				{
					Name:       "ec-only",
					Issuer:     issuer.URL(),
					Audiences:  []string{"flowstate"},
					Algorithms: []jwa.Algorithm{jwa.ES256},
					Role:       "ec-caller",
				},
			},
		},
		auth.WithClock(clock.Now),
	)

	claims := issuer.Claims(authtest.WithSubject("runner"), authtest.WithAudience("flowstate"))

	principal, err := verifier.Verify(t.Context(), issuer.MintToken(claims, authtest.SignedBy(rsaKey)))
	require.NoError(t, err)
	require.Equal(t, "rsa-caller", principal.Role)
	require.Equal(t, "rsa-only", principal.IssuerName)

	principal, err = verifier.Verify(t.Context(), issuer.MintToken(claims, authtest.SignedBy(ecKey)))
	require.NoError(t, err)
	require.Equal(t, "ec-caller", principal.Role, "the EC token must be admitted by the entry that allows ES256")
	require.Equal(t, "ec-only", principal.IssuerName)
}

// TestOIDCVerifierDeclaredAlgorithmMismatch checks that a key published for one
// algorithm cannot be used to verify a token claiming another.
func TestOIDCVerifierDeclaredAlgorithmMismatch(t *testing.T) {
	// Published as ES512, but signs ES256 below.
	key := authtest.GenerateKey("ec", jwa.ES256, authtest.PublishAlgorithm(jwa.ES512))

	var (
		clock  = authtest.NewClock(referenceTime)
		issuer = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))
	)

	verifier := newVerifier(t,
		auth.Policy{
			Issuers: []auth.TrustedIssuer{{
				Name:      "test",
				Issuer:    issuer.URL(),
				Audiences: []string{"flowstate"},
			}},
		},
		auth.WithClock(clock.Now),
	)

	token := key.Sign(map[string]any{
		header.Type:      jwt.Type,
		header.Algorithm: jwa.ES256,
		header.KeyID:     key.ID(),
	}, issuer.Claims(authtest.WithSubject("runner"), authtest.WithAudience("flowstate")))

	_, err := verifier.Verify(t.Context(), token)
	require.ErrorIs(t, err, auth.ErrDisallowedAlgorithm)
}

// TestNewOIDCVerifierRejectsBadConfiguration checks that unusable configuration
// is reported when the verifier is built, not when a request arrives.
func TestNewOIDCVerifierRejectsBadConfiguration(t *testing.T) {
	validIssuer := auth.TrustedIssuer{
		Name:      "test",
		Issuer:    "https://issuer.example.com",
		Audiences: []string{"flowstate"},
	}

	tests := []struct {
		name   string
		policy auth.Policy
		opts   []auth.Option
	}{
		{
			name:   "no issuers",
			policy: auth.Policy{},
		},
		{
			name: "issuer name too long for exact audit provenance",
			policy: auth.Policy{Issuers: []auth.TrustedIssuer{{
				Name:   strings.Repeat("n", auth.MaxPolicyProvenanceBytes+1),
				Issuer: validIssuer.Issuer, Audiences: validIssuer.Audiences,
			}}},
		},
		{
			name: "role too long for exact audit provenance",
			policy: auth.Policy{Issuers: []auth.TrustedIssuer{{
				Name: validIssuer.Name, Issuer: validIssuer.Issuer, Audiences: validIssuer.Audiences,
				Role: strings.Repeat("r", auth.MaxPolicyProvenanceBytes+1),
			}}},
		},
		{
			name:   "negative clock skew",
			policy: auth.Policy{Issuers: []auth.TrustedIssuer{validIssuer}},
			opts:   []auth.Option{auth.WithClockSkew(-time.Second)},
		},
		{
			name:   "clock skew large enough to defeat short lifetimes",
			policy: auth.Policy{Issuers: []auth.TrustedIssuer{validIssuer}},
			opts:   []auth.Option{auth.WithClockSkew(time.Hour)},
		},
		{
			name:   "key cache that never expires",
			policy: auth.Policy{Issuers: []auth.TrustedIssuer{validIssuer}},
			opts:   []auth.Option{auth.WithKeyCacheTTL(0)},
		},
		{
			name:   "no fetch timeout",
			policy: auth.Policy{Issuers: []auth.TrustedIssuer{validIssuer}},
			opts:   []auth.Option{auth.WithFetchTimeout(0)},
		},
		{
			name:   "negative refresh interval",
			policy: auth.Policy{Issuers: []auth.TrustedIssuer{validIssuer}},
			opts:   []auth.Option{auth.WithMinKeyRefreshInterval(-time.Second)},
		},
		{
			// Otherwise the keys expire during a window in which they may not be
			// fetched again, and every valid token is refused until it closes.
			name:   "a refresh interval longer than the key cache lifetime",
			policy: auth.Policy{Issuers: []auth.TrustedIssuer{validIssuer}},
			opts: []auth.Option{
				auth.WithKeyCacheTTL(30 * time.Second),
				auth.WithMinKeyRefreshInterval(time.Minute),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			verifier, err := auth.NewOIDCVerifier(test.policy, append([]auth.Option{auth.WithEgressPolicy(authtest.EgressPolicy())}, test.opts...)...)
			require.Error(t, err)
			require.ErrorIs(t, err, auth.ErrInvalidPolicy)
			require.Nil(t, verifier)
		})
	}
}

// TestOIDCVerifierFetchTimeout checks that a hanging issuer fails the request
// instead of holding it open.
func TestOIDCVerifierFetchTimeout(t *testing.T) {
	var (
		key    = authtest.GenerateKey("primary", jwa.ES256)
		clock  = authtest.NewClock(referenceTime)
		issuer = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))
		block  = make(chan struct{})
	)

	issuer.SetDiscoveryHandler(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-block:
		case <-r.Context().Done():
		}
	})
	t.Cleanup(func() { close(block) })

	verifier := newVerifier(t,
		auth.Policy{
			Issuers: []auth.TrustedIssuer{{
				Name:      "test",
				Issuer:    issuer.URL(),
				Audiences: []string{"flowstate"},
			}},
		},
		auth.WithClock(clock.Now),
		auth.WithFetchTimeout(50*time.Millisecond),
	)

	start := time.Now()
	_, err := verifier.Verify(t.Context(), issuer.MintToken(nil, authtest.WithSubject("runner"), authtest.WithAudience("flowstate")))
	require.ErrorIs(t, err, auth.ErrIssuerUnavailable)
	require.Less(t, time.Since(start), 5*time.Second)
}

// TestOIDCVerifierPublicErrorsSayLittle checks that the errors reaching a caller
// do not describe the trust policy, while the errors an operator sees do.
func TestOIDCVerifierErrorDetail(t *testing.T) {
	var (
		key    = authtest.GenerateKey("primary", jwa.ES256)
		clock  = authtest.NewClock(referenceTime)
		issuer = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))
	)

	verifier := newVerifier(t,
		auth.Policy{
			Issuers: []auth.TrustedIssuer{{
				Name:      "test",
				Issuer:    issuer.URL(),
				Audiences: []string{"flowstate"},
				Require:   []auth.ClaimRule{auth.RequireClaim("repository", "picatz/flowstate")},
			}},
		},
		auth.WithClock(clock.Now),
	)

	claims := issuer.Claims(authtest.WithSubject("runner"), authtest.WithAudience("flowstate"))
	claims["repository"] = "attacker/fork"

	_, err := verifier.Verify(t.Context(), issuer.MintToken(claims))
	require.ErrorIs(t, err, auth.ErrClaimMismatch)

	// The operator-facing error names the rule, the expected values, and what the
	// token actually asserted.
	var mismatch *auth.ClaimMismatchError
	require.ErrorAs(t, err, &mismatch)
	require.Equal(t, "repository", mismatch.Claim)
	require.Equal(t, []string{"picatz/flowstate"}, mismatch.Want)
	require.Equal(t, "attacker/fork", mismatch.Got)
	require.Contains(t, err.Error(), `trusted issuer "test"`)

	// Errors are values, not strings: the wrapping does not get in the way of
	// telling one failure from another.
	require.False(t, errors.Is(err, auth.ErrInvalidSignature))
}
