package auth_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/jose/pkg/header"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/picatz/jose/pkg/jwt"
	"github.com/stretchr/testify/require"
)

// The tests here pin behaviors that are easy to regress into a vulnerability, and
// each names the failure it prevents.

// TestOIDCVerifierRejectsHMACBeforeResolvingAKey checks the mechanism behind the
// algorithm confusion defense, not just its outcome: a symmetric algorithm is
// refused by name, before any key is resolved. If it were instead refused because
// the MAC failed to verify, the defense would depend on a key comparison rather
// than on the algorithm never being eligible.
func TestOIDCVerifierRejectsHMACBeforeResolvingAKey(t *testing.T) {
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

	// The classic attack: the issuer's published public key, used as an HMAC
	// secret, so that a verifier which treats "a key is a key" accepts it.
	token := hmacToken(t, key.ID(), publicKeyBytes(t, key.Public()),
		issuer.Claims(authtest.WithSubject("attacker"), authtest.WithAudience("flowstate")))

	_, err := verifier.Verify(t.Context(), token)
	require.ErrorIs(t, err, auth.ErrDisallowedAlgorithm)

	requests := issuer.Requests()
	discovery, jwks := requests.Discovery, requests.JWKS
	require.Zero(t, discovery, "an HMAC token must be refused before the issuer is contacted")
	require.Zero(t, jwks, "an HMAC token must never reach a published key")
}

// TestOIDCVerifierCancelledRequestDoesNotPoisonKeyCache checks that one caller
// hanging up cannot deny authentication to everyone else.
//
// The key set fetch is shared work whose outcome is cached for every caller, so
// it must not inherit the cancellation of whichever request happened to trigger
// it. Otherwise a single aborted request per refresh interval, from anyone who
// can reach the port, keeps this host from ever caching keys.
func TestOIDCVerifierCancelledRequestDoesNotPoisonKeyCache(t *testing.T) {
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

	token := issuer.MintToken(nil, authtest.WithSubject("runner"), authtest.WithAudience("flowstate"))

	// A caller that has already given up, arriving before any keys are cached.
	abandoned, cancel := context.WithCancel(t.Context())
	cancel()

	_, _ = verifier.Verify(abandoned, token)

	// The next caller must not inherit that outcome, and the clock has not moved,
	// so a rate-limited retry would not save it.
	principal, err := verifier.Verify(t.Context(), token)
	require.NoError(t, err, "an aborted request must not deny authentication to the next caller")
	require.Equal(t, "runner", principal.Subject)
}

// TestOIDCVerifierRefusesKeySetRedirectedToPlainHTTP checks that the promise of a
// protected transport covers the whole redirect chain.
//
// Validating only the advertised URL is not enough: an issuer that redirects its
// key set to plain http would have its signing keys, and so the identity of every
// caller it vouches for, decided by whoever is on the network path.
func TestOIDCVerifierRefusesKeySetRedirectedToPlainHTTP(t *testing.T) {
	tests := []struct {
		name    string
		target  func(issuer *authtest.Issuer) string
		wantErr error
	}{
		{
			name:    "redirected to an unprotected host",
			target:  func(*authtest.Issuer) string { return "http://keys.example.invalid/jwks" },
			wantErr: auth.ErrIssuerUnavailable,
		},
		{
			name: "redirected within the local development issuer",
			// Redirects are still followed when they stay somewhere the transport
			// rules allow, so this pins that the check is about the scheme rather
			// than about redirects in general.
			target: func(issuer *authtest.Issuer) string { return issuer.URL() + "/jwks" },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				key    = authtest.GenerateKey("primary", jwa.ES256)
				clock  = authtest.NewClock(referenceTime)
				issuer = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))
			)

			issuer.RedirectKeySet(test.target(issuer))

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
			if test.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, test.wantErr)
		})
	}
}

// TestOIDCVerifierNamesAnEgressPolicyDenialSeparatelyFromAnUnreachableIssuer
// checks that a JWKS fetch the identity egress policy refuses is reported
// differently from an issuer that genuinely never answers. Both used to wrap
// [auth.ErrIssuerUnavailable] with the identical "temporarily unavailable"
// message, which sent an operator chasing a network problem for what was
// really the deployment's own trust-policy configuration
// (picatz/flowstate#1303).
func TestOIDCVerifierNamesAnEgressPolicyDenialSeparatelyFromAnUnreachableIssuer(t *testing.T) {
	var (
		key   = authtest.GenerateKey("primary", jwa.ES256)
		clock = authtest.NewClock(referenceTime)
	)

	t.Run("blocked by the egress policy", func(t *testing.T) {
		issuer := newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))

		// No WithAllowLoopback: this is exactly what a trust policy with no
		// egress: section (or one that never named loopback) produces, and the
		// test issuer listens on loopback like a real deployment's would not.
		restrictive, err := netpolicy.New()
		require.NoError(t, err)

		verifier, err := auth.NewOIDCVerifier(
			auth.Policy{
				Issuers: []auth.TrustedIssuer{{
					Name:      "test",
					Issuer:    issuer.URL(),
					Audiences: []string{"flowstate"},
				}},
			},
			auth.WithClock(clock.Now),
			auth.WithEgressPolicy(restrictive),
		)
		require.NoError(t, err)

		token := issuer.MintToken(nil, authtest.WithSubject("runner"), authtest.WithAudience("flowstate"))
		_, err = verifier.Verify(t.Context(), token)
		require.Error(t, err)

		// It still classifies as issuer-unavailable, so any existing
		// errors.Is(err, auth.ErrIssuerUnavailable) caller keeps working...
		require.ErrorIs(t, err, auth.ErrIssuerUnavailable)
		// ...but it is also, distinguishably, a policy denial.
		require.ErrorIs(t, err, netpolicy.ErrDenied)

		var blocked *auth.IssuerBlockedError
		require.ErrorAs(t, err, &blocked)
		require.Equal(t, issuer.URL(), blocked.Issuer)
		require.Equal(t, netpolicy.ReasonAddress, blocked.Reason)
		require.Contains(t, blocked.Detail, "loopback")

		require.Contains(t, err.Error(), "blocked by the identity egress policy")
		require.Contains(t, err.Error(), "egress:", "the message must point at the remedy")
		require.NotContains(t, err.Error(), "temporarily unavailable",
			"a deliberate policy denial must not read like a network failure")
	})

	t.Run("issuer genuinely unreachable", func(t *testing.T) {
		issuer := newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))
		token := issuer.MintToken(nil, authtest.WithSubject("runner"), authtest.WithAudience("flowstate"))

		// Nothing listens here any more: the egress policy would allow the
		// request, but there is no peer to answer it.
		require.NoError(t, issuer.Close())

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

		_, err := verifier.Verify(t.Context(), token)
		require.Error(t, err)

		require.ErrorIs(t, err, auth.ErrIssuerUnavailable)
		require.False(t, errors.Is(err, netpolicy.ErrDenied), "a down issuer is not a policy denial")

		var blocked *auth.IssuerBlockedError
		require.False(t, errors.As(err, &blocked), "a down issuer must not produce a policy-denial error")

		// The wording an operator and an unauthenticated caller both see is
		// unchanged for this case.
		require.Equal(t, "issuer keys are temporarily unavailable", auth.PublicReason(err))
	})
}

// TestOIDCVerifierPrimeFailureDoesNotBlockRecovery checks that priming stays
// advisory. Its documentation invites a server to report a failure and start
// anyway, which is only true if a failed prime does not also spend the refresh
// allowance that on-demand fetching needs.
func TestOIDCVerifierPrimeFailureDoesNotBlockRecovery(t *testing.T) {
	var (
		key    = authtest.GenerateKey("primary", jwa.ES256)
		clock  = authtest.NewClock(referenceTime)
		issuer = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))
	)

	issuer.SetKeySetResponse(http.StatusServiceUnavailable, nil)

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

	require.Error(t, verifier.Prime(t.Context()), "the issuer is down, so priming must fail")

	// The issuer recovers immediately. Without moving the clock, the next request
	// must still be able to fetch.
	issuer.SetKeySetResponse(http.StatusOK, nil)

	principal, err := verifier.Verify(t.Context(), issuer.MintToken(nil, authtest.WithSubject("runner"), authtest.WithAudience("flowstate")))
	require.NoError(t, err, "a failed prime must not rate limit the first real request")
	require.Equal(t, "runner", principal.Subject)
}

// TestOIDCVerifierRejectsTokenNominatedKeys checks that a token cannot name the
// key that verifies it. Only keys an issuer publishes at its own key set URL are
// ever used, so these headers are refused rather than quietly ignored, which
// keeps the guarantee here rather than in a dependency's omissions.
func TestOIDCVerifierRejectsTokenNominatedKeys(t *testing.T) {
	var (
		key      = authtest.GenerateKey("primary", jwa.ES256)
		attacker = authtest.GenerateKey("primary", jwa.ES256) // same key id, attacker's key
		clock    = authtest.NewClock(referenceTime)
		issuer   = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))
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

	tests := []struct {
		name  string
		param header.ParameterName
		value any
	}{
		{
			name:  "an inline key",
			param: header.JSONWebKey,
			value: attacker.JWK(),
		},
		{
			name:  "a key set URL of its own",
			param: header.JWKSetURL,
			value: "https://attacker.example.invalid/jwks",
		},
		{
			name:  "a certificate URL",
			param: header.X509URL,
			value: "https://attacker.example.invalid/cert.pem",
		},
		{
			name:  "an inline certificate chain",
			param: header.X509CertificateChain,
			value: []string{"MIIB..."},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			token := attacker.Sign(map[string]any{
				header.Type:      jwt.Type,
				header.Algorithm: jwa.ES256,
				header.KeyID:     attacker.ID(),
				test.param:       test.value,
			}, issuer.Claims(authtest.WithSubject("attacker"), authtest.WithAudience("flowstate")))

			_, err := verifier.Verify(t.Context(), token)
			require.ErrorIs(t, err, auth.ErrMalformedToken)
		})
	}
}

// TestOIDCVerifierKeyIDHandling checks how the key id header is read. The key a
// token is verified with is looked up by this value, so it must be a string and
// nothing else: two different readings of the same header, one to find a key and
// one to verify with it, must not be able to disagree.
func TestOIDCVerifierKeyIDHandling(t *testing.T) {
	var (
		key    = authtest.GenerateKey("", jwa.ES256)
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
		auth.WithMinKeyRefreshInterval(0),
	)

	tests := []struct {
		name    string
		keyID   any
		wantErr error
	}{
		{
			name:  "absent, with a single published key",
			keyID: nil,
		},
		{
			name:  "an empty string, which names no key",
			keyID: "",
		},
		{
			name:    "a number",
			keyID:   42,
			wantErr: auth.ErrMalformedToken,
		},
		{
			name:    "an object",
			keyID:   map[string]any{"kid": "primary"},
			wantErr: auth.ErrMalformedToken,
		},
		{
			name:    "a key id the issuer does not publish",
			keyID:   "invented",
			wantErr: auth.ErrUnknownKey,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			params := map[string]any{
				header.Type:      jwt.Type,
				header.Algorithm: jwa.ES256,
			}
			if test.keyID != nil {
				params[header.KeyID] = test.keyID
			}

			token := key.Sign(params, issuer.Claims(authtest.WithSubject("runner"), authtest.WithAudience("flowstate")))

			_, err := verifier.Verify(t.Context(), token)
			if test.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, test.wantErr)
		})
	}
}

// TestOIDCVerifierRejectsImplausibleTimestamps checks that a time claim far
// outside any plausible range is refused. Such a value is not clock skew, and
// letting one through yields a Principal whose times cannot be formatted or
// serialized into an audit record.
func TestOIDCVerifierRejectsImplausibleTimestamps(t *testing.T) {
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

	tests := []struct {
		name  string
		claim string
		value any
	}{
		{name: "issued at the dawn of time", claim: jwt.IssuedAt, value: -1e300},
		{name: "expiring past the end of it", claim: jwt.ExpirationTime, value: 1e300},
		{name: "not valid before the far future", claim: jwt.NotBefore, value: 1e19},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			claims := issuer.Claims(authtest.WithSubject("runner"), authtest.WithAudience("flowstate"))
			claims[test.claim] = test.value

			_, err := verifier.Verify(t.Context(), key.Sign(map[string]any{
				header.Type:      jwt.Type,
				header.Algorithm: jwa.ES256,
				header.KeyID:     key.ID(),
			}, claims))
			require.ErrorIs(t, err, auth.ErrMalformedToken)
		})
	}

	t.Run("an accepted principal can always be recorded", func(t *testing.T) {
		principal, err := verifier.Verify(t.Context(), issuer.MintToken(nil, authtest.WithSubject("runner"), authtest.WithAudience("flowstate")))
		require.NoError(t, err)

		recorded, err := json.Marshal(principal)
		require.NoError(t, err, "a Principal must be serializable for an audit record")
		require.Contains(t, string(recorded), "runner")
	})
}

// TestOIDCVerifierRejectsAlgorithmHeaderTricks checks that the algorithm
// allowlist is matched exactly. A verifier that normalized case, or accepted a
// non-string, would give an attacker a way to name an algorithm the policy
// believes it excluded.
func TestOIDCVerifierRejectsAlgorithmHeaderTricks(t *testing.T) {
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

	tests := []struct {
		name      string
		algorithm any
		wantErr   error
	}{
		{name: "lowercase", algorithm: "es256", wantErr: auth.ErrDisallowedAlgorithm},
		{name: "lowercase none", algorithm: "none", wantErr: auth.ErrDisallowedAlgorithm},
		{name: "uppercase none", algorithm: "NONE", wantErr: auth.ErrDisallowedAlgorithm},
		{name: "mixed case HMAC", algorithm: "Hs256", wantErr: auth.ErrDisallowedAlgorithm},
		{name: "an unknown algorithm", algorithm: "ES256K", wantErr: auth.ErrDisallowedAlgorithm},
		// A non-string algorithm is rendered as text when the token is parsed, so
		// it becomes a name no allowlist contains rather than a type error.
		{name: "a number", algorithm: 256, wantErr: auth.ErrDisallowedAlgorithm},
		{name: "a list", algorithm: []string{"ES256"}, wantErr: auth.ErrDisallowedAlgorithm},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			token := key.Sign(map[string]any{
				header.Type:      jwt.Type,
				header.Algorithm: test.algorithm,
				header.KeyID:     key.ID(),
			}, issuer.Claims(authtest.WithSubject("runner"), authtest.WithAudience("flowstate")))

			_, err := verifier.Verify(t.Context(), token)
			require.ErrorIs(t, err, test.wantErr)
		})
	}
}

// TestOIDCVerifierRejectsOversizedToken checks that the work an unauthenticated
// caller can ask for is bounded even when a Verifier is used directly, where no
// HTTP header limit applies.
func TestOIDCVerifierRejectsOversizedToken(t *testing.T) {
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

	claims := issuer.Claims(authtest.WithSubject("runner"), authtest.WithAudience("flowstate"))
	claims["padding"] = strings.Repeat("x", 128<<10)

	_, err := verifier.Verify(t.Context(), issuer.MintToken(claims))
	require.ErrorIs(t, err, auth.ErrMalformedToken)

	requests := issuer.Requests()
	discovery, jwks := requests.Discovery, requests.JWKS
	require.Zero(t, discovery, "an oversized token must be refused before any work is done for it")
	require.Zero(t, jwks)
}

// TestOIDCVerifierVerifiesEveryAdvertisedAlgorithm checks that every algorithm in
// the default allowlist can actually verify a token end to end.
//
// An algorithm that is advertised but unverifiable is worse than one that is
// absent: it turns a configuration choice into a signature failure that looks
// like an attack. ES384 is excluded from the allowlist for exactly this reason,
// and this test is what keeps that list honest.
func TestOIDCVerifierVerifiesEveryAdvertisedAlgorithm(t *testing.T) {
	for _, alg := range auth.DefaultAlgorithms() {
		t.Run(alg, func(t *testing.T) {
			var (
				key    = authtest.GenerateKey("signing", alg)
				clock  = authtest.NewClock(referenceTime)
				issuer = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))
			)

			verifier := newVerifier(t,
				auth.Policy{
					Issuers: []auth.TrustedIssuer{{
						Name:      "test",
						Issuer:    issuer.URL(),
						Audiences: []string{"flowstate"},
						// The policy allows only this algorithm, so the token can
						// only be verified the way it claims to be signed.
						Algorithms: []jwa.Algorithm{alg},
					}},
				},
				auth.WithClock(clock.Now),
			)

			principal, err := verifier.Verify(t.Context(), issuer.MintToken(nil, authtest.WithSubject("runner"), authtest.WithAudience("flowstate")))
			require.NoError(t, err, "algorithm %q is advertised by DefaultAlgorithms but cannot verify a token", alg)
			require.Equal(t, "runner", principal.Subject)

			// A tampered signature must still fail, so that a passing case above
			// cannot be a signature check that was skipped.
			_, err = verifier.Verify(t.Context(), tamperSignature(t, issuer.MintToken(nil, authtest.WithSubject("runner"), authtest.WithAudience("flowstate"))))
			require.Error(t, err, "algorithm %q accepted a tampered signature", alg)
		})
	}
}

// zeroPrincipalVerifier vouches for nobody while reporting success, which is what
// a buggy third-party [auth.Verifier] looks like.
type zeroPrincipalVerifier struct{}

// Verify returns no identity and no error.
func (zeroPrincipalVerifier) Verify(context.Context, string) (auth.Principal, error) {
	return auth.Principal{}, nil
}

// TestAuthenticatorRejectsVerifierWithNoIdentity checks that a Verifier reporting
// success without an identity is treated as a rejection. Reaching a handler with a
// principal that reads as unauthenticated is worse than being refused.
func TestAuthenticatorRejectsVerifierWithNoIdentity(t *testing.T) {
	server := serveAuthenticated(t, auth.NewAuthenticator(zeroPrincipalVerifier{}))

	status, body := callRPC(t, server, "Bearer anything")
	require.Equal(t, http.StatusUnauthorized, status)
	require.NotContains(t, body, "handler reached")
}

// TestAuthenticatorChallengesWithWWWAuthenticate checks for the RFC 6750
// challenge, which is how a client learns that its token, rather than its
// request, is the problem.
func TestAuthenticatorChallengesWithWWWAuthenticate(t *testing.T) {
	server := serveAuthenticated(t, auth.NewAuthenticator(nil))

	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		server.URL+"/flowstate.v1.WorkflowService/Run", strings.NewReader("{}"))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")

	response, err := server.Client().Do(request)
	require.NoError(t, err)
	t.Cleanup(func() { _ = response.Body.Close() })

	_, err = io.Copy(io.Discard, response.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusUnauthorized, response.StatusCode)
	require.Equal(t, `Bearer error="invalid_token"`, response.Header.Get("WWW-Authenticate"))
}
