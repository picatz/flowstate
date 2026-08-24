package auth_test

import (
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/picatz/jose/pkg/header"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/picatz/jose/pkg/jwt"
	"github.com/stretchr/testify/require"
)

// TestOIDCVerifierMalformedClaims covers claims whose JSON types are not what a
// JWT requires. A trusted issuer should never send these, but a compromised or
// buggy one might, and the verifier has to refuse them rather than misread them.
func TestOIDCVerifierMalformedClaims(t *testing.T) {
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
				Require:   []auth.ClaimRule{auth.RequireClaim("tenant", "acme")},
			}},
		},
		auth.WithClock(clock.Now),
	)

	// sign builds a token with arbitrary claim values, bypassing the checks the
	// JOSE builder applies to registered claims.
	sign := func(t *testing.T, spoil func(claims map[string]any)) string {
		t.Helper()

		claims := issuer.Claims(authtest.WithSubject("runner"), authtest.WithAudience("flowstate"))
		claims["tenant"] = "acme"
		spoil(claims)

		return key.Sign(map[string]any{
			header.Type:      jwt.Type,
			header.Algorithm: jwa.ES256,
			header.KeyID:     key.ID(),
		}, claims)
	}

	tests := []struct {
		name    string
		spoil   func(claims map[string]any)
		wantErr error
	}{
		{
			name:    "audience is a number",
			spoil:   func(claims map[string]any) { claims[jwt.Audience] = 42 },
			wantErr: auth.ErrMalformedToken,
		},
		{
			name:    "audience list holds a number",
			spoil:   func(claims map[string]any) { claims[jwt.Audience] = []any{"flowstate", 42} },
			wantErr: auth.ErrMalformedToken,
		},
		{
			name:    "audience is empty",
			spoil:   func(claims map[string]any) { claims[jwt.Audience] = "" },
			wantErr: auth.ErrMissingClaim,
		},
		{
			name:    "audience list holds only empty values",
			spoil:   func(claims map[string]any) { claims[jwt.Audience] = []any{"", ""} },
			wantErr: auth.ErrMissingClaim,
		},
		{
			name:    "not-before is a string",
			spoil:   func(claims map[string]any) { claims[jwt.NotBefore] = "yesterday" },
			wantErr: auth.ErrMalformedToken,
		},
		{
			name:    "issuer is a number",
			spoil:   func(claims map[string]any) { claims[jwt.Issuer] = 42 },
			wantErr: auth.ErrMalformedToken,
		},
		{
			name:    "issuer is empty",
			spoil:   func(claims map[string]any) { claims[jwt.Issuer] = "" },
			wantErr: auth.ErrMissingClaim,
		},
		{
			name:    "subject is empty",
			spoil:   func(claims map[string]any) { claims[jwt.Subject] = "" },
			wantErr: auth.ErrMissingClaim,
		},
		{
			name: "a claim rule cannot be satisfied by an object",
			spoil: func(claims map[string]any) {
				claims["tenant"] = map[string]any{"name": "acme"}
			},
			wantErr: auth.ErrClaimMismatch,
		},
		{
			name: "a claim rule cannot be satisfied by null",
			spoil: func(claims map[string]any) {
				claims["tenant"] = nil
			},
			wantErr: auth.ErrClaimMismatch,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			principal, err := verifier.Verify(t.Context(), sign(t, test.spoil))

			require.ErrorIs(t, err, test.wantErr)
			require.True(t, principal.IsZero())
		})
	}
}

// TestOIDCVerifierBoundsClaimValuesInErrors checks that a claim value from a
// trusted-but-misbehaving issuer cannot flood an operator's logs through an error
// message.
func TestOIDCVerifierBoundsClaimValuesInErrors(t *testing.T) {
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
				Require:   []auth.ClaimRule{auth.RequireClaim("tenant", "acme")},
			}},
		},
		auth.WithClock(clock.Now),
	)

	claims := issuer.Claims(authtest.WithSubject("runner"), authtest.WithAudience("flowstate"))
	claims["tenant"] = strings.Repeat("padding", 1_000)

	_, err := verifier.Verify(t.Context(), issuer.MintToken(claims))
	require.ErrorIs(t, err, auth.ErrClaimMismatch)

	var mismatch *auth.ClaimMismatchError
	require.ErrorAs(t, err, &mismatch)
	require.Less(t, len(mismatch.Got), 200, "a claim value in an error must be bounded")
	require.True(t, strings.HasSuffix(mismatch.Got, "..."), "a truncated value should say so, got %q", mismatch.Got)
	require.Less(t, len(err.Error()), 500)
}

// countingTransport records how many requests an HTTP client makes.
type countingTransport struct {
	requests atomic.Int64
	next     http.RoundTripper
}

// RoundTrip implements [http.RoundTripper].
func (t *countingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.requests.Add(1)
	return t.next.RoundTrip(req)
}

// TestOIDCVerifierWithHTTPClient checks that a caller-supplied HTTP client is the
// one used to reach an issuer, which is how a deployment adds a proxy, custom
// roots, or instrumentation.
func TestOIDCVerifierWithHTTPClient(t *testing.T) {
	var (
		key    = authtest.GenerateKey("primary", jwa.ES256)
		clock  = authtest.NewClock(referenceTime)
		issuer = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))
	)

	transport := &countingTransport{next: http.DefaultTransport}

	verifier := newVerifierWithClient(t,
		auth.Policy{
			Issuers: []auth.TrustedIssuer{{
				Name:      "test",
				Issuer:    issuer.URL(),
				Audiences: []string{"flowstate"},
			}},
		},
		auth.WithClock(clock.Now),
		auth.WithHTTPClient(&http.Client{Transport: transport}),
	)

	_, err := verifier.Verify(t.Context(), issuer.MintToken(nil, authtest.WithSubject("runner"), authtest.WithAudience("flowstate")))
	require.NoError(t, err)

	// One discovery request and one key set request, both through our client.
	require.Equal(t, int64(2), transport.requests.Load())

	// A nil client is ignored rather than replacing the boundary with nothing:
	// the egress policy is still the one in force, so this fetch is still made
	// under it.
	verifier = newVerifier(t,
		auth.Policy{
			Issuers: []auth.TrustedIssuer{{
				Name:      "test",
				Issuer:    issuer.URL(),
				Audiences: []string{"flowstate"},
			}},
		},
		auth.WithClock(clock.Now),
		auth.WithHTTPClient(nil),
	)

	_, err = verifier.Verify(t.Context(), issuer.MintToken(nil, authtest.WithSubject("runner"), authtest.WithAudience("flowstate")))
	require.NoError(t, err)
}
