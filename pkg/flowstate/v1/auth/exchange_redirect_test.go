package auth_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
)

// An exchange sends a signed assertion, and it sends it in the request *body*.
//
// That is the difference between this path and fetching an issuer's keys, which
// shares the same client. A key set is a GET carrying nothing, so following a
// redirect anywhere is ordinary and the only thing worth refusing is a downgrade
// off https — which is what the shared guard checks, correctly, for that use.
//
// A token exchange is a POST whose body is a bearer credential. Go strips the
// Authorization header across hosts and has nothing to say about a body, and
// 307 and 308 are defined to replay one. So a scheme check alone let the
// configured endpoint name any other https host and have the assertion delivered
// to it.
//
// No redirect at all is the answer rather than pinning the host, because a token
// endpoint does not redirect: RFC 8693 has no such step, and 301, 302 and 303 on
// a POST are turned into a bodyless GET by net/http, so an exchange that met one
// was already failing. What was reachable was exactly the pair that leaks.

// TestAnExchangeDoesNotReplayTheAssertionToAnotherHost is the leak.
//
// Two servers: the one an operator configured, and one it chooses to point at.
// The second must never see the assertion, and the operator must be told why
// rather than left with a transport error.
func TestAnExchangeDoesNotReplayTheAssertionToAnotherHost(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	elsewhere := newRelyingParty(t, func(w http.ResponseWriter, r *http.Request, body recordedRequest) {
		writeJSON(t, w, http.StatusOK, map[string]any{
			"access_token":      "a token the operator never asked anyone for",
			"issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
			"token_type":        "Bearer",
			"expires_in":        3600,
		})
	})

	configured := newRelyingParty(t, func(w http.ResponseWriter, r *http.Request, body recordedRequest) {
		// 307 rather than 302, because 302 on a POST becomes a bodyless GET and
		// would prove nothing: the assertion has to survive the hop for there to
		// be anything to leak.
		http.Redirect(w, r, elsewhere.url+"/token", http.StatusTemporaryRedirect)
	})

	exchanger, err := auth.NewTokenExchanger(auth.TokenExchangeConfig{
		TokenURL:       configured.url + "/token",
		Audience:       "https://as.example.com",
		TargetAudience: "https://api.partner.example.com",
		Clock:          clock.Now,
	})
	require.NoError(t, err)

	assertion := mintAssertion(t, issuer, exchanger.Requirement().Audience)

	_, err = exchanger.Exchange(t.Context(), assertion)
	require.Error(t, err, "an exchange followed a redirect off the endpoint it was configured with")

	for _, received := range elsewhere.received() {
		assert.NotContains(t, received.form, "subject_token",
			"the assertion was replayed to a host other than the configured endpoint")
	}
	assert.Empty(t, elsewhere.received(),
		"a host the operator never configured was contacted at all")

	assert.ErrorIs(t, err, auth.ErrExchangeFailed,
		"the refusal is not reported as an exchange failure, so a caller cannot tell it "+
			"from a transport error")
	assert.Contains(t, err.Error(), "redirect",
		"the operator is not told that a redirect is what stopped this, which is the one "+
			"thing they would need to change")
}

// TestFetchingKeysStillFollowsARedirect is the other half, and the reason this is
// two clients rather than one rule.
//
// Issuers do move their key sets, and a verifier that refused to follow would
// stop verifying anything for an issuer that had done so — a fail-closed choice
// with no security to buy, because a key set is public and the request carries
// nothing. Refusing redirects everywhere would have been the tidier change and
// the wrong one.
func TestFetchingKeysStillFollowsARedirect(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, keys := newIssuer(t, clock)

	// Where the key set actually lives now: the issuer's own server, which is
	// still serving it at the path it always did.
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 302, the ordinary "this moved" answer to a GET.
		http.Redirect(w, r, keys.URL+r.URL.Path, http.StatusFound)
	}))
	t.Cleanup(origin.Close)

	verifier, err := auth.NewOIDCVerifier(auth.Policy{Issuers: []auth.TrustedIssuer{{
		Name:      "idp",
		Issuer:    keys.URL,
		Audiences: []string{"https://api.example.com"},
		JWKSURL:   origin.URL + "/.well-known/jwks.json",
	}}}, auth.WithClock(clock.Now))
	require.NoError(t, err)

	assertion := mintAssertion(t, issuer, "https://api.example.com")

	_, err = verifier.Verify(t.Context(), assertion.Token())
	require.NoError(t, err, "a key set that had moved could no longer be fetched")
}
