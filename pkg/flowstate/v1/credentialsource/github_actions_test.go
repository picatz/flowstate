package credentialsource_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/jose/pkg/jwa"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/credentialsource"
)

// referenceTime anchors every test's clock, so what they assert does not
// depend on when they run.
var referenceTime = time.Date(2026, time.August, 14, 12, 0, 0, 0, time.UTC)

// TestGitHubActionsSource_MissingEnvironment_FailsClosed is the negative
// direction the package's whole promise rests on: outside a job granted
// id-token: write, there is no ambient identity to mint from, and a Source
// built for this name must say so rather than returning an empty, unusable
// Token with no error — which a caller could mistake for "anonymous is fine
// here".
func TestGitHubActionsSource_MissingEnvironment_FailsClosed(t *testing.T) {
	// Deliberately not calling newTestGitHubActionsSource, which sets these:
	// this test needs them absent, the way a job with no id-token: write
	// permission actually is.
	t.Setenv("ACTIONS_ID_TOKEN_REQUEST_URL", "")
	t.Setenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "")

	source, err := credentialsource.NewGitHubActionsSource("flowstate")
	require.NoError(t, err, "construction only validates the audience; the environment is checked when a token is asked for")

	token, err := source.Token(t.Context())
	require.Error(t, err)
	assert.ErrorIs(t, err, credentialsource.ErrSourceUnusable)
	assert.True(t, token.IsZero(), "a failed mint must not return a usable token alongside its error")
}

// TestGitHubActionsSource_NoAudience_FailsClosed asserts the audience is
// required at construction, before any network call — the same refusal
// [auth.Issuer.Mint] makes for an assertion with no audience, and for the
// same reason: a token minted for nobody in particular is a token any relying
// party would accept.
func TestGitHubActionsSource_NoAudience_FailsClosed(t *testing.T) {
	_, err := credentialsource.NewGitHubActionsSource("")
	require.Error(t, err)
	assert.ErrorIs(t, err, credentialsource.ErrSourceUnusable)
}

func TestGitHubActionsSource_RefusesUnprotectedEndpoint(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests++
	}))
	t.Cleanup(server.Close)
	t.Setenv("ACTIONS_ID_TOKEN_REQUEST_URL", server.URL)
	t.Setenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "runner-request-token")

	source, err := credentialsource.NewGitHubActionsSource("flowstate")
	require.NoError(t, err)
	_, err = source.Token(t.Context())
	require.Error(t, err)
	assert.ErrorIs(t, err, credentialsource.ErrSourceUnusable)
	assert.Zero(t, requests, "the runner credential must not be sent over plaintext HTTP")
}

func TestGitHubActionsSource_RefusesRedirect(t *testing.T) {
	finalRequests := 0
	minted := mintedJWT(t, referenceTime.Add(time.Hour))
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/start" {
			http.Redirect(w, r, "/final", http.StatusFound)
			return
		}
		finalRequests++
		_ = json.NewEncoder(w).Encode(map[string]string{"value": minted})
	}))
	t.Cleanup(server.Close)
	t.Setenv("ACTIONS_ID_TOKEN_REQUEST_URL", server.URL+"/start")
	t.Setenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "runner-request-token")

	source, err := credentialsource.NewGitHubActionsSource("flowstate",
		credentialsource.WithGitHubActionsHTTPClient(server.Client()))
	require.NoError(t, err)
	_, err = source.Token(t.Context())
	require.Error(t, err)
	assert.ErrorIs(t, err, credentialsource.ErrSourceUnusable)
	assert.Zero(t, finalRequests, "the runner credential must not be sent to a redirect target")
}

// TestGitHubActionsSource_RequestsTheConfiguredAudience proves the audience a
// caller configured is the one actually sent to the runner, not a default
// the runner would otherwise pick — the request-side half of "a token
// acquired for one audience must not satisfy another".
func TestGitHubActionsSource_RequestsTheConfiguredAudience(t *testing.T) {
	server, requests := stubGitHubActionsEndpoint(t, mintedJWT(t, referenceTime.Add(time.Hour)))
	defer server.Close()

	source := newTestGitHubActionsSource(t, server.URL, "https://flowstate.example.com/prod", nil)

	_, err := source.Token(t.Context())
	require.NoError(t, err)

	assert.Equal(t, "https://flowstate.example.com/prod", requests.lastAudience())
}

// TestGitHubActionsSource_TokenAcquiredForOneAudienceRefusedForAnother is the
// negative direction end to end: a token this source acquires, addressed to
// one audience, is refused by a verifier that trusts the issuer but requires
// a different audience — the same refusal [auth.OIDCVerifier] gives any
// mis-addressed token, proving this source does not somehow produce something
// looser.
func TestGitHubActionsSource_TokenAcquiredForOneAudienceRefusedForAnother(t *testing.T) {
	issuer := authtest.NewIssuer(authtest.WithKeys(authtest.GenerateKey("gha-stub", jwa.RS256)))
	defer func() { _ = issuer.Close() }()

	// A raw JWT string shaped like a real one, minted by the same key the
	// verifier below will fetch from this issuer's JWKS endpoint.
	rawToken := issuer.MintToken(map[string]any{
		"repository": "acme/infra",
	}, authtest.WithAudience("https://flowstate.example.com/prod"))

	server, _ := stubGitHubActionsEndpoint(t, rawToken)
	defer server.Close()

	source := newTestGitHubActionsSource(t, server.URL, "https://flowstate.example.com/prod", nil)

	token, err := source.Token(t.Context())
	require.NoError(t, err)
	bearer, ok := token.Bearer()
	require.True(t, ok)

	verifier, err := auth.NewOIDCVerifier(auth.Policy{
		Issuers: []auth.TrustedIssuer{{
			Name:      "gha-stub",
			Issuer:    issuer.URL(),
			Audiences: []string{"https://flowstate.example.com/staging"},
			Namespace: "infra",
		}},
	})
	require.NoError(t, err)

	_, err = verifier.Verify(t.Context(), bearer)
	require.Error(t, err, "a token acquired for the prod audience must not verify against a policy requiring staging")
	assert.ErrorIs(t, err, auth.ErrInvalidAudience)

	// And it does verify against the audience it was actually acquired for,
	// proving the refusal above is about the audience and nothing else.
	sameAudienceVerifier, err := auth.NewOIDCVerifier(auth.Policy{
		Issuers: []auth.TrustedIssuer{{
			Name:      "gha-stub",
			Issuer:    issuer.URL(),
			Audiences: []string{"https://flowstate.example.com/prod"},
			Namespace: "infra",
		}},
	})
	require.NoError(t, err)

	_, err = sameAudienceVerifier.Verify(t.Context(), bearer)
	require.NoError(t, err)
}

// TestGitHubActionsSource_CachesUntilTheRefreshMargin proves a token is not
// re-minted on every call while it remains fresh, and that it is re-minted
// once the fake clock crosses the margin — the acquisition-side half of the
// mid-script expiry problem: a caller polling for minutes must never be
// handed a token known to be about to die.
func TestGitHubActionsSource_CachesUntilTheRefreshMargin(t *testing.T) {
	server, requests := stubGitHubActionsEndpoint(t, mintedJWT(t, referenceTime.Add(10*time.Minute)))
	defer server.Close()

	now := referenceTime
	clock := func() time.Time { return now }

	source := newTestGitHubActionsSource(t, server.URL, "flowstate", clock)

	_, err := source.Token(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, requests.count())

	// Still well within the token's lifetime: no second mint.
	now = referenceTime.Add(2 * time.Minute)
	_, err = source.Token(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, requests.count(), "a token far from its margin must be served from cache")

	// Inside the default one-minute margin of the 10-minute token: re-mint.
	now = referenceTime.Add(9*time.Minute + 30*time.Second)
	_, err = source.Token(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 2, requests.count(), "a token inside its refresh margin must be re-minted, not served stale")
}

// TestGitHubActionsSource_ExpiredCachedToken_NeverReturnedAsValid is the
// sharpest negative case: force the cache to hold a token whose expiry has
// already passed relative to the clock, and require that a subsequent
// re-mint failure surfaces as an error rather than handing back the expired
// cached value. A cache that falls back to "whatever I had" on a mint error
// is a cache that can hand an already-dead token to a caller who has no way
// to tell.
func TestGitHubActionsSource_ExpiredCachedToken_NeverReturnedAsValid(t *testing.T) {
	now := referenceTime
	clock := func() time.Time { return now }

	server, requests := stubGitHubActionsEndpoint(t, mintedJWT(t, referenceTime.Add(time.Minute)))
	defer server.Close()

	source := newTestGitHubActionsSource(t, server.URL, "flowstate", clock)

	token, err := source.Token(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, requests.count())
	assert.False(t, token.Expired(now), "the token just minted must not already read as expired")

	// The clock advances well past the token's expiry, and the runner's
	// endpoint (env vars unset from here on) becomes unreachable — the shape
	// of a runner whose job has ended mid-poll.
	now = referenceTime.Add(time.Hour)
	t.Setenv("ACTIONS_ID_TOKEN_REQUEST_URL", "")
	t.Setenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "")

	failed, err := source.Token(t.Context())
	require.Error(t, err, "a source whose cached token is past its margin and cannot re-mint must fail, not return the stale token")
	assert.ErrorIs(t, err, credentialsource.ErrSourceUnusable)
	assert.True(t, failed.IsZero())
}

// TestGitHubActionsSource_TruncatedResponse_FailsClosed proves the response
// bound actually refuses an oversized body rather than reading it into
// memory in full, per this repository's rule that anything reading from the
// network is bounded by bytes.
func TestGitHubActionsSource_TruncatedResponse_FailsClosed(t *testing.T) {
	huge := `{"value":"` + string(make([]byte, 1<<20)) + `"}`

	server, _ := stubGitHubActionsEndpoint(t, huge)
	defer server.Close()

	source := newTestGitHubActionsSource(t, server.URL, "flowstate", nil)

	_, err := source.Token(t.Context())
	require.Error(t, err)
	assert.ErrorIs(t, err, credentialsource.ErrSourceUnusable)
}

// TestGitHubActionsSource_MalformedResponse_FailsClosed proves a well-formed
// {"value": ...} envelope around something that is not a parseable JWT is
// refused rather than cached as a token with no known expiry — which would
// otherwise defeat the whole point of tracking expiry in the first place.
func TestGitHubActionsSource_MalformedResponse_FailsClosed(t *testing.T) {
	server, _ := stubGitHubActionsEndpoint(t, "not a jwt, and not json either")
	defer server.Close()

	source := newTestGitHubActionsSource(t, server.URL, "flowstate", nil)

	_, err := source.Token(t.Context())
	require.Error(t, err, "a value that is not a parseable JWT must be refused, not cached as if it had no expiry")
	assert.ErrorIs(t, err, credentialsource.ErrSourceUnusable)
}
