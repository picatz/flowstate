package main

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/stretchr/testify/require"
)

// TestIdentityDocumentsAreReachableWithoutCredentials is the regression guard for
// the mistake this routing exists to prevent.
//
// A relying party fetches the discovery document and the key set before it holds any
// credential. If they end up behind authentication, federation stops working and the
// symptom — rejected assertions — points at signing rather than at a route. So this
// asserts the property directly: the documents answer with no credential, and the API
// still refuses one.
func TestIdentityDocumentsAreReachableWithoutCredentials(t *testing.T) {
	t.Parallel()

	broker := testBroker(t)

	// A verifier that refuses everything, so an authenticated route answering at
	// all would mean the middleware was not applied.
	handler := serverHandler(refusingVerifier{}, broker, http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"api":"reached"}`))
		},
	))

	server := httptest.NewServer(handler)
	defer server.Close()

	t.Run("discovery answers without a credential", func(t *testing.T) {
		response, err := server.Client().Get(server.URL + auth.DiscoveryPath)
		require.NoError(t, err)
		defer response.Body.Close()

		require.Equal(t, http.StatusOK, response.StatusCode,
			"the discovery document must be reachable unauthenticated; a relying party "+
				"fetches it before it has any credential to present")

		var document map[string]any
		require.NoError(t, json.NewDecoder(response.Body).Decode(&document))
		require.NotEmpty(t, document["issuer"], "discovery document should name its issuer")
		require.NotEmpty(t, document["jwks_uri"], "discovery document should name its key set")
	})

	t.Run("the key set answers without a credential", func(t *testing.T) {
		response, err := server.Client().Get(server.URL + broker.Issuer().JWKSPath())
		require.NoError(t, err)
		defer response.Body.Close()

		require.Equal(t, http.StatusOK, response.StatusCode,
			"the key set must be reachable unauthenticated; it contains only public keys")

		var document map[string]any
		require.NoError(t, json.NewDecoder(response.Body).Decode(&document))
		require.Contains(t, document, "keys")
	})

	t.Run("the API still refuses an unauthenticated caller", func(t *testing.T) {
		// The other half of the property. Without this, a handler that simply
		// applied no authentication anywhere would pass the two tests above.
		response, err := server.Client().Get(server.URL + "/flowstate.v1.WorkflowService/Run")
		require.NoError(t, err)
		defer response.Body.Close()

		require.NotEqual(t, http.StatusOK, response.StatusCode,
			"the API must not answer an unauthenticated caller; if it does, the "+
				"middleware is not wrapping the default route")
	})
}

// TestNoUnauthenticatedRoutesWithoutFederation checks that a deployment which does
// not federate outward exposes nothing unauthenticated at all.
func TestNoUnauthenticatedRoutesWithoutFederation(t *testing.T) {
	t.Parallel()

	handler := serverHandler(refusingVerifier{}, nil, http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) },
	))

	server := httptest.NewServer(handler)
	defer server.Close()

	for _, path := range []string{auth.DiscoveryPath, auth.DefaultJWKSPath, "/"} {
		response, err := server.Client().Get(server.URL + path)
		require.NoError(t, err)
		response.Body.Close()

		require.NotEqual(t, http.StatusOK, response.StatusCode,
			"%s answered without a credential on a deployment that issues no assertions", path)
	}
}

// refusingVerifier rejects every credential, so any route that answers proves the
// authentication middleware was not applied to it.
type refusingVerifier struct{}

func (refusingVerifier) Verify(context.Context, string) (auth.Principal, error) {
	return auth.Principal{}, auth.ErrMalformedToken
}

// testBroker builds a broker with a throwaway signing key.
func testBroker(t *testing.T) *auth.Broker {
	t.Helper()

	_, private, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	key, err := auth.NewSigningKey("test-key", private)
	require.NoError(t, err)

	issuer, err := auth.NewIssuer("https://flowstate.test", key)
	require.NoError(t, err)

	broker, err := auth.NewBroker(issuer)
	require.NoError(t, err)

	return broker
}

// TestHealthzAnswersWithoutCredentialsAndWithoutInformation pins both halves of
// the liveness route: a prober holding no credential gets its status code, and
// gets nothing else — an unauthenticated endpoint that described the deployment
// would be reconnaissance served on request.
func TestHealthzAnswersWithoutCredentialsAndWithoutInformation(t *testing.T) {
	t.Parallel()

	handler := serverHandler(refusingVerifier{}, nil, http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			t.Error("a health probe reached the RPC handler")
		}))

	server := httptest.NewServer(handler)
	defer server.Close()

	resp, err := http.Get(server.URL + "/healthz")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode,
		"a load balancer probing before it holds any credential was refused")

	var body [64]byte
	n, _ := resp.Body.Read(body[:])
	require.Zero(t, n, "the health endpoint answered with content: %q", body[:n])

	// The method discipline the identity documents' handler keeps.
	post, err := http.Post(server.URL+"/healthz", "text/plain", nil)
	require.NoError(t, err)
	defer post.Body.Close()
	require.Equal(t, http.StatusMethodNotAllowed, post.StatusCode)
}
