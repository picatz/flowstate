package credentialsource_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/picatz/jose/pkg/jwa"

	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/credentialsource"
)

// requestCounter records how many times a stub endpoint was asked for a
// token, and the audience each request named — the two facts the tests below
// need and a real runner does not expose.
type requestCounter struct {
	mu        sync.Mutex
	n         atomic.Int64
	audiences []string
}

func (c *requestCounter) record(audience string) {
	c.n.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.audiences = append(c.audiences, audience)
}

func (c *requestCounter) count() int { return int(c.n.Load()) }

func (c *requestCounter) lastAudience() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.audiences) == 0 {
		return ""
	}
	return c.audiences[len(c.audiences)-1]
}

// stubGitHubActionsEndpoint starts an HTTP server shaped like the runner's
// token endpoint: it checks the bearer request token, records the requested
// audience, and returns the given token as the endpoint's documented
// {"value": "..."} body.
func stubGitHubActionsEndpoint(t *testing.T, token string) (*httptest.Server, *requestCounter) {
	t.Helper()

	counter := &requestCounter{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer runner-request-token" {
			http.Error(w, "missing or wrong runner request token", http.StatusUnauthorized)
			return
		}

		counter.record(r.URL.Query().Get("audience"))

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"value": token})
	}))
	t.Cleanup(server.Close)

	return server, counter
}

// mintedJWT returns a token shaped like a real GitHub Actions OIDC token: a
// real JWS signature over a claims set carrying "exp", using a throwaway key
// this test generates. credentialsource never checks the signature — the
// server does that on arrival — so any key that produces a well-formed JWT
// exercises the same code path a real one would.
func mintedJWT(t *testing.T, expiresAt time.Time) string {
	t.Helper()

	key := authtest.GenerateKey("stub-runner-key", jwa.RS256)

	return key.Sign(
		map[string]any{"typ": "JWT", "alg": "RS256", "kid": key.ID()},
		map[string]any{
			"iss": "https://token.actions.githubusercontent.com",
			"sub": "repo:acme/infra:ref:refs/heads/main",
			"aud": "flowstate",
			"exp": expiresAt.Unix(),
			"iat": time.Now().Unix(),
		},
	)
}

// newTestGitHubActionsSource points a github-actions [credentialsource.Source]
// at a stub endpoint, with the runner's request env vars set for the
// duration of the test.
func newTestGitHubActionsSource(t *testing.T, endpoint, audience string, clock func() time.Time) credentialsource.Source {
	t.Helper()

	t.Setenv("ACTIONS_ID_TOKEN_REQUEST_URL", endpoint)
	t.Setenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "runner-request-token")

	opts := []credentialsource.GitHubActionsOption{}
	if clock != nil {
		opts = append(opts, credentialsource.WithGitHubActionsClock(clock))
	}

	source, err := credentialsource.NewGitHubActionsSource(audience, opts...)
	if err != nil {
		t.Fatalf("NewGitHubActionsSource: %v", err)
	}

	return source
}
