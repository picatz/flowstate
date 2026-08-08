package auth_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/authn"
	"connectrpc.com/connect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/picatz/jose/pkg/jwt"
	"github.com/stretchr/testify/require"
)

// authenticatedResponse is what the test handler reports about its caller.
type authenticatedResponse struct {
	ID        string `json:"id"`
	Role      string `json:"role"`
	Anonymous bool   `json:"anonymous"`
}

// serveAuthenticated starts an HTTP server that authenticates requests the way
// the Flowstate API server does, and reports the caller each request
// authenticated as.
func serveAuthenticated(t *testing.T, authenticator *auth.Authenticator) *httptest.Server {
	t.Helper()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := auth.PrincipalFromContext(r.Context())
		if !ok {
			// The middleware must never let an unauthenticated request through.
			http.Error(w, "handler reached without a principal", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(authenticatedResponse{
			ID:        principal.ID(),
			Role:      principal.Role,
			Anonymous: principal.IsAnonymous(),
		})
	})

	server := httptest.NewServer(authn.NewMiddleware(authenticator.Authenticate).Wrap(handler))
	t.Cleanup(server.Close)

	return server
}

// callRPC makes a request shaped like a Connect unary call, with the given
// Authorization header verbatim. An empty header is not sent at all.
func callRPC(t *testing.T, server *httptest.Server, authorization string) (int, string) {
	t.Helper()

	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		server.URL+"/flowstate.v1.WorkflowService/Run", strings.NewReader("{}"))
	require.NoError(t, err)

	request.Header.Set("Content-Type", "application/json")
	if authorization != "" {
		request.Header.Set("Authorization", authorization)
	}

	response, err := server.Client().Do(request)
	require.NoError(t, err)
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)

	return response.StatusCode, string(body)
}

// TestAuthenticator covers the middleware end to end: which requests reach a
// handler, which are refused, and what a refused caller is told.
func TestAuthenticator(t *testing.T) {
	var (
		key    = authtest.GenerateKey("primary", jwa.ES256)
		clock  = authtest.NewClock(referenceTime)
		issuer = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))
	)

	verifier := newVerifier(t,
		auth.Policy{
			Issuers: []auth.TrustedIssuer{{
				Name:      "idp",
				Issuer:    issuer.URL(),
				Audiences: []string{"flowstate"},
				Require:   []auth.ClaimRule{auth.RequireClaim("repository", "picatz/flowstate")},
				Role:      "operator",
			}},
		},
		auth.WithClock(clock.Now),
	)

	var (
		observed   []error
		observedMu sync.Mutex
	)
	authenticator := auth.NewAuthenticator(verifier,
		auth.WithFailureObserver(func(ctx context.Context, req *http.Request, err error) {
			observedMu.Lock()
			defer observedMu.Unlock()
			observed = append(observed, err)
		}),
	)

	server := serveAuthenticated(t, authenticator)

	validClaims := func() map[string]any {
		claims := issuer.Claims(authtest.WithSubject("runner"), authtest.WithAudience("flowstate"))
		claims["repository"] = "picatz/flowstate"
		return claims
	}

	tests := []struct {
		name          string
		authorization func(t *testing.T) string
		wantStatus    int
		wantMessage   string
		// mustNotContain are strings from the trust policy that an
		// unauthenticated caller must not be able to learn from the response.
		mustNotContain []string
	}{
		{
			name: "valid token",
			authorization: func(t *testing.T) string {
				return "Bearer " + issuer.MintToken(validClaims())
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "lowercase bearer scheme, which RFC 9110 allows",
			authorization: func(t *testing.T) string {
				return "bearer " + issuer.MintToken(validClaims())
			},
			wantStatus: http.StatusOK,
		},
		{
			name:          "no Authorization header",
			authorization: func(*testing.T) string { return "" },
			wantStatus:    http.StatusUnauthorized,
			wantMessage:   "missing bearer token",
		},
		{
			name:          "basic authentication",
			authorization: func(*testing.T) string { return "Basic dXNlcjpwYXNzd29yZA==" },
			wantStatus:    http.StatusUnauthorized,
			wantMessage:   "missing bearer token",
		},
		{
			name:          "bearer scheme with no token",
			authorization: func(*testing.T) string { return "Bearer " },
			wantStatus:    http.StatusUnauthorized,
			wantMessage:   "missing bearer token",
		},
		{
			name:          "token in the wrong place in the header",
			authorization: func(*testing.T) string { return "Token abc.def.ghi" },
			wantStatus:    http.StatusUnauthorized,
			wantMessage:   "missing bearer token",
		},
		{
			name:          "not a token at all",
			authorization: func(*testing.T) string { return "Bearer hunter2" },
			wantStatus:    http.StatusUnauthorized,
			wantMessage:   "malformed token",
		},
		{
			name: "expired token",
			authorization: func(t *testing.T) string {
				claims := validClaims()
				claims[jwt.ExpirationTime] = referenceTime.Add(-time.Hour).Unix()
				return "Bearer " + issuer.MintToken(claims)
			},
			wantStatus:  http.StatusUnauthorized,
			wantMessage: "token is expired",
		},
		{
			name: "token for another audience",
			authorization: func(t *testing.T) string {
				claims := validClaims()
				claims[jwt.Audience] = "some-other-service"
				return "Bearer " + issuer.MintToken(claims)
			},
			wantStatus:  http.StatusUnauthorized,
			wantMessage: "token audience is not accepted",
			// The response must not disclose which audience would have worked.
			mustNotContain: []string{"flowstate"},
		},
		{
			name: "token whose claims the policy refuses",
			authorization: func(t *testing.T) string {
				claims := validClaims()
				claims["repository"] = "attacker/fork"
				return "Bearer " + issuer.MintToken(claims)
			},
			wantStatus:  http.StatusUnauthorized,
			wantMessage: "token is not accepted by the trust policy",
			// Nor which claim, nor what value it must have.
			mustNotContain: []string{"repository", "picatz/flowstate"},
		},
		{
			name: "token from an issuer we do not trust",
			authorization: func(t *testing.T) string {
				claims := validClaims()
				claims[jwt.Issuer] = "https://issuer.invalid"
				return "Bearer " + issuer.MintToken(claims)
			},
			wantStatus:  http.StatusUnauthorized,
			wantMessage: "untrusted token issuer",
		},
		{
			name: "tampered token",
			authorization: func(t *testing.T) string {
				return "Bearer " + tamperSignature(t, issuer.MintToken(validClaims()))
			},
			wantStatus:  http.StatusUnauthorized,
			wantMessage: "invalid token signature",
		},
		{
			name: "unsigned token",
			authorization: func(t *testing.T) string {
				return "Bearer " + noneToken(t, validClaims())
			},
			wantStatus: http.StatusUnauthorized,
			// An unsigned token is reported the same way as a bad signature, so
			// a caller learns nothing from the difference.
			wantMessage: "invalid token signature",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status, body := callRPC(t, server, test.authorization(t))

			require.Equal(t, test.wantStatus, status)

			if test.wantStatus == http.StatusOK {
				var response authenticatedResponse
				require.NoError(t, json.Unmarshal([]byte(body), &response))
				require.Equal(t, issuer.URL()+"#runner", response.ID)
				require.Equal(t, "operator", response.Role)
				require.False(t, response.Anonymous)
				return
			}

			// Connect renders the error as JSON with a machine-readable code.
			var wireError struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			}
			require.NoError(t, json.Unmarshal([]byte(body), &wireError))
			require.Equal(t, connect.CodeUnauthenticated.String(), wireError.Code)
			require.Equal(t, test.wantMessage, wireError.Message)

			for _, secret := range test.mustNotContain {
				require.NotContains(t, body, secret, "the response must not describe the trust policy")
			}
		})
	}

	// Every rejection above reached the observer, which is how an operator sees
	// the detail that the caller was not told.
	observedMu.Lock()
	defer observedMu.Unlock()
	require.NotEmpty(t, observed)
	require.ErrorIs(t, observed[0], auth.ErrNoToken)
}

// TestAuthenticatorWithoutVerifier checks that an Authenticator with no verifier
// rejects everything, so that leaving authentication unconfigured cannot leave
// the API open.
func TestAuthenticatorWithoutVerifier(t *testing.T) {
	var (
		key    = authtest.GenerateKey("primary", jwa.ES256)
		clock  = authtest.NewClock(referenceTime)
		issuer = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))
	)

	tests := []struct {
		name          string
		authenticator *auth.Authenticator
	}{
		{
			name:          "constructed with a nil verifier",
			authenticator: auth.NewAuthenticator(nil),
		},
		{
			name:          "zero value",
			authenticator: &auth.Authenticator{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := serveAuthenticated(t, test.authenticator)

			// Even a well-formed token from a real issuer is refused.
			token := issuer.MintToken(nil, authtest.WithSubject("runner"), authtest.WithAudience("flowstate"))

			status, body := callRPC(t, server, "Bearer "+token)
			require.Equal(t, http.StatusUnauthorized, status)
			require.Contains(t, body, connect.CodeUnauthenticated.String())

			status, _ = callRPC(t, server, "")
			require.Equal(t, http.StatusUnauthorized, status)
		})
	}
}

// TestAuthenticatorWithUnavailableIssuer checks that an issuer Flowstate cannot
// reach fails requests closed, and says so in a way that distinguishes an
// infrastructure problem from a bad credential.
func TestAuthenticatorWithUnavailableIssuer(t *testing.T) {
	var (
		key    = authtest.GenerateKey("primary", jwa.ES256)
		clock  = authtest.NewClock(referenceTime)
		issuer = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))
	)

	issuer.SetKeySetResponse(http.StatusServiceUnavailable, nil)

	verifier := newVerifier(t,
		auth.Policy{
			Issuers: []auth.TrustedIssuer{{
				Name:      "idp",
				Issuer:    issuer.URL(),
				Audiences: []string{"flowstate"},
			}},
		},
		auth.WithClock(clock.Now),
	)

	server := serveAuthenticated(t, auth.NewAuthenticator(verifier))

	token := issuer.MintToken(nil, authtest.WithSubject("runner"), authtest.WithAudience("flowstate"))

	status, body := callRPC(t, server, "Bearer "+token)
	require.Equal(t, http.StatusUnauthorized, status)
	require.Contains(t, body, "issuer keys are temporarily unavailable")
}

// TestInsecureAnonymousVerifier checks the local development path: anonymous
// access when it is explicitly asked for, and a caller that authorization can
// recognize as anonymous.
func TestInsecureAnonymousVerifier(t *testing.T) {
	server := serveAuthenticated(t, auth.NewAuthenticator(auth.InsecureAnonymousVerifier()))

	tests := []struct {
		name          string
		authorization string
	}{
		{name: "with no credentials at all", authorization: ""},
		{name: "with a nonsense token", authorization: "Bearer hunter2"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status, body := callRPC(t, server, test.authorization)
			require.Equal(t, http.StatusOK, status)

			var response authenticatedResponse
			require.NoError(t, json.Unmarshal([]byte(body), &response))
			require.True(t, response.Anonymous, "the caller must be recognizable as anonymous")
			require.Equal(t, auth.AnonymousRole, response.Role)
			require.Equal(t, auth.AnonymousIssuer+"#"+auth.AnonymousSubject, response.ID)
		})
	}
}

// TestPrincipalFromContext checks the context helpers handlers use.
func TestPrincipalFromContext(t *testing.T) {
	t.Run("without a principal", func(t *testing.T) {
		principal, ok := auth.PrincipalFromContext(t.Context())
		require.False(t, ok)
		require.True(t, principal.IsZero())
	})

	t.Run("with a principal", func(t *testing.T) {
		want := auth.Principal{
			Issuer:  "https://issuer.example.com",
			Subject: "runner",
			Role:    "operator",
		}

		got, ok := auth.PrincipalFromContext(auth.ContextWithPrincipal(t.Context(), want))
		require.True(t, ok)
		require.Equal(t, want, got)
		require.Equal(t, "https://issuer.example.com#runner", got.ID())
	})

	t.Run("with a value that is not a principal", func(t *testing.T) {
		ctx := authn.SetInfo(t.Context(), "not-a-principal")

		principal, ok := auth.PrincipalFromContext(ctx)
		require.False(t, ok)
		require.True(t, principal.IsZero())
	})
}

// TestAuthenticatorAuthenticateReturnsConnectError checks the error an
// Authenticator hands back directly, which is what any middleware other than
// authn's would render.
func TestAuthenticatorAuthenticateReturnsConnectError(t *testing.T) {
	authenticator := auth.NewAuthenticator(nil)

	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "https://flowstate.example.com/", nil)
	require.NoError(t, err)

	info, err := authenticator.Authenticate(t.Context(), request)
	require.Nil(t, info)
	require.Error(t, err)
	require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))
}
