package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// whoamiToken is the credential these tests present. It is distinctive so a
// leak of any part of it into output is a substring match.
const whoamiToken = "whoami-test-credential-9f3c1d7a5b"

// serveRealWhoami serves the real server handler behind a stand-in for the
// authentication interceptor: a request carrying whoamiToken becomes the
// verified caller below, and one with no credential becomes the anonymous
// caller an insecure development server admits. The handler under test is the
// real one, so the CLI, the wire and the server are exercised together.
func serveRealWhoami(t *testing.T) {
	t.Helper()

	flowstate, err := server.New(nil)
	require.NoError(t, err)

	_, handler := flowstatev1connect.NewWorkflowServiceHandler(flowstate)

	guarded := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal := auth.AnonymousPrincipal()
		if bearer, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer "); ok {
			if bearer != whoamiToken {
				http.Error(w, "bad credential", http.StatusUnauthorized)
				return
			}
			carried, err := auth.MapClaims(auth.TrustedIssuer{
				CarryClaims: []auth.CarryClaim{{Claim: "repository", Type: auth.ClaimTypeString}},
			}, map[string]any{"repository": "picatz/flowstate", "email": "person@example.com"})
			require.NoError(t, err)

			principal = auth.Principal{
				Issuer:     "https://issuer.example",
				Subject:    "runner-7",
				IssuerName: "ci",
				Namespace:  "acme",
				Kind:       auth.PrincipalKindWorkload,
				Actions:    auth.ActionScopes{"workload.read"},
				Claims:     carried,
			}
		}
		handler.ServeHTTP(w, r.WithContext(auth.ContextWithPrincipal(r.Context(), principal)))
	})

	httpServer := httptest.NewServer(guarded)
	t.Cleanup(httpServer.Close)
	t.Setenv("FLOWSTATE_ADDRESS", httpServer.URL)
}

func writeWhoamiTokenFile(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte(whoamiToken+"\n"), 0o600))

	return path
}

// TestAuthWhoamiPrintsThePrincipalAndNeverTheToken drives the whole path: the
// credential goes out in the request, the server answers with the principal it
// built from it, and the output names every documented field while holding no
// byte of the credential, in either format or on either stream.
func TestAuthWhoamiPrintsThePrincipalAndNeverTheToken(t *testing.T) {
	serveRealWhoami(t)
	tokenFile := writeWhoamiTokenFile(t)

	text, textErr, err := runCLI(t, "auth", "whoami", "--token-file", tokenFile)
	require.NoError(t, err)

	for _, want := range []string{
		"authenticated: true",
		"issuer: https://issuer.example",
		"subject: runner-7",
		"namespace: acme",
		"kind: workload",
		"issuer_entry: ci",
		"actions: workload.read",
		`  repository: "picatz/flowstate"`,
	} {
		require.Contains(t, text, want)
	}
	require.NotContains(t, text, "email", "a claim the entry does not carry reached the output")

	asJSON, jsonErr, err := runCLI(t, "auth", "whoami", "--token-file", tokenFile, "--output", "json")
	require.NoError(t, err)

	var document map[string]any
	require.NoError(t, json.Unmarshal([]byte(asJSON), &document), "stdout is not one JSON document:\n%s", asJSON)
	principal, _ := document["principal"].(map[string]any)
	require.Equal(t, "runner-7", principal["subject"])
	require.Equal(t, "ci", principal["issuerEntry"])
	require.Equal(t, true, document["authenticated"])

	for name, output := range map[string]string{"text": text, "text stderr": textErr, "json": asJSON, "json stderr": jsonErr} {
		require.NotContains(t, output, whoamiToken, "the credential reached %s output", name)
		require.NotContains(t, output, "Bearer", "an authorization header reached %s output", name)
	}
}

// TestAuthWhoamiAnonymousIsAnAnswerNotAnError: against a server that admits
// anonymous callers the command succeeds and says it is not authenticated.
func TestAuthWhoamiAnonymousIsAnAnswerNotAnError(t *testing.T) {
	serveRealWhoami(t)
	t.Setenv("FLOWSTATE_TOKEN", "")
	t.Setenv("FLOWSTATE_TOKEN_FILE", "")

	text, _, err := runCLI(t, "auth", "whoami")
	require.NoError(t, err)
	require.Contains(t, text, "authenticated: false")
	require.Contains(t, text, "issuer: "+auth.AnonymousIssuer)
}

// TestAuthWhoamiRefusesABadCredential: a credential the server rejects is an
// error that names neither the token nor its value.
func TestAuthWhoamiRefusesABadCredential(t *testing.T) {
	serveRealWhoami(t)

	path := filepath.Join(t.TempDir(), "token")
	const wrong = "wrong-credential-aa11bb22"
	require.NoError(t, os.WriteFile(path, []byte(wrong+"\n"), 0o600))

	_, stderr, err := runCLI(t, "auth", "whoami", "--token-file", path)
	require.Error(t, err)
	require.NotContains(t, err.Error()+stderr, wrong)
}
