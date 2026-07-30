package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// withTokenConfig points the credential machinery at a fixture for one test.
//
// The token file is passed to [tokenFor] rather than set globally, so it is returned
// here. allowPlaintextCredential is still package-level — it has no flag, only an
// environment variable read at startup — so it is restored.
func withTokenConfig(t *testing.T, file, env string, allowPlaintext bool) string {
	t.Helper()

	oldAllow := allowPlaintextCredential
	t.Cleanup(func() { allowPlaintextCredential = oldAllow })

	allowPlaintextCredential = allowPlaintext
	t.Setenv("FLOWSTATE_TOKEN", env)

	return file
}

// writeToken puts a token in a file and returns the path.
func writeToken(t *testing.T, contents string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("writing the fixture: %v", err)
	}
	return path
}

// TestNoCredentialConfiguredIsAnonymous keeps a development server usable.
//
// `flow run` against `flow server --insecure-no-auth` must work with nothing
// configured. A CLI that started requiring a token would make the first five
// minutes of using this project a login problem.
func TestNoCredentialConfiguredIsAnonymous(t *testing.T) {
	tokenFile := withTokenConfig(t, "", "", false)

	token, err := tokenFor("https://flowstate.example.com", tokenFile)
	if err != nil {
		t.Fatalf("no credential configured should not be an error: %v", err)
	}
	if token != "" {
		t.Errorf("token = %q, want empty", token)
	}
}

// TestATokenFileIsPreferredOverTheEnvironment pins the precedence.
//
// The file wins because it is the one that can rotate. Someone with both set has
// almost certainly moved to the file and left the variable behind.
func TestATokenFileIsPreferredOverTheEnvironment(t *testing.T) {
	tokenFile := withTokenConfig(t, writeToken(t, "from-the-file"), "from-the-environment", false)

	token, err := tokenFor("https://flowstate.example.com", tokenFile)
	if err != nil {
		t.Fatalf("tokenFor: %v", err)
	}
	if token != "from-the-file" {
		t.Errorf("token = %q, want the file's", token)
	}
}

// TestATokenFileIsReadPerRequest is the reason the file form exists at all.
//
// A projected service account token is rewritten in place as it rotates. A client
// that read it once at startup would keep presenting the old one and start being
// refused partway through a long command, for a reason nothing in its output could
// explain.
func TestATokenFileIsReadPerRequest(t *testing.T) {
	path := writeToken(t, "first")
	tokenFile := withTokenConfig(t, path, "", false)

	if token, err := tokenFor("https://flowstate.example.com", tokenFile); err != nil || token != "first" {
		t.Fatalf("token = %q, %v; want the original", token, err)
	}

	if err := os.WriteFile(path, []byte("rotated"), 0o600); err != nil {
		t.Fatalf("rotating the fixture: %v", err)
	}

	token, err := tokenFor("https://flowstate.example.com", tokenFile)
	if err != nil {
		t.Fatalf("tokenFor after rotation: %v", err)
	}
	if token != "rotated" {
		t.Errorf("token = %q, want the rotated one; it was cached", token)
	}
}

// TestATokenFileIsTrimmed covers the newline every editor and every `echo` adds.
//
// net/http rejects a header value containing a newline, so without trimming this
// surfaces as a transport error about header injection rather than as anything
// pointing at the file.
func TestATokenFileIsTrimmed(t *testing.T) {
	tokenFile := withTokenConfig(t, writeToken(t, "  padded\n"), "", false)

	token, err := tokenFor("https://flowstate.example.com", tokenFile)
	if err != nil {
		t.Fatalf("tokenFor: %v", err)
	}
	if token != "padded" {
		t.Errorf("token = %q, want it trimmed", token)
	}
}

// TestAnOversizedTokenFileIsRefused bounds a file read on every request.
//
// It is also the likelier mistake by far: a path pointing at the wrong thing. A
// diagnostic beats hashing whatever it found into an Authorization header.
func TestAnOversizedTokenFileIsRefused(t *testing.T) {
	tokenFile := withTokenConfig(t, writeToken(t, strings.Repeat("x", maxTokenBytes+1)), "", false)

	_, err := tokenFor("https://flowstate.example.com", tokenFile)
	if err == nil {
		t.Fatal("an oversized credential file was accepted")
	}
	if !strings.Contains(err.Error(), "check the path") {
		t.Errorf("the refusal does not suggest what is actually wrong: %v", err)
	}
}

// TestACredentialIsRefusedOverPlaintextToARemoteServer is the one that matters.
//
// A bearer token is a bearer token: whoever holds it is the caller. Sending one in
// the clear hands it to everything on the path, and this used to be a warning
// printed before the request went out — which is not a control, and is printed too
// late to be one anyway.
func TestACredentialIsRefusedOverPlaintextToARemoteServer(t *testing.T) {
	tokenFile := withTokenConfig(t, writeToken(t, "secret-token"), "", false)

	_, err := tokenFor("http://flowstate.example.com:9233", tokenFile)
	if err == nil {
		t.Fatal("a credential was sent to a remote server over plain HTTP")
	}
	if strings.Contains(err.Error(), "secret-token") {
		t.Errorf("the refusal quotes the credential it is protecting: %v", err)
	}
	if !strings.Contains(err.Error(), "https://") {
		t.Errorf("the refusal does not say what to do instead: %v", err)
	}
}

// TestACredentialIsSentWhereItIsSafeTo is the positive direction, and the reason
// it is worth writing: a rule that refused every destination would satisfy the
// test above perfectly.
func TestACredentialIsSentWhereItIsSafeTo(t *testing.T) {
	for _, test := range []struct {
		name           string
		baseURL        string
		allowPlaintext bool
	}{
		{name: "https anywhere", baseURL: "https://flowstate.example.com"},
		{name: "plaintext to loopback", baseURL: "http://127.0.0.1:9233"},
		{name: "plaintext to localhost", baseURL: "http://localhost:9233"},
		{name: "plaintext when overridden", baseURL: "http://flowstate.example.com:9233", allowPlaintext: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			tokenFile := withTokenConfig(t, writeToken(t, "secret-token"), "", test.allowPlaintext)

			token, err := tokenFor(test.baseURL, tokenFile)
			if err != nil {
				t.Fatalf("tokenFor(%s): %v", test.baseURL, err)
			}
			if token != "secret-token" {
				t.Errorf("token = %q, want it sent", token)
			}
		})
	}
}

// TestTheTransportPresentsTheCredential checks the header an actual server sees,
// rather than the value a helper returned.
func TestTheTransportPresentsTheCredential(t *testing.T) {
	var got string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Authorization")
	}))
	t.Cleanup(server.Close)

	tokenFile := withTokenConfig(t, writeToken(t, "secret-token"), "", false)

	transport := &authorizingTransport{base: http.DefaultTransport, baseURL: server.URL, tokenFile: tokenFile}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL, nil)
	if err != nil {
		t.Fatalf("building the request: %v", err)
	}

	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	resp.Body.Close()

	if want := "Bearer secret-token"; got != want {
		t.Errorf("Authorization = %q, want %q", got, want)
	}

	// The transport must not have mutated the caller's request, since Go's own
	// retry and redirect paths reuse it.
	if header := req.Header.Get("Authorization"); header != "" {
		t.Errorf("the caller's request was mutated: Authorization = %q", header)
	}
}

// TestTheTransportSendsNothingWhenNothingIsConfigured keeps anonymous anonymous.
//
// An empty `Authorization: Bearer` header is not the same as no header: a server
// that distinguishes "no credential" from "a credential that does not parse"
// would report the wrong one.
func TestTheTransportSendsNothingWhenNothingIsConfigured(t *testing.T) {
	present := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, present = r.Header["Authorization"]
	}))
	t.Cleanup(server.Close)

	tokenFile := withTokenConfig(t, "", "", false)

	transport := &authorizingTransport{base: http.DefaultTransport, baseURL: server.URL, tokenFile: tokenFile}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL, nil)
	if err != nil {
		t.Fatalf("building the request: %v", err)
	}

	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	resp.Body.Close()

	if present {
		t.Error("an Authorization header was sent with no credential configured")
	}
}
