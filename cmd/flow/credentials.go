package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

// How the CLI proves who it is.
//
// The server has verified bearer tokens since it grew a trust policy: OIDC,
// workload identity federation, per-tenant namespaces, the lot. The CLI had no
// code path that sent one. Its own messages already described the capability —
// `serverBaseURL` warns that "any credential sent travels in the clear", and a
// refusal reads "a tenant your credentials do not establish" — while nothing
// anywhere set an Authorization header. This is the third time in this repo a
// subsystem has been complete, tested, and unreachable.
//
// Three sources, in the order they are consulted:
//
//   - --token-file, or FLOWSTATE_TOKEN_FILE. The one that matters, because it is
//     the shape federated identity actually arrives in: Kubernetes projects a
//     service account token to a path and rotates the file underneath you.
//   - FLOWSTATE_TOKEN, for a token already in the environment.
//   - Nothing, which is anonymous — the right answer against a development
//     server started with --insecure-no-auth.
//
// There is deliberately no --token flag taking the token itself. A credential in
// argv is a credential in `ps`, in shell history, and in whatever collects the
// command lines of processes on the machine. Somebody who really wants one can
// write `--token-file <(printf %s "$t")`, which is explicit about what it costs.

// Credential configuration, resolved from flags and the environment.
var (
	tokenFilePath = os.Getenv("FLOWSTATE_TOKEN_FILE")

	// allowPlaintextCredential permits sending a token over plain HTTP to
	// somewhere that is not this machine. Off by default; see [tokenFor].
	allowPlaintextCredential = os.Getenv("FLOWSTATE_INSECURE_PLAINTEXT_TOKEN") == "true"
)

// maxTokenBytes bounds a token file.
//
// Read on every request, so an enormous file would be an enormous read every
// time. A JWT with a generous set of claims is a few kilobytes; anything past this
// is a path pointing at the wrong thing, and saying so beats hashing a core file
// into an Authorization header.
const maxTokenBytes = 64 << 10

// tokenFor returns the credential to present to the given base URL.
//
// The URL is a parameter because whether a credential may be sent at all depends
// on where it is going. A bearer token is a bearer token: whoever holds it is the
// caller, so putting one on the wire in the clear hands it to anything between
// here and there. That was a warning while no credential existed. Now that one
// does, it is a refusal — this repo's rule is that the default and the error path
// both deny, and "we told them" is not a control.
//
// Loopback is exempt because a development server does not speak TLS and the
// packets do not leave the machine. FLOWSTATE_INSECURE_PLAINTEXT_TOKEN overrides
// the rest, for the person terminating TLS at a sidecar who knows what their
// network is; it is named so that finding it in a shell profile is alarming.
func tokenFor(baseURL string) (string, error) {
	token, err := readToken()
	if err != nil || token == "" {
		return "", err
	}

	if strings.HasPrefix(baseURL, "https://") || allowPlaintextCredential {
		return token, nil
	}

	if isLoopbackAddress(strings.TrimPrefix(baseURL, "http://")) {
		return token, nil
	}

	return "", fmt.Errorf(
		"refusing to send a credential to %s over plain HTTP, where anything on the "+
			"path can read it and then be you. Use an https:// address, or set "+
			"FLOWSTATE_INSECURE_PLAINTEXT_TOKEN=true if something else is providing "+
			"the encryption", baseURL)
}

// readToken returns the configured token, or empty when there is none.
//
// The file is read on every call rather than once at startup, which is the whole
// reason the file form is the important one. A projected service account token is
// rewritten in place as it rotates, and a long-running command that cached the
// first one would start failing partway through for no reason its user could see.
func readToken() (string, error) {
	if tokenFilePath == "" {
		return strings.TrimSpace(os.Getenv("FLOWSTATE_TOKEN")), nil
	}

	file, err := os.Open(tokenFilePath)
	if err != nil {
		return "", fmt.Errorf("reading the credential from %s: %w", tokenFilePath, err)
	}
	defer file.Close()

	// One byte past the limit, so a token at exactly the limit still works and one
	// over it is refused rather than silently truncated into a token that would be
	// rejected for a reason nobody could diagnose.
	contents, err := io.ReadAll(io.LimitReader(file, maxTokenBytes+1))
	if err != nil {
		return "", fmt.Errorf("reading the credential from %s: %w", tokenFilePath, err)
	}
	if len(contents) > maxTokenBytes {
		return "", fmt.Errorf("the credential in %s is larger than %d bytes, which is not a token; "+
			"check the path", tokenFilePath, maxTokenBytes)
	}

	// Trimmed because a file almost always ends in a newline, and a newline in a
	// header value is rejected by net/http as header injection — which would report
	// a transport error rather than "your token file has a newline in it".
	return strings.TrimSpace(string(contents)), nil
}

// authorizingTransport attaches the credential to every request.
//
// A transport rather than a Connect interceptor, for the same reason the response
// bound is one: it is the layer every request passes through, whatever the RPC
// library does above it. An interceptor covers the calls the library routes
// through it, and this must cover all of them.
type authorizingTransport struct {
	base    http.RoundTripper
	baseURL string
}

// RoundTrip implements [http.RoundTripper].
func (t *authorizingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	token, err := tokenFor(t.baseURL)
	if err != nil {
		return nil, err
	}
	if token == "" {
		return t.base.RoundTrip(req)
	}

	// Cloned before mutating: a RoundTripper does not own the request it is given,
	// and Go's own retry and redirect paths reuse it.
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+token)

	return t.base.RoundTrip(req)
}
