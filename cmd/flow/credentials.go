package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/picatz/flowstate/pkg/flowstate/v1/credentialsource"
)

// How the CLI proves who it is.
//
// The server has verified bearer tokens since it grew a trust policy: OIDC,
// workload identity federation, per-tenant namespaces, the lot. For a long
// time the CLI had no code that actually acquired one — every deployment
// hand-wrote a dozen lines of curl against $ACTIONS_ID_TOKEN_REQUEST_URL,
// piped the result through jq into a file, and hoped nobody logged it. That
// gap is what [credentialsource.Source] closes: an importable package that
// turns an ambient workload identity into a token, which this file wires the
// CLI to.
//
// # Choosing a source
//
// --credential-source (or FLOWSTATE_CREDENTIAL_SOURCE) names one explicitly:
// "github-actions" mints from the runner's OIDC token endpoint, addressed to
// --audience / FLOWSTATE_AUDIENCE; "gitlab" and "terraform-cloud" present the
// token their platform minted before the job or run began (GitLab's
// `id_tokens:` variable, HCP Terraform's TFC_WORKLOAD_IDENTITY_TOKEN), and
// check it against --audience rather than minting for it, because neither
// platform can be asked for a second token; "file" and "env" force the
// token-file or FLOWSTATE_TOKEN reading below even when the other would
// otherwise win.
// Naming a source is asking for a credential, so any of them failing to
// produce one is a refusal — never a silent slide into anonymous.
//
// Left unnamed, the CLI keeps its original default chain, unchanged:
//
//   - --token-file, or FLOWSTATE_TOKEN_FILE. The one that matters, because it
//     is the shape federated identity actually arrives in: Kubernetes
//     projects a service account token to a path and rotates the file
//     underneath you.
//   - FLOWSTATE_TOKEN, for a token already in the environment.
//   - Nothing, which is anonymous — the right answer against a development
//     server started with --insecure-no-auth.
//
// There is deliberately no --token flag taking the token itself. A credential
// in argv is a credential in `ps`, in shell history, and in whatever collects
// the command lines of processes on the machine. Somebody who really wants
// one can write `--token-file <(printf %s "$t")`, which is explicit about
// what it costs.

// allowPlaintextCredential permits sending a token over plain HTTP to
// somewhere that is not this machine. Off by default; see [tokenFor].
var allowPlaintextCredential = os.Getenv("FLOWSTATE_INSECURE_PLAINTEXT_TOKEN") == "true"

// credentialSourceFor builds the [credentialsource.Source] a command's
// credentials come from, given its server flags.
//
// An explicitly named source ([serverFlags.credentialSource] set, from
// --credential-source or FLOWSTATE_CREDENTIAL_SOURCE) is resolved through
// [credentialsource.Resolve], which fails closed on a name it does not know
// and on one that cannot presently produce a token — that refusal is the
// whole point of naming one. Left unnamed, this returns [defaultSource],
// which is the CLI's original chain, preserved exactly, where reaching
// neither a token file nor FLOWSTATE_TOKEN is legitimately anonymous rather
// than an error.
func credentialSourceFor(server serverFlags) (credentialsource.Source, error) {
	if server.credentialSource != "" {
		return credentialsource.Resolve(server.credentialSource, credentialsource.Config{
			Audience:  server.audience,
			TokenFile: server.tokenFile,
		})
	}

	return defaultSource{tokenFile: server.tokenFile}, nil
}

// defaultSource is the CLI's behavior when no --credential-source was named:
// a token file if one is configured, else FLOWSTATE_TOKEN, else anonymous.
//
// Kept as its own [credentialsource.Source] rather than a special case in the
// transport, so [authorizingTransport] has exactly one code path regardless
// of whether a source was named — and so this default gets the same
// re-read-every-call treatment [credentialsource.NewFileSource] documents,
// through the same implementation an explicitly named "file" or "env" source
// uses.
type defaultSource struct{ tokenFile string }

func (defaultSource) Name() string { return "default" }

func (d defaultSource) Token(ctx context.Context) (credentialsource.Token, error) {
	if d.tokenFile != "" {
		return credentialsource.NewFileSource(d.tokenFile).Token(ctx)
	}

	// FLOWSTATE_TOKEN unset is anonymous here, unlike an explicitly named
	// "env" source, for which the same absence is a refusal — see
	// [credentialsource.NewEnvSource]. The difference is exactly whether a
	// credential was asked for by name.
	if strings.TrimSpace(os.Getenv("FLOWSTATE_TOKEN")) == "" {
		return credentialsource.Token{}, nil
	}

	return credentialsource.NewEnvSource("FLOWSTATE_TOKEN").Token(ctx)
}

// readToken reads a bearer token the CLI's original way: a file if one is
// named, else FLOWSTATE_TOKEN, else empty.
//
// This exists for callers outside the server-credential path that still want
// exactly that precedence — registerVaultProvider in secrets.go reads a
// Vault token file through it, bounded and re-read per call the same way a
// Flowstate server token is.
func readToken(tokenFile string) (string, error) {
	token, err := defaultSource{tokenFile: tokenFile}.Token(context.Background())
	if err != nil {
		return "", err
	}
	raw, _ := token.Bearer()
	return raw, nil
}

// tokenFor returns the bearer token to present to the given base URL, from
// the given source.
//
// The URL is a parameter because whether a credential may be sent at all
// depends on where it is going. A bearer token is a bearer token: whoever
// holds it is the caller, so putting one on the wire in the clear hands it to
// anything between here and there. This repo's rule is that the default and
// the error path both deny, and "we told them" is not a control.
//
// Loopback is exempt because a development server does not speak TLS and the
// packets do not leave the machine. FLOWSTATE_INSECURE_PLAINTEXT_TOKEN
// overrides the rest, for the person terminating TLS at a sidecar who knows
// what their network is; it is named so that finding it in a shell profile is
// alarming.
func tokenFor(ctx context.Context, baseURL string, source credentialsource.Source) (string, error) {
	token, err := source.Token(ctx)
	if err != nil {
		return "", err
	}

	raw, ok := token.Bearer()
	if !ok {
		return "", nil
	}

	if strings.HasPrefix(baseURL, "https://") || allowPlaintextCredential {
		return raw, nil
	}

	if isLoopbackAddress(strings.TrimPrefix(baseURL, "http://")) {
		return raw, nil
	}

	return "", fmt.Errorf(
		"refusing to send a credential to %s over plain HTTP, where anything on the "+
			"path can read it and then be you. Use an https:// address, or set "+
			"FLOWSTATE_INSECURE_PLAINTEXT_TOKEN=true if something else is providing "+
			"the encryption", baseURL)
}

// authorizingTransport attaches the credential to every request.
//
// A transport rather than a Connect interceptor, for the same reason the
// response bound is one: it is the layer every request passes through,
// whatever the RPC library does above it. An interceptor covers the calls
// the library routes through it, and this must cover all of them.
type authorizingTransport struct {
	base    http.RoundTripper
	baseURL string

	// source produces the credential, re-consulted on every request — the
	// same pattern that made the token-file re-read correct: a rotating file
	// or a re-mintable ambient identity both need this to run per request
	// rather than once at construction.
	source credentialsource.Source

	// sourceErr is set when [credentialSourceFor] itself failed — an unknown
	// or misconfigured --credential-source. Building the client does not
	// return an error, so the refusal is carried here and surfaced on the
	// first request instead, where every caller already handles an RPC
	// error.
	sourceErr error

	// tlsConfigErr is set when [clientTLSConfig] itself failed — a
	// misconfigured --tls-client-cert-file/--tls-client-key-file/--tls-ca-file
	// triple (cmd/flow/clientcert.go). Carried the same way as sourceErr, and
	// checked first: a client certificate this process cannot even load is a
	// more fundamental refusal than which bearer credential to attach.
	tlsConfigErr error
}

// clientSideError marks a refusal this process produced before any bytes
// reached the network: a credential source that cannot be built, client TLS
// files that cannot be loaded, a token that cannot be produced or must not be
// sent. Connect wraps every RoundTrip failure as CodeUnavailable, so without
// the mark a renderer keyed on that code — [mcpRPCErrorDecorator], and the
// same misreading a `flow list` with a missing token file gets from the
// no-server headline — writes "fix the address, start the server" onto an
// error no server change can fix. The message passes through verbatim; the
// type is the only addition, and Unwrap keeps every errors.Is/As match a
// caller already relies on.
type clientSideError struct{ err error }

func (e *clientSideError) Error() string { return e.err.Error() }
func (e *clientSideError) Unwrap() error { return e.err }

// RoundTrip implements [http.RoundTripper].
func (t *authorizingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.tlsConfigErr != nil {
		return nil, &clientSideError{err: t.tlsConfigErr}
	}
	if t.sourceErr != nil {
		return nil, &clientSideError{err: t.sourceErr}
	}

	token, err := tokenFor(req.Context(), t.baseURL, t.source)
	if err != nil {
		return nil, &clientSideError{err: err}
	}
	if token == "" {
		return t.base.RoundTrip(req)
	}

	// Cloned before mutating: a RoundTripper does not own the request it is
	// given, and Go's own retry and redirect paths reuse it.
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+token)

	return t.base.RoundTrip(req)
}
