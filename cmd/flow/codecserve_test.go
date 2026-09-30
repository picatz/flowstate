package main

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/codecserver"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/envelope"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider/local"
)

// TestCodecServeAuthenticatesWithTheTrustPolicy drives the command's handler
// composition with the real OIDC verifier: a token from the trusted issuer,
// naming its tenant in a claim the policy maps, with the policy granting
// payload.decode. It decodes its own namespace, is refused another's, and a
// request with no token is refused before the codec handler runs, while a
// browser's credential-less preflight is answered.
func TestCodecServeAuthenticatesWithTheTrustPolicy(t *testing.T) {
	now := time.Now()
	issuer := authtest.NewIssuer(authtest.WithClock(func() time.Time { return now }))
	t.Cleanup(func() { _ = issuer.Close() })

	policy := auth.Policy{
		Issuers: []auth.TrustedIssuer{{
			Name:           "ci",
			Issuer:         issuer.URL(),
			Audiences:      []string{"https://codec.example.com", "https://rpc.example.com"},
			NamespaceClaim: "tenant",
			Actions:        []string{"payload.decode"},
		}},
		Tenancy: &auth.Tenancy{Temporal: map[string]string{"team-a": "ns-a", "team-b": "ns-b"}},
	}
	verifier, err := auth.NewOIDCVerifier(policy, auth.WithEgressPolicy(authtest.EgressPolicy()))
	require.NoError(t, err)

	env := map[string]string{"A": string(local.Generate()), "B": string(local.Generate())}
	cfg, err := envelope.ParseConfig([]byte("namespaces:\n  ns-a: {current: a-1, keys: [{id: a-1, env: A}]}\n  ns-b: {current: b-1, keys: [{id: b-1, env: B}]}\n"))
	require.NoError(t, err)
	kr, err := envelope.Open(t.Context(), cfg, envelope.OpenOptions{Getenv: func(n string) string { return env[n] }})
	require.NoError(t, err)
	codecs := kr.PayloadCodecConfig()

	handler, err := codecserver.New(codecserver.Options{
		Codecs:         codecs,
		Tenancy:        policy.Tenancy,
		AllowedOrigins: []string{"https://temporal.example.com"},
	})
	require.NoError(t, err)
	resource, err := resolveCodecResource("https://codec.example.com", authFlags{policyPath: "policy.yaml"}, &policy)
	require.NoError(t, err)
	srv := httptest.NewServer(codecServeHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), verifier, nil, resource, handler))
	t.Cleanup(srv.Close)

	nsA, err := codecs.ForNamespace("ns-a")
	require.NoError(t, err)
	sealed, err := nsA.DataConverter().ToPayload("synthetic-codec-serve-6b1a")
	require.NoError(t, err)
	body, err := protojson.Marshal(&commonpb.Payloads{Payloads: []*commonpb.Payload{sealed}})
	require.NoError(t, err)

	post := func(token, namespace string) int {
		req, err := http.NewRequest(http.MethodPost, srv.URL+"/decode", bytes.NewReader(body))
		require.NoError(t, err)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		req.Header.Set(codecserver.NamespaceHeader, namespace)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		return resp.StatusCode
	}
	tokenFor := func(tenant string) string {
		return issuer.MintToken(map[string]any{"tenant": tenant},
			authtest.WithSubject("person-"+tenant), authtest.WithAudience("https://codec.example.com"))
	}

	require.Equal(t, http.StatusOK, post(tokenFor("team-a"), "ns-a"))
	require.Equal(t, http.StatusForbidden, post(tokenFor("team-a"), "ns-b"), "a forged namespace header was honoured")
	require.Equal(t, http.StatusForbidden, post(tokenFor("team-b"), "ns-a"))
	require.Equal(t, http.StatusUnauthorized, post("", "ns-a"))
	require.Equal(t, http.StatusUnauthorized, post(issuer.MintToken(map[string]any{"tenant": "team-a"},
		authtest.WithSubject("x"), authtest.WithAudience("someone-else")), "ns-a"), "a token for another audience was accepted")
	// The issuer's entry admits the RPC audience too, and a token minted for
	// that surface must not be spendable here, where it would release history.
	require.Equal(t, http.StatusUnauthorized, post(issuer.MintToken(map[string]any{"tenant": "team-a"},
		authtest.WithSubject("person-team-a"), authtest.WithAudience("https://rpc.example.com")), "ns-a"),
		"a token minted for the RPC surface was spent on the codec server")

	_, err = resolveCodecResource("", authFlags{policyPath: "policy.yaml"}, &policy)
	require.ErrorContains(t, err, "--codec-resource")

	pre, err := http.NewRequest(http.MethodOptions, srv.URL+"/decode", nil)
	require.NoError(t, err)
	pre.Header.Set("Origin", "https://temporal.example.com")
	resp, err := http.DefaultClient.Do(pre)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	// A browser at an allowed origin whose token was refused must be able to
	// read the refusal: without the CORS grant on the 401, it sees only an
	// opaque network failure.
	browser := func(origin string) *http.Response {
		req, err := http.NewRequest(http.MethodPost, srv.URL+"/decode", bytes.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Origin", origin)
		req.Header.Set(codecserver.NamespaceHeader, "ns-a")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		return resp
	}
	refused := browser("https://temporal.example.com")
	require.Equal(t, http.StatusUnauthorized, refused.StatusCode)
	require.Equal(t, "https://temporal.example.com", refused.Header.Get("Access-Control-Allow-Origin"))
	require.Equal(t, "no-store", refused.Header.Get("Cache-Control"))

	foreign := browser("https://elsewhere.example.com")
	require.Equal(t, http.StatusForbidden, foreign.StatusCode, "an origin not allowed reached authentication")
	require.Empty(t, foreign.Header.Get("Access-Control-Allow-Origin"))
}

// TestCodecServeRefusesInsecureModeOffLoopback: the unauthenticated development
// mode must not be reachable from another machine.
func TestCodecServeRefusesInsecureModeOffLoopback(t *testing.T) {
	t.Setenv(requirePayloadEncryptionEnv, "")
	_, _, err := runCLI(t, "codec", "serve", "--insecure-no-auth", "--listen", "0.0.0.0:0",
		"--payload-keyring", writeTestKeyring(t))
	require.ErrorContains(t, err, "not loopback")
}

// TestCodecServeResolvesClientCertificatesAsTheServerDoes: a trust policy's
// kind: mtls entry is honoured by `flow codec serve` exactly as by `flow
// server`. Without --tls-client-auth require nothing would ever ask for a
// certificate, and a certificate-only policy would start and then refuse
// every caller; it is refused at startup instead, naming the flag.
func TestCodecServeResolvesClientCertificatesAsTheServerDoes(t *testing.T) {
	t.Setenv(requirePayloadEncryptionEnv, "")
	policy := filepath.Join(t.TempDir(), "policy.yaml")
	require.NoError(t, os.WriteFile(policy, []byte(`issuers:
  - name: mesh
    kind: mtls
    issuer: flowstate:mtls/mesh
    client_ca_file: `+testClientCAFile(t)+`
    subject_from: uri_san
`), 0o600))

	_, _, err := runCLI(t, "codec", "serve", "--listen", "127.0.0.1:0",
		"--auth-policy", policy, "--payload-keyring", writeTestKeyring(t))
	require.ErrorContains(t, err, "--tls-client-auth")
	require.ErrorContains(t, err, "kind: mtls")
}
