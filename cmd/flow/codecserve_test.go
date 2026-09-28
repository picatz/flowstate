package main

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/codecserver"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/envelope"
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
			Audiences:      []string{"flowstate-codec"},
			NamespaceClaim: "tenant",
			Actions:        []string{"payload.decode"},
		}},
		Tenancy: &auth.Tenancy{Temporal: map[string]string{"team-a": "ns-a", "team-b": "ns-b"}},
	}
	verifier, err := auth.NewOIDCVerifier(policy, auth.WithEgressPolicy(authtest.EgressPolicy()))
	require.NoError(t, err)

	env := map[string]string{"A": string(envelope.GenerateKey()), "B": string(envelope.GenerateKey())}
	cfg, err := envelope.ParseConfig([]byte("namespaces:\n  ns-a: {current: a-1, keys: [{id: a-1, env: A}]}\n  ns-b: {current: b-1, keys: [{id: b-1, env: B}]}\n"))
	require.NoError(t, err)
	kr, err := envelope.Open(cfg, envelope.OpenOptions{Getenv: func(n string) string { return env[n] }})
	require.NoError(t, err)
	codecs := kr.PayloadCodecConfig()

	handler, err := codecserver.New(codecserver.Options{
		Codecs:         codecs,
		Tenancy:        policy.Tenancy,
		AllowedOrigins: []string{"https://temporal.example.com"},
	})
	require.NoError(t, err)
	srv := httptest.NewServer(codecServeHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), verifier, handler))
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
			authtest.WithSubject("person-"+tenant), authtest.WithAudience("flowstate-codec"))
	}

	require.Equal(t, http.StatusOK, post(tokenFor("team-a"), "ns-a"))
	require.Equal(t, http.StatusForbidden, post(tokenFor("team-a"), "ns-b"), "a forged namespace header was honoured")
	require.Equal(t, http.StatusForbidden, post(tokenFor("team-b"), "ns-a"))
	require.Equal(t, http.StatusUnauthorized, post("", "ns-a"))
	require.Equal(t, http.StatusUnauthorized, post(issuer.MintToken(map[string]any{"tenant": "team-a"},
		authtest.WithSubject("x"), authtest.WithAudience("someone-else")), "ns-a"), "a token for another audience was accepted")

	pre, err := http.NewRequest(http.MethodOptions, srv.URL+"/decode", nil)
	require.NoError(t, err)
	pre.Header.Set("Origin", "https://temporal.example.com")
	resp, err := http.DefaultClient.Do(pre)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
}

// TestCodecServeRefusesInsecureModeOffLoopback: the unauthenticated development
// mode must not be reachable from another machine.
func TestCodecServeRefusesInsecureModeOffLoopback(t *testing.T) {
	t.Setenv(requirePayloadEncryptionEnv, "")
	_, _, err := runCLI(t, "codec", "serve", "--insecure-no-auth", "--listen", "0.0.0.0:0",
		"--payload-keyring", writeTestKeyring(t))
	require.ErrorContains(t, err, "not loopback")
}
