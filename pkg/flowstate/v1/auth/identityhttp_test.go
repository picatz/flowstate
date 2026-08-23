package auth

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func identityRequest(t *testing.T, client *http.Client, target string, credentials bool) (*http.Response, error) {
	t.Helper()
	req, err := http.NewRequestWithContext(ContextWithIdentityEndpoint(t.Context(), IdentityEndpoint{
		Purpose: EndpointToken, Provider: "bounded-provider", OriginalURL: target, Credentials: credentials,
	}), http.MethodGet, target, nil)
	require.NoError(t, err)
	return client.Do(req)
}

func TestIdentityTransportRejectsNonPublicResolution(t *testing.T) {
	for _, raw := range []string{"10.0.0.1", "169.254.169.254", "::", "fc00::1", "224.0.0.1"} {
		require.False(t, identityPublicIP(netip.MustParseAddr(raw)), raw)
	}
	require.True(t, identityPublicIP(netip.MustParseAddr("192.0.2.1")))
}

func TestIdentityTransportDisablesAmbientProxy(t *testing.T) {
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	client, err := NewIdentityHTTPClient(IdentityTransportConfig{})
	require.NoError(t, err)
	guard := client.Transport.(*identityTransport)
	transport := guard.base.(*http.Transport)
	require.Nil(t, transport.Proxy, "identity traffic must not inherit a proxy from the process environment")
}

func TestIdentityTransportRejectsRedirectToPrivateNetwork(t *testing.T) {
	client, err := NewIdentityHTTPClient(IdentityTransportConfig{})
	require.NoError(t, err)
	_, err = identityRequest(t, client, "https://10.0.0.1/metadata", false)
	require.ErrorContains(t, err, "non-public")
}

func TestIdentityTransportCredentialRedirect(t *testing.T) {
	sink := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("credential-bearing redirect reached its new origin")
	}))
	defer sink.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", sink.URL)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer origin.Close()
	client, err := NewIdentityHTTPClient(IdentityTransportConfig{})
	require.NoError(t, err)
	resp, err := identityRequest(t, client, origin.URL, true)
	require.ErrorContains(t, err, "another origin")
	if resp != nil {
		resp.Body.Close()
	}
}

func TestIdentityTransportBoundsResponsesBeforeProtocolDecoding(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, strings.Repeat("x", 4096))
	}))
	defer server.Close()
	client, err := NewIdentityHTTPClient(IdentityTransportConfig{MaxResponseBytes: 64})
	require.NoError(t, err)
	resp, err := identityRequest(t, client, server.URL, false)
	require.NoError(t, err)
	defer resp.Body.Close()
	_, err = io.ReadAll(resp.Body)
	require.ErrorContains(t, err, "configured limit")
}

func TestIdentityTransportDoesNotDecompress(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		zw := gzip.NewWriter(w)
		_, _ = io.WriteString(zw, strings.Repeat("expanded", 10000))
		require.NoError(t, zw.Close())
	}))
	defer server.Close()
	client, err := NewIdentityHTTPClient(IdentityTransportConfig{MaxResponseBytes: 4096})
	require.NoError(t, err)
	resp, err := identityRequest(t, client, server.URL, false)
	require.NoError(t, err)
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NotEqual(t, byte('e'), raw[0], "transport silently decompressed an attacker-controlled body")
}

func TestIdentityTransportBoundsDurationAndChecksCertificates(t *testing.T) {
	t.Run("slow response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { time.Sleep(100 * time.Millisecond) }))
		defer server.Close()
		client, err := NewIdentityHTTPClient(IdentityTransportConfig{Timeout: 20 * time.Millisecond})
		require.NoError(t, err)
		_, err = identityRequest(t, client, server.URL, false)
		require.Error(t, err)
	})

	t.Run("certificate mismatch", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
		defer server.Close()
		client, err := NewIdentityHTTPClient(IdentityTransportConfig{})
		require.NoError(t, err)
		_, err = identityRequest(t, client, server.URL, false)
		require.Error(t, err)
	})
}

func TestIdentityTransportPolicyAndTelemetryAreBounded(t *testing.T) {
	var purpose EndpointPurpose
	var provider string
	client, err := NewIdentityHTTPClient(IdentityTransportConfig{
		Allow:     []string{`endpoint.purpose == "jwks" && !endpoint.credentials && endpoint.tenant == "acme"`},
		Telemetry: func(p EndpointPurpose, name string) { purpose, provider = p, name },
	})
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer server.Close()
	req, err := http.NewRequestWithContext(ContextWithIdentityEndpoint(t.Context(), IdentityEndpoint{
		Purpose: EndpointJWKS, Provider: strings.Repeat("p", 100), Tenant: "acme", OriginalURL: server.URL,
	}), http.MethodGet, server.URL, nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, EndpointJWKS, purpose)
	require.Len(t, provider, 64)
	require.NotContains(t, provider, server.URL)
}
