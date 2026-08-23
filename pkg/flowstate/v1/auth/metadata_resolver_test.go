package auth

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/stretchr/testify/require"
)

func TestWellKnownAuthorizationServerURL(t *testing.T) {
	t.Parallel()
	u, canonical, err := WellKnownAuthorizationServerURL("https://issuer.example/tenant/a", false)
	require.NoError(t, err)
	require.Equal(t, "https://issuer.example/.well-known/oauth-authorization-server/tenant/a", u.String())
	require.Equal(t, "https://issuer.example/tenant/a", canonical)
	_, _, err = WellKnownAuthorizationServerURL("http://issuer.example", true)
	require.ErrorContains(t, err, "HTTPS")
}

func TestMetadataDecoderRejectsAdversarialJSON(t *testing.T) {
	t.Parallel()
	limits := DefaultResolverLimits()
	_, err := decodeMetadata([]byte(`{"issuer":"https://good","issuer":"https://substitute"}`), limits, nil)
	require.ErrorContains(t, err, "duplicate JSON key")
	limits.MaxResponseBytes = 10
	limits.MaxArrayItems = 1
	_, err = decodeMetadata([]byte(`{"issuer":"x","grant_types_supported":["a","b"]}`), limits, nil)
	require.ErrorContains(t, err, "array limit")
	_, err = decodeMetadata([]byte(`{"issuer":"x","draft_profile_10":true}`), DefaultResolverLimits(), []string{"draft_profile_11"})
	require.ErrorContains(t, err, "exact revision")
}

func TestMetadataResolverValidatesCachesAndFailsClosed(t *testing.T) {
	var requests atomic.Int32
	var issuer string
	grant := atomic.Value{}
	grant.Store("authorization_code")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Cache-Control", "max-age=60")
		_, _ = fmt.Fprintf(w, `{"issuer":%q,"authorization_endpoint":%q,"token_endpoint":%q,"grant_types_supported":[%q],"token_endpoint_auth_methods_supported":["private_key_jwt"],"code_challenge_methods_supported":["S256"]}`,
			issuer, issuer+"/authorize", issuer+"/token", grant.Load().(string))
	}))
	t.Cleanup(server.Close)
	issuer = server.URL

	network, err := netpolicy.New(netpolicy.WithAllowLoopback())
	require.NoError(t, err)
	limits := DefaultResolverLimits()
	limits.MinFreshness = time.Minute
	limits.MaxFreshness = time.Minute
	limits.StaleWindow = time.Minute
	resolver, err := NewMetadataResolver(network, limits)
	require.NoError(t, err)
	now := time.Now()
	resolver.now = func() time.Time { return now }
	profile := TrustProfile{Name: "oauth-client", AllowLoopbackHTTP: true, Requirements: CapabilityRequirements{
		GrantTypes: []string{"authorization_code"}, TokenEndpointAuthMethods: []string{"private_key_jwt"},
		PKCEMethods: []string{"S256"}, RequireAuthorization: true, RequireToken: true,
	}}

	doc, err := resolver.Resolve(context.Background(), issuer, profile)
	require.NoError(t, err)
	require.Equal(t, issuer, doc.Issuer)
	_, err = resolver.Resolve(context.Background(), issuer, profile)
	require.NoError(t, err)
	require.EqualValues(t, 1, requests.Load(), "fresh metadata is served without another round trip")
	grant.Store("client_credentials")
	now = now.Add(time.Minute + time.Second)
	_, err = resolver.Resolve(context.Background(), issuer, profile)
	require.ErrorContains(t, err, `lacks required grant type "authorization_code"`,
		"a newly fetched capability removal must not fall back to stale metadata")

	_, err = resolver.Resolve(context.Background(), issuer+"/other", profile)
	require.ErrorContains(t, err, "issuer mismatch")
	require.NotContains(t, err.Error(), "grant_types_supported", "diagnostics never print response bodies")
}

func TestMetadataResolverRejectsRedirectAndOversize(t *testing.T) {
	for name, tc := range map[string]struct {
		handler http.HandlerFunc
		want    string
	}{
		"redirect": {func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "http://127.0.0.1/poison", http.StatusFound)
		}, "redirect"},
		"oversize": {func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(strings.Repeat("x", 65))) }, "byte limit"},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(tc.handler)
			defer server.Close()
			network, err := netpolicy.New(netpolicy.WithAllowLoopback())
			require.NoError(t, err)
			limits := DefaultResolverLimits()
			limits.MaxResponseBytes = 64
			resolver, err := NewMetadataResolver(network, limits)
			require.NoError(t, err)
			_, err = resolver.Resolve(context.Background(), server.URL, TrustProfile{Name: name, AllowLoopbackHTTP: true})
			require.ErrorContains(t, err, tc.want)
		})
	}
}
