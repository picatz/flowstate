package oauthclient_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/oauthclient"
)

func newClient(t *testing.T, profile oauthclient.ProfileName, now time.Time) *oauthclient.Client {
	t.Helper()
	c, err := oauthclient.New(oauthclient.Config{
		Profile: profile, Issuer: "https://issuer.example", ClientID: "client",
		RedirectURI: "https://client.example/callback", ClientAuthenticationMethod: "private_key_jwt",
		RequestObjectAlgorithm: "ES256", DPoPAlgorithm: "ES256", Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	return c
}

func metadata(now time.Time) oauthclient.Metadata {
	return oauthclient.Metadata{Issuer: "https://issuer.example", FetchedAt: now, PAR: true,
		RequestObjectSigningAlgorithms: []string{"ES256"}, CodeChallengeMethods: []string{"S256"},
		ClientAuthenticationMethods: []string{"private_key_jwt"}, ResponseModes: []string{"query"},
		DPoPSigningAlgorithms: []string{"ES256"}, MTLS: true, ResourceIndicators: true}
}

// TestWireLevelConformance pins the refusals at the values crossing the OAuth
// wire boundary. No case is allowed to silently retry with a weaker request.
func TestWireLevelConformance(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	t.Run("PKCE downgrade", func(t *testing.T) {
		c := newClient(t, oauthclient.SenderConstrainedAgent, now)
		m := metadata(now)
		m.CodeChallengeMethods = []string{"plain"}
		require.ErrorContains(t, c.ValidateMetadata(m), "requires PKCE method S256; downgrade is prohibited")
	})

	t.Run("PAR request-URI swapping and replay", func(t *testing.T) {
		c := newClient(t, oauthclient.SenderConstrainedAgent, now)
		a := transaction()
		require.NoError(t, c.BindPAR("urn:ietf:params:oauth:request_uri:one", time.Minute, a))
		swapped := a
		swapped.Scope = "admin"
		require.ErrorContains(t, c.ConsumePAR("urn:ietf:params:oauth:request_uri:one", swapped), "binding mismatch")
		require.NoError(t, c.ConsumePAR("urn:ietf:params:oauth:request_uri:one", a))
		require.ErrorContains(t, c.ConsumePAR("urn:ietf:params:oauth:request_uri:one", a), "already consumed")
		require.ErrorContains(t, c.ConsumePAR("urn:ietf:params:oauth:request_uri:two", a), "unknown")
	})

	t.Run("redirect mismatch", func(t *testing.T) {
		c := newClient(t, oauthclient.BaselinePublicClient, now)
		require.ErrorContains(t, c.ValidateCallback("https://client.example/callback/", "https://issuer.example"), "redirect URI mismatch")
	})

	t.Run("mix-up and issuer mismatch", func(t *testing.T) {
		c := newClient(t, oauthclient.BaselinePublicClient, now)
		require.ErrorContains(t, c.ValidateCallback("https://client.example/callback", "https://evil.example"), "issuer mismatch")
	})

	t.Run("request-object substitution", func(t *testing.T) {
		c := newClient(t, oauthclient.SenderConstrainedAgent, now)
		a := transaction()
		require.NoError(t, c.BindPAR("urn:ietf:params:oauth:request_uri:jar", time.Minute, a))
		substitute := a
		substitute.AuthorizationDetails = `{"type":"administrator"}`
		require.ErrorContains(t, c.ConsumePAR("urn:ietf:params:oauth:request_uri:jar", substitute), "binding mismatch")
	})

	t.Run("profile downgrade", func(t *testing.T) {
		c := newClient(t, oauthclient.SenderConstrainedAgent, now)
		m := metadata(now)
		m.PAR = false
		require.ErrorContains(t, c.ValidateMetadata(m), "requires pushed authorization requests (PAR); downgrade is prohibited")
	})
}

func transaction() oauthclient.Authorization {
	return oauthclient.Authorization{Issuer: "https://issuer.example", ClientID: "client",
		RedirectURI: "https://client.example/callback", Resource: "https://resource.example",
		Scope: "openid read", AuthorizationDetails: `{"type":"payment"}`,
		PKCEChallenge: oauthclient.S256("verifier"), DPoPKeyID: "proof-key", TransactionID: "state-and-nonce"}
}

func TestEveryNamedProfileIsImmutableAndComplete(t *testing.T) {
	names := []oauthclient.ProfileName{oauthclient.BaselinePublicClient, oauthclient.BaselineConfidentialClient,
		oauthclient.SenderConstrainedAgent, oauthclient.EnterpriseInteractive,
		oauthclient.HighAssuranceAdministration, oauthclient.WorkloadFederation, oauthclient.ExperimentalXAA}
	for _, name := range names {
		p, err := oauthclient.Profile(name)
		require.NoError(t, err)
		require.Equal(t, "S256", p.PKCEMethod)
		require.True(t, p.ExactRedirectURI)
		require.True(t, p.IssuerIdentification)
		require.True(t, p.ResourceIndicatorRequired)
		require.NotEmpty(t, p.ClientAuthenticationMethods)
		require.NotEmpty(t, p.ResponseModes)
		require.NotEmpty(t, p.IDTokenAlgorithms)
		require.True(t, p.ProhibitDowngrade)
		p.ResponseModes[0] = "fragment"
		again, err := oauthclient.Profile(name)
		require.NoError(t, err)
		require.Equal(t, "query", again.ResponseModes[0])
	}
}
