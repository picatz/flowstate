package auth

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIDJAGDraft04WireFixture(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	var idp, ras *httptest.Server
	var forms []url.Values
	metadata := func(issuer, endpoint string, idp bool) []byte {
		m := map[string]any{"issuer": issuer, "token_endpoint": endpoint}
		if idp {
			m["identity_chaining_requested_token_types_supported"] = []string{idjagRequestedTokenType}
		} else {
			m["authorization_grant_profiles_supported"] = []string{idjagGrantProfile}
			m["grant_types_supported"] = []string{idjagJWTBearerGrant}
		}
		body, _ := json.Marshal(m)
		return body
	}
	idp = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_, _ = w.Write(metadata(idp.URL, idp.URL+"/token", true))
			return
		}
		require.Equal(t, "/token", r.URL.Path)
		require.NoError(t, r.ParseForm())
		forms = append(forms, r.Form)
		_, _ = io.WriteString(w, `{"access_token":"signed-id-jag","issued_token_type":"urn:ietf:params:oauth:token-type:id-jag","token_type":"N_A","expires_in":120,"scope":"files.read"}`)
	}))
	ras = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_, _ = w.Write(metadata(ras.URL, ras.URL+"/token", false))
			return
		}
		require.NoError(t, r.ParseForm())
		forms = append(forms, r.Form)
		_, _ = io.WriteString(w, `{"access_token":"downstream","token_type":"Bearer","expires_in":60,"scope":"files.read","resource":"https://api.example/files"}`)
	}))
	client := idp.Client()
	client.Transport = &multiTLSTransport{transports: map[string]http.RoundTripper{idp.URL: idp.Client().Transport, ras.URL: ras.Client().Transport}}
	stages := []string{}
	cfg := validIDJAGProfile(idp.URL, ras.URL)
	exchanger, err := NewIDJAGExchanger("fixture", cfg, IDJAGRuntime{
		HTTPClient: client, Clock: func() time.Time { return now },
		Subject: func(context.Context) (IDJAGSubject, error) { return validIDJAGSubject(now), nil },
		VerifyGrant: func(context.Context, Material) (IDJAGClaims, error) {
			return validIDJAGClaims(now, idp.URL, ras.URL), nil
		},
		AuthenticateIDP: func(_ context.Context, r *http.Request, _ string) error {
			r.Header.Set("Authorization", "private-key-jwt")
			return nil
		},
		AuthenticateResource: func(_ context.Context, r *http.Request, _ string) error {
			r.Header.Set("Authorization", "mTLS")
			return nil
		},
		Authorize: func(_ context.Context, d IDJAGDecision) error { stages = append(stages, d.Stage); return nil },
	})
	require.NoError(t, err)
	credential, err := exchanger.Exchange(context.Background(), Assertion{token: NewSingleMaterial("flow-assertion"), ID: "txn-1", Subject: "user-1", Audience: "flow-client", ExpiresAt: now.Add(time.Minute)})
	require.NoError(t, err)
	token, ok := credential.Bearer()
	require.True(t, ok)
	require.Equal(t, "downstream", token)
	require.Equal(t, []string{"before_assertion", "before_grant", "before_accept"}, stages)
	require.Len(t, forms, 2)
	require.Equal(t, grantTypeTokenExchange, forms[0].Get("grant_type"))
	require.Equal(t, idjagRequestedTokenType, forms[0].Get("requested_token_type"))
	require.Empty(t, forms[0].Get("actor_token"))
	require.Equal(t, idjagJWTBearerGrant, forms[1].Get("grant_type"))
	require.Equal(t, "signed-id-jag", forms[1].Get("assertion"))
	require.Empty(t, forms[1].Get("subject_token"))
}

type multiTLSTransport struct{ transports map[string]http.RoundTripper }

func (m *multiTLSTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return m.transports[r.URL.Scheme+"://"+r.URL.Host].RoundTrip(r)
}

func validIDJAGProfile(idp, ras string) *IDJAGProfile {
	return &IDJAGProfile{Revision: IDJAGProfile_DRAFT_IETF_OAUTH_IDENTITY_ASSERTION_AUTHZ_GRANT_04, IdentityProviderIssuer: idp, IdentityProviderTokenEndpoint: idp + "/token", ResourceAuthorizationServerIssuer: ras, ResourceAuthorizationServerTokenEndpoint: ras + "/token", AssertionAudience: "flow-client", ClientId: "flow-client", ResourceApplication: "files", TargetResource: "https://api.example/files", RequestedScopes: []string{"files.read"}, ClientAuthenticationMethod: IDJAGProfile_PRIVATE_KEY_JWT, ProofRequirement: IDJAGProfile_BEARER, AcceptedIdentityClasses: []string{"human"}, AcceptedActorClasses: []string{"workload"}, ClientIdInterpretation: IDJAGProfile_GLOBAL, TenantRelationship: "idp-tenant -> resource-tenant", MaximumAssertionLifetimeSeconds: 300, RequireConsent: true, RequiredAcrValues: []string{"mfa"}}
}
func validIDJAGSubject(now time.Time) IDJAGSubject {
	return IDJAGSubject{Material: NewSingleMaterial("id-token"), Type: "urn:ietf:params:oauth:token-type:id_token", ID: "identity-1", Subject: "user-1", Audience: "flow-client", ClientID: "flow-client", IdentityClass: "human", ActorClass: "workload", ActorChain: "flow/workflow/run/step", TransactionID: "txn-1", ExpiresAt: now.Add(2 * time.Minute), Consent: true, ACR: "mfa"}
}
func validIDJAGClaims(now time.Time, idp, ras string) IDJAGClaims {
	return IDJAGClaims{ID: "jag-1", Issuer: idp, Subject: "user-1", Audience: ras, ClientID: "flow-client", Resource: "https://api.example/files", ActorChain: "flow/workflow/run/step", TransactionID: "txn-1", IdentityClass: "human", ActorClass: "workload", IssuedAt: now, ExpiresAt: now.Add(2 * time.Minute), Scopes: []string{"files.read"}}
}

func TestIDJAGRejectsSecurityDowngrades(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	cfg := validIDJAGProfile("https://idp.example", "https://ras.example")
	e := &idjagExchanger{name: "test", cfg: cfg, clock: func() time.Time { return now }}
	tx := Assertion{ID: "txn-1", Subject: "user-1", Audience: "flow-client"}
	for _, test := range []struct {
		name  string
		spoil func(*IDJAGSubject)
	}{
		{"replay", func(s *IDJAGSubject) { s.ID = "" }}, {"wrong client", func(s *IDJAGSubject) { s.ClientID = "other" }}, {"wrong audience", func(s *IDJAGSubject) { s.Audience = "other" }}, {"assertion substitution", func(s *IDJAGSubject) { s.Subject = "other" }}, {"missing step-up", func(s *IDJAGSubject) { s.ACR = "password" }}, {"consent downgrade", func(s *IDJAGSubject) { s.Consent = false }},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := validIDJAGSubject(now)
			test.spoil(&s)
			require.Error(t, e.validateSubject(s, tx))
		})
	}
	for _, test := range []struct {
		name  string
		spoil func(*IDJAGClaims)
	}{
		{"wrong resource", func(c *IDJAGClaims) { c.Resource = "https://api.example/admin" }}, {"excessive scope", func(c *IDJAGClaims) { c.Scopes = append(c.Scopes, "files.write") }}, {"cross-tenant confusion", func(c *IDJAGClaims) { c.Tenant = "other" }}, {"assertion substitution", func(c *IDJAGClaims) { c.TransactionID = "other" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := validIDJAGClaims(now, "https://idp.example", "https://ras.example")
			test.spoil(&c)
			if test.name == "cross-tenant confusion" {
				cfg.IdentityProviderTenant = "expected"
			}
			require.Error(t, e.validateGrant(c, validIDJAGSubject(now), tx, idjagTokenResponse{}))
		})
	}
}

func TestIDJAGHasNoGrantFallback(t *testing.T) {
	t.Parallel()
	cfg := validIDJAGProfile("https://idp.example", "https://ras.example")
	cfg.Revision = IDJAGProfile_REVISION_UNSPECIFIED
	_, err := NewIDJAGExchanger("", cfg, IDJAGRuntime{})
	require.ErrorContains(t, err, "draft-ietf-oauth-identity-assertion-authz-grant-04")
	require.False(t, strings.Contains(err.Error(), "client_credentials"))
}

func TestIDJAGReplayAndProofDowngrade(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	cfg := validIDJAGProfile("https://idp.example", "https://ras.example")
	e := &idjagExchanger{name: "test", cfg: cfg, clock: func() time.Time { return now }}
	require.NoError(t, e.rememberGrant("jag-1", now.Add(time.Minute)))
	require.ErrorContains(t, e.rememberGrant("jag-1", now.Add(time.Minute)), "replayed")

	cfg.ProofRequirement = IDJAGProfile_DPOP
	claims := validIDJAGClaims(now, "https://idp.example", "https://ras.example")
	require.Error(t, e.validateGrant(claims, validIDJAGSubject(now), Assertion{ID: "txn-1"}, idjagTokenResponse{}))
}
