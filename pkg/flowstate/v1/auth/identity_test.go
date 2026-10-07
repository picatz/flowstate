package auth_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

// TestIdentityFromPrincipal covers deriving a workload's identity from the caller
// that submitted the run, which is where the two halves of federation meet.
func TestIdentityFromPrincipal(t *testing.T) {
	principal := auth.Principal{
		Subject:    "repo:picatz/flowstate:ref:refs/heads/main",
		Issuer:     "https://token.actions.githubusercontent.com",
		IssuerName: "github-actions",
		Kind:       auth.PrincipalKindWorkload,
		Actions:    auth.ActionScopes{"run.start"},
		Claims: map[string]any{
			"repository": "picatz/flowstate",
			"ref":        "refs/heads/main",
			"email":      "someone@example.com",
			"verified":   true,
			"groups":     []any{"eng", "oncall"},
			"slack":      map[string]any{"user": "U1"},
		},
	}

	identity := auth.IdentityFromPrincipal(principal, "acme", "prod", "repository", "ref", "absent", "verified", "groups", "slack")

	require.Equal(t, principal.Subject, identity.Subject)
	require.Equal(t, principal.Issuer, identity.Issuer)
	require.Equal(t, "github-actions", identity.IssuerEntry)
	require.Equal(t, "workload", identity.Kind)
	require.Equal(t, []string{"run.start"}, identity.Actions)
	require.Equal(t, "acme", identity.Namespace)
	require.Equal(t, "prod", identity.Deployment)

	// Only the named claims are carried, of every JSON shape the entry names: a
	// list such as groups and a nested object are what a rule needs to read, and an
	// assertion goes to a third party, so what it says about the caller should be
	// what an operator chose to say.
	require.Equal(t, map[string]any{
		"repository": "picatz/flowstate",
		"ref":        "refs/heads/main",
		"verified":   true,
		"groups":     []any{"eng", "oncall"},
		"slack":      map[string]any{"user": "U1"},
	}, identity.Claims)

	require.NotContains(t, identity.Claims, "email", "a claim nobody named must not be carried")
	require.NotContains(t, identity.Claims, "absent")

	// The identity owns its claims: a later change to the caller's token cannot
	// change what an assertion will say.
	principal.Claims["groups"].([]any)[0] = "attacker"
	require.Equal(t, []any{"eng", "oncall"}, identity.Claims["groups"])

	t.Run("the caller a rule reads is the one rendering", func(t *testing.T) {
		caller := identity.Caller()
		require.Equal(t, "workload", caller.Kind)
		require.Equal(t, principal.Issuer+"#"+principal.Subject, caller.Principal)
		require.Equal(t, []string{"run.start"}, caller.Actions)
		require.Equal(t, []any{"eng", "oncall"}, caller.Claims.Map()["groups"])
	})

	t.Run("a claim over the bounds is not carried rather than trimmed", func(t *testing.T) {
		many := make([]any, auth.MaxCarriedClaimNodes+1)
		for i := range many {
			many[i] = "g"
		}
		deep := any("leaf")
		for range auth.MaxCarriedClaimDepth + 1 {
			deep = []any{deep}
		}
		big := auth.Principal{Subject: "s", Issuer: "i", Claims: map[string]any{
			"many": many, "deep": deep, "long": strings.Repeat("x", auth.MaxCarriedClaimValueBytes+1), "ok": "v",
		}}

		identity := auth.IdentityFromPrincipal(big, "", "", "many", "deep", "long", "ok")
		require.Equal(t, map[string]any{"ok": "v"}, identity.Claims)
	})

	t.Run("naming no claims carries none", func(t *testing.T) {
		identity := auth.IdentityFromPrincipal(principal, "acme", "prod")
		require.Empty(t, identity.Claims)
		require.NoError(t, identity.Validate())
	})

	t.Run("an unauthenticated caller yields no identity", func(t *testing.T) {
		identity := auth.IdentityFromPrincipal(auth.Principal{}, "", "")
		require.True(t, identity.IsZero())
		require.ErrorIs(t, identity.Validate(), auth.ErrInvalidIdentity)
	})
}

// TestClaimsRoundTripThroughTheWireForm pins that the proto reader and writer
// agree with each other and refuse the same shapes the mint does.
func TestClaimsRoundTripThroughTheWireForm(t *testing.T) {
	claims := map[string]any{
		"groups": []any{"eng", "oncall"},
		"slack":  map[string]any{"user": "U1", "n": float64(2), "on": true, "none": nil},
		"repo":   "x/y",
	}

	wire := auth.ClaimsToStruct(claims)
	require.Len(t, wire, 3)
	require.Equal(t, claims, auth.ClaimsFromStruct(wire))

	t.Run("a hostile value is bounded where it is read", func(t *testing.T) {
		deep := structpb.NewStringValue("leaf")
		for range 10_000 {
			deep = structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{deep}})
		}
		got := auth.ClaimsFromStruct(map[string]*structpb.Value{"deep": deep, "ok": structpb.NewStringValue("v")})
		require.Equal(t, map[string]any{"ok": "v"}, got)
	})

	t.Run("no more than the claim count is read", func(t *testing.T) {
		in := map[string]*structpb.Value{}
		for i := range auth.MaxCarriedClaims * 2 {
			in[fmt.Sprintf("c%03d", i)] = structpb.NewStringValue("v")
		}
		require.Len(t, auth.ClaimsFromStruct(in), auth.MaxCarriedClaims)
	})
}

// TestOutboundValuesNeverLogSecrets checks that every value involved in outbound
// federation can be handed to a logger without leaking. Logging is the most common
// way a credential escapes, and each of these types is one an operator will
// reasonably want to log.
func TestOutboundValuesNeverLogSecrets(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	party := newRelyingParty(t, func(w http.ResponseWriter, r *http.Request, body recordedRequest) {
		writeJSON(t, w, http.StatusOK, map[string]any{
			"access_token":      "super-secret-token",
			"issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
			"token_type":        "Bearer",
			"expires_in":        3600,
		})
	})

	exchanger, err := auth.NewTokenExchanger(auth.TokenExchangeConfig{
		Name:         "partner",
		TokenURL:     party.url + "/token",
		Audience:     "https://as.example.com",
		Clock:        clock.Now,
		EgressPolicy: authtest.EgressPolicy(),
	})
	require.NoError(t, err)
	require.Equal(t, "partner", exchanger.Name())

	assertion := mintAssertion(t, issuer, "https://as.example.com")

	credential, err := exchanger.Exchange(t.Context(), assertion)
	require.NoError(t, err)

	key, err := auth.GenerateSigningKey("logged-key", jwa.ES256)
	require.NoError(t, err)

	identity := testIdentity()
	identity.Claims = map[string]any{"email": "someone@example.com"}

	var buffer bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buffer, nil))

	logger.Info("outbound",
		"identity", identity,
		"assertion", assertion,
		"credential", credential,
		"key", key,
	)

	logged := buffer.String()

	// What must never appear.
	for _, secret := range []string{
		assertion.Token(),
		"super-secret-token",
		"someone@example.com",
	} {
		require.NotContains(t, logged, secret)
	}

	// What must appear, or the log is useless for audit: who acted, as what, and
	// against which system.
	for _, wanted := range []string{
		"repo:picatz/flowstate:ref:refs/heads/main",
		"flowstate:acme/prod/deploy-service/push-image",
		assertion.ID,
		"partner",
		"logged-key",
	} {
		require.Contains(t, logged, wanted)
	}
}

// TestWorkloadIdentityString checks the human-readable form, which says who is
// acting for whom without carrying claims.
func TestWorkloadIdentityString(t *testing.T) {
	require.Equal(t, "acme/prod acting for repo:picatz/flowstate:ref:refs/heads/main", testIdentity().String())
	require.Equal(t, "no identity", auth.WorkloadIdentity{}.String())
}

// TestDefaultNamespaceIsUnforgeable checks the negative direction of the
// placeholder that stands in for "no namespace": a tenant literally named
// "default" — which [auth.ValidateNamespace] permits, being lowercase letters
// only — must mint a subject that DIFFERS from an untenanted run's, not one
// that collides with it. Before this, both minted
// "flowstate:default/prod/deploy/push", so an AWS trust policy an operator
// wrote for a single-tenant deployment would have admitted a later tenant that
// simply claimed the name "default".
func TestDefaultNamespaceIsUnforgeable(t *testing.T) {
	ref := auth.StepRef{Workflow: "deploy", Step: "push"}

	untenanted := auth.WorkloadIdentity{Subject: "s", Issuer: "https://idp.example.com", Deployment: "prod"}
	untenantedSubject, err := untenanted.SubjectFor(ref)
	require.NoError(t, err)

	tenantNamedDefault := auth.WorkloadIdentity{
		Subject: "s", Issuer: "https://idp.example.com", Namespace: "default", Deployment: "prod",
	}
	tenantSubject, err := tenantNamedDefault.SubjectFor(ref)
	require.NoError(t, err)

	require.NotEqual(t, untenantedSubject, tenantSubject,
		"a tenant named \"default\" must not mint the same subject as an untenanted run")
	require.Equal(t, "flowstate:_default/prod/deploy/push", untenantedSubject)
	require.Equal(t, "flowstate:default/prod/deploy/push", tenantSubject)
}

// TestNamespaceGrammarAppliesAtSubjectMinting checks the negative direction of
// unifying the namespace grammar: a namespace that [secrets.ValidateNamespace]
// would refuse must never reach a signed assertion subject either, because
// before this, [auth.WorkloadIdentity.SubjectFor] only rejected a namespace
// containing "/" or ":" — not a space, "..", a control character, or one far
// longer than a namespace is ever allowed to be.
func TestNamespaceGrammarAppliesAtSubjectMinting(t *testing.T) {
	ref := auth.StepRef{Workflow: "deploy", Step: "push"}

	tests := []struct {
		name      string
		namespace string
	}{
		{"a space", "Prod Team"},
		{"path traversal shape", ".."},
		{"a control character", "team\na"},
		{"over the length limit", strings.Repeat("a", auth.MaxNamespaceLen+1)},
		{"uppercase", "TeamA"},
		{"underscore", "team_a"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			identity := auth.WorkloadIdentity{
				Subject: "s", Issuer: "https://idp.example.com",
				Namespace: test.namespace, Deployment: "prod",
			}

			_, err := identity.SubjectFor(ref)
			require.ErrorIs(t, err, auth.ErrInvalidIdentity,
				"a namespace secrets.ValidateNamespace would refuse must not reach a signed subject")
		})
	}
}

// TestFederationHTTPClientIsUsed checks that a caller-supplied HTTP client reaches
// the exchangers a policy builds, since that is the only way a deployment behind a
// proxy can federate at all.
func TestFederationHTTPClientIsUsed(t *testing.T) {
	clock := authtest.NewClock(referenceTime)

	party := newRelyingParty(t, func(w http.ResponseWriter, r *http.Request, body recordedRequest) {
		writeJSON(t, w, http.StatusOK, map[string]any{
			"access_token":      "downstream-token",
			"issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
			"token_type":        "Bearer",
			"expires_in":        3600,
		})
	})

	policy, err := auth.ParseFederationPolicy([]byte(`
issuer: https://flowstate.example.com
declared_claims: [repository]
allow: ['true']
targets:
  - name: partner
    token_exchange:
      token_url: ` + party.url + `/token
      audience: https://as.example.com
`))
	require.NoError(t, err)

	key, err := auth.GenerateSigningKey("k", jwa.ES256)
	require.NoError(t, err)

	transport := &countingTransport{next: http.DefaultTransport}

	broker, err := policy.Broker(key,
		auth.WithFederationHTTPClient(&http.Client{Transport: transport}),
		auth.WithFederationClock(clock.Now),
	)
	require.NoError(t, err)

	_, err = broker.Credential(t.Context(), testIdentity(), testStepRef(), "partner")
	require.NoError(t, err)

	require.Equal(t, int64(1), transport.requests.Load(), "the exchange must go through the configured client")
}
