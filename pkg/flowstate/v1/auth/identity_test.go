package auth_test

import (
	"bytes"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/stretchr/testify/require"
)

// protoIdentity has the accessors the generated flowstate.v1.WorkloadIdentity
// message has, and stands in for it here so this package's tests do not depend on
// the generated types.
//
// That [auth.IdentityFrom] accepts this at all is the point of the
// [auth.IdentitySource] interface: the auth package cannot import the generated
// protobuf types, because the package that defines them needs to import auth.
type protoIdentity struct {
	subject    string
	issuer     string
	claims     map[string]string
	namespace  string
	deployment string
}

func (p *protoIdentity) GetSubject() string           { return p.subject }
func (p *protoIdentity) GetIssuer() string            { return p.issuer }
func (p *protoIdentity) GetClaims() map[string]string { return p.claims }
func (p *protoIdentity) GetNamespace() string         { return p.namespace }
func (p *protoIdentity) GetDeployment() string        { return p.deployment }

// TestIdentityFrom covers the conversion the engine uses to turn the identity
// carried in run state into one this package can mint for.
func TestIdentityFrom(t *testing.T) {
	t.Run("a populated identity", func(t *testing.T) {
		source := &protoIdentity{
			subject:    "repo:picatz/flowstate:ref:refs/heads/main",
			issuer:     "https://token.actions.githubusercontent.com",
			claims:     map[string]string{"repository": "picatz/flowstate"},
			namespace:  "acme",
			deployment: "prod",
		}

		identity := auth.IdentityFrom(source)
		require.Equal(t, testIdentity(), identity)
		require.NoError(t, identity.Validate())

		// The claims are copied, so a later change to the run state cannot change
		// what an assertion will say.
		source.claims["repository"] = "attacker/fork"
		require.Equal(t, "picatz/flowstate", identity.Claims["repository"])
	})

	t.Run("no identity at all", func(t *testing.T) {
		// A run submitted before identity was recorded, or by a path that does not
		// establish one, must not become a usable identity.
		identity := auth.IdentityFrom(nil)
		require.True(t, identity.IsZero())
		require.ErrorIs(t, identity.Validate(), auth.ErrInvalidIdentity)
	})

	t.Run("a typed nil, as an unset protobuf field arrives", func(t *testing.T) {
		var source *protoIdentity

		identity := auth.IdentityFrom(source)
		require.True(t, identity.IsZero())
		require.ErrorIs(t, identity.Validate(), auth.ErrInvalidIdentity)
	})
}

// TestIdentityFromPrincipal covers deriving a workload's identity from the caller
// that submitted the run, which is where the two halves of federation meet.
func TestIdentityFromPrincipal(t *testing.T) {
	principal := auth.Principal{
		Subject: "repo:picatz/flowstate:ref:refs/heads/main",
		Issuer:  "https://token.actions.githubusercontent.com",
		Claims: map[string]any{
			"repository": "picatz/flowstate",
			"ref":        "refs/heads/main",
			"email":      "someone@example.com",
			"verified":   true,
		},
	}

	identity := auth.IdentityFromPrincipal(principal, "acme", "prod", "repository", "ref", "absent", "verified")

	require.Equal(t, principal.Subject, identity.Subject)
	require.Equal(t, principal.Issuer, identity.Issuer)
	require.Equal(t, "acme", identity.Namespace)
	require.Equal(t, "prod", identity.Deployment)

	// Only the named claims are carried, and only the ones that are strings: an
	// assertion goes to a third party, so what it says about the caller should be
	// what an operator chose to say.
	require.Equal(t, map[string]string{
		"repository": "picatz/flowstate",
		"ref":        "refs/heads/main",
	}, identity.Claims)

	require.NotContains(t, identity.Claims, "email", "a claim nobody named must not be carried")
	require.NotContains(t, identity.Claims, "absent")
	require.NotContains(t, identity.Claims, "verified")

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

// TestOutboundValuesNeverLogSecrets checks that every value involved in outbound
// federation can be handed to a logger without leaking. Logging is the most common
// way a credential escapes, and each of these types is one an operator will
// reasonably want to log.
func TestOutboundValuesNeverLogSecrets(t *testing.T) {
	clock := newTestClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	party := newRelyingParty(t, func(w http.ResponseWriter, r *http.Request, body recordedRequest) {
		writeJSON(t, w, http.StatusOK, map[string]any{
			"access_token": "super-secret-token",
			"token_type":   "Bearer",
			"expires_in":   3600,
		})
	})

	exchanger, err := auth.NewTokenExchanger(auth.TokenExchangeConfig{
		Name:     "partner",
		TokenURL: party.url + "/token",
		Audience: "https://as.example.com",
		Clock:    clock.Now,
	})
	require.NoError(t, err)
	require.Equal(t, "partner", exchanger.Name())

	assertion := mintAssertion(t, issuer, "https://as.example.com")

	credential, err := exchanger.Exchange(t.Context(), assertion)
	require.NoError(t, err)

	key, err := auth.GenerateSigningKey("logged-key", jwa.ES256)
	require.NoError(t, err)

	identity := testIdentity()
	identity.Claims = map[string]string{"email": "someone@example.com"}

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
	clock := newTestClock(referenceTime)

	party := newRelyingParty(t, func(w http.ResponseWriter, r *http.Request, body recordedRequest) {
		writeJSON(t, w, http.StatusOK, map[string]any{
			"access_token": "downstream-token",
			"token_type":   "Bearer",
			"expires_in":   3600,
		})
	})

	policy, err := auth.ParseFederationPolicy([]byte(`
issuer: https://flowstate.example.com
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
