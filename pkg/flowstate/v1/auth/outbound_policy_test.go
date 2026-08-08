package auth_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/stretchr/testify/require"
)

// TestParseFederationPolicySimpleCases checks that the common cases are short. A
// policy an operator will not write is a policy that does not get written.
func TestParseFederationPolicySimpleCases(t *testing.T) {
	t.Run("one AWS role", func(t *testing.T) {
		policy, err := auth.ParseFederationPolicy([]byte(`
issuer: https://flowstate.example.com
targets:
  - name: aws-prod
    aws:
      role_arn: arn:aws:iam::123456789012:role/flowstate
`))
		require.NoError(t, err)
		require.Equal(t, "https://flowstate.example.com", policy.Issuer)
		require.Len(t, policy.Targets, 1)
		require.Equal(t, "arn:aws:iam::123456789012:role/flowstate", policy.Targets[0].AWS.RoleARN)

		key, err := auth.GenerateSigningKey("k", jwa.ES256)
		require.NoError(t, err)

		broker, err := policy.Broker(key)
		require.NoError(t, err)
		require.Equal(t, []string{"aws-prod"}, broker.Targets())
		require.Equal(t, "https://flowstate.example.com", broker.Issuer().URL())
	})

	t.Run("one trusted authorization server", func(t *testing.T) {
		policy, err := auth.ParseFederationPolicy([]byte(`
issuer: https://flowstate.example.com
targets:
  - name: partner
    token_exchange:
      token_url: https://as.partner.example.com/oauth2/token
      audience: https://as.partner.example.com
`))
		require.NoError(t, err)

		key, err := auth.GenerateSigningKey("k", jwa.ES256)
		require.NoError(t, err)

		broker, err := policy.Broker(key)
		require.NoError(t, err)
		require.Equal(t, []string{"partner"}, broker.Targets())
	})
}

// TestParseFederationPolicyFullCase checks that the complicated case uses the same
// structure as the simple one: more targets across more providers, with rules,
// rather than a different mechanism.
func TestParseFederationPolicyFullCase(t *testing.T) {
	policy, err := auth.ParseFederationPolicy([]byte(`
issuer: https://flowstate.example.com
assertion_lifetime: 2m
key_retention: 48h
jwks_path: /keys.json

deny:
  - 'workload.step == "debug"'
allow:
  - 'target == "aws-prod" && workload.on_behalf_of.startsWith("repo:picatz/flowstate:")'
  - 'target == "gcp-analytics" && workload.namespace == "acme"'
  - 'target == "partner" && "repository" in workload.claims'
  - 'target == "internal" && workload.deployment == "prod"'

targets:
  - name: aws-prod
    aws:
      role_arn: arn:aws:iam::123456789012:role/flowstate
      region: us-east-1
      duration: 15m
      session_policy_arns:
        - arn:aws:iam::aws:policy/ReadOnlyAccess
  - name: gcp-analytics
    gcp:
      audience: //iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/flowstate
      service_account_email: flowstate@project.iam.gserviceaccount.com
      scopes:
        - https://www.googleapis.com/auth/bigquery
      lifetime: 30m
  - name: partner
    token_exchange:
      token_url: https://as.partner.example.com/oauth2/token
      audience: https://as.partner.example.com
      target_audience: https://api.partner.example.com
      scopes: [orders.read]
  - name: internal
    client_credentials:
      token_url: https://as.internal.example.com/oauth2/token
      client_id: flowstate-prod
      scopes: [internal.write]
`))
	require.NoError(t, err)

	require.Equal(t, 2*time.Minute, policy.AssertionLifetime)
	require.Equal(t, 48*time.Hour, policy.KeyRetention)
	require.Equal(t, "/keys.json", policy.JWKSPath)
	require.Len(t, policy.Allow, 4)
	require.Len(t, policy.Deny, 1)
	require.Equal(t, 15*time.Minute, policy.Targets[0].AWS.Duration)
	require.Equal(t, 30*time.Minute, policy.Targets[1].GCP.Lifetime)

	key, err := auth.GenerateSigningKey("k", jwa.ES256)
	require.NoError(t, err)

	broker, err := policy.Broker(key)
	require.NoError(t, err)

	require.Equal(t, []string{"aws-prod", "gcp-analytics", "internal", "partner"}, broker.Targets())
	require.Equal(t, 2*time.Minute, broker.Issuer().AssertionLifetime())
	require.Equal(t, "/keys.json", broker.Issuer().JWKSPath())
	require.Equal(t, "https://flowstate.example.com/keys.json", broker.Issuer().JWKSURL())
}

// TestParseFederationPolicyRejects covers configuration that must not start a
// server. Every one of these is a mistake that would otherwise show up as a
// puzzling refusal, or worse, as a rule that never fires.
func TestParseFederationPolicyRejects(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "no issuer",
			input: "targets:\n  - name: aws-prod\n    aws:\n      role_arn: arn:aws:iam::1:role/r\n",
		},
		{
			name:  "an issuer that is not https",
			input: "issuer: http://flowstate.example.com\n",
		},
		{
			name:  "a target with no provider",
			input: "issuer: https://flowstate.example.com\ntargets:\n  - name: aws-prod\n",
		},
		{
			name: "a target with two providers, which names two systems",
			input: `issuer: https://flowstate.example.com
targets:
  - name: both
    aws:
      role_arn: arn:aws:iam::1:role/r
    token_exchange:
      token_url: https://as.example.com/token
      audience: https://as.example.com
`,
		},
		{
			name: "two targets with the same name",
			input: `issuer: https://flowstate.example.com
targets:
  - name: aws
    aws:
      role_arn: arn:aws:iam::1:role/r
  - name: aws
    aws:
      role_arn: arn:aws:iam::2:role/other
`,
		},
		{
			name:  "a target with no name",
			input: "issuer: https://flowstate.example.com\ntargets:\n  - aws:\n      role_arn: arn:aws:iam::1:role/r\n",
		},
		{
			name: "a misspelled field, which would silently drop a restriction",
			input: `issuer: https://flowstate.example.com
targets:
  - name: aws
    aws:
      role_arm: arn:aws:iam::1:role/r
`,
		},
		{
			name: "a rule that does not compile",
			input: `issuer: https://flowstate.example.com
allow:
  - 'workload.namespace =='
targets:
  - name: aws
    aws:
      role_arn: arn:aws:iam::1:role/r
`,
		},
		{
			name: "a rule naming an attribute that does not exist",
			input: `issuer: https://flowstate.example.com
allow:
  - 'workload.tenant == "acme"'
targets:
  - name: aws
    aws:
      role_arn: arn:aws:iam::1:role/r
`,
		},
		{
			name: "a rule that does not produce a boolean",
			input: `issuer: https://flowstate.example.com
allow:
  - 'workload.namespace'
targets:
  - name: aws
    aws:
      role_arn: arn:aws:iam::1:role/r
`,
		},
		{
			name: "an AWS session longer than AWS allows",
			input: `issuer: https://flowstate.example.com
targets:
  - name: aws
    aws:
      role_arn: arn:aws:iam::1:role/r
      duration: 24h
`,
		},
		{
			name: "a token endpoint on an unprotected host",
			input: `issuer: https://flowstate.example.com
targets:
  - name: partner
    token_exchange:
      token_url: http://as.partner.example.com/token
      audience: https://as.partner.example.com
`,
		},
		{
			name: "a token exchange with no audience",
			input: `issuer: https://flowstate.example.com
targets:
  - name: partner
    token_exchange:
      token_url: https://as.partner.example.com/token
`,
		},
		{
			name:  "not YAML at all",
			input: "\t\tissuer: [",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy, err := auth.ParseFederationPolicy([]byte(test.input))
			require.ErrorIs(t, err, auth.ErrInvalidPolicy)
			require.Empty(t, policy.Targets)

			// And the same mistake refuses to build a broker, for a policy
			// constructed in Go rather than parsed.
			require.Error(t, policy.Validate())
		})
	}
}

// TestPolicyCarriesBothDirections checks that one reviewable file can describe both
// directions of trust: who Flowstate accepts callers from, and who it presents its
// own identity to.
func TestPolicyCarriesBothDirections(t *testing.T) {
	policy, err := auth.ParsePolicy([]byte(`
issuers:
  - name: github-actions
    issuer: https://token.actions.githubusercontent.com
    audiences: [flowstate]
    role: deployer
    require:
      - claim: repository
        any_of: [picatz/flowstate]

federation:
  issuer: https://flowstate.example.com
  allow:
    - 'target == "aws-prod" && workload.on_behalf_of.startsWith("repo:picatz/flowstate:")'
  targets:
    - name: aws-prod
      aws:
        role_arn: arn:aws:iam::123456789012:role/flowstate
`))
	require.NoError(t, err)

	// Inbound, unchanged.
	require.Len(t, policy.Issuers, 1)
	require.Equal(t, "github-actions", policy.Issuers[0].Name)

	// Outbound, from the same file.
	require.NotNil(t, policy.Federation)
	require.Equal(t, "https://flowstate.example.com", policy.Federation.Issuer)
	require.Len(t, policy.Federation.Targets, 1)

	key, err := auth.GenerateSigningKey("k", jwa.ES256)
	require.NoError(t, err)

	broker, err := policy.Federation.Broker(key)
	require.NoError(t, err)
	require.Equal(t, []string{"aws-prod"}, broker.Targets())

	t.Run("a federation section that does not validate fails the whole policy", func(t *testing.T) {
		_, err := auth.ParsePolicy([]byte(`
issuers:
  - name: github-actions
    issuer: https://token.actions.githubusercontent.com
    audiences: [flowstate]

federation:
  issuer: https://flowstate.example.com
  targets:
    - name: aws-prod
      aws:
        role_arn: not-an-arn
`))
		require.ErrorIs(t, err, auth.ErrInvalidPolicy)
	})

	t.Run("a policy with no federation section still works", func(t *testing.T) {
		policy, err := auth.ParsePolicy([]byte(`
issuers:
  - name: github-actions
    issuer: https://token.actions.githubusercontent.com
    audiences: [flowstate]
`))
		require.NoError(t, err)
		require.Nil(t, policy.Federation)
	})
}

// TestFederationRoundTrip is the whole point, end to end: a Flowstate workload
// obtains a credential from a relying party that has never been given a secret, and
// that relying party decides to trust it by verifying the assertion against the keys
// Flowstate publishes.
//
// Both halves of federation are exercised at once. The relying party verifies with
// this package's own [auth.OIDCVerifier], which is the same code path an external
// system would use if it happened to be written in Go, and the same one Flowstate
// uses to verify tokens from GitHub or Kubernetes.
func TestFederationRoundTrip(t *testing.T) {
	clock := authtest.NewClock(referenceTime)

	// Flowstate's own identity endpoint, whose URL has to exist before the issuer
	// that serves it can be told what it is.
	var (
		mu      sync.RWMutex
		handler http.Handler
	)
	identityServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		current := handler
		mu.RUnlock()

		require.NotNil(t, current, "the issuer must be serving before a relying party fetches its keys")
		current.ServeHTTP(w, r)
	}))
	t.Cleanup(identityServer.Close)

	// The relying party: an authorization server that trusts Flowstate's issuer,
	// verifies presented assertions properly, and issues its own token in exchange.
	var relyingParty *httptest.Server
	verified := make(chan auth.Principal, 4)

	relyingParty = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())

		verifier, err := auth.NewOIDCVerifier(
			auth.Policy{
				Issuers: []auth.TrustedIssuer{{
					Name:      "flowstate",
					Issuer:    identityServer.URL,
					Audiences: []string{relyingParty.URL},
					// The relying party's own authorization: only this workload,
					// acting for this repository, may exchange here.
					Require: []auth.ClaimRule{
						auth.RequireClaim(auth.ClaimWorkflow, "deploy-service"),
						auth.RequireClaim(auth.ClaimOnBehalfOf, "repo:picatz/flowstate:ref:refs/heads/main"),
					},
					Role: "partner-client",
				}},
			},
			auth.WithClock(clock.Now),
		)
		require.NoError(t, err)

		principal, err := verifier.Verify(r.Context(), r.PostForm.Get("subject_token"))
		if err != nil {
			writeJSON(t, w, http.StatusBadRequest, map[string]any{
				"error":             "invalid_grant",
				"error_description": err.Error(),
			})
			return
		}

		verified <- principal

		writeJSON(t, w, http.StatusOK, map[string]any{
			"access_token": "partner-token-for-" + principal.Subject,
			"token_type":   "Bearer",
			"expires_in":   3600,
		})
	}))
	t.Cleanup(relyingParty.Close)

	// Flowstate's configuration: one file, describing the identity it presents and
	// the one system it may present it to.
	policy, err := auth.ParseFederationPolicy([]byte(`
issuer: ` + identityServer.URL + `
allow:
  - 'target == "partner" && workload.on_behalf_of.startsWith("repo:picatz/flowstate:")'
targets:
  - name: partner
    token_exchange:
      token_url: ` + relyingParty.URL + `/token
      audience: ` + relyingParty.URL + `
`))
	require.NoError(t, err)

	key, err := auth.GenerateSigningKey("2026-07", jwa.ES256)
	require.NoError(t, err)

	broker, err := policy.Broker(key, auth.WithFederationClock(clock.Now))
	require.NoError(t, err)

	mu.Lock()
	handler = broker.Issuer().Handler()
	mu.Unlock()

	credential, err := broker.Credential(t.Context(), testIdentity(), testStepRef(), "partner")
	require.NoError(t, err, "a workload should be able to reach a system that trusts Flowstate's identity")

	bearer, ok := credential.Bearer()
	require.True(t, ok)
	require.Equal(t, "partner-token-for-flowstate:acme/prod/deploy-service/push-image", bearer)
	require.Equal(t, "partner", credential.Target)

	// What the relying party saw: a workload identity it could authorize on,
	// including who the workload was acting for.
	principal := <-verified
	require.Equal(t, "flowstate:acme/prod/deploy-service/push-image", principal.Subject)
	require.Equal(t, identityServer.URL, principal.Issuer)
	require.Equal(t, "partner-client", principal.Role)

	onBehalfOf, ok := principal.StringClaim(auth.ClaimOnBehalfOf)
	require.True(t, ok)
	require.Equal(t, "repo:picatz/flowstate:ref:refs/heads/main", onBehalfOf)

	t.Run("the relying party refuses a workload its own policy excludes", func(t *testing.T) {
		other := testIdentity()
		other.Subject = "repo:attacker/fork:ref:refs/heads/main"

		// Flowstate's assumption rule refuses this one before anything is minted.
		_, err := broker.Credential(t.Context(), other, testStepRef(), "partner")
		require.ErrorIs(t, err, auth.ErrAssumeDenied)

		// With Flowstate's rule relaxed, the relying party's own rule still
		// refuses it: both sides get a say, which is what federation means.
		permissive, err := auth.ParseFederationPolicy([]byte(`
issuer: ` + identityServer.URL + `
targets:
  - name: partner
    token_exchange:
      token_url: ` + relyingParty.URL + `/token
      audience: ` + relyingParty.URL + `
`))
		require.NoError(t, err)

		permissiveBroker, err := permissive.Broker(key, auth.WithFederationClock(clock.Now))
		require.NoError(t, err)

		mu.Lock()
		handler = permissiveBroker.Issuer().Handler()
		mu.Unlock()

		_, err = permissiveBroker.Credential(t.Context(), other, testStepRef(), "partner")
		require.ErrorIs(t, err, auth.ErrExchangeFailed)
		require.Contains(t, err.Error(), "invalid_grant")
	})

	t.Run("a different step of the same workload gets its own identity", func(t *testing.T) {
		mu.Lock()
		handler = broker.Issuer().Handler()
		mu.Unlock()

		ref := auth.StepRef{Workflow: "deploy-service", Run: "run-2", Step: "notify"}

		credential, err := broker.Credential(t.Context(), testIdentity(), ref, "partner")
		require.NoError(t, err)

		bearer, ok := credential.Bearer()
		require.True(t, ok)
		require.Equal(t, "partner-token-for-flowstate:acme/prod/deploy-service/notify", bearer,
			"a credential is scoped to one step, so one leaked credential is not the whole workload")
	})
}

// TestFederationPolicyHasNoPlaceForSecrets checks the property that makes a policy
// file safe to review in a pull request: there is no field that could hold one.
func TestFederationPolicyHasNoPlaceForSecrets(t *testing.T) {
	// A client secret is the one long-lived credential the OAuth flows could want,
	// and the file form deliberately cannot express it.
	_, err := auth.ParseFederationPolicy([]byte(`
issuer: https://flowstate.example.com
targets:
  - name: internal
    client_credentials:
      token_url: https://as.internal.example.com/oauth2/token
      client_id: flowstate-prod
      client_secret: hunter2
`))
	require.ErrorIs(t, err, auth.ErrInvalidPolicy)
	require.Contains(t, err.Error(), "client_secret")

	// Nor does the round trip of a policy ever carry one.
	policy, err := auth.ParseFederationPolicy([]byte(`
issuer: https://flowstate.example.com
targets:
  - name: internal
    client_credentials:
      token_url: https://as.internal.example.com/oauth2/token
      client_id: flowstate-prod
`))
	require.NoError(t, err)

	encoded, err := json.Marshal(policy)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "secret")
}
