package auth_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/authn"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
)

// TestFlowstateToFlowstateFederation is the claim the `assertion` target exists
// to make good: one Flowstate deployment calls another's RPC surface with
// nothing but the assertion it minted for itself, and the second deployment
// admits the call, names the caller, and lands it in a namespace.
//
// Both halves of this repository's identity story are already written; what was
// missing was the join. Deployment B's inbound half is `auth.Policy` plus
// `auth.OIDCVerifier` plus the Connect authenticator — the same code path that
// admits a GitHub Actions token, pointed at a Flowstate issuer instead.
// Deployment A's outbound half is `FederationPolicy` plus `Broker`, and until
// the `assertion` target every one of its kinds had to trade the assertion for
// something else first. There was no middleman here to be, so there was no way
// to make the call at all.
//
// Nothing reaches the network. Deployment A's issuer serves its own discovery
// document and key set from an httptest server, and deployment B fetches them
// over that server, so the trust really is established by verification against
// published keys rather than by the two halves sharing a Go value.
func TestFlowstateToFlowstateFederation(t *testing.T) {
	clock := authtest.NewClock(referenceTime)

	// ── Deployment A: the caller ────────────────────────────────────────────
	//
	// Its identity endpoint has to have a URL before the issuer that serves it
	// can be told what that URL is, so the server dispatches to a handler
	// installed once the broker exists.
	var (
		mu      sync.RWMutex
		handler http.Handler
	)
	deploymentA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		current := handler
		mu.RUnlock()

		require.NotNil(t, current, "deployment A must be serving its keys before B fetches them")
		current.ServeHTTP(w, r)
	}))
	t.Cleanup(deploymentA.Close)

	// ── Deployment B: the callee ────────────────────────────────────────────
	//
	// An ordinary Flowstate API surface: every request is authenticated by the
	// same authenticator `flow server` installs, against a trust policy whose
	// only entry names deployment A's issuer. The audience is B's own URL, so
	// an assertion A minted for anyone else does not verify here.
	var deploymentB *httptest.Server
	admitted := make(chan auth.Principal, 4)

	deploymentB = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		verifier, err := auth.NewOIDCVerifier(
			auth.Policy{
				Issuers: []auth.TrustedIssuer{{
					Name:      "peer-flowstate",
					Issuer:    deploymentA.URL,
					Audiences: []string{deploymentB.URL},
					// B decides which of A's workloads it admits, and under
					// what role, exactly as it would for any other issuer.
					Require: []auth.ClaimRule{
						auth.RequireClaim(auth.ClaimWorkflow, "deploy-service"),
						auth.RequireClaim(auth.ClaimDeployment, "prod"),
					},
					// The tenant a run from A lands in is read off the
					// assertion's own namespace claim rather than pinned, so
					// two tenants in A stay two tenants in B.
					NamespaceClaim: auth.ClaimNamespace,
					Role:           "peer",
				}},
			},
			auth.WithClock(clock.Now),
		)
		require.NoError(t, err)

		authenticator := auth.NewAuthenticator(verifier)

		authn.NewMiddleware(authenticator.Authenticate).Wrap(http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				principal, ok := auth.PrincipalFromContext(r.Context())
				require.True(t, ok, "the middleware must never let an unauthenticated request through")

				admitted <- principal

				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"run": "run-7"})
			},
		)).ServeHTTP(w, r)
	}))
	t.Cleanup(deploymentB.Close)

	// Deployment A's outbound configuration: one target, presenting the
	// assertion itself, bound to the one deployment it may be presented to.
	policy, err := auth.ParseFederationPolicy([]byte(`
issuer: ` + deploymentA.URL + `
assertion_lifetime: 2m
declared_claims: [repository]
allow:
  - 'target == "peer-flowstate" && workload.namespace == "acme"'
targets:
  - name: peer-flowstate
    assertion:
      audience: ` + deploymentB.URL + `
`))
	require.NoError(t, err)

	key, err := auth.GenerateSigningKey("2026-08", jwa.ES256)
	require.NoError(t, err)

	broker, err := policy.Broker(key, auth.WithFederationClock(clock.Now))
	require.NoError(t, err)

	mu.Lock()
	handler = broker.Issuer().Handler()
	mu.Unlock()

	// ── The call ────────────────────────────────────────────────────────────
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		deploymentB.URL+"/flowstate.v1.WorkflowService/Run", strings.NewReader("{}"))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")

	// Authorize is the whole outbound path: policy, mint, "exchange", header.
	// A workload never holds the token, and here there is not even a relying
	// party in between.
	require.NoError(t, broker.Authorize(t.Context(), request, testIdentity(), testStepRef(), "peer-flowstate"))

	response, err := deploymentB.Client().Do(request)
	require.NoError(t, err)
	t.Cleanup(func() { _ = response.Body.Close() })

	require.Equal(t, http.StatusOK, response.StatusCode,
		"deployment B should admit a run from a deployment its policy trusts")

	// ── What B saw ──────────────────────────────────────────────────────────
	principal := <-admitted
	require.Equal(t, deploymentA.URL, principal.Issuer, "the caller is deployment A's issuer, verified against its published keys")
	require.Equal(t, "peer-flowstate", principal.IssuerName)
	require.Equal(t, "flowstate:acme/prod/deploy-service/push-image", principal.Subject,
		"the subject names the run and the step, not just the deployment")
	require.Equal(t, "peer", principal.Role)
	require.Equal(t, "acme", principal.Namespace, "the namespace claim A minted decides the tenant B lands the caller in")

	workflow, ok := principal.StringClaim(auth.ClaimWorkflow)
	require.True(t, ok)
	require.Equal(t, "deploy-service", workflow)

	run, ok := principal.StringClaim(auth.ClaimRun)
	require.True(t, ok)
	require.Equal(t, "run-1", run, "which run called is legible to the callee, which is what makes an audit trail cross the boundary")

	// The credential is the assertion, so its expiry is the assertion's: the
	// issuer's lifetime knob is the only one, and this is the assertion below
	// that it is set to.
	credential, err := broker.Credential(t.Context(), testIdentity(), testStepRef(), "peer-flowstate")
	require.NoError(t, err)
	require.Equal(t, auth.CredentialBearer, credential.Type)
	require.Equal(t, "peer-flowstate", credential.Target)
	require.Equal(t, clock.Now().Add(2*time.Minute), credential.ExpiresAt.UTC(),
		"the credential expires when the assertion does, not on a lifetime this target chose")

	t.Run("B refuses an assertion A minted for somebody else", func(t *testing.T) {
		// The same workload, the same deployment, the same signing key — and an
		// audience naming a different relying party. This is the direction that
		// matters for a bearer assertion: what stops the one presented to a
		// partner from being replayed here is the "aud" claim, and nothing else.
		elsewhere, err := auth.ParseFederationPolicy([]byte(`
issuer: ` + deploymentA.URL + `
declared_claims: [repository]
targets:
  - name: somewhere-else
    assertion:
      audience: https://partner.example.com
`))
		require.NoError(t, err)

		misdirected, err := elsewhere.Broker(key, auth.WithFederationClock(clock.Now))
		require.NoError(t, err)

		mu.Lock()
		previous := handler
		handler = misdirected.Issuer().Handler()
		mu.Unlock()
		t.Cleanup(func() {
			mu.Lock()
			handler = previous
			mu.Unlock()
		})

		replay, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
			deploymentB.URL+"/flowstate.v1.WorkflowService/Run", strings.NewReader("{}"))
		require.NoError(t, err)
		replay.Header.Set("Content-Type", "application/json")
		require.NoError(t, misdirected.Authorize(t.Context(), replay, testIdentity(), testStepRef(), "somewhere-else"))

		refused, err := deploymentB.Client().Do(replay)
		require.NoError(t, err)
		t.Cleanup(func() { _ = refused.Body.Close() })

		require.Equal(t, http.StatusUnauthorized, refused.StatusCode,
			"an assertion minted for another relying party must not be usable here")
	})

	t.Run("A refuses a workload its own assumption rule excludes", func(t *testing.T) {
		// Both sides get a say, and A's say comes first: nothing signed by A
		// exists for a workload A would not have let ask.
		other := testIdentity()
		other.Namespace = "other"

		_, err := broker.Credential(t.Context(), other, testStepRef(), "peer-flowstate")
		require.ErrorIs(t, err, auth.ErrAssumeDenied)
	})
}
