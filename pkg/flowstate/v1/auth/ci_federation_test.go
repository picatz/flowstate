package auth_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/picatz/jose/pkg/jwa"
	"github.com/picatz/jose/pkg/jwk"
	"github.com/picatz/jose/pkg/jwt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// These tests describe what a trust policy can and cannot do with a token from a
// CI platform's OIDC provider, using GitHub Actions as the concrete shape: an
// RS256 token whose "sub" reads
// "repo:<owner>/<name>:ref:refs/heads/<branch>" and which carries "repository",
// "repository_owner", "ref", "workflow", "job_workflow_ref", "event_name" and
// "runner_environment" beside it.
//
// Nothing here reaches the network. The issuer is an httptest server in this
// process publishing a key generated here, so the claim shape is the only thing
// borrowed from the real provider.
//
// They exist because the two halves of "can this be configured" are answered in
// different files: whether a token verifies at all (verifier.go, jwks.go,
// policy.go) and whether the tenant it lands in can be read off a claim
// (policy.go's namespaceFor, through namespace.go's ValidateNamespace). The
// second is where a CI token's claim values and the namespace grammar disagree,
// and a test that only asserted the first would report the whole thing working.

// ciClaims returns the claims a CI-issued token carries, in the shape GitHub
// Actions mints them.
func ciClaims(issuer, owner, repo, branch, audience string, now time.Time) jwt.ClaimsSet {
	claims := standardClaims(
		issuer,
		"repo:"+owner+"/"+repo+":ref:refs/heads/"+branch,
		audience,
		now,
	)

	claims["repository"] = owner + "/" + repo
	claims["repository_owner"] = owner
	claims["repository_visibility"] = "private"
	claims["ref"] = "refs/heads/" + branch
	claims["ref_type"] = "branch"
	claims["workflow"] = "deploy"
	claims["workflow_ref"] = owner + "/" + repo + "/.github/workflows/deploy.yml@refs/heads/" + branch
	claims["job_workflow_ref"] = owner + "/" + repo + "/.github/workflows/deploy.yml@refs/heads/" + branch
	claims["event_name"] = "push"
	claims["runner_environment"] = "github-hosted"
	claims["actor"] = "octocat"
	claims["run_id"] = "1234567890"
	claims["run_attempt"] = "1"

	return claims
}

// TestCIIssuedTokenVerifies is the base case: a trust policy naming the CI
// issuer, one audience, and claim rules narrowing it to one repository and one
// branch admits a token of that shape and refuses the neighbours.
func TestCIIssuedTokenVerifies(t *testing.T) {
	t.Parallel()

	key := newRSAKey(t, "gha-key-1")
	issuer := newTestIssuer(t, key)
	clock := newTestClock(time.Now())

	policy := auth.Policy{
		Issuers: []auth.TrustedIssuer{{
			Name:       "ci-deploy",
			Issuer:     issuer.url,
			Audiences:  []string{"flowstate"},
			Algorithms: []jwa.Algorithm{jwa.RS256},
			Require: []auth.ClaimRule{
				auth.RequireClaim("repository", "octo-org/octo-repo"),
				auth.RequireClaim("ref", "refs/heads/main"),
				auth.RequireClaim("runner_environment", "github-hosted"),
			},
			Role:        "deployer",
			Namespace:   "platform",
			MaxTokenAge: 10 * time.Minute,
		}},
	}

	verifier, err := auth.NewOIDCVerifier(policy, auth.WithClock(clock.Now))
	require.NoError(t, err)

	t.Run("admits the workload the rules name", func(t *testing.T) {
		token := key.sign(t, ciClaims(issuer.url, "octo-org", "octo-repo", "main", "flowstate", clock.Now()))

		principal, err := verifier.Verify(context.Background(), token)
		require.NoError(t, err)

		assert.Equal(t, issuer.url, principal.Issuer)
		assert.Equal(t, "repo:octo-org/octo-repo:ref:refs/heads/main", principal.Subject)
		assert.Equal(t, "ci-deploy", principal.IssuerName)
		assert.Equal(t, "deployer", principal.Role)
		assert.Equal(t, "platform", principal.Namespace)

		// Every claim the token carried is on the principal, whether or not a
		// rule named it. The narrowing happens later, where an identity is
		// derived; see TestCIClaimsCarriedIntoRunIdentity.
		repository, ok := principal.StringClaim("repository")
		require.True(t, ok)
		assert.Equal(t, "octo-org/octo-repo", repository)

		workflowRef, ok := principal.StringClaim("job_workflow_ref")
		require.True(t, ok)
		assert.Contains(t, workflowRef, ".github/workflows/deploy.yml@")
	})

	t.Run("refuses another branch of the same repository", func(t *testing.T) {
		token := key.sign(t, ciClaims(issuer.url, "octo-org", "octo-repo", "topic", "flowstate", clock.Now()))

		_, err := verifier.Verify(context.Background(), token)
		require.Error(t, err)
		assert.ErrorIs(t, err, auth.ErrClaimMismatch)
	})

	t.Run("refuses another repository of the same owner", func(t *testing.T) {
		token := key.sign(t, ciClaims(issuer.url, "octo-org", "other-repo", "main", "flowstate", clock.Now()))

		_, err := verifier.Verify(context.Background(), token)
		require.Error(t, err)
		assert.ErrorIs(t, err, auth.ErrClaimMismatch)
	})

	t.Run("refuses the default audience the platform mints", func(t *testing.T) {
		// A CI job that requests a token without naming an audience gets one
		// addressed to the repository owner's URL. That token is refused here,
		// which is the whole point of requiring an audience: it is what a job
		// has to opt into, per job, to address this deployment.
		token := key.sign(t, ciClaims(issuer.url, "octo-org", "octo-repo", "main", "https://github.com/octo-org", clock.Now()))

		_, err := verifier.Verify(context.Background(), token)
		require.Error(t, err)
		assert.ErrorIs(t, err, auth.ErrInvalidAudience)
	})

	t.Run("refuses a token older than the issuer entry allows", func(t *testing.T) {
		token := key.sign(t, ciClaims(issuer.url, "octo-org", "octo-repo", "main", "flowstate", clock.Now()))

		aged := newTestClock(clock.Now().Add(11 * time.Minute))
		agedVerifier, err := auth.NewOIDCVerifier(policy, auth.WithClock(aged.Now))
		require.NoError(t, err)

		_, err = agedVerifier.Verify(context.Background(), token)
		require.Error(t, err)
		assert.ErrorIs(t, err, auth.ErrTokenExpired)
	})
}

// TestCIIssuerWithPathDiscovers covers the enterprise spelling of the same
// provider, whose issuer identifier carries a path segment. Discovery has to
// append its well-known path to the whole issuer rather than to its host, or
// every such deployment is unreachable.
func TestCIIssuerWithPathDiscovers(t *testing.T) {
	t.Parallel()

	key := newRSAKey(t, "gha-key-1")
	clock := newTestClock(time.Now())

	var base string

	mux := http.NewServeMux()
	mux.HandleFunc("/octo-enterprise/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":   base + "/octo-enterprise",
			"jwks_uri": base + "/.well-known/jwks",
		})
	})
	mux.HandleFunc("/.well-known/jwks", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(jwk.Set{Keys: []jwk.Value{key.jwk(t)}})
	})

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	base = server.URL

	issuerURL := base + "/octo-enterprise"

	verifier, err := auth.NewOIDCVerifier(auth.Policy{
		Issuers: []auth.TrustedIssuer{{
			Name:      "ci-enterprise",
			Issuer:    issuerURL,
			Audiences: []string{"flowstate"},
			Namespace: "platform",
		}},
	}, auth.WithClock(clock.Now))
	require.NoError(t, err)

	token := key.sign(t, ciClaims(issuerURL, "octo-org", "octo-repo", "main", "flowstate", clock.Now()))

	principal, err := verifier.Verify(context.Background(), token)
	require.NoError(t, err)
	assert.Equal(t, issuerURL, principal.Issuer)
}

// TestCITenantFromClaim is the gap. A CI platform serving several teams is
// exactly the case TrustedIssuer.NamespaceClaim exists for, and the claims that
// platform actually mints do not satisfy the namespace grammar: "repository" is
// "<owner>/<name>", and an owner login may carry uppercase letters or an
// underscore. Both are refused by ValidateNamespace, so the caller is refused
// rather than admitted to a shared tenant.
//
// Only an owner login that is already lowercase, digits and dashes maps.
func TestCITenantFromClaim(t *testing.T) {
	t.Parallel()

	key := newRSAKey(t, "gha-key-1")
	issuer := newTestIssuer(t, key)
	clock := newTestClock(time.Now())

	verifierFor := func(t *testing.T, claim string) *auth.OIDCVerifier {
		t.Helper()

		verifier, err := auth.NewOIDCVerifier(auth.Policy{
			Issuers: []auth.TrustedIssuer{{
				Name:           "ci",
				Issuer:         issuer.url,
				Audiences:      []string{"flowstate"},
				NamespaceClaim: claim,
			}},
		}, auth.WithClock(clock.Now))
		require.NoError(t, err)

		return verifier
	}

	t.Run("repository_owner maps when the login is already a legal namespace", func(t *testing.T) {
		verifier := verifierFor(t, "repository_owner")
		token := key.sign(t, ciClaims(issuer.url, "octo-org", "octo-repo", "main", "flowstate", clock.Now()))

		principal, err := verifier.Verify(context.Background(), token)
		require.NoError(t, err)
		assert.Equal(t, "octo-org", principal.Namespace)
	})

	t.Run("repository_owner is refused when the login carries uppercase", func(t *testing.T) {
		verifier := verifierFor(t, "repository_owner")
		token := key.sign(t, ciClaims(issuer.url, "Octo-Org", "octo-repo", "main", "flowstate", clock.Now()))

		_, err := verifier.Verify(context.Background(), token)
		require.Error(t, err)
		assert.ErrorIs(t, err, auth.ErrNoNamespace)
	})

	t.Run("repository_owner is refused when the login carries an underscore", func(t *testing.T) {
		verifier := verifierFor(t, "repository_owner")
		token := key.sign(t, ciClaims(issuer.url, "octo_org", "octo-repo", "main", "flowstate", clock.Now()))

		_, err := verifier.Verify(context.Background(), token)
		require.Error(t, err)
		assert.ErrorIs(t, err, auth.ErrNoNamespace)
	})

	t.Run("repository can never map, because it always contains a separator", func(t *testing.T) {
		verifier := verifierFor(t, "repository")
		token := key.sign(t, ciClaims(issuer.url, "octo-org", "octo-repo", "main", "flowstate", clock.Now()))

		_, err := verifier.Verify(context.Background(), token)
		require.Error(t, err)
		assert.ErrorIs(t, err, auth.ErrNoNamespace)
	})
}

// TestCIClaimsCarriedIntoRunIdentity records which claims survive the step from
// a verified caller to the identity a run acts as, since that is what an
// authorization rule downstream reads.
func TestCIClaimsCarriedIntoRunIdentity(t *testing.T) {
	t.Parallel()

	key := newRSAKey(t, "gha-key-1")
	issuer := newTestIssuer(t, key)
	clock := newTestClock(time.Now())

	verifier, err := auth.NewOIDCVerifier(auth.Policy{
		Issuers: []auth.TrustedIssuer{{
			Name:      "ci",
			Issuer:    issuer.url,
			Audiences: []string{"flowstate"},
			Namespace: "platform",
		}},
	}, auth.WithClock(clock.Now))
	require.NoError(t, err)

	claims := ciClaims(issuer.url, "octo-org", "octo-repo", "main", "flowstate", clock.Now())
	// A claim that is not a string, to show what the carrying step does with
	// one. A CI platform mints its own counters as strings, but an operator can
	// name any claim here.
	claims["private_repo"] = true

	principal, err := verifier.Verify(context.Background(), key.sign(t, claims))
	require.NoError(t, err)

	identity := auth.IdentityFromPrincipal(
		principal,
		"fallback-namespace",
		"prod",
		"repository", "ref", "job_workflow_ref", "private_repo",
	)

	assert.Equal(t, "repo:octo-org/octo-repo:ref:refs/heads/main", identity.Subject)
	assert.Equal(t, issuer.url, identity.Issuer)

	// The verified caller's namespace wins over the deployment's fallback.
	assert.Equal(t, "platform", identity.Namespace)

	assert.Equal(t, map[string]string{
		"repository":       "octo-org/octo-repo",
		"ref":              "refs/heads/main",
		"job_workflow_ref": "octo-org/octo-repo/.github/workflows/deploy.yml@refs/heads/main",
	}, identity.Claims, "only the named string claims are carried")

	// Named but not a string, so it is dropped silently rather than rendered.
	_, carried := identity.Claims["private_repo"]
	assert.False(t, carried, "a non-string claim is not carried")

	// Claims nobody named stay behind, whatever the token held.
	_, carried = identity.Claims["actor"]
	assert.False(t, carried, "an unnamed claim is not carried")

	// The subject a downstream relying party sees for a step of this run. It is
	// built from the namespace and deployment, never from the CI subject, which
	// travels as on_behalf_of instead.
	subject, err := identity.SubjectFor(auth.StepRef{Workflow: "deploy", Run: "r1", Step: "push"})
	require.NoError(t, err)
	assert.Equal(t, "flowstate:platform/prod/deploy/push", subject)
}
