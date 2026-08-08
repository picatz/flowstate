package auth_test

import (
	"context"
	"testing"
	"time"

	"github.com/picatz/jose/pkg/jwa"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
)

// These tests describe what a trust policy can and cannot do with a token from a
// CI platform's OIDC provider, using GitHub Actions as the concrete shape: an
// RS256 token whose "sub" reads
// "repo:<owner>/<name>:ref:refs/heads/<branch>" and which carries "repository",
// "repository_owner", "ref", "workflow", "job_workflow_ref", "event_name" and
// "runner_environment" beside it.
//
// Nothing here reaches the network. The issuer is an authtest.Issuer in this
// process publishing a key generated here, so the claim shape is the only thing
// borrowed from the real provider. The claim set itself stays in this file:
// authtest knows nothing about any provider's claim names, which is what keeps
// it usable for the next one.
//
// They exist because the two halves of "can this be configured" are answered in
// different files: whether a token verifies at all (verifier.go, jwks.go,
// policy.go) and whether the tenant it lands in can be read off a claim
// (policy.go's namespaceFor, through namespace.go's ValidateNamespace). The
// second is where a CI token's claim values and the namespace grammar disagree,
// and a test that only asserted the first would report the whole thing working.
//
// These stay offline, and a second file carries the other half. What is written
// here is this package's reading of the specification and of a provider's
// documented claim shape: it runs on a laptop, on a fork, and with the network
// unplugged, and it fails when this package's own behavior changes. What it
// cannot tell anyone is whether the provider still serves the discovery
// document, key set, algorithm and claim names that reading assumes, because
// the issuer answering is one written in this file. That is realtoken_test.go,
// which asks a GitHub Actions runner for a real token and verifies it through
// live discovery, and which skips everywhere the runner's token endpoint is
// absent. Neither replaces the other: a change in this package fails here, a
// change at the provider fails there, and only the pair distinguishes the two.

// ciClaims returns the claims a CI-issued token carries beside the registered
// ones, in the shape GitHub Actions mints them.
//
// This lives here rather than in the authtest package, which knows nothing
// about any particular provider's claim names. A claim set is what a test
// describes; the double mints what it is given.
func ciClaims(owner, repo, branch string) map[string]any {
	return map[string]any{
		"repository":            owner + "/" + repo,
		"repository_owner":      owner,
		"repository_visibility": "private",
		"ref":                   "refs/heads/" + branch,
		"ref_type":              "branch",
		"workflow":              "deploy",
		"workflow_ref":          owner + "/" + repo + "/.github/workflows/deploy.yml@refs/heads/" + branch,
		"job_workflow_ref":      owner + "/" + repo + "/.github/workflows/deploy.yml@refs/heads/" + branch,
		"event_name":            "push",
		"runner_environment":    "github-hosted",
		"actor":                 "octocat",
		"run_id":                "1234567890",
		"run_attempt":           "1",
	}
}

// ciSubject returns the "sub" such a token carries, which names a repository
// and a ref rather than a person.
func ciSubject(owner, repo, branch string) string {
	return "repo:" + owner + "/" + repo + ":ref:refs/heads/" + branch
}

// ciToken mints a token of that shape, addressed to the given audience.
func ciToken(issuer *authtest.Issuer, owner, repo, branch, audience string) string {
	return issuer.MintToken(
		ciClaims(owner, repo, branch),
		authtest.WithSubject(ciSubject(owner, repo, branch)),
		authtest.WithAudience(audience),
	)
}

// TestCIIssuedTokenVerifies is the base case: a trust policy naming the CI
// issuer, one audience, and claim rules narrowing it to one repository and one
// branch admits a token of that shape and refuses the neighbours.
func TestCIIssuedTokenVerifies(t *testing.T) {
	t.Parallel()

	key := authtest.GenerateKey("gha-key-1", jwa.RS256)
	clock := authtest.NewClock(time.Now())
	issuer := newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))

	policy := auth.Policy{
		Issuers: []auth.TrustedIssuer{{
			Name:       "ci-deploy",
			Issuer:     issuer.URL(),
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
		token := ciToken(issuer, "octo-org", "octo-repo", "main", "flowstate")

		principal, err := verifier.Verify(context.Background(), token)
		require.NoError(t, err)

		assert.Equal(t, issuer.URL(), principal.Issuer)
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
		token := ciToken(issuer, "octo-org", "octo-repo", "topic", "flowstate")

		_, err := verifier.Verify(context.Background(), token)
		require.Error(t, err)
		assert.ErrorIs(t, err, auth.ErrClaimMismatch)
	})

	t.Run("refuses another repository of the same owner", func(t *testing.T) {
		token := ciToken(issuer, "octo-org", "other-repo", "main", "flowstate")

		_, err := verifier.Verify(context.Background(), token)
		require.Error(t, err)
		assert.ErrorIs(t, err, auth.ErrClaimMismatch)
	})

	t.Run("refuses the default audience the platform mints", func(t *testing.T) {
		// A CI job that requests a token without naming an audience gets one
		// addressed to the repository owner's URL. That token is refused here,
		// which is the whole point of requiring an audience: it is what a job
		// has to opt into, per job, to address this deployment.
		token := ciToken(issuer, "octo-org", "octo-repo", "main", "https://github.com/octo-org")

		_, err := verifier.Verify(context.Background(), token)
		require.Error(t, err)
		assert.ErrorIs(t, err, auth.ErrInvalidAudience)
	})

	t.Run("refuses a token older than the issuer entry allows", func(t *testing.T) {
		token := ciToken(issuer, "octo-org", "octo-repo", "main", "flowstate")

		aged := authtest.NewClock(clock.Now().Add(11 * time.Minute))
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

	key := authtest.GenerateKey("gha-key-1", jwa.RS256)
	clock := authtest.NewClock(time.Now())

	// The identifier carries the organization, and the keys are published at
	// the host root, which is the arrangement a self-hosted deployment of such
	// a provider serves.
	issuer := newTestIssuer(t,
		authtest.WithClock(clock.Now),
		authtest.WithKeys(key),
		authtest.WithIssuerPath("/octo-enterprise"),
		authtest.WithJWKSPath("/.well-known/jwks"),
	)

	verifier, err := auth.NewOIDCVerifier(auth.Policy{
		Issuers: []auth.TrustedIssuer{{
			Name:      "ci-enterprise",
			Issuer:    issuer.URL(),
			Audiences: []string{"flowstate"},
			Namespace: "platform",
		}},
	}, auth.WithClock(clock.Now))
	require.NoError(t, err)

	token := ciToken(issuer, "octo-org", "octo-repo", "main", "flowstate")

	principal, err := verifier.Verify(context.Background(), token)
	require.NoError(t, err)
	assert.Equal(t, issuer.URL(), principal.Issuer)
	assert.Contains(t, issuer.URL(), "/octo-enterprise")
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

	key := authtest.GenerateKey("gha-key-1", jwa.RS256)
	clock := authtest.NewClock(time.Now())
	issuer := newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))

	verifierFor := func(t *testing.T, claim string) *auth.OIDCVerifier {
		t.Helper()

		verifier, err := auth.NewOIDCVerifier(auth.Policy{
			Issuers: []auth.TrustedIssuer{{
				Name:           "ci",
				Issuer:         issuer.URL(),
				Audiences:      []string{"flowstate"},
				NamespaceClaim: claim,
			}},
		}, auth.WithClock(clock.Now))
		require.NoError(t, err)

		return verifier
	}

	t.Run("repository_owner maps when the login is already a legal namespace", func(t *testing.T) {
		verifier := verifierFor(t, "repository_owner")
		token := ciToken(issuer, "octo-org", "octo-repo", "main", "flowstate")

		principal, err := verifier.Verify(context.Background(), token)
		require.NoError(t, err)
		assert.Equal(t, "octo-org", principal.Namespace)
	})

	t.Run("repository_owner is refused when the login carries uppercase", func(t *testing.T) {
		verifier := verifierFor(t, "repository_owner")
		token := ciToken(issuer, "Octo-Org", "octo-repo", "main", "flowstate")

		_, err := verifier.Verify(context.Background(), token)
		require.Error(t, err)
		assert.ErrorIs(t, err, auth.ErrNoNamespace)
	})

	t.Run("repository_owner is refused when the login carries an underscore", func(t *testing.T) {
		verifier := verifierFor(t, "repository_owner")
		token := ciToken(issuer, "octo_org", "octo-repo", "main", "flowstate")

		_, err := verifier.Verify(context.Background(), token)
		require.Error(t, err)
		assert.ErrorIs(t, err, auth.ErrNoNamespace)
	})

	t.Run("repository can never map, because it always contains a separator", func(t *testing.T) {
		verifier := verifierFor(t, "repository")
		token := ciToken(issuer, "octo-org", "octo-repo", "main", "flowstate")

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

	key := authtest.GenerateKey("gha-key-1", jwa.RS256)
	clock := authtest.NewClock(time.Now())
	issuer := newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))

	verifier, err := auth.NewOIDCVerifier(auth.Policy{
		Issuers: []auth.TrustedIssuer{{
			Name:      "ci",
			Issuer:    issuer.URL(),
			Audiences: []string{"flowstate"},
			Namespace: "platform",
		}},
	}, auth.WithClock(clock.Now))
	require.NoError(t, err)

	claims := ciClaims("octo-org", "octo-repo", "main")
	// A claim that is not a string, to show what the carrying step does with
	// one. A CI platform mints its own counters as strings, but an operator can
	// name any claim here.
	claims["private_repo"] = true

	token := issuer.MintToken(claims,
		authtest.WithSubject(ciSubject("octo-org", "octo-repo", "main")),
		authtest.WithAudience("flowstate"),
	)

	principal, err := verifier.Verify(context.Background(), token)
	require.NoError(t, err)

	identity := auth.IdentityFromPrincipal(
		principal,
		"fallback-namespace",
		"prod",
		"repository", "ref", "job_workflow_ref", "private_repo",
	)

	assert.Equal(t, "repo:octo-org/octo-repo:ref:refs/heads/main", identity.Subject)
	assert.Equal(t, issuer.URL(), identity.Issuer)

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
