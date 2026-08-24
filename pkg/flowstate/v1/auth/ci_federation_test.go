package auth_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/goccy/go-yaml"
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

// TestCITenantFromNamespaceMap covers the answer to TestCITenantFromClaim's
// gap: NamespaceMap maps the raw "repository" claim (which always contains "/"
// and so can never satisfy the namespace grammar on its own) to a namespace
// through an exact, operator-authored table rather than a derived grammar.
//
// The load-bearing property is isolation, not round-tripping: a claim value
// with an entry must land in exactly that tenant, a claim value without one
// must be refused rather than falling back to anything, and no verified token
// — however it is shaped — may reach a namespace its entry does not name. See
// CLAUDE.md's "test the traversal, not just the step": a tenancy boundary that
// only proves "team-a reads team-a" is a functionality test wearing a security
// test's clothes.
func TestCITenantFromNamespaceMap(t *testing.T) {
	t.Parallel()

	key := authtest.GenerateKey("gha-key-1", jwa.RS256)
	clock := authtest.NewClock(time.Now())
	issuer := newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))

	verifier, err := auth.NewOIDCVerifier(auth.Policy{
		Issuers: []auth.TrustedIssuer{{
			Name:           "ci",
			Issuer:         issuer.URL(),
			Audiences:      []string{"flowstate"},
			NamespaceClaim: "repository",
			NamespaceMap: map[string]string{
				"acme-org/service-a": "team-a",
				"acme-org/service-b": "team-b",
				// Two different claim values sharing one namespace is a
				// deliberate, supported shape, not a collision.
				"acme-org/service-a-canary": "team-a",
			},
		}},
	}, auth.WithClock(clock.Now))
	require.NoError(t, err)

	verify := func(t *testing.T, owner, repo string) (auth.Principal, error) {
		t.Helper()
		token := ciToken(issuer, owner, repo, "main", "flowstate")
		return verifier.Verify(context.Background(), token)
	}

	t.Run("a mapped repository lands in its own tenant", func(t *testing.T) {
		principal, err := verify(t, "acme-org", "service-a")
		require.NoError(t, err)
		assert.Equal(t, "team-a", principal.Namespace)
	})

	t.Run("a different mapped repository lands in the other tenant", func(t *testing.T) {
		principal, err := verify(t, "acme-org", "service-b")
		require.NoError(t, err)
		assert.Equal(t, "team-b", principal.Namespace)
	})

	t.Run("two repositories may share a tenant on purpose", func(t *testing.T) {
		principal, err := verify(t, "acme-org", "service-a-canary")
		require.NoError(t, err)
		assert.Equal(t, "team-a", principal.Namespace)
	})

	t.Run("an unmapped repository is refused, not admitted to any tenant", func(t *testing.T) {
		// A verified token from the same trusted issuer, same organization,
		// signed the same way — the only difference is the repository has no
		// entry. Fail closed: it must not land in team-a, team-b, or a "" default.
		principal, err := verify(t, "acme-org", "service-c")
		require.Error(t, err)
		assert.ErrorIs(t, err, auth.ErrNoNamespace)
		assert.True(t, principal.IsZero(), "an unmapped claim must not authenticate as anyone")
	})

	t.Run("a repository from another organization entirely cannot spoof a mapped entry", func(t *testing.T) {
		// Same repository *name* as a mapped tenant, different owner: the map key
		// is the whole "<owner>/<name>" claim, so this must not collide with
		// "acme-org/service-a".
		principal, err := verify(t, "other-org", "service-a")
		require.Error(t, err)
		assert.ErrorIs(t, err, auth.ErrNoNamespace)
		assert.True(t, principal.IsZero())
	})

	t.Run("no token, however shaped, reaches a namespace absent from the map", func(t *testing.T) {
		// The exhaustive negative: sweep every repository this fixture did not
		// map, plus values chosen to probe the lookup itself (empty-looking,
		// case variants, and a value equal to a mapped namespace rather than a
		// mapped key), and confirm every one is refused and none is silently
		// admitted into "team-a" or "team-b".
		unmapped := []string{
			"acme-org/service-c",
			"acme-org/Service-A",  // case differs from the mapped key
			"acme-org/service-a ", // trailing space
			"team-a/service-a",    // owner equals a namespace, not a mapped key
			"acme-org/team-a",     // repo equals a namespace, not a mapped key
			"other-org/other-repo",
		}
		for _, repository := range unmapped {
			owner, repo, ok := strings.Cut(repository, "/")
			require.True(t, ok, repository)

			principal, err := verify(t, owner, repo)
			require.Errorf(t, err, "expected %q to be refused", repository)
			assert.ErrorIs(t, err, auth.ErrNoNamespace)
			assert.Truef(t, principal.IsZero(), "%q must not authenticate as anyone", repository)
		}
	})
}

// TestNamespaceMapValidation covers the policy-load-time checks: a
// namespace_map is refused at configuration time, not left to fail
// opaquely (or worse, permissively) at the first token it sees.
func TestNamespaceMapValidation(t *testing.T) {
	t.Parallel()

	base := func() auth.TrustedIssuer {
		return auth.TrustedIssuer{
			Name:      "ci",
			Issuer:    "https://token.actions.githubusercontent.com",
			Audiences: []string{"flowstate"},
		}
	}

	tests := []struct {
		name    string
		mutate  func(*auth.TrustedIssuer)
		wantErr string
	}{
		{
			name: "requires namespace_claim",
			mutate: func(i *auth.TrustedIssuer) {
				i.NamespaceMap = map[string]string{"acme-org/repo": "team-a"}
			},
			wantErr: "namespace_map requires namespace_claim",
		},
		{
			name: "an empty map is refused",
			mutate: func(i *auth.TrustedIssuer) {
				i.NamespaceClaim = "repository"
				i.NamespaceMap = map[string]string{}
			},
			wantErr: "empty",
		},
		{
			name: "a mapped value that fails the namespace grammar is refused at load, not at verification",
			mutate: func(i *auth.TrustedIssuer) {
				i.NamespaceClaim = "repository"
				i.NamespaceMap = map[string]string{"acme-org/repo": "Team-A"}
			},
			wantErr: "lowercase letters, digits, and dashes",
		},
		{
			name: "a mapped value that is empty is refused",
			mutate: func(i *auth.TrustedIssuer) {
				i.NamespaceClaim = "repository"
				i.NamespaceMap = map[string]string{"acme-org/repo": ""}
			},
			wantErr: "empty namespace",
		},
		{
			// Namespace and NamespaceClaim are already mutually exclusive, and a
			// namespace_map with no namespace_claim is refused before this policy
			// is ever consulted, so combining Namespace with NamespaceMap alone
			// (no NamespaceClaim) is refused for the latter reason first — which
			// is itself the point: there is no path that admits a namespace_map
			// without the claim it maps.
			name: "cannot be combined with a fixed namespace and no claim",
			mutate: func(i *auth.TrustedIssuer) {
				i.Namespace = "platform"
				i.NamespaceMap = map[string]string{"acme-org/repo": "team-a"}
			},
			wantErr: "namespace_map requires namespace_claim",
		},
		{
			name: "namespace and namespace_claim remain alternatives even with a map",
			mutate: func(i *auth.TrustedIssuer) {
				i.Namespace = "platform"
				i.NamespaceClaim = "repository"
				i.NamespaceMap = map[string]string{"acme-org/repo": "team-a"}
			},
			wantErr: "alternatives",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			issuer := base()
			test.mutate(&issuer)

			_, err := auth.NewOIDCVerifier(auth.Policy{Issuers: []auth.TrustedIssuer{issuer}})
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.wantErr)
		})
	}
}

// TestNamespaceMapExplicitNullIsRefused is the regression for the finding
// Codex raised on PR #744: constructing a TrustedIssuer directly in Go can
// never produce a NamespaceMap that is present-but-null, because a Go map
// literal has no such state — only decoding a document that spells the key
// out can. So this goes through [auth.ParsePolicy] with literal YAML, the
// only path that can reproduce the bug: namespace_map: null (or a bare
// namespace_map: with nothing after the colon, YAML's other spelling of the
// same null) must be refused at load, not silently treated the same as the
// key never having been written — which would fall the entry back to
// namespace_claim's raw-value grammar check with no map involved at all.
func TestNamespaceMapExplicitNullIsRefused(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		yaml string
	}{
		{
			name: "namespace_map: null",
			yaml: `
issuers:
  - name: ci
    issuer: https://token.actions.githubusercontent.com
    audiences: [flowstate]
    namespace_claim: repository_owner
    namespace_map: null
`,
		},
		{
			name: "namespace_map: with nothing after the colon",
			yaml: `
issuers:
  - name: ci
    issuer: https://token.actions.githubusercontent.com
    audiences: [flowstate]
    namespace_claim: repository_owner
    namespace_map:
`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := auth.ParsePolicy([]byte(test.yaml))
			require.Error(t, err, "a null namespace_map must be refused at load")
			assert.ErrorIs(t, err, auth.ErrInvalidPolicy)
			assert.Contains(t, err.Error(), "namespace_map")
		})
	}

	t.Run("a genuinely absent namespace_map is unaffected", func(t *testing.T) {
		// The control: namespace_claim alone, with no namespace_map key
		// anywhere in the document, is the pre-existing, supported,
		// grammar-checked mode and must keep loading cleanly.
		policy, err := auth.ParsePolicy([]byte(`
issuers:
  - name: ci
    issuer: https://token.actions.githubusercontent.com
    audiences: [flowstate]
    namespace_claim: repository_owner
`))
		require.NoError(t, err)
		require.Len(t, policy.Issuers, 1)
		assert.Nil(t, policy.Issuers[0].NamespaceMap)
	})

	t.Run("a populated namespace_map still loads and resolves through the map", func(t *testing.T) {
		policy, err := auth.ParsePolicy([]byte(`
issuers:
  - name: ci
    issuer: https://token.actions.githubusercontent.com
    audiences: [flowstate]
    namespace_claim: repository
    namespace_map:
      acme-org/service-a: team-a
`))
		require.NoError(t, err)
		require.Len(t, policy.Issuers, 1)
		assert.Equal(t, map[string]string{"acme-org/service-a": "team-a"}, map[string]string(policy.Issuers[0].NamespaceMap))
	})
}

// TestNamespaceMapEmptyRoundTripsAsPresent is the regression for the finding
// that landed on PR #744 after TestNamespaceMapExplicitNullIsRefused: a
// caller that builds a non-nil, empty [auth.NamespaceMap] directly in Go
// (a deliberate deny-all, distinct from never configuring the field at all)
// must have that emptiness survive marshal -> unmarshal, for both JSON and
// YAML.
//
// Before the fix, both encoding/json's `omitempty` and goccy/go-yaml's
// `omitempty` decided whether to omit the field by reflecting on len(m) --
// which a non-nil empty map satisfies exactly as a nil map does -- before
// ever calling [auth.NamespaceMap]'s MarshalJSON/MarshalYAML method. So the
// field vanished from the wire exactly as if it had never been set, and
// re-parsing decoded it back as nil (absent, unrestricted): the marshal
// round trip silently turned a deny-all into no restriction at all, defeated
// the null-rejection this same file's TestNamespaceMapExplicitNullIsRefused
// exists to enforce, and did so without hitting [auth.ParsePolicy] at all --
// a plain encoding/json or goccy/go-yaml round trip is enough.
func TestNamespaceMapEmptyRoundTripsAsPresent(t *testing.T) {
	t.Parallel()

	issuer := auth.TrustedIssuer{
		Name:           "ci",
		Issuer:         "https://token.actions.githubusercontent.com",
		Audiences:      []string{"flowstate"},
		NamespaceClaim: "repository",
		NamespaceMap:   auth.NamespaceMap{}, // non-nil, empty: deliberate deny-all.
	}
	policy := auth.Policy{Issuers: []auth.TrustedIssuer{issuer}}

	t.Run("JSON", func(t *testing.T) {
		data, err := json.Marshal(policy)
		require.NoError(t, err)
		assert.Contains(t, string(data), `"namespace_map":{}`,
			"an empty NamespaceMap must be written, not omitted")

		var back auth.Policy
		require.NoError(t, json.Unmarshal(data, &back))
		require.Len(t, back.Issuers, 1)
		assert.NotNil(t, back.Issuers[0].NamespaceMap,
			"an empty namespace_map must decode as present, not absent")
		assert.Empty(t, map[string]string(back.Issuers[0].NamespaceMap))
	})

	t.Run("YAML", func(t *testing.T) {
		data, err := yaml.Marshal(policy)
		require.NoError(t, err)
		assert.Contains(t, string(data), "namespace_map: {}",
			"an empty NamespaceMap must be written, not omitted")

		var back auth.Policy
		require.NoError(t, yaml.Unmarshal(data, &back))
		require.Len(t, back.Issuers, 1)
		assert.NotNil(t, back.Issuers[0].NamespaceMap,
			"an empty namespace_map must decode as present, not absent")
		assert.Empty(t, map[string]string(back.Issuers[0].NamespaceMap))
	})

	t.Run("nil NamespaceMap is still omitted, both formats", func(t *testing.T) {
		nilPolicy := auth.Policy{Issuers: []auth.TrustedIssuer{{
			Name:           "ci",
			Issuer:         "https://token.actions.githubusercontent.com",
			Audiences:      []string{"flowstate"},
			NamespaceClaim: "repository",
		}}}

		jdata, err := json.Marshal(nilPolicy)
		require.NoError(t, err)
		assert.NotContains(t, string(jdata), "namespace_map",
			"a never-configured NamespaceMap must not appear on the wire at all")

		ydata, err := yaml.Marshal(nilPolicy)
		require.NoError(t, err)
		assert.NotContains(t, string(ydata), "namespace_map",
			"a never-configured NamespaceMap must not appear on the wire at all")
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
