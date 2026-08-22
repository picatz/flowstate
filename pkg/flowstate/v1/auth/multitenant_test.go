package auth_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// These tests cover the last of the four gaps recorded in #243: a policy naming
// a public multi-tenant issuer — GitHub Actions and its peers — with an audience
// and nothing else. Every field such an entry sets is satisfied by a workflow
// belonging to anybody, because the audience is named by whoever requests the
// token rather than assigned by the platform, so the entry admits every workload
// on that platform as one caller.
//
// The check refuses that when the policy loads, which is where CLAUDE.md's
// fail-closed rule puts it: "rules compile and type-check when configuration
// loads rather than when a request arrives". Nothing here reaches the network,
// and nothing here reads a token: this is a property of an operator's file.
//
// Both directions are written, per "test that A cannot reach B". The refusal is
// the reached direction; the negatives are that a pinned entry still loads, and
// that an issuer this package has *not* named — a single-tenant corporate IdP,
// or a self-hosted GitLab — is left alone. The second matters as much as the
// first: a check that refused every ruleless issuer would be enforcing an
// opinion the deployment is entitled to disagree with.

// knownMultiTenantIssuers is the list this package refuses to trust unpinned,
// with the claim each platform's diagnostic points an operator at.
var knownMultiTenantIssuers = []struct {
	issuer   string
	platform string
	claim    string
}{
	{"https://token.actions.githubusercontent.com", "GitHub Actions", "repository_owner"},
	{"https://gitlab.com", "GitLab.com CI/CD", "namespace_path"},
	{"https://app.terraform.io", "HCP Terraform", "terraform_organization_name"},
}

func TestMultiTenantIssuerRefusedWithoutPinning(t *testing.T) {
	t.Parallel()

	for _, known := range knownMultiTenantIssuers {
		t.Run(known.platform, func(t *testing.T) {
			policy := auth.Policy{Issuers: []auth.TrustedIssuer{{
				Name:      "ci",
				Issuer:    known.issuer,
				Audiences: []string{"flowstate"},
			}}}

			err := policy.Validate()
			require.Error(t, err, "an unpinned %s entry must not load", known.platform)
			assert.ErrorIs(t, err, auth.ErrInvalidPolicy)

			// The diagnostic is the feature, not the refusal: an operator who
			// hits this has to be able to fix it without reading this package.
			// It names the entry, the platform, why an audience does not
			// substitute, and both remedies in copyable YAML.
			message := err.Error()
			for _, want := range []string{
				known.issuer,
				known.platform,
				"require:",
				"namespace_claim:",
				known.claim,
				"audience",
			} {
				assert.Containsf(t, message, want, "diagnostic should mention %q: %s", want, message)
			}
		})
	}
}

func TestMultiTenantIssuerAcceptedWhenPinned(t *testing.T) {
	t.Parallel()

	const issuer = "https://token.actions.githubusercontent.com"

	t.Run("a require rule narrows who is admitted", func(t *testing.T) {
		policy := auth.Policy{Issuers: []auth.TrustedIssuer{{
			Name:      "github-actions",
			Issuer:    issuer,
			Audiences: []string{"flowstate"},
			Require:   []auth.ClaimRule{auth.RequireClaim("repository", "picatz/flowstate")},
		}}}

		require.NoError(t, policy.Validate())
	})

	t.Run("a namespace_claim keeps each account in its own tenant", func(t *testing.T) {
		// The other legitimate posture: admit everyone the platform will mint a
		// token for, but read the tenant off a claim the issuer signed, so no
		// two accounts share a namespace. This is what
		// examples/operations/tenant-routing/trust.yaml does.
		policy := auth.Policy{Issuers: []auth.TrustedIssuer{{
			Name:           "github-actions",
			Issuer:         issuer,
			Audiences:      []string{"flowstate"},
			NamespaceClaim: "repository_owner",
		}}}

		require.NoError(t, policy.Validate())
	})

	t.Run("a fixed namespace alone is not pinning", func(t *testing.T) {
		// A fixed namespace answers "which tenant do admitted callers belong
		// to", never "which callers are admitted" — so with it alone every
		// workflow on GitHub lands in that one tenant together. This is the
		// case most likely to look pinned to a reader, which is why it has its
		// own test rather than a line in a table.
		policy := auth.Policy{Issuers: []auth.TrustedIssuer{{
			Name:      "github-actions",
			Issuer:    issuer,
			Audiences: []string{"flowstate"},
			Namespace: "acme",
		}}}

		err := policy.Validate()
		require.Error(t, err)
		assert.ErrorIs(t, err, auth.ErrInvalidPolicy)
	})
}

func TestMultiTenantCheckLeavesOtherIssuersAlone(t *testing.T) {
	t.Parallel()

	// The negative direction, and the reason the built-in list is a list rather
	// than "every issuer". For an issuer whose tokens only one operator can
	// obtain, an audience *is* a restriction, and refusing it would be this
	// package overruling a deployment about its own IdP.
	unaffected := []struct {
		name   string
		issuer string
	}{
		{"a single-tenant corporate IdP", "https://login.corp.example.com/oauth2/default"},
		{"a self-hosted GitLab, which is not gitlab.com", "https://gitlab.example.com"},
		{"a Kubernetes API server", "https://kubernetes.default.svc.cluster.local"},

		// Host matching is exact, so a lookalike neither matches the table nor
		// escapes it by decoration. Both directions are checked here because
		// each is a bug: the first would be a refusal an operator cannot
		// explain, the second a hole in the one this ships.
		{"a host that merely ends in the known one", "https://token.actions.githubusercontent.com.example.net"},
		{"a host that merely contains the known one", "https://not-gitlab.com"},
		{"a subdomain of a known multi-tenant host", "https://enterprise.gitlab.com"},
	}

	for _, entry := range unaffected {
		t.Run(entry.name, func(t *testing.T) {
			policy := auth.Policy{Issuers: []auth.TrustedIssuer{{
				Name:      "idp",
				Issuer:    entry.issuer,
				Audiences: []string{"flowstate"},
			}}}

			require.NoErrorf(t, policy.Validate(), "%s must still load with no require rules", entry.issuer)
		})
	}
}

func TestMultiTenantIssuerMatchedRegardlessOfSpelling(t *testing.T) {
	t.Parallel()

	// One host, several legal spellings of the same URL. A check that only
	// caught the canonical one would be defeated by a copy-paste, which is not
	// a threat model so much as a Tuesday.
	spellings := []struct {
		name   string
		issuer string
	}{
		{"as written in the vendor's docs", "https://token.actions.githubusercontent.com"},
		{"with mixed case in the host", "https://Token.Actions.GitHubUserContent.com"},
		{"with a fully qualified trailing dot", "https://token.actions.githubusercontent.com."},
		{"with a trailing slash", "https://token.actions.githubusercontent.com/"},
		{"with an enterprise path segment", "https://token.actions.githubusercontent.com/acme-corp"},
	}

	for _, spelling := range spellings {
		t.Run(spelling.name, func(t *testing.T) {
			policy := auth.Policy{Issuers: []auth.TrustedIssuer{{
				Name:      "github-actions",
				Issuer:    spelling.issuer,
				Audiences: []string{"flowstate"},
			}}}

			err := policy.Validate()
			require.Errorf(t, err, "%s is the same multi-tenant issuer and must be refused unpinned", spelling.issuer)
			assert.ErrorIs(t, err, auth.ErrInvalidPolicy)
		})
	}
}

func TestMultiTenantIssuerRefusedThroughParsePolicy(t *testing.T) {
	t.Parallel()

	// The path an operator actually takes: a YAML file, through the same entry
	// point `flow server` uses. A check that only ran on a hand-built Go value
	// would be the "tested the engine, not the file" mistake CLAUDE.md names.
	unpinned := []byte(`
issuers:
  - name: github-actions
    issuer: https://token.actions.githubusercontent.com
    audiences: [flowstate]
    algorithms: [RS256]
`)

	_, refusal := auth.ParsePolicy(unpinned)
	require.Error(t, refusal)
	assert.ErrorIs(t, refusal, auth.ErrInvalidPolicy)
	assert.Contains(t, refusal.Error(), "GitHub Actions")

	// And the remedy the diagnostic prints, pasted back in, loads. The YAML in
	// the message is indented for a terminal, so it is dedented here the way an
	// operator would when putting it under their entry — what is being checked
	// is that the keys and claim name are right, not the leading spaces.
	pinned := []byte(`
issuers:
  - name: github-actions
    issuer: https://token.actions.githubusercontent.com
    audiences: [flowstate]
    algorithms: [RS256]
    require:
      - claim: repository_owner
        any_of: [picatz]
`)

	policy, err := auth.ParsePolicy(pinned)
	require.NoError(t, err)
	require.Len(t, policy.Issuers, 1)
	require.Len(t, policy.Issuers[0].Require, 1)
	assert.Equal(t, "repository_owner", policy.Issuers[0].Require[0].Claim)

	// The diagnostic's own claim name is the one that works, rather than one
	// this test happens to agree with: read it back out of the refusal.
	assert.Contains(t, strings.ReplaceAll(refusal.Error(), " ", ""), "claim:repository_owner")
}
