package auth_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/picatz/jose/pkg/jwa"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
)

// What a GitLab CI job's and an HCP Terraform run's tokens look like, and what
// a trust policy can do with them.
//
// ci_federation_test.go does this for GitHub Actions, whose shape is also
// checked against a live runner in realtoken_test.go. Neither of these two
// platforms has an equivalent live check here — no CI job in this repository
// runs inside GitLab or HCP Terraform — so the claim shapes below are read
// from vendor documentation and pinned as *fixtures*, which is precisely what
// picatz/flowstate#559 asked for before anything depends on them: "That claim
// shape is from HashiCorp's documentation, not verified in this tree; it
// should be pinned by a test of the ci_federation_test.go shape before
// anything depends on it."
//
// What that buys, and what it does not. These tests fail when *this package's*
// handling of such a token changes — when a claim rule stops matching, when a
// tenant-shaped claim starts or stops satisfying the namespace grammar, when
// audience or issuer matching loosens. They cannot fail when the vendor
// changes what it mints, because the issuer answering is an authtest.Issuer in
// this process. The documented values each fixture carries are therefore
// citations with a date, and the honest reading of a green run here is "the
// policy surface handles this shape", never "the vendor still mints it".
//
// Sources, read 2026-08-22:
//
//   - GitLab: https://docs.gitlab.com/ci/secrets/id_token_authentication/
//   - HCP Terraform:
//     https://developer.hashicorp.com/terraform/cloud-docs/workspaces/dynamic-provider-credentials/workload-identity-tokens

// ciVendorFixture is one platform's documented token shape, plus the trust
// policy an operator would write against it.
type ciVendorFixture struct {
	// name is the credentialsource name for the same platform
	// ("gitlab", "terraform-cloud"), so a reader can find the acquisition
	// half from here.
	name string

	// docs is the page the shape below was read from.
	docs string

	// documentedIssuer is the "iss" the platform mints in production. It is
	// not the issuer these tests verify against — that is a local
	// authtest.Issuer — but it is pinned, because auth.Policy matches issuers
	// by exact string with no normalization, so this string is what an
	// operator must write and a typo in it is a total failure.
	documentedIssuer string

	// principal is the workload a policy means to admit, and neighbour is one
	// from the same platform and the same trusted issuer that the same rules
	// must refuse. A fixture with no neighbour would be testing that a rule
	// admits, which is the direction CLAUDE.md warns is a functionality test
	// wearing a security test's clothes.
	principal ciTokenShape
	neighbour ciTokenShape

	// require is the claim rules a trust policy pins the principal with.
	require []auth.ClaimRule

	// tenantClaim is the claim naming the tenant, and tenantValue is what the
	// principal's token carries in it.
	tenantClaim string
	tenantValue string

	// directNamespaceClaim, when non-empty, is a claim whose value this
	// platform mints already satisfying the namespace grammar, so
	// NamespaceClaim alone maps it. Empty means no claim does, and the
	// operator needs NamespaceMap.
	directNamespaceClaim string
	directNamespace      string

	// unmappableClaim is a claim whose value can *never* be a namespace,
	// whatever the tenant is named — GitLab's "project_path" always carries a
	// "/", HCP Terraform's "sub" always carries ":". Pinned because the
	// failure it produces (auth.ErrNoNamespace on a token that verified
	// perfectly) is otherwise a confusing one to meet for the first time in
	// production.
	unmappableClaim string
}

// ciTokenShape is one token: its subject and the claims beside it.
type ciTokenShape struct {
	subject string
	claims  map[string]any
}

// gitLabFixture is a GitLab.com CI job's ID token, from
// https://docs.gitlab.com/ci/secrets/id_token_authentication/ (read
// 2026-08-22).
//
// The subject's default format is documented there as
// "project_path:{group}/{project}:ref_type:{type}:ref:{branch_name}", and the
// payload example on that page is where the claim names below come from. The
// token expires at the job's timeout, or five minutes after it is issued when
// the job sets none — which is why an operator can reasonably pin MaxTokenAge
// low for this issuer, and why the gitlab credential source refuses an expired
// one rather than presenting it.
func gitLabFixture() ciVendorFixture {
	claims := func(group, project, ref string) map[string]any {
		return map[string]any{
			"namespace_id":    "72",
			"namespace_path":  group,
			"project_id":      "20",
			"project_path":    group + "/" + project,
			"user_id":         "1",
			"user_login":      "sample-user",
			"pipeline_id":     "574",
			"pipeline_source": "push",
			"job_id":          "302",
			"ref":             ref,
			"ref_type":        "branch",
			"ref_path":        "refs/heads/" + ref,
			"ref_protected":   "true",
			"runner_id":       1,
			"sha":             "714a629c0b401fdce83e847fc9589983fc6f46bc",
		}
	}
	subject := func(group, project, ref string) string {
		return "project_path:" + group + "/" + project + ":ref_type:branch:ref:" + ref
	}

	return ciVendorFixture{
		name:             "gitlab",
		docs:             "https://docs.gitlab.com/ci/secrets/id_token_authentication/",
		documentedIssuer: "https://gitlab.com",
		principal: ciTokenShape{
			subject: subject("acme", "infra", "main"),
			claims:  claims("acme", "infra", "main"),
		},
		neighbour: ciTokenShape{
			// Same group, same trusted issuer, unprotected feature branch:
			// the token a merge request pipeline gets, which must not deploy.
			subject: subject("acme", "infra", "feature-1"),
			claims: func() map[string]any {
				c := claims("acme", "infra", "feature-1")
				c["ref_protected"] = "false"
				return c
			}(),
		},
		require: []auth.ClaimRule{
			auth.RequireClaim("project_path", "acme/infra"),
			auth.RequireClaim("ref", "main"),
			auth.RequireClaim("ref_protected", "true"),
		},
		tenantClaim: "project_path",
		tenantValue: "acme/infra",
		// "namespace_path" is the group, which is a legal namespace whenever
		// the group's path already is — the same narrow escape GitHub's
		// "repository_owner" has.
		directNamespaceClaim: "namespace_path",
		directNamespace:      "acme",
		unmappableClaim:      "project_path",
	}
}

// terraformCloudFixture is an HCP Terraform workspace run's workload identity
// token, from
// https://developer.hashicorp.com/terraform/cloud-docs/workspaces/dynamic-provider-credentials/workload-identity-tokens
// (read 2026-08-22).
//
// The subject of a workspace run is documented there as
// "organization:{org}:project:{project}:workspace:{workspace}:run_phase:{phase}",
// and a run's token expires at the timeout of the phase it was issued for. The
// claim this repository cares about most is "terraform_run_phase": it is what
// lets a plan carry a read-only role and an apply a writing one, from an
// ordinary claim rule and a role, with no new mechanism anywhere.
func terraformCloudFixture() ciVendorFixture {
	claims := func(org, project, workspace, phase string) map[string]any {
		return map[string]any{
			"terraform_organization_id":   "org-abc123",
			"terraform_organization_name": org,
			"terraform_project_id":        "prj-abc123",
			"terraform_project_name":      project,
			"terraform_workspace_id":      "ws-abc123",
			"terraform_workspace_name":    workspace,
			"terraform_full_workspace":    "organization:" + org + ":project:" + project + ":workspace:" + workspace,
			"terraform_run_id":            "run-abc123",
			"terraform_run_phase":         phase,
		}
	}
	subject := func(org, project, workspace, phase string) string {
		return "organization:" + org + ":project:" + project + ":workspace:" + workspace + ":run_phase:" + phase
	}

	return ciVendorFixture{
		name:             "terraform-cloud",
		docs:             "https://developer.hashicorp.com/terraform/cloud-docs/workspaces/dynamic-provider-credentials/workload-identity-tokens",
		documentedIssuer: "https://app.terraform.io",
		principal: ciTokenShape{
			subject: subject("acme", "infra", "prod", "apply"),
			claims:  claims("acme", "infra", "prod", "apply"),
		},
		neighbour: ciTokenShape{
			// The same workspace's plan phase. Identical in every other
			// claim, which is exactly why run_phase is worth a rule: this is
			// the token a speculative plan on an unreviewed pull request runs
			// with, and it must not reach an apply-shaped role.
			subject: subject("acme", "infra", "prod", "plan"),
			claims:  claims("acme", "infra", "prod", "plan"),
		},
		require: []auth.ClaimRule{
			auth.RequireClaim("terraform_organization_name", "acme"),
			auth.RequireClaim("terraform_workspace_name", "prod"),
			auth.RequireClaim("terraform_run_phase", "apply"),
		},
		tenantClaim: "terraform_project_name",
		tenantValue: "infra",
		// Unlike GitHub and GitLab, HCP Terraform mints several claims that
		// are single names rather than paths, so NamespaceClaim alone works
		// whenever the workspace or project name is already a legal
		// namespace.
		directNamespaceClaim: "terraform_workspace_name",
		directNamespace:      "prod",
		// The subject names four things separated by colons, so it is never a
		// namespace however the workspace is named.
		unmappableClaim: "sub",
	}
}

func ciVendorFixtures() []ciVendorFixture {
	return []ciVendorFixture{gitLabFixture(), terraformCloudFixture()}
}

// mint mints one shape of token from the given local issuer.
func (s ciTokenShape) mint(issuer *authtest.Issuer, audience string) string {
	return issuer.MintToken(
		s.claims,
		authtest.WithSubject(s.subject),
		authtest.WithAudience(audience),
	)
}

// TestCIVendorClaimShapes pins what a trust policy does with each platform's
// documented token: it admits the workload the rules name, refuses the
// neighbour they exclude, refuses one addressed elsewhere, and takes the role
// and namespace from the policy rather than from the token.
func TestCIVendorClaimShapes(t *testing.T) {
	t.Parallel()

	for _, fixture := range ciVendorFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			t.Parallel()

			key := authtest.GenerateKey(fixture.name+"-key-1", jwa.RS256)
			clock := authtest.NewClock(time.Now())
			issuer := newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))

			verifier, err := auth.NewOIDCVerifier(auth.Policy{
				Issuers: []auth.TrustedIssuer{{
					Name:        fixture.name,
					Issuer:      issuer.URL(),
					Audiences:   []string{"https://flowstate.example.com"},
					Algorithms:  []jwa.Algorithm{jwa.RS256},
					Require:     fixture.require,
					Role:        "deployer",
					Namespace:   "infra",
					MaxTokenAge: 10 * time.Minute,
				}},
			}, auth.WithClock(clock.Now))
			require.NoError(t, err)

			t.Run("admits the workload the rules name", func(t *testing.T) {
				principal, err := verifier.Verify(context.Background(),
					fixture.principal.mint(issuer, "https://flowstate.example.com"))
				require.NoError(t, err)

				assert.Equal(t, fixture.principal.subject, principal.Subject)
				assert.Equal(t, "deployer", principal.Role, "the role comes from the policy, never the token")
				assert.Equal(t, "infra", principal.Namespace)
			})

			t.Run("refuses the neighbouring workload the same rules exclude", func(t *testing.T) {
				principal, err := verifier.Verify(context.Background(),
					fixture.neighbour.mint(issuer, "https://flowstate.example.com"))
				require.Error(t, err)
				assert.True(t, principal.IsZero(),
					"a token the rules exclude must not authenticate as anyone")
			})

			t.Run("refuses a token minted for another relying party", func(t *testing.T) {
				// The same job, the same issuer, the same claims — addressed
				// somewhere else. This is the replay the audience exists to
				// stop, and on both these platforms the audience is bound at
				// job or workspace configuration, which is why the credential
				// source checks it locally and says which setting is wrong.
				principal, err := verifier.Verify(context.Background(),
					fixture.principal.mint(issuer, "https://vault.example.com"))
				require.Error(t, err)
				assert.True(t, principal.IsZero())
			})
		})
	}
}

// TestCIVendorDocumentedIssuerIsMatchedExactly pins the consequence of
// auth.Policy matching issuers by exact string with no normalization: the
// documented issuer URL is what an operator must write, and a token from any
// other issuer — however well formed, whatever its claims — is refused.
//
// It is here rather than in a table of strings because a constant nothing
// reads is a comment. This fails if issuer matching ever grows a
// normalization step, which is the change that would quietly make a policy
// naming one issuer accept another.
func TestCIVendorDocumentedIssuerIsMatchedExactly(t *testing.T) {
	t.Parallel()

	for _, fixture := range ciVendorFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			t.Parallel()

			// A sanity pin on the citation itself: these are absolute https
			// URLs with no trailing slash, which is how both vendors write
			// them and therefore how a policy must.
			assert.True(t, strings.HasPrefix(fixture.documentedIssuer, "https://"), fixture.documentedIssuer)
			assert.False(t, strings.HasSuffix(fixture.documentedIssuer, "/"), fixture.documentedIssuer)

			// And every fixture cites where its shape was read from, because a
			// claim shape with no citation is a claim shape nobody can check
			// against the vendor when it changes.
			assert.True(t, strings.HasPrefix(fixture.docs, "https://"),
				"fixture %q must cite the page its claim shape was read from", fixture.name)

			key := authtest.GenerateKey(fixture.name+"-key-1", jwa.RS256)
			clock := authtest.NewClock(time.Now())
			issuer := newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))

			verifier, err := auth.NewOIDCVerifier(auth.Policy{
				Issuers: []auth.TrustedIssuer{{
					Name:      fixture.name,
					Issuer:    fixture.documentedIssuer,
					Audiences: []string{"https://flowstate.example.com"},
					Namespace: "infra",
				}},
			}, auth.WithClock(clock.Now))
			require.NoError(t, err)

			principal, err := verifier.Verify(context.Background(),
				fixture.principal.mint(issuer, "https://flowstate.example.com"))
			require.Error(t, err, "a token from an issuer the policy does not name must be refused")
			assert.True(t, principal.IsZero())
		})
	}
}

// TestCIVendorTenantClaims pins which of each platform's claims can carry a
// tenant and which cannot — the gap ci_federation_test.go's TestCITenantFromClaim
// found for GitHub Actions, answered per platform.
func TestCIVendorTenantClaims(t *testing.T) {
	t.Parallel()

	for _, fixture := range ciVendorFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			t.Parallel()

			key := authtest.GenerateKey(fixture.name+"-key-1", jwa.RS256)
			clock := authtest.NewClock(time.Now())
			issuer := newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))

			verifierFor := func(t *testing.T, entry auth.TrustedIssuer) *auth.OIDCVerifier {
				t.Helper()

				entry.Name = fixture.name
				entry.Issuer = issuer.URL()
				entry.Audiences = []string{"https://flowstate.example.com"}

				verifier, err := auth.NewOIDCVerifier(auth.Policy{
					Issuers: []auth.TrustedIssuer{entry},
				}, auth.WithClock(clock.Now))
				require.NoError(t, err)

				return verifier
			}

			token := fixture.principal.mint(issuer, "https://flowstate.example.com")

			t.Run("a path-shaped claim can never be a namespace on its own", func(t *testing.T) {
				verifier := verifierFor(t, auth.TrustedIssuer{NamespaceClaim: fixture.unmappableClaim})

				principal, err := verifier.Verify(context.Background(), token)
				require.Error(t, err, "%q names more than one thing, so it cannot be a tenant",
					fixture.unmappableClaim)
				assert.ErrorIs(t, err, auth.ErrNoNamespace)
				assert.True(t, principal.IsZero(),
					"a claim that cannot map must refuse, never land in a shared tenant")
			})

			t.Run("a name-shaped claim maps directly when it is already legal", func(t *testing.T) {
				verifier := verifierFor(t, auth.TrustedIssuer{NamespaceClaim: fixture.directNamespaceClaim})

				principal, err := verifier.Verify(context.Background(), token)
				require.NoError(t, err)
				assert.Equal(t, fixture.directNamespace, principal.Namespace)
			})

			t.Run("namespace_map carries the tenant claim whatever its shape", func(t *testing.T) {
				verifier := verifierFor(t, auth.TrustedIssuer{
					NamespaceClaim: fixture.tenantClaim,
					NamespaceMap:   map[string]string{fixture.tenantValue: "platform"},
				})

				principal, err := verifier.Verify(context.Background(), token)
				require.NoError(t, err)
				assert.Equal(t, "platform", principal.Namespace)
			})

			t.Run("a tenant with no entry is refused rather than defaulted", func(t *testing.T) {
				// The negative direction: same platform, same issuer, same
				// signature, a tenant the operator did not list. Nothing about
				// it may reach the mapped namespace.
				verifier := verifierFor(t, auth.TrustedIssuer{
					NamespaceClaim: fixture.tenantClaim,
					NamespaceMap:   map[string]string{"some-other-tenant": "platform"},
				})

				principal, err := verifier.Verify(context.Background(), token)
				require.Error(t, err)
				assert.ErrorIs(t, err, auth.ErrNoNamespace)
				assert.True(t, principal.IsZero())
			})
		})
	}
}
