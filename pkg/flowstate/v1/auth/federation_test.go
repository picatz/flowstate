package auth_test

import (
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/picatz/jose/pkg/jwt"
	"github.com/stretchr/testify/require"
)

// TestWorkloadIdentityFederation exercises the case this package exists for: a
// workload authenticating with a token its own platform issued, with no Flowstate
// credential deployed anywhere.
//
// The three issuers below stand in for a CI provider, a Kubernetes cluster, and a
// cloud provider's metadata service. They differ only in the claims their tokens
// carry and the rules the policy places on them, which is the point: adding a
// platform is configuration, not code.
func TestWorkloadIdentityFederation(t *testing.T) {
	var (
		clock = authtest.NewClock(referenceTime)

		actionsKey = authtest.GenerateKey("actions", jwa.ES256)
		actions    = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(actionsKey))

		clusterKey = authtest.GenerateKey("cluster", jwa.RS256)
		cluster    = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(clusterKey))

		cloudKey = authtest.GenerateKey("cloud", jwa.ES256)
		cloud    = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(cloudKey))
	)

	policy := auth.Policy{
		Issuers: []auth.TrustedIssuer{
			// A CI provider grants a privileged role only to the main branch of
			// one repository. Ordering matters: this entry is narrower than the
			// one that follows, so it is listed first.
			{
				Name:      "ci-main",
				Issuer:    actions.URL(),
				Audiences: []string{"flowstate"},
				Require: []auth.ClaimRule{
					auth.RequireClaim("repository", "picatz/flowstate"),
					auth.RequireClaim("ref", "refs/heads/main"),
				},
				Role:        "deployer",
				MaxTokenAge: 10 * time.Minute,
			},
			// The same issuer, same repository, any other branch: allowed, but
			// with a lesser role.
			{
				Name:      "ci-branch",
				Issuer:    actions.URL(),
				Audiences: []string{"flowstate"},
				Require: []auth.ClaimRule{
					auth.RequireClaim("repository", "picatz/flowstate"),
				},
				Role: "reader",
			},
			// A Kubernetes cluster, restricted to two service accounts. Its
			// tokens carry an array audience.
			{
				Name:      "cluster-runners",
				Issuer:    cluster.URL(),
				Audiences: []string{"flowstate"},
				Require: []auth.ClaimRule{
					auth.RequireClaimAnyOf("sub",
						"system:serviceaccount:flowstate:runner",
						"system:serviceaccount:flowstate:scheduler",
					),
				},
				Role: "runner",
			},
			// A cloud provider identity, restricted by an attested service
			// account email and a verification claim that is a JSON boolean.
			{
				Name:      "cloud-workload",
				Issuer:    cloud.URL(),
				Audiences: []string{"https://flowstate.example.com"},
				Require: []auth.ClaimRule{
					auth.RequireClaim("email", "flowstate@project.iam.example.com"),
					auth.RequireClaim("email_verified", "true"),
				},
				Role: "runner",
			},
		},
	}

	verifier := newVerifier(t, policy, auth.WithClock(clock.Now))

	tests := []struct {
		name    string
		token   func(t *testing.T) string
		wantErr error
		want    auth.Principal
	}{
		{
			name: "CI token matching privileged and broad mappings is refused as ambiguous",
			token: func(t *testing.T) string {
				claims := actions.Claims(authtest.WithSubject("repo:picatz/flowstate:ref:refs/heads/main"), authtest.WithAudience("flowstate"))
				claims["repository"] = "picatz/flowstate"
				claims["ref"] = "refs/heads/main"
				return actions.MintToken(claims)
			},
			wantErr: auth.ErrAmbiguousIdentity,
		},
		{
			name: "CI token from another branch falls through to the lesser role",
			token: func(t *testing.T) string {
				claims := actions.Claims(authtest.WithSubject("repo:picatz/flowstate:ref:refs/heads/topic"), authtest.WithAudience("flowstate"))
				claims["repository"] = "picatz/flowstate"
				claims["ref"] = "refs/heads/topic"
				return actions.MintToken(claims)
			},
			want: auth.Principal{
				Issuer:     actions.URL(),
				IssuerName: "ci-branch",
				Subject:    "repo:picatz/flowstate:ref:refs/heads/topic",
				Role:       "reader",
			},
		},
		{
			name: "CI token from a fork of the repository is refused",
			token: func(t *testing.T) string {
				claims := actions.Claims(authtest.WithSubject("repo:attacker/flowstate:ref:refs/heads/main"), authtest.WithAudience("flowstate"))
				claims["repository"] = "attacker/flowstate"
				claims["ref"] = "refs/heads/main"
				return actions.MintToken(claims)
			},
			wantErr: auth.ErrClaimMismatch,
		},
		{
			name: "CI token missing the claims the policy requires is refused",
			token: func(t *testing.T) string {
				claims := actions.Claims(authtest.WithSubject("repo:picatz/flowstate:ref:refs/heads/main"), authtest.WithAudience("flowstate"))
				return actions.MintToken(claims)
			},
			wantErr: auth.ErrClaimMismatch,
		},
		{
			name: "CI token older than the issuer's maximum age is refused",
			token: func(t *testing.T) string {
				claims := actions.Claims(authtest.WithSubject("repo:picatz/flowstate:ref:refs/heads/main"), authtest.WithAudience("flowstate"))
				claims[jwt.IssuedAt] = referenceTime.Add(-time.Hour).Unix()
				claims[jwt.ExpirationTime] = referenceTime.Add(time.Hour).Unix()
				claims["repository"] = "picatz/flowstate"
				claims["ref"] = "refs/heads/main"
				return actions.MintToken(claims)
			},
			// The narrow entry rejects it on age, so exactly one mapping remains.
			want: auth.Principal{
				Issuer:     actions.URL(),
				IssuerName: "ci-branch",
				Subject:    "repo:picatz/flowstate:ref:refs/heads/main",
				Role:       "reader",
			},
		},
		{
			name: "cluster token with an array audience is accepted",
			token: func(t *testing.T) string {
				claims := cluster.Claims(authtest.WithSubject("system:serviceaccount:flowstate:runner"), authtest.WithoutAudience())
				claims[jwt.Audience] = []string{"flowstate", "https://kubernetes.default.svc"}
				return cluster.MintToken(claims)
			},
			want: auth.Principal{
				Issuer:     cluster.URL(),
				IssuerName: "cluster-runners",
				Subject:    "system:serviceaccount:flowstate:runner",
				Role:       "runner",
			},
		},
		{
			name: "cluster token for the other permitted service account is accepted",
			token: func(t *testing.T) string {
				claims := cluster.Claims(authtest.WithSubject("system:serviceaccount:flowstate:scheduler"), authtest.WithAudience("flowstate"))
				return cluster.MintToken(claims)
			},
			want: auth.Principal{
				Issuer:     cluster.URL(),
				IssuerName: "cluster-runners",
				Subject:    "system:serviceaccount:flowstate:scheduler",
				Role:       "runner",
			},
		},
		{
			name: "cluster token for a service account in another namespace is refused",
			token: func(t *testing.T) string {
				claims := cluster.Claims(authtest.WithSubject("system:serviceaccount:default:runner"), authtest.WithAudience("flowstate"))
				return cluster.MintToken(claims)
			},
			wantErr: auth.ErrClaimMismatch,
		},
		{
			name: "cloud token with a boolean claim is accepted",
			token: func(t *testing.T) string {
				claims := cloud.Claims(authtest.WithSubject("108452345678901234567"), authtest.WithAudience("https://flowstate.example.com"))
				claims["email"] = "flowstate@project.iam.example.com"
				claims["email_verified"] = true
				return cloud.MintToken(claims)
			},
			want: auth.Principal{
				Issuer:     cloud.URL(),
				IssuerName: "cloud-workload",
				Subject:    "108452345678901234567",
				Role:       "runner",
			},
		},
		{
			name: "cloud token whose email is not verified is refused",
			token: func(t *testing.T) string {
				claims := cloud.Claims(authtest.WithSubject("108452345678901234567"), authtest.WithAudience("https://flowstate.example.com"))
				claims["email"] = "flowstate@project.iam.example.com"
				claims["email_verified"] = false
				return cloud.MintToken(claims)
			},
			wantErr: auth.ErrClaimMismatch,
		},
		{
			name: "cloud token addressed to another deployment is refused",
			token: func(t *testing.T) string {
				claims := cloud.Claims(authtest.WithSubject("108452345678901234567"), authtest.WithAudience("https://other.example.com"))
				claims["email"] = "flowstate@project.iam.example.com"
				claims["email_verified"] = true
				return cloud.MintToken(claims)
			},
			wantErr: auth.ErrInvalidAudience,
		},
		{
			name: "a token from one trusted platform signed by another's key is refused",
			token: func(t *testing.T) string {
				// The claims say the cluster, the signature is the CI
				// provider's: trusting several issuers must not mean trusting
				// any of their keys for all of them.
				claims := cluster.Claims(authtest.WithSubject("system:serviceaccount:flowstate:runner"), authtest.WithAudience("flowstate"))
				return actions.MintToken(claims)
			},
			wantErr: auth.ErrUnknownKey,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			principal, err := verifier.Verify(t.Context(), test.token(t))

			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
				require.True(t, principal.IsZero())
				return
			}

			require.NoError(t, err)
			require.Equal(t, test.want.Issuer, principal.Issuer)
			require.Equal(t, test.want.IssuerName, principal.IssuerName, "the wrong policy entry admitted this caller")
			require.Equal(t, test.want.Subject, principal.Subject)
			require.Equal(t, test.want.Role, principal.Role, "a caller must get the role its policy entry assigns")
			require.Equal(t, test.want.Issuer+"#"+test.want.Subject, principal.ID())
		})
	}
}

// TestWorkloadIdentityFederationListClaim checks that a rule on a list-valued
// claim, such as group membership, holds when any member matches.
func TestWorkloadIdentityFederationListClaim(t *testing.T) {
	var (
		key    = authtest.GenerateKey("primary", jwa.ES256)
		clock  = authtest.NewClock(referenceTime)
		issuer = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))
	)

	verifier := newVerifier(t,
		auth.Policy{
			Issuers: []auth.TrustedIssuer{{
				Name:      "idp",
				Issuer:    issuer.URL(),
				Audiences: []string{"flowstate"},
				Require:   []auth.ClaimRule{auth.RequireClaim("groups", "platform")},
				Role:      "operator",
			}},
		},
		auth.WithClock(clock.Now),
	)

	tests := []struct {
		name    string
		groups  any
		wantErr error
	}{
		{
			name:   "membership among several groups",
			groups: []string{"security", "platform", "sre"},
		},
		{
			name:   "membership as the only group",
			groups: []string{"platform"},
		},
		{
			name:   "membership as a bare string",
			groups: "platform",
		},
		{
			name:    "no matching membership",
			groups:  []string{"security", "sre"},
			wantErr: auth.ErrClaimMismatch,
		},
		{
			name:    "no groups at all",
			groups:  []string{},
			wantErr: auth.ErrClaimMismatch,
		},
		{
			name:    "a group name that merely contains the required one",
			groups:  []string{"platform-readonly"},
			wantErr: auth.ErrClaimMismatch,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			claims := issuer.Claims(authtest.WithSubject("someone"), authtest.WithAudience("flowstate"))
			claims["groups"] = test.groups

			principal, err := verifier.Verify(t.Context(), issuer.MintToken(claims))
			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, "operator", principal.Role)
		})
	}
}
