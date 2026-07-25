package auth_test

import (
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
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
		actionsKey = newECDSAKey(t, "actions")
		actions    = newTestIssuer(t, actionsKey)

		clusterKey = newRSAKey(t, "cluster")
		cluster    = newTestIssuer(t, clusterKey)

		cloudKey = newECDSAKey(t, "cloud")
		cloud    = newTestIssuer(t, cloudKey)

		clock = newTestClock(referenceTime)
	)

	policy := auth.Policy{
		Issuers: []auth.TrustedIssuer{
			// A CI provider grants a privileged role only to the main branch of
			// one repository. Ordering matters: this entry is narrower than the
			// one that follows, so it is listed first.
			{
				Name:      "ci-main",
				Issuer:    actions.url,
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
				Issuer:    actions.url,
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
				Issuer:    cluster.url,
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
				Issuer:    cloud.url,
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
			name: "CI token from the main branch gets the privileged role",
			token: func(t *testing.T) string {
				claims := standardClaims(actions.url, "repo:picatz/flowstate:ref:refs/heads/main", "flowstate", referenceTime)
				claims["repository"] = "picatz/flowstate"
				claims["ref"] = "refs/heads/main"
				return actionsKey.sign(t, claims)
			},
			want: auth.Principal{
				Issuer:     actions.url,
				IssuerName: "ci-main",
				Subject:    "repo:picatz/flowstate:ref:refs/heads/main",
				Role:       "deployer",
			},
		},
		{
			name: "CI token from another branch falls through to the lesser role",
			token: func(t *testing.T) string {
				claims := standardClaims(actions.url, "repo:picatz/flowstate:ref:refs/heads/topic", "flowstate", referenceTime)
				claims["repository"] = "picatz/flowstate"
				claims["ref"] = "refs/heads/topic"
				return actionsKey.sign(t, claims)
			},
			want: auth.Principal{
				Issuer:     actions.url,
				IssuerName: "ci-branch",
				Subject:    "repo:picatz/flowstate:ref:refs/heads/topic",
				Role:       "reader",
			},
		},
		{
			name: "CI token from a fork of the repository is refused",
			token: func(t *testing.T) string {
				claims := standardClaims(actions.url, "repo:attacker/flowstate:ref:refs/heads/main", "flowstate", referenceTime)
				claims["repository"] = "attacker/flowstate"
				claims["ref"] = "refs/heads/main"
				return actionsKey.sign(t, claims)
			},
			wantErr: auth.ErrClaimMismatch,
		},
		{
			name: "CI token missing the claims the policy requires is refused",
			token: func(t *testing.T) string {
				claims := standardClaims(actions.url, "repo:picatz/flowstate:ref:refs/heads/main", "flowstate", referenceTime)
				return actionsKey.sign(t, claims)
			},
			wantErr: auth.ErrClaimMismatch,
		},
		{
			name: "CI token older than the issuer's maximum age is refused",
			token: func(t *testing.T) string {
				claims := standardClaims(actions.url, "repo:picatz/flowstate:ref:refs/heads/main", "flowstate", referenceTime)
				claims[jwt.IssuedAt] = referenceTime.Add(-time.Hour).Unix()
				claims[jwt.ExpirationTime] = referenceTime.Add(time.Hour).Unix()
				claims["repository"] = "picatz/flowstate"
				claims["ref"] = "refs/heads/main"
				return actionsKey.sign(t, claims)
			},
			// The narrow entry rejects it on age, the broader one has no age
			// limit but does not require the ref, so it admits the caller as a
			// reader. This is what entry ordering means, and why the age limit
			// belongs on every entry that needs it.
			want: auth.Principal{
				Issuer:     actions.url,
				IssuerName: "ci-branch",
				Subject:    "repo:picatz/flowstate:ref:refs/heads/main",
				Role:       "reader",
			},
		},
		{
			name: "cluster token with an array audience is accepted",
			token: func(t *testing.T) string {
				claims := standardClaims(cluster.url, "system:serviceaccount:flowstate:runner", "", referenceTime)
				claims[jwt.Audience] = []string{"flowstate", "https://kubernetes.default.svc"}
				return clusterKey.sign(t, claims)
			},
			want: auth.Principal{
				Issuer:     cluster.url,
				IssuerName: "cluster-runners",
				Subject:    "system:serviceaccount:flowstate:runner",
				Role:       "runner",
			},
		},
		{
			name: "cluster token for the other permitted service account is accepted",
			token: func(t *testing.T) string {
				claims := standardClaims(cluster.url, "system:serviceaccount:flowstate:scheduler", "flowstate", referenceTime)
				return clusterKey.sign(t, claims)
			},
			want: auth.Principal{
				Issuer:     cluster.url,
				IssuerName: "cluster-runners",
				Subject:    "system:serviceaccount:flowstate:scheduler",
				Role:       "runner",
			},
		},
		{
			name: "cluster token for a service account in another namespace is refused",
			token: func(t *testing.T) string {
				claims := standardClaims(cluster.url, "system:serviceaccount:default:runner", "flowstate", referenceTime)
				return clusterKey.sign(t, claims)
			},
			wantErr: auth.ErrClaimMismatch,
		},
		{
			name: "cloud token with a boolean claim is accepted",
			token: func(t *testing.T) string {
				claims := standardClaims(cloud.url, "108452345678901234567", "https://flowstate.example.com", referenceTime)
				claims["email"] = "flowstate@project.iam.example.com"
				claims["email_verified"] = true
				return cloudKey.sign(t, claims)
			},
			want: auth.Principal{
				Issuer:     cloud.url,
				IssuerName: "cloud-workload",
				Subject:    "108452345678901234567",
				Role:       "runner",
			},
		},
		{
			name: "cloud token whose email is not verified is refused",
			token: func(t *testing.T) string {
				claims := standardClaims(cloud.url, "108452345678901234567", "https://flowstate.example.com", referenceTime)
				claims["email"] = "flowstate@project.iam.example.com"
				claims["email_verified"] = false
				return cloudKey.sign(t, claims)
			},
			wantErr: auth.ErrClaimMismatch,
		},
		{
			name: "cloud token addressed to another deployment is refused",
			token: func(t *testing.T) string {
				claims := standardClaims(cloud.url, "108452345678901234567", "https://other.example.com", referenceTime)
				claims["email"] = "flowstate@project.iam.example.com"
				claims["email_verified"] = true
				return cloudKey.sign(t, claims)
			},
			wantErr: auth.ErrInvalidAudience,
		},
		{
			name: "a token from one trusted platform signed by another's key is refused",
			token: func(t *testing.T) string {
				// The claims say the cluster, the signature is the CI
				// provider's: trusting several issuers must not mean trusting
				// any of their keys for all of them.
				claims := standardClaims(cluster.url, "system:serviceaccount:flowstate:runner", "flowstate", referenceTime)
				return actionsKey.sign(t, claims)
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
		key    = newECDSAKey(t, "primary")
		issuer = newTestIssuer(t, key)
		clock  = newTestClock(referenceTime)
	)

	verifier := newVerifier(t,
		auth.Policy{
			Issuers: []auth.TrustedIssuer{{
				Name:      "idp",
				Issuer:    issuer.url,
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
			claims := standardClaims(issuer.url, "someone", "flowstate", referenceTime)
			claims["groups"] = test.groups

			principal, err := verifier.Verify(t.Context(), key.sign(t, claims))
			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, "operator", principal.Role)
		})
	}
}
