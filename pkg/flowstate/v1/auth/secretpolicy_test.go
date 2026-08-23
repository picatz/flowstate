package auth_test

import (
	"strings"
	"sync"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// protoSecretRef has the accessors the generated flowstate.v1.SecretRef message
// has, and stands in for it so this package's tests do not depend on the generated
// types. That it satisfies [auth.SecretReference] at all is the point of the
// interface.
type protoSecretRef struct {
	scheme string
	name   string
}

func (r *protoSecretRef) GetScheme() string { return r.scheme }
func (r *protoSecretRef) GetName() string   { return r.name }

// secretRef is shorthand for a reference in a test table.
func secretRef(scheme, name string) auth.SecretReference {
	return &protoSecretRef{scheme: scheme, name: name}
}

// TestSecretPolicyDefaultsToNothing is the requirement stated on its own, because it
// is the one that matters most: a deployment that has not said what its workloads
// may read permits nothing, and says so in a way an operator can act on.
func TestSecretPolicyDefaultsToNothing(t *testing.T) {
	tests := []struct {
		name   string
		policy auth.SecretAccessPolicy
	}{
		{name: "no rules at all", policy: auth.SecretAccessPolicy{}},
		{
			name:   "deny rules only, which cannot imply everything else is allowed",
			policy: auth.SecretAccessPolicy{Deny: []string{`secret.scheme == "env"`}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy, err := test.policy.Compile()
			require.NoError(t, err)

			err = policy.Authorize(t.Context(), testIdentity(), testStepRef(), secretRef("env", "API_KEY"))
			require.ErrorIs(t, err, auth.ErrSecretDenied)

			// The message must read as a policy decision, not as a missing secret:
			// an operator told the secret is absent goes hunting for one that exists.
			require.Contains(t, err.Error(), "no rule permits")
			require.Contains(t, err.Error(), "env:API_KEY")
			require.Contains(t, err.Error(), "acme", "the namespace belongs in the message")
			require.NotContains(t, strings.ToLower(err.Error()), "not found")
		})
	}

	t.Run("the zero policy permits nothing", func(t *testing.T) {
		var policy auth.SecretPolicy

		err := policy.Authorize(t.Context(), testIdentity(), testStepRef(), secretRef("env", "API_KEY"))
		require.ErrorIs(t, err, auth.ErrSecretDenied)
	})
}

// TestSecretPolicyAuthorize covers the decisions themselves.
func TestSecretPolicyAuthorize(t *testing.T) {
	tests := []struct {
		name       string
		allow      []string
		deny       []string
		identity   func() auth.WorkloadIdentity
		reference  auth.SecretReference
		wantErr    bool
		wantReason auth.SecretReason
	}{
		{
			name:      "an allow rule naming the scheme and tenant",
			allow:     []string{`secret.scheme == "env" && workload.namespace == "acme"`},
			reference: secretRef("env", "API_KEY"),
		},
		{
			name:      "an allow rule scoping names to the tenant's own prefix",
			allow:     []string{`secret.name.startsWith(workload.namespace + "/")`},
			reference: secretRef("vault", "acme/db-password"),
		},
		{
			// The same rule, and the same reference, refused for another tenant:
			// this is what "the same reference resolves differently per namespace"
			// has to mean for it to be a boundary.
			name:      "another tenant cannot read through the same rule",
			allow:     []string{`secret.name.startsWith(workload.namespace + "/")`},
			reference: secretRef("vault", "acme/db-password"),
			identity: func() auth.WorkloadIdentity {
				other := testIdentity()
				other.Namespace = "other-tenant"
				return other
			},
			wantErr:    true,
			wantReason: auth.ReasonSecretNoAllowRule,
		},
		{
			name:      "an allow rule naming the workload's step",
			allow:     []string{`workload.step == "push-image" && secret.scheme == "env"`},
			reference: secretRef("env", "REGISTRY_TOKEN"),
		},
		{
			name:      "an allow rule naming who the workload acts for",
			allow:     []string{`workload.on_behalf_of.startsWith("repo:picatz/flowstate:")`},
			reference: secretRef("env", "API_KEY"),
		},
		{
			name:       "no allow rule matches",
			allow:      []string{`secret.scheme == "vault"`},
			reference:  secretRef("env", "API_KEY"),
			wantErr:    true,
			wantReason: auth.ReasonSecretNoAllowRule,
		},
		{
			name:       "a deny rule matches",
			allow:      []string{`secret.scheme == "env"`},
			deny:       []string{`secret.name == "ROOT_PASSWORD"`},
			reference:  secretRef("env", "ROOT_PASSWORD"),
			wantErr:    true,
			wantReason: auth.ReasonSecretDenyRule,
		},
		{
			name:       "deny beats allow",
			allow:      []string{`true`},
			deny:       []string{`secret.scheme == "env"`},
			reference:  secretRef("env", "API_KEY"),
			wantErr:    true,
			wantReason: auth.ReasonSecretDenyRule,
		},
		{
			name:      "a rule that cannot be evaluated refuses",
			allow:     []string{`workload.claims["absent"] == "x"`},
			reference: secretRef("env", "API_KEY"),
			identity: func() auth.WorkloadIdentity {
				return auth.WorkloadIdentity{Subject: "s", Issuer: "https://i.example.com"}
			},
			wantErr:    true,
			wantReason: auth.ReasonSecretRuleError,
		},
		{
			name:       "a reference with no scheme",
			allow:      []string{`true`},
			reference:  secretRef("", "API_KEY"),
			wantErr:    true,
			wantReason: auth.ReasonSecretMalformed,
		},
		{
			name:       "a reference with no name",
			allow:      []string{`true`},
			reference:  secretRef("env", ""),
			wantErr:    true,
			wantReason: auth.ReasonSecretMalformed,
		},
		{
			name:       "no reference at all",
			allow:      []string{`true`},
			reference:  nil,
			wantErr:    true,
			wantReason: auth.ReasonSecretMalformed,
		},
		{
			name:       "a typed nil reference, as an unset protobuf field arrives",
			allow:      []string{`true`},
			reference:  (*protoSecretRef)(nil),
			wantErr:    true,
			wantReason: auth.ReasonSecretMalformed,
		},
		{
			name:       "a workload that cannot be named",
			allow:      []string{`true`},
			reference:  secretRef("env", "API_KEY"),
			identity:   func() auth.WorkloadIdentity { return auth.WorkloadIdentity{} },
			wantErr:    true,
			wantReason: auth.ReasonSecretNoIdentity,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy, err := auth.SecretAccessPolicy{Allow: test.allow, Deny: test.deny}.Compile()
			require.NoError(t, err)

			identity := testIdentity()
			if test.identity != nil {
				identity = test.identity()
			}

			err = policy.Authorize(t.Context(), identity, testStepRef(), test.reference)

			if !test.wantErr {
				require.NoError(t, err)
				return
			}

			require.ErrorIs(t, err, auth.ErrSecretDenied)

			var denied *auth.SecretDeniedError
			require.ErrorAs(t, err, &denied)
			require.Equal(t, test.wantReason, denied.Reason)
			require.NotEmpty(t, denied.Detail)
		})
	}
}

// TestSecretAccessPolicyRejectsBadRules checks that a rule mistake fails at startup,
// where an operator sees it, rather than the first time a workload reads a secret.
func TestSecretAccessPolicyRejectsBadRules(t *testing.T) {
	tests := []struct {
		name   string
		policy auth.SecretAccessPolicy
	}{
		{name: "a rule that does not compile", policy: auth.SecretAccessPolicy{Allow: []string{`secret.scheme ==`}}},
		{name: "a misspelled field", policy: auth.SecretAccessPolicy{Allow: []string{`secret.schema == "env"`}}},
		{name: "an attribute that does not exist", policy: auth.SecretAccessPolicy{Allow: []string{`tenant == "acme"`}}},
		{name: "a rule that does not produce a boolean", policy: auth.SecretAccessPolicy{Allow: []string{`secret.name`}}},
		{name: "an empty rule", policy: auth.SecretAccessPolicy{Deny: []string{"   "}}},
		{
			// Credential targets are not part of this decision, so naming one is a
			// mistake worth catching rather than an attribute that is always empty.
			name:   "an attribute from the assumption rules",
			policy: auth.SecretAccessPolicy{Allow: []string{`target == "aws-prod"`}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy, err := test.policy.Compile()
			require.ErrorIs(t, err, auth.ErrInvalidPolicy)
			require.Nil(t, policy)

			require.ErrorIs(t, test.policy.Validate(), auth.ErrInvalidPolicy)
		})
	}
}

// TestPolicyCarriesSecretRules checks that all three policy surfaces can be
// described in one reviewable file, in one language.
func TestPolicyCarriesSecretRules(t *testing.T) {
	policy, err := auth.ParsePolicy([]byte(`
issuers:
  - name: github-actions
    issuer: https://token.actions.githubusercontent.com
    audiences: [flowstate]
    namespace_claim: repository_owner
    require:
      - claim: repository
        any_of: [picatz/flowstate]

secrets:
  allow:
    - 'secret.scheme == "env" && secret.name.startsWith(workload.namespace + "_")'
    - 'secret.scheme == "vault" && workload.namespace == "picatz"'
  deny:
    - 'secret.name.endsWith("_ROOT")'

tenancy:
  temporal:
    picatz: flowstate-picatz
  default: flowstate-shared

federation:
  issuer: https://flowstate.example.com
  allow:
    - 'target == "aws-prod" && workload.namespace == "picatz"'
  targets:
    - name: aws-prod
      profile: aws-sts-web-identity-2011-06-15
      aws:
        role_arn: arn:aws:iam::123456789012:role/flowstate
`))
	require.NoError(t, err)

	require.Equal(t, "repository_owner", policy.Issuers[0].NamespaceClaim)
	require.NotNil(t, policy.Secrets)
	require.Len(t, policy.Secrets.Allow, 2)
	require.Len(t, policy.Secrets.Deny, 1)
	require.NotNil(t, policy.Tenancy)
	require.NotNil(t, policy.Federation)

	secrets, err := policy.Secrets.Compile()
	require.NoError(t, err)

	// A workload in the tenant the reference belongs to.
	identity := auth.WorkloadIdentity{
		Subject:   "repo:picatz/flowstate:ref:refs/heads/main",
		Issuer:    "https://token.actions.githubusercontent.com",
		Namespace: "picatz",
	}

	require.NoError(t, secrets.Authorize(t.Context(), identity, testStepRef(), secretRef("env", "picatz_API_KEY")))
	require.NoError(t, secrets.Authorize(t.Context(), identity, testStepRef(), secretRef("vault", "anything")))

	// The deny rule wins over both allow rules.
	require.ErrorIs(t,
		secrets.Authorize(t.Context(), identity, testStepRef(), secretRef("vault", "DB_ROOT")),
		auth.ErrSecretDenied)

	// Another tenant, refused by the same rules.
	other := identity
	other.Namespace = "someone-else"
	require.ErrorIs(t,
		secrets.Authorize(t.Context(), other, testStepRef(), secretRef("env", "picatz_API_KEY")),
		auth.ErrSecretDenied)

	mapped, ok, err := policy.Tenancy.TemporalNamespace("picatz")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "flowstate-picatz", mapped)

	t.Run("a bad secret rule fails the whole policy", func(t *testing.T) {
		_, err := auth.ParsePolicy([]byte(`
issuers:
  - name: idp
    issuer: https://idp.example.com
    audiences: [flowstate]

secrets:
  allow:
    - 'secret.schema == "env"'
`))
		require.ErrorIs(t, err, auth.ErrInvalidPolicy)
		require.Contains(t, err.Error(), "secrets:")
	})
}

// TestSecretPolicyConcurrent checks that a compiled policy can be asked from many
// goroutines, which is how a worker resolving secrets for parallel steps will use it.
func TestSecretPolicyConcurrent(t *testing.T) {
	policy, err := auth.SecretAccessPolicy{
		Allow: []string{`secret.scheme == "env" && workload.namespace == "acme"`},
		Deny:  []string{`secret.name == "FORBIDDEN"`},
	}.Compile()
	require.NoError(t, err)

	var wait sync.WaitGroup
	for i := range 32 {
		wait.Go(func() {
			if i%2 == 0 {
				assert.NoError(t, policy.Authorize(t.Context(), testIdentity(), testStepRef(), secretRef("env", "API_KEY")))
				return
			}
			assert.ErrorIs(t,
				policy.Authorize(t.Context(), testIdentity(), testStepRef(), secretRef("env", "FORBIDDEN")),
				auth.ErrSecretDenied)
		})
	}
	wait.Wait()
}
