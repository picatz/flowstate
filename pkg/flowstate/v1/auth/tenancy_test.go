package auth_test

import (
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/jose/pkg/jwt"
	"github.com/stretchr/testify/require"
)

// TestNamespaceFromTrustPolicy covers the rule the whole tenant boundary rests on:
// a namespace comes from the verified caller, decided by the trust policy, and a
// caller whose namespace cannot be determined is refused rather than admitted to a
// shared one.
func TestNamespaceFromTrustPolicy(t *testing.T) {
	tests := []struct {
		name          string
		issuer        func(url string) auth.TrustedIssuer
		claims        func(claims jwt.ClaimsSet)
		wantNamespace string
		wantErr       error
	}{
		{
			name: "fixed for an issuer that belongs to one team",
			issuer: func(url string) auth.TrustedIssuer {
				return auth.TrustedIssuer{
					Name: "cluster", Issuer: url, Audiences: []string{"flowstate"},
					Namespace: "platform",
				}
			},
			wantNamespace: "platform",
		},
		{
			name: "taken from a claim for an issuer serving several teams",
			issuer: func(url string) auth.TrustedIssuer {
				return auth.TrustedIssuer{
					Name: "ci", Issuer: url, Audiences: []string{"flowstate"},
					NamespaceClaim: "tenant",
				}
			},
			claims:        func(claims jwt.ClaimsSet) { claims["tenant"] = "acme" },
			wantNamespace: "acme",
		},
		{
			name: "single-tenant policy leaves it empty",
			issuer: func(url string) auth.TrustedIssuer {
				return auth.TrustedIssuer{Name: "idp", Issuer: url, Audiences: []string{"flowstate"}}
			},
			wantNamespace: "",
		},
		{
			name: "the claim is missing",
			issuer: func(url string) auth.TrustedIssuer {
				return auth.TrustedIssuer{
					Name: "ci", Issuer: url, Audiences: []string{"flowstate"},
					NamespaceClaim: "tenant",
				}
			},
			wantErr: auth.ErrNoNamespace,
		},
		{
			name: "the claim is empty",
			issuer: func(url string) auth.TrustedIssuer {
				return auth.TrustedIssuer{
					Name: "ci", Issuer: url, Audiences: []string{"flowstate"},
					NamespaceClaim: "tenant",
				}
			},
			claims:  func(claims jwt.ClaimsSet) { claims["tenant"] = "" },
			wantErr: auth.ErrNoNamespace,
		},
		{
			name: "the claim is not a string",
			issuer: func(url string) auth.TrustedIssuer {
				return auth.TrustedIssuer{
					Name: "ci", Issuer: url, Audiences: []string{"flowstate"},
					NamespaceClaim: "tenant",
				}
			},
			claims:  func(claims jwt.ClaimsSet) { claims["tenant"] = 42 },
			wantErr: auth.ErrNoNamespace,
		},
		{
			name: "the claim spells out another tenant's path",
			issuer: func(url string) auth.TrustedIssuer {
				return auth.TrustedIssuer{
					Name: "ci", Issuer: url, Audiences: []string{"flowstate"},
					NamespaceClaim: "tenant",
				}
			},
			// A namespace reaches an assertion subject, so a value containing the
			// separator could make one tenant's workload look like another's.
			claims:  func(claims jwt.ClaimsSet) { claims["tenant"] = "acme/prod/deploy" },
			wantErr: auth.ErrNoNamespace,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				key    = newECDSAKey(t, "primary")
				issuer = newTestIssuer(t, key)
				clock  = newTestClock(referenceTime)
			)

			verifier := newVerifier(t,
				auth.Policy{Issuers: []auth.TrustedIssuer{test.issuer(issuer.url)}},
				auth.WithClock(clock.Now),
			)

			claims := standardClaims(issuer.url, "runner", "flowstate", referenceTime)
			if test.claims != nil {
				test.claims(claims)
			}

			principal, err := verifier.Verify(t.Context(), key.sign(t, claims))

			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
				require.True(t, principal.IsZero(), "a caller with no determinable tenant must not be authenticated")
				return
			}

			require.NoError(t, err)
			require.Equal(t, test.wantNamespace, principal.Namespace)
		})
	}
}

// TestNamespaceIsNotTakenFromTheRequest checks that the identity a run acts as takes
// its namespace from the verified caller in preference to anything the submitting
// server passes, which is what stops a caller choosing its own tenant.
func TestNamespaceIsNotTakenFromTheRequest(t *testing.T) {
	principal := auth.Principal{
		Subject:   "repo:picatz/flowstate:ref:refs/heads/main",
		Issuer:    "https://token.actions.githubusercontent.com",
		Namespace: "acme",
	}

	// Even when the caller passes a different namespace, the verified one wins.
	identity := auth.IdentityFromPrincipal(principal, "someone-else", "prod")
	require.Equal(t, "acme", identity.Namespace)

	// A single-tenant deployment, whose policy determines no namespace, uses the
	// one the deployment was configured with.
	single := auth.IdentityFromPrincipal(auth.Principal{Subject: "s", Issuer: "https://idp.example.com"}, "default", "prod")
	require.Equal(t, "default", single.Namespace)
}

// TestPolicyTenancyIsAllOrNothing checks that a policy cannot be half tenant-aware.
// An issuer admitted without a namespace would share one with tenants meant to be
// separated, so mixing the two is a configuration error rather than a default.
func TestPolicyTenancyIsAllOrNothing(t *testing.T) {
	tests := []struct {
		name    string
		policy  auth.Policy
		wantErr bool
	}{
		{
			name: "every issuer determines a namespace",
			policy: auth.Policy{Issuers: []auth.TrustedIssuer{
				{Name: "a", Issuer: "https://a.example.com", Audiences: []string{"flowstate"}, Namespace: "team-a"},
				{Name: "b", Issuer: "https://b.example.com", Audiences: []string{"flowstate"}, NamespaceClaim: "tenant"},
			}},
		},
		{
			name: "no issuer determines a namespace",
			policy: auth.Policy{Issuers: []auth.TrustedIssuer{
				{Name: "a", Issuer: "https://a.example.com", Audiences: []string{"flowstate"}},
				{Name: "b", Issuer: "https://b.example.com", Audiences: []string{"flowstate"}},
			}},
		},
		{
			name: "one does and one does not",
			policy: auth.Policy{Issuers: []auth.TrustedIssuer{
				{Name: "a", Issuer: "https://a.example.com", Audiences: []string{"flowstate"}, Namespace: "team-a"},
				{Name: "b", Issuer: "https://b.example.com", Audiences: []string{"flowstate"}},
			}},
			wantErr: true,
		},
		{
			name: "an issuer naming both a fixed namespace and a claim",
			policy: auth.Policy{Issuers: []auth.TrustedIssuer{
				{
					Name: "a", Issuer: "https://a.example.com", Audiences: []string{"flowstate"},
					Namespace: "team-a", NamespaceClaim: "tenant",
				},
			}},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.policy.Validate()

			if !test.wantErr {
				require.NoError(t, err)
				return
			}

			require.ErrorIs(t, err, auth.ErrInvalidPolicy)

			verifier, err := auth.NewOIDCVerifier(test.policy)
			require.Error(t, err)
			require.Nil(t, verifier)
		})
	}
}

// TestTemporalNamespaceMapping covers the optional isolation of run history, and
// that its absence is not an error while its half-configuration is.
func TestTemporalNamespaceMapping(t *testing.T) {
	tests := []struct {
		name      string
		tenancy   *auth.Tenancy
		namespace string
		want      string
		wantOK    bool
		wantErr   error
	}{
		{
			name:      "no tenancy at all, so the deployment's own namespace is used",
			tenancy:   nil,
			namespace: "acme",
			wantOK:    false,
		},
		{
			name:      "an empty tenancy behaves the same",
			tenancy:   &auth.Tenancy{},
			namespace: "acme",
			wantOK:    false,
		},
		{
			name:      "a mapped namespace",
			tenancy:   &auth.Tenancy{Temporal: map[string]string{"acme": "flowstate-acme"}},
			namespace: "acme",
			want:      "flowstate-acme",
			wantOK:    true,
		},
		{
			name: "an unmapped namespace falls back to the default when there is one",
			tenancy: &auth.Tenancy{
				Temporal: map[string]string{"acme": "flowstate-acme"},
				Default:  "flowstate-shared",
			},
			namespace: "other",
			want:      "flowstate-shared",
			wantOK:    true,
		},
		{
			name:      "an unmapped namespace with no default is refused",
			tenancy:   &auth.Tenancy{Temporal: map[string]string{"acme": "flowstate-acme"}},
			namespace: "other",
			wantErr:   auth.ErrNoTemporalNamespace,
		},
		{
			name:      "a default alone maps everything",
			tenancy:   &auth.Tenancy{Default: "flowstate-shared"},
			namespace: "anything",
			want:      "flowstate-shared",
			wantOK:    true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, test.tenancy.Validate())

			namespace, ok, err := test.tenancy.TemporalNamespace(test.namespace)

			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
				require.False(t, ok)
				return
			}

			require.NoError(t, err)
			require.Equal(t, test.wantOK, ok)
			require.Equal(t, test.want, namespace)
		})
	}

	t.Run("the namespaces a connection layer must dial", func(t *testing.T) {
		tenancy := &auth.Tenancy{
			Temporal: map[string]string{
				"acme":  "flowstate-acme",
				"other": "flowstate-other",
				"third": "flowstate-acme", // two tenants may share one namespace
			},
			Default: "flowstate-shared",
		}

		require.Equal(t,
			[]string{"flowstate-acme", "flowstate-other", "flowstate-shared"},
			tenancy.TemporalNamespaces())

		require.Nil(t, (*auth.Tenancy)(nil).TemporalNamespaces())
	})

	t.Run("a mapping to an empty namespace is a configuration error", func(t *testing.T) {
		tenancy := &auth.Tenancy{Temporal: map[string]string{"acme": ""}}
		require.ErrorIs(t, tenancy.Validate(), auth.ErrInvalidPolicy)

		policy := auth.Policy{
			Issuers: []auth.TrustedIssuer{{Name: "a", Issuer: "https://a.example.com", Audiences: []string{"flowstate"}}},
			Tenancy: tenancy,
		}
		require.ErrorIs(t, policy.Validate(), auth.ErrInvalidPolicy)
	})
}
