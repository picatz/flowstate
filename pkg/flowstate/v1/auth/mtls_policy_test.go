package auth_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/stretchr/testify/require"
)

// TestPolicyValidateMTLS checks that a kind: mtls entry is refused unless it
// says exactly what it needs to, and that a field belonging to the bearer-
// token half of this package (audiences, algorithms, jwks_url,
// max_token_age, namespace_claim) is refused outright rather than silently
// ignored on an entry of this kind — the "one value, written down twice"
// class of mistake CLAUDE.md warns about, applied to a field nobody meant to
// set.
func TestPolicyValidateMTLS(t *testing.T) {
	caFile := newTestCA(t, "test-ca").clientCAFile(t)

	valid := auth.TrustedIssuer{
		Name:         "mesh",
		Kind:         auth.IssuerKindMTLS,
		Issuer:       "flowstate:mtls/mesh",
		ClientCAFile: caFile,
		SubjectFrom:  auth.SubjectFromURISAN,
	}

	spoil := func(change func(*auth.TrustedIssuer)) auth.Policy {
		issuer := valid
		change(&issuer)
		return auth.Policy{Issuers: []auth.TrustedIssuer{issuer}}
	}

	tests := []struct {
		name    string
		policy  auth.Policy
		wantErr bool
	}{
		{
			name:   "a valid kind: mtls entry",
			policy: auth.Policy{Issuers: []auth.TrustedIssuer{valid}},
		},
		{
			name:    "no issuer label",
			policy:  spoil(func(i *auth.TrustedIssuer) { i.Issuer = "" }),
			wantErr: true,
		},
		{
			name:    "no client_ca_file",
			policy:  spoil(func(i *auth.TrustedIssuer) { i.ClientCAFile = "" }),
			wantErr: true,
		},
		{
			name:    "no subject_from: there is no safe default",
			policy:  spoil(func(i *auth.TrustedIssuer) { i.SubjectFrom = "" }),
			wantErr: true,
		},
		{
			name:    "an unsupported subject_from",
			policy:  spoil(func(i *auth.TrustedIssuer) { i.SubjectFrom = "common_name" }),
			wantErr: true,
		},
		{
			name:   "dns_san is supported",
			policy: spoil(func(i *auth.TrustedIssuer) { i.SubjectFrom = auth.SubjectFromDNSSAN }),
		},
		{
			name:   "email_san is supported",
			policy: spoil(func(i *auth.TrustedIssuer) { i.SubjectFrom = auth.SubjectFromEmailSAN }),
		},
		{
			name:    "audiences is not meaningful for a certificate",
			policy:  spoil(func(i *auth.TrustedIssuer) { i.Audiences = []string{"flowstate"} }),
			wantErr: true,
		},
		{
			name: "algorithms is not meaningful: the certificate's signature is crypto/tls's problem",
			policy: spoil(func(i *auth.TrustedIssuer) {
				i.Algorithms = auth.DefaultAlgorithms()
			}),
			wantErr: true,
		},
		{
			name:    "jwks_url is not meaningful: there is no key set",
			policy:  spoil(func(i *auth.TrustedIssuer) { i.JWKSURL = "https://issuer.example.com/jwks.json" }),
			wantErr: true,
		},
		{
			name:    "max_token_age is not meaningful: a certificate has no issued-at claim",
			policy:  spoil(func(i *auth.TrustedIssuer) { i.MaxTokenAge = 1 }),
			wantErr: true,
		},
		{
			name: "namespace_claim is refused: a certificate exposes only the subject SAN",
			policy: spoil(func(i *auth.TrustedIssuer) {
				i.Namespace = ""
				i.NamespaceClaim = "namespace"
			}),
			wantErr: true,
		},
		{
			name: "namespace_map is refused too: it only interprets a namespace_claim value",
			policy: spoil(func(i *auth.TrustedIssuer) {
				i.Namespace = ""
				i.NamespaceClaim = "namespace"
				i.NamespaceMap = map[string]string{"raw": "mapped"}
			}),
			wantErr: true,
		},
		{
			name:   "a fixed namespace is fine",
			policy: spoil(func(i *auth.TrustedIssuer) { i.Namespace = "team-a" }),
		},
		{
			name: "require rules are checked the same as every other kind",
			policy: spoil(func(i *auth.TrustedIssuer) {
				i.Require = []auth.ClaimRule{{Claim: "subject"}}
			}),
			wantErr: true,
		},
		{
			name: "an unsupported kind",
			policy: spoil(func(i *auth.TrustedIssuer) {
				i.Kind = "spiffe-x509-svid"
			}),
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.policy.Validate()
			if test.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, auth.ErrInvalidPolicy)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestPolicyValidateOIDCRefusesMTLSFields checks the other direction: an
// ordinary (kind: oidc, or kind unset) entry that sets client_ca_file or
// subject_from is refused rather than having those fields silently do
// nothing.
func TestPolicyValidateOIDCRefusesMTLSFields(t *testing.T) {
	caFile := newTestCA(t, "test-ca").clientCAFile(t)

	base := auth.TrustedIssuer{
		Name:      "idp",
		Issuer:    "https://issuer.example.com",
		Audiences: []string{"flowstate"},
	}

	tests := []struct {
		name   string
		change func(*auth.TrustedIssuer)
	}{
		{"client_ca_file set on an oidc entry", func(i *auth.TrustedIssuer) { i.ClientCAFile = caFile }},
		{"subject_from set on an oidc entry", func(i *auth.TrustedIssuer) { i.SubjectFrom = auth.SubjectFromURISAN }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			issuer := base
			test.change(&issuer)
			err := (auth.Policy{Issuers: []auth.TrustedIssuer{issuer}}).Validate()
			require.Error(t, err)
			require.ErrorIs(t, err, auth.ErrInvalidPolicy)
		})
	}
}

// TestNewMTLSVerifierNilWhenNoMTLSEntries pins that a policy with only
// kind: oidc entries never allocates an MTLSVerifier at all, which is what
// keeps every deployment that never configures mTLS unaffected by this
// package's existence.
func TestNewMTLSVerifierNilWhenNoMTLSEntries(t *testing.T) {
	policy := auth.Policy{Issuers: []auth.TrustedIssuer{{
		Name: "idp", Issuer: "https://issuer.example.com", Audiences: []string{"flowstate"},
	}}}

	verifier, err := auth.NewMTLSVerifier(policy)
	require.NoError(t, err)
	require.Nil(t, verifier)
}

// TestNewMTLSVerifierBoundsClientCAFile checks the file-size bound this
// package's own doc names: a client_ca_file over the limit is refused at
// load time, before any listener uses the resulting pool.
func TestNewMTLSVerifierBoundsClientCAFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "huge-ca.pem")
	huge := make([]byte, 2<<20) // 2 MiB, over the 1 MiB bound
	for i := range huge {
		huge[i] = 'A'
	}
	require.NoError(t, os.WriteFile(path, huge, 0o600))

	policy := auth.Policy{Issuers: []auth.TrustedIssuer{{
		Name: "mesh", Kind: auth.IssuerKindMTLS, Issuer: "flowstate:mtls/mesh",
		ClientCAFile: path, SubjectFrom: auth.SubjectFromURISAN,
	}}}

	_, err := auth.NewMTLSVerifier(policy)
	require.Error(t, err)
}
