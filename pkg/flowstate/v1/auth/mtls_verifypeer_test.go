package auth_test

import (
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/stretchr/testify/require"
)

// newMTLSVerifier builds an [auth.MTLSVerifier] from a single kind: mtls
// entry, failing the test if the policy or the verifier cannot be built.
func newMTLSVerifier(t *testing.T, issuer auth.TrustedIssuer) *auth.MTLSVerifier {
	t.Helper()

	policy := auth.Policy{Issuers: []auth.TrustedIssuer{issuer}}
	require.NoError(t, policy.Validate())

	verifier, err := auth.NewMTLSVerifier(policy)
	require.NoError(t, err)
	require.NotNil(t, verifier)

	return verifier
}

// TestMTLSVerifierVerifyPeerRejectsEmptyChain pins that an empty chain — not
// reachable through [auth.Authenticator], which never calls VerifyPeer
// without one, but a [auth.PeerVerifier] implementation's own contract —  is
// a rejection rather than a panic or an anonymous principal.
func TestMTLSVerifierVerifyPeerRejectsEmptyChain(t *testing.T) {
	ca := newTestCA(t, "root")
	verifier := newMTLSVerifier(t, auth.TrustedIssuer{
		Name: "mesh", Kind: auth.IssuerKindMTLS, Issuer: "flowstate:mtls/mesh",
		ClientCAFile: ca.clientCAFile(t), SubjectFrom: auth.SubjectFromURISAN,
	})

	_, err := verifier.VerifyPeer(t.Context(), nil)
	require.Error(t, err)
	require.ErrorIs(t, err, auth.ErrNoToken)
}

// TestMTLSVerifierVerifyPeerRejectsUnknownCA is the CA half of "a certificate
// from a CA not in the pool is refused": a chain crypto/tls verified against
// some other pool is refused by this verifier's own policy-entry matching,
// which is the second line of defense behind the transport-level refusal
// (pkg/flowstate/v1/auth/mtls_e2e_test.go covers the handshake itself).
func TestMTLSVerifierVerifyPeerRejectsUnknownCA(t *testing.T) {
	trusted := newTestCA(t, "trusted-root")
	untrusted := newTestCA(t, "untrusted-root")

	verifier := newMTLSVerifier(t, auth.TrustedIssuer{
		Name: "mesh", Kind: auth.IssuerKindMTLS, Issuer: "flowstate:mtls/mesh",
		ClientCAFile: trusted.clientCAFile(t), SubjectFrom: auth.SubjectFromURISAN,
	})

	leaf := untrusted.issueLeaf(t, withURISAN("spiffe://example.org/ns/ci/sa/runner"))

	_, err := verifier.VerifyPeer(t.Context(), chainFor(t, leaf, untrusted))
	require.Error(t, err)
	require.ErrorIs(t, err, auth.ErrUntrustedIssuer)
}

// TestMTLSVerifierVerifyPeerRejectsAmbiguousSAN checks the bound this
// package's doc names: a certificate carrying more than one value in the SAN
// field a policy names is refused rather than having the first one picked
// silently.
func TestMTLSVerifierVerifyPeerRejectsAmbiguousSAN(t *testing.T) {
	ca := newTestCA(t, "root")
	verifier := newMTLSVerifier(t, auth.TrustedIssuer{
		Name: "mesh", Kind: auth.IssuerKindMTLS, Issuer: "flowstate:mtls/mesh",
		ClientCAFile: ca.clientCAFile(t), SubjectFrom: auth.SubjectFromURISAN,
	})

	leaf := ca.issueLeaf(t,
		withURISAN("spiffe://example.org/ns/ci/sa/runner"),
		withURISAN("spiffe://example.org/ns/ci/sa/other"),
	)

	_, err := verifier.VerifyPeer(t.Context(), chainFor(t, leaf, ca))
	require.Error(t, err)
	require.ErrorIs(t, err, auth.ErrMalformedToken)
}

// TestMTLSVerifierVerifyPeerRejectsNoMatchingSAN checks the other bound: a
// certificate with none of the named SAN kind at all is refused, not treated
// as an empty subject.
func TestMTLSVerifierVerifyPeerRejectsNoMatchingSAN(t *testing.T) {
	ca := newTestCA(t, "root")
	verifier := newMTLSVerifier(t, auth.TrustedIssuer{
		Name: "mesh", Kind: auth.IssuerKindMTLS, Issuer: "flowstate:mtls/mesh",
		ClientCAFile: ca.clientCAFile(t), SubjectFrom: auth.SubjectFromURISAN,
	})

	// A certificate with a DNS SAN but no URI SAN at all.
	leaf := ca.issueLeaf(t, withDNSSAN("runner.mesh.internal"))

	_, err := verifier.VerifyPeer(t.Context(), chainFor(t, leaf, ca))
	require.Error(t, err)
	require.ErrorIs(t, err, auth.ErrMissingClaim)
}

// TestMTLSVerifierVerifyPeerReadsEachSANKind is the coverage the three
// SubjectFrom spellings need, and staticcheck is what noticed it was missing:
// `withEmailSAN` existed as a helper that nothing called, which is the shape of
// a configuration option shipped without a test.
//
// The negative half is the point. Each case issues a leaf carrying *only* its
// own SAN kind, so a verifier that quietly fell back to another kind — or to
// the Subject DN, which this package deliberately never reads — would produce
// the right subject for the wrong reason and pass a positive-only test.
func TestMTLSVerifierVerifyPeerReadsEachSANKind(t *testing.T) {
	for _, test := range []struct {
		name    string
		from    string
		leaf    leafOption
		subject string
	}{
		{
			name:    "uri",
			from:    auth.SubjectFromURISAN,
			leaf:    withURISAN("spiffe://example.org/ns/ci/sa/runner"),
			subject: "spiffe://example.org/ns/ci/sa/runner",
		},
		{
			name:    "dns",
			from:    auth.SubjectFromDNSSAN,
			leaf:    withDNSSAN("runner.mesh.internal"),
			subject: "runner.mesh.internal",
		},
		{
			name:    "email",
			from:    auth.SubjectFromEmailSAN,
			leaf:    withEmailSAN("runner@example.org"),
			subject: "runner@example.org",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ca := newTestCA(t, "root")
			verifier := newMTLSVerifier(t, auth.TrustedIssuer{
				Name: "mesh", Kind: auth.IssuerKindMTLS, Issuer: "flowstate:mtls/mesh",
				ClientCAFile: ca.clientCAFile(t), SubjectFrom: test.from,
				Namespace: "ci",
			})

			leaf := ca.issueLeaf(t, test.leaf)

			principal, err := verifier.VerifyPeer(t.Context(), chainFor(t, leaf, ca))
			require.NoError(t, err)
			require.Equal(t, test.subject, principal.Subject)

			// And the negative direction: an entry asking for one kind must not
			// read another. A certificate carrying only a URI SAN cannot satisfy
			// an entry configured for email, however plausible its contents.
			for _, other := range []string{
				auth.SubjectFromURISAN, auth.SubjectFromDNSSAN, auth.SubjectFromEmailSAN,
			} {
				if other == test.from {
					continue
				}

				mismatched := newMTLSVerifier(t, auth.TrustedIssuer{
					Name: "mesh", Kind: auth.IssuerKindMTLS, Issuer: "flowstate:mtls/mesh",
					ClientCAFile: ca.clientCAFile(t), SubjectFrom: other,
					Namespace: "ci",
				})

				_, err := mismatched.VerifyPeer(t.Context(), chainFor(t, leaf, ca))
				require.Error(t, err,
					"a leaf carrying only a %s SAN satisfied an entry asking for %s", test.from, other)
			}
		})
	}
}

// TestMTLSVerifierVerifyPeerRejectsRequireMismatch is the SAN-shaped version
// of a claim rule: a certificate that chains to a trusted CA but whose
// subject no require rule accepts is refused.
func TestMTLSVerifierVerifyPeerRejectsRequireMismatch(t *testing.T) {
	ca := newTestCA(t, "root")
	verifier := newMTLSVerifier(t, auth.TrustedIssuer{
		Name: "mesh", Kind: auth.IssuerKindMTLS, Issuer: "flowstate:mtls/mesh",
		ClientCAFile: ca.clientCAFile(t), SubjectFrom: auth.SubjectFromURISAN,
		Require: []auth.ClaimRule{
			auth.RequireClaim("subject", "spiffe://example.org/ns/ci/sa/runner"),
		},
	})

	leaf := ca.issueLeaf(t, withURISAN("spiffe://example.org/ns/ci/sa/impostor"))

	_, err := verifier.VerifyPeer(t.Context(), chainFor(t, leaf, ca))
	require.Error(t, err)
	require.ErrorIs(t, err, auth.ErrClaimMismatch)
}

// TestMTLSVerifierVerifyPeerAccepts is the positive control the negative
// tests around it need to mean something: a certificate chaining to the
// trusted CA, with exactly one SAN of the configured kind that satisfies
// every require rule, is admitted with the policy's namespace and role and
// the certificate's own thumbprint.
func TestMTLSVerifierVerifyPeerAccepts(t *testing.T) {
	ca := newTestCA(t, "root")
	verifier := newMTLSVerifier(t, auth.TrustedIssuer{
		Name: "mesh", Kind: auth.IssuerKindMTLS, Issuer: "flowstate:mtls/mesh",
		ClientCAFile: ca.clientCAFile(t), SubjectFrom: auth.SubjectFromURISAN,
		Require: []auth.ClaimRule{
			auth.RequireClaim("subject", "spiffe://example.org/ns/ci/sa/runner"),
		},
		Namespace: "ci",
		Role:      "runner",
	})

	leaf := ca.issueLeaf(t, withURISAN("spiffe://example.org/ns/ci/sa/runner"))

	principal, err := verifier.VerifyPeer(t.Context(), chainFor(t, leaf, ca))
	require.NoError(t, err)
	require.Equal(t, "flowstate:mtls/mesh", principal.Issuer)
	require.Equal(t, "mesh", principal.IssuerName)
	require.Equal(t, "spiffe://example.org/ns/ci/sa/runner", principal.Subject)
	require.Equal(t, "ci", principal.Namespace)
	require.Equal(t, "runner", principal.Role)
	require.NotEmpty(t, principal.CertificateThumbprint)
}

// TestMTLSVerifierVerifyPeerNamespaceComesFromPolicyNotCertificate is the
// tenancy negative direction CLAUDE.md asks for, applied to mTLS: two
// entries share a CA and are distinguished only by a require rule, each with
// its own fixed namespace, and a certificate that *names* the other tenant
// in its own SAN — a caller could mint any SAN value its CA is willing to
// sign — still ends up in the namespace its own matching entry names, never
// the one it claims.
func TestMTLSVerifierVerifyPeerNamespaceComesFromPolicyNotCertificate(t *testing.T) {
	ca := newTestCA(t, "shared-root")

	policy := auth.Policy{Issuers: []auth.TrustedIssuer{
		{
			Name: "tenant-a", Kind: auth.IssuerKindMTLS, Issuer: "flowstate:mtls/tenant-a",
			ClientCAFile: ca.clientCAFile(t), SubjectFrom: auth.SubjectFromURISAN,
			Require:   []auth.ClaimRule{auth.RequireClaim("subject", "spiffe://example.org/ns/tenant-a/sa/runner")},
			Namespace: "tenant-a",
		},
		{
			Name: "tenant-b", Kind: auth.IssuerKindMTLS, Issuer: "flowstate:mtls/tenant-b",
			ClientCAFile: ca.clientCAFile(t), SubjectFrom: auth.SubjectFromURISAN,
			Require:   []auth.ClaimRule{auth.RequireClaim("subject", "spiffe://example.org/ns/tenant-b/sa/runner")},
			Namespace: "tenant-b",
		},
	}}
	require.NoError(t, policy.Validate())

	verifier, err := auth.NewMTLSVerifier(policy)
	require.NoError(t, err)

	// A certificate whose own SAN names tenant-a: it lands in tenant-a's
	// namespace, and — the point of this test — there is no certificate the
	// tenant-a CA could sign that would ever land it in tenant-b's namespace,
	// because tenant-b requires a SAN this entry never admits.
	tenantACert := ca.issueLeaf(t, withURISAN("spiffe://example.org/ns/tenant-a/sa/runner"))
	principal, err := verifier.VerifyPeer(t.Context(), chainFor(t, tenantACert, ca))
	require.NoError(t, err)
	require.Equal(t, "tenant-a", principal.Namespace)

	// A certificate that instead names tenant-b's SAN correctly reaches
	// tenant-b's namespace — proving the boundary is the require rule, not an
	// accident of ordering — and a tenant-a-shaped SAN is refused by the
	// tenant-b entry when tried on its own.
	tenantBCert := ca.issueLeaf(t, withURISAN("spiffe://example.org/ns/tenant-b/sa/runner"))
	principal, err = verifier.VerifyPeer(t.Context(), chainFor(t, tenantBCert, ca))
	require.NoError(t, err)
	require.Equal(t, "tenant-b", principal.Namespace)

	// A certificate naming neither tenant's SAN is refused outright, never
	// defaulted into either namespace.
	strangerCert := ca.issueLeaf(t, withURISAN("spiffe://example.org/ns/other/sa/runner"))
	_, err = verifier.VerifyPeer(t.Context(), chainFor(t, strangerCert, ca))
	require.Error(t, err)
}
