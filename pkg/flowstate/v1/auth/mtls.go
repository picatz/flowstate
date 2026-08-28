package auth

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
)

// mTLS composes with the rest of this package's trust policy rather than
// introducing a second way to be authenticated. A client certificate is
// another issuer, admitted through a kind: mtls [TrustedIssuer] entry and
// producing the exact same [Principal] an OIDC token produces — same ID,
// same Namespace and Role rules, same CEL vocabulary at [attrIdentity].
//
// # Why this is not a [Verifier]
//
// [Verifier.Verify] is handed a string that came out of an Authorization
// header. A client certificate is not a string and does not come from a
// header; it arrives as a chain crypto/tls has already verified against the
// listener's ClientCAs, hanging off [http.Request.TLS]. Widening [Verifier]'s
// signature to carry that would give every implementation — the OIDC
// verifier, the anonymous one, the unconfigured one — a parameter only one of
// them reads. [PeerVerifier] is the seam one level up instead, consulted by
// [Authenticator] beside the ordinary [Verifier]; see [WithPeerVerifier].
//
// # What a certificate is allowed to say about itself
//
// Nothing is ever read from a leaf's Subject DN. CN-as-identity is the
// mistake every mTLS system regrets — a DN is unstructured and comparable
// only by convention — so [TrustedIssuer.SubjectFrom] names one typed SAN
// field instead, the same shape as a verified "sub" claim. [Principal.Issuer]
// is the trust policy's own name for the CA ([TrustedIssuer.Issuer]), never a
// value taken from the certificate, for the identical reason
// [Principal.Namespace] never comes from the request: a value a caller
// controls cannot be the thing that names the caller's trust boundary.
//
// # Revocation
//
// crypto/tls performs no CRL or OCSP checking, and this package adds none.
// The posture that composes with that honestly is short-lived certificates —
// which is what a service mesh issues anyway — so that a compromised
// certificate stops working on its own within minutes to hours rather than
// staying valid until an operator notices. A deployment issuing long-lived
// client certificates has no revocation path here; this is a limitation to
// know about, not a feature to assume.

// Issuer kinds a [TrustedIssuer] entry may declare. The zero value is
// [IssuerKindOIDC].
const (
	// IssuerKindOIDC verifies a bearer token against an OpenID Connect or
	// workload-identity-federation issuer. It is the default when
	// [TrustedIssuer.Kind] is empty, so every trust policy written before this
	// field existed keeps meaning exactly what it always meant.
	IssuerKindOIDC = "oidc"

	// IssuerKindMTLS admits a caller whose client certificate crypto/tls has
	// already verified against [TrustedIssuer.ClientCAFile].
	IssuerKindMTLS = "mtls"
)

// The SAN fields [TrustedIssuer.SubjectFrom] may name. There is deliberately
// no option to read the Subject DN — see this file's package doc.
const (
	SubjectFromURISAN   = "uri_san"
	SubjectFromDNSSAN   = "dns_san"
	SubjectFromEmailSAN = "email_san"
)

// maxClientCABytes bounds how much of a [TrustedIssuer.ClientCAFile] this
// package will read into memory, per CLAUDE.md's "bound anything that
// consumes untrusted input" — the file is operator-configured, but it is read
// at every server start-up and a certificate pool is not a place to discover
// an arbitrarily large file. Far beyond any real CA bundle, which is a few
// kilobytes even with a long intermediate chain.
const maxClientCABytes = 1 << 20 // 1 MiB

// PeerVerifier turns a client certificate chain crypto/tls has already
// verified against a listener's ClientCAs into the same [Principal] a
// [Verifier] produces from a bearer token.
//
// chains is exactly [tls.ConnectionState.VerifiedChains]: each element is one
// verified path from the peer's leaf certificate to a root crypto/tls trusted
// it against. Implementations must not re-verify or rebuild a chain — that is
// the standard library's job, already done by the time this is called — and
// must read nothing from a certificate's Subject DN; see this file's package
// doc for why.
//
// Implementations must be safe for concurrent use, and must return an error
// wrapping one of this package's sentinel errors for every chain they reject.
type PeerVerifier interface {
	// VerifyPeer verifies chains and returns the caller it authenticates.
	//
	// An empty chains is a rejection, not a special case: [Authenticator]
	// never calls VerifyPeer with an empty chains today, but an
	// implementation must still refuse one rather than assume its caller
	// checked.
	VerifyPeer(ctx context.Context, chains [][]*x509.Certificate) (Principal, error)
}

// mtlsEntry is one resolved kind: mtls [TrustedIssuer]: the policy entry
// together with the CA certificates its ClientCAFile named, loaded once at
// construction so [MTLSVerifier.VerifyPeer] never touches the filesystem.
type mtlsEntry struct {
	issuer  TrustedIssuer
	caCerts []*x509.Certificate

	// policyIndex is this entry's row in [Policy.Issuers], carried because
	// this list holds only the kind: mtls entries and a position in it is
	// therefore not a row an operator can count to. See [oidcEntry], where the
	// reasoning is written down once for both verifiers.
	policyIndex int
}

// MTLSVerifier is the [PeerVerifier] built from a [Policy]'s kind: mtls
// entries.
//
// It is also the source of truth for the public listener's client CA pool
// ([MTLSVerifier.ClientCAPool]): the trust policy is the one place an
// operator names a trusted CA, so the transport-level fence
// (tls.Config.ClientCAs) and the identity mapping consulted after a handshake
// succeeds are built from the same certificates rather than two
// independently-configured ones that could drift apart — the "one value,
// written down twice" defect CLAUDE.md names.
//
// An MTLSVerifier is safe for concurrent use by many goroutines.
type MTLSVerifier struct {
	entries []mtlsEntry
}

// Ensure MTLSVerifier satisfies the PeerVerifier interface.
var _ PeerVerifier = (*MTLSVerifier)(nil)

// NewMTLSVerifier builds a PeerVerifier from the kind: mtls entries of
// policy, loading and bounding every entry's ClientCAFile.
//
// nil, nil means the policy names no kind: mtls entry at all — the caller's
// cue that client-certificate authentication was not configured and nothing
// about mTLS should be wired up. Every other outcome that is not a fully
// loadable set of entries is an error, checked before any listener binds.
//
// policy is assumed already validated by [Policy.Validate].
func NewMTLSVerifier(policy Policy) (*MTLSVerifier, error) {
	var entries []mtlsEntry

	for policyIndex, issuer := range policy.Issuers {
		if issuer.kind() != IssuerKindMTLS {
			continue
		}

		certs, err := loadClientCAFile(issuer.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("trusted issuer %q: %w", issuer.Name, err)
		}

		// Copied, not aliased, for the identical reason [NewOIDCVerifier]
		// copies each entry: this verifier is read from many goroutines on
		// every request and must not be something a caller can still change.
		//
		// policyIndex, not len(entries): this list is filtered by kind, so a
		// position in it is not a row an operator can count to in their file.
		// See [oidcEntry], which carries the same field for the same reason.
		entries = append(entries, mtlsEntry{
			issuer:      issuer.clone(),
			caCerts:     certs,
			policyIndex: policyIndex,
		})
	}

	if len(entries) == 0 {
		return nil, nil
	}

	return &MTLSVerifier{entries: entries}, nil
}

// ClientCAPool returns the union of every kind: mtls entry's CA certificates,
// for use as the public listener's tls.Config.ClientCAs. Every call returns a
// fresh *x509.CertPool, so a caller may hold onto it without racing a later
// mutation.
func (v *MTLSVerifier) ClientCAPool() *x509.CertPool {
	pool := x509.NewCertPool()
	for _, entry := range v.entries {
		for _, cert := range entry.caCerts {
			pool.AddCert(cert)
		}
	}
	return pool
}

// VerifyPeer implements [PeerVerifier].
//
// Every kind: mtls entry whose CA pool the verified chain's certificates
// intersect is asked, exactly like [OIDCVerifier.Verify] asks every candidate
// entry sharing a token's issuer, and exactly one of them has to admit the
// certificate: a chain two entries both admit is refused with
// [AmbiguousIssuerError] rather than attributed to whichever comes first. The
// entries' order in the policy decides nothing.
//
// That is the same refusal [subjectFromLeaf] already makes one level down, for
// the same reason. A leaf carrying two URI SANs is refused rather than having
// one of them picked, because silently choosing between two names a
// certificate legitimately carries authenticates a subject nobody chose; two
// entries admitting one certificate — one selecting `uri_san` and one
// `dns_san`, say, each with its own namespace and role — is that choice made
// between two *policies* instead of two SANs.
//
// Namespace determination stays a rejection and never a reason to look
// further, per [TrustedIssuer.namespaceFor]; with exactly one entry admitting,
// there is nothing further to look at in any case.
func (v *MTLSVerifier) VerifyPeer(ctx context.Context, chains [][]*x509.Certificate) (Principal, error) {
	if len(chains) == 0 || len(chains[0]) == 0 {
		return Principal{}, fmt.Errorf("%w: no verified client certificate chain was presented", ErrNoToken)
	}

	leaf := chains[0][0]

	// The subject a certificate presents is per entry, because SubjectFrom is:
	// two entries selecting different SANs read different names off one leaf.
	// So the winning entry and the subject it read are kept together, and
	// winner is meaningful only where it is read, under exactly one match.
	type peerMatch struct {
		entry   mtlsEntry
		subject string
	}
	var (
		failures []error
		admitted []matchedEntry
		winner   peerMatch
	)
	for _, entry := range v.entries {
		if !chainMatchesCA(chains, entry.caCerts) {
			continue
		}

		subject, err := subjectFromLeaf(leaf, entry.issuer.SubjectFrom)
		if err != nil {
			failures = append(failures, fmt.Errorf("trusted issuer %q: %w", entry.issuer.Name, err))
			continue
		}

		// The only claim a certificate-derived Principal carries: the SAN
		// [TrustedIssuer.SubjectFrom] named. Require rules read it exactly
		// the way an OIDC rule reads a verified token claim.
		claims := map[string]any{"subject": subject}

		if err := entry.issuer.admitsPeer(claims); err != nil {
			failures = append(failures, fmt.Errorf("trusted issuer %q: %w", entry.issuer.Name, err))
			continue
		}

		admitted = append(admitted, matchedEntry{
			policyIndex: entry.policyIndex,
			name:        entry.issuer.Name,
			issuer:      entry.issuer.Issuer,
		})
		winner = peerMatch{entry: entry, subject: subject}
	}

	if len(admitted) > 1 {
		return Principal{}, ambiguousIssuer(admitted)
	}

	if len(admitted) == 0 {
		switch len(failures) {
		case 0:
			// No configured entry's CA pool contains any certificate in the
			// verified chain. Reachable when several MTLSVerifiers coexist behind
			// one listener's combined ClientCAs, or defensively if this is ever
			// called with a chain crypto/tls verified against a broader pool than
			// this policy's own — this package never assumes its own CA pool was
			// the only one composing the listener's ClientCAs.
			return Principal{}, fmt.Errorf("%w: client certificate does not chain to a trusted issuer's CA", ErrUntrustedIssuer)
		case 1:
			return Principal{}, failures[0]
		default:
			return Principal{}, errors.Join(failures...)
		}
	}

	entry := winner.entry
	subject := winner.subject
	claims := map[string]any{"subject": subject}

	// The tenant is established here, from the verified certificate, and
	// nowhere else — see [TrustedIssuer.namespaceFor]'s own doc.
	namespace, err := entry.issuer.namespaceFor(claims)
	if err != nil {
		return Principal{}, fmt.Errorf("trusted issuer %q: %w", entry.issuer.Name, err)
	}

	return Principal{
		Issuer:                entry.issuer.Issuer,
		IssuerName:            entry.issuer.Name,
		Subject:               subject,
		Namespace:             namespace,
		Role:                  entry.issuer.Role,
		Claims:                claims,
		CertificateThumbprint: sha256Hex(leaf.Raw),
	}, nil
}

// admitsPeer checks the claim rules common to every kind against a
// certificate-derived claims set. A kind: mtls entry carries no algorithm,
// audience, or max-token-age fields to check — [TrustedIssuer.validateMTLS]
// refuses a policy that sets any of them — so this is the whole of what a
// certificate must additionally satisfy beyond chaining to the entry's CA.
func (t TrustedIssuer) admitsPeer(claims map[string]any) error {
	for _, rule := range t.Require {
		if err := rule.check(claims); err != nil {
			return err
		}
	}
	return nil
}

// chainMatchesCA reports whether any certificate in any verified chain is one
// of caCerts, which is how [MTLSVerifier.VerifyPeer] decides which policy
// entry (and therefore which SubjectFrom and Require rules) applies to a
// chain that was verified against the listener's combined ClientCAs pool
// rather than against any single entry's pool alone.
func chainMatchesCA(chains [][]*x509.Certificate, caCerts []*x509.Certificate) bool {
	for _, chain := range chains {
		for _, cert := range chain {
			for _, ca := range caCerts {
				if cert.Equal(ca) {
					return true
				}
			}
		}
	}
	return false
}

// subjectFromLeaf extracts the one named SAN field of leaf, refusing a
// certificate that carries zero or more than one value there. More than one
// is refused rather than the first one taken, because silently picking a SAN
// is how a certificate legitimately issued for one purpose (several DNS
// names, say) ends up authenticating as a subject nobody chose on purpose.
//
// This is also the bound CLAUDE.md asks for: leaf.URIs, leaf.DNSNames, and
// leaf.EmailAddresses are already fully parsed by [x509.ParseCertificate]
// before this is ever called, so checking len() first means a certificate
// naming an enormous number of SANs is rejected in O(1) rather than being
// scanned.
func subjectFromLeaf(leaf *x509.Certificate, from string) (string, error) {
	switch from {
	case SubjectFromURISAN:
		switch len(leaf.URIs) {
		case 0:
			return "", fmt.Errorf("%w: certificate has no URI SAN", ErrMissingClaim)
		case 1:
			return leaf.URIs[0].String(), nil
		default:
			return "", fmt.Errorf("%w: certificate has %d URI SANs; subject_from: %s requires exactly one",
				ErrMalformedToken, len(leaf.URIs), SubjectFromURISAN)
		}
	case SubjectFromDNSSAN:
		switch len(leaf.DNSNames) {
		case 0:
			return "", fmt.Errorf("%w: certificate has no DNS SAN", ErrMissingClaim)
		case 1:
			return leaf.DNSNames[0], nil
		default:
			return "", fmt.Errorf("%w: certificate has %d DNS SANs; subject_from: %s requires exactly one",
				ErrMalformedToken, len(leaf.DNSNames), SubjectFromDNSSAN)
		}
	case SubjectFromEmailSAN:
		switch len(leaf.EmailAddresses) {
		case 0:
			return "", fmt.Errorf("%w: certificate has no email SAN", ErrMissingClaim)
		case 1:
			return leaf.EmailAddresses[0], nil
		default:
			return "", fmt.Errorf("%w: certificate has %d email SANs; subject_from: %s requires exactly one",
				ErrMalformedToken, len(leaf.EmailAddresses), SubjectFromEmailSAN)
		}
	default:
		// Unreachable once the policy has passed [TrustedIssuer.validateMTLS],
		// stated anyway because this function does not itself trust that
		// validation ran.
		return "", fmt.Errorf("%w: unsupported subject_from %q", ErrInvalidPolicy, from)
	}
}

// loadClientCAFile reads and parses a kind: mtls entry's ClientCAFile,
// bounded in bytes before anything is parsed.
func loadClientCAFile(path string) ([]*x509.Certificate, error) {
	if path == "" {
		return nil, fmt.Errorf("client_ca_file is required for kind: %s", IssuerKindMTLS)
	}

	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("reading client_ca_file %s: %w", path, err)
	}
	if info.Size() > maxClientCABytes {
		return nil, fmt.Errorf("client_ca_file %s is %d bytes, over the %d byte limit",
			path, info.Size(), maxClientCABytes)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading client_ca_file %s: %w", path, err)
	}

	var certs []*x509.Certificate
	rest := data
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parsing a certificate in client_ca_file %s: %w", path, err)
		}
		certs = append(certs, cert)
	}
	if len(certs) == 0 {
		return nil, fmt.Errorf("client_ca_file %s contains no PEM CERTIFICATE blocks", path)
	}

	return certs, nil
}

// sha256Hex returns the hex-encoded SHA-256 digest of raw, used for
// [Principal.CertificateThumbprint].
func sha256Hex(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
