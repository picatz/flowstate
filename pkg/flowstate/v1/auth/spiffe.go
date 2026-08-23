package auth

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"path"
	"strings"
)

// SPIFFE identities deliberately remain ordinary Principals. Consequently a
// caller admitted here reaches the same PARC authorization path as OIDC and
// conventional mTLS callers; a trust bundle is authentication, not permission.
func spiffePrincipalClaims(leaf *x509.Certificate, profile TrustedIssuer) (string, map[string]any, error) {
	if leaf == nil || len(leaf.URIs) != 1 {
		return "", nil, fmt.Errorf("%w: an X.509-SVID must contain exactly one URI SAN", ErrMalformedToken)
	}
	id, domain, selectors, err := parseSPIFFEID(leaf.URIs[0])
	if err != nil {
		return "", nil, err
	}
	if domain != profile.TrustDomain && !containsString(profile.FederatedTrustDomains, domain) {
		return "", nil, fmt.Errorf("%w: SPIFFE trust domain %q", ErrUntrustedIssuer, domain)
	}
	allowed := false
	for _, pattern := range profile.AllowedSPIFFEIDs {
		if match, _ := path.Match(pattern, id); match {
			allowed = true
			break
		}
	}
	if !allowed {
		return "", nil, fmt.Errorf("%w: SPIFFE ID is not allowed by profile %q", ErrUntrustedIssuer, profile.Name)
	}
	claims := map[string]any{"subject": id, "spiffe.trust_domain": domain}
	for key, value := range selectors {
		claims["spiffe.selector."+key] = value
	}
	return id, claims, nil
}

func parseSPIFFEID(u *url.URL) (string, string, map[string]string, error) {
	if u == nil || u.Scheme != "spiffe" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Port() != "" {
		return "", "", nil, fmt.Errorf("%w: invalid SPIFFE URI SAN", ErrMalformedToken)
	}
	if err := validateTrustDomain(u.Host); err != nil {
		return "", "", nil, fmt.Errorf("%w: %v", ErrMalformedToken, err)
	}
	if u.EscapedPath() != u.Path || u.Path == "" || u.Path == "/" || strings.Contains(u.Path, "//") {
		return "", "", nil, fmt.Errorf("%w: SPIFFE workload path is not canonical", ErrMalformedToken)
	}
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	selectors := make(map[string]string, len(parts)/2)
	if len(parts)%2 == 0 {
		for i := 0; i < len(parts); i += 2 {
			if parts[i] != "" && parts[i+1] != "" {
				selectors[parts[i]] = parts[i+1]
			}
		}
	}
	return "spiffe://" + u.Host + u.Path, u.Host, selectors, nil
}

func validateTrustDomain(domain string) error {
	if domain == "" || domain != strings.ToLower(domain) || strings.ContainsAny(domain, "/:@ ") {
		return fmt.Errorf("trust_domain %q is not a lowercase DNS name", domain)
	}
	u, err := url.Parse("spiffe://" + domain + "/workload")
	if err != nil || u.Hostname() != domain || !strings.Contains(domain, ".") {
		return fmt.Errorf("trust_domain %q is not a lowercase DNS name", domain)
	}
	return nil
}

func validateSPIFFEPattern(pattern, primary string, federated []string) error {
	if !strings.HasPrefix(pattern, "spiffe://") {
		return fmt.Errorf("pattern must be a complete spiffe:// ID")
	}
	if _, err := path.Match(pattern, pattern); err != nil {
		return fmt.Errorf("invalid pattern: %w", err)
	}
	hostAndPath := strings.TrimPrefix(pattern, "spiffe://")
	host, _, ok := strings.Cut(hostAndPath, "/")
	if !ok || (host != primary && !containsString(federated, host)) {
		return fmt.Errorf("pattern trust domain %q is not primary or federated", host)
	}
	return nil
}

func containsString(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

// X509SVIDSource is the deliberately non-serializable Workload API boundary.
// Implementations return a certificate whose PrivateKey remains owned by the
// source or its activity-side signer. Neither this interface nor TLSConfig has
// a protobuf representation, so SVID material cannot enter workflow history.
type X509SVIDSource interface {
	GetX509SVID(*tls.CertificateRequestInfo) (*tls.Certificate, error)
}

// WorkloadAPITLSConfig acquires each outbound certificate at handshake time.
// Rotation and an unavailable Workload API therefore take effect immediately;
// failure is returned to TLS rather than falling back to another credential.
func WorkloadAPITLSConfig(source X509SVIDSource, roots *x509.CertPool) (*tls.Config, error) {
	if source == nil {
		return nil, fmt.Errorf("workload API X.509-SVID source is required")
	}
	return &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS13, GetClientCertificate: source.GetX509SVID}, nil
}
