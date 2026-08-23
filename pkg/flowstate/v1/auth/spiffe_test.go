package auth

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSPIFFEPrincipalNormalizationAndSelectors(t *testing.T) {
	id, err := url.Parse("spiffe://prod.example/ns/payments/sa/worker")
	require.NoError(t, err)
	profile := TrustedIssuer{
		Name: "prod", Kind: IssuerKindSPIFFE, TrustDomain: "prod.example",
		AllowedSPIFFEIDs: []string{"spiffe://prod.example/ns/payments/sa/worker"},
	}
	subject, claims, err := spiffePrincipalClaims(&x509.Certificate{URIs: []*url.URL{id}}, profile)
	require.NoError(t, err)
	require.Equal(t, "spiffe://prod.example/ns/payments/sa/worker", subject)
	require.Equal(t, "prod.example", claims["spiffe.trust_domain"])
	require.Equal(t, "payments", claims["spiffe.selector.ns"])
	require.Equal(t, "worker", claims["spiffe.selector.sa"])
}

func TestSPIFFEProfileRefusesWrongDomainCrossTenantAndUnlistedID(t *testing.T) {
	profile := TrustedIssuer{Name: "tenant-a", Kind: IssuerKindSPIFFE, TrustDomain: "a.example",
		AllowedSPIFFEIDs: []string{"spiffe://a.example/ns/a/sa/flow"}}
	for _, raw := range []string{
		"spiffe://b.example/ns/a/sa/flow",
		"spiffe://a.example/ns/b/sa/flow",
		"spiffe://a.example/ns/a/sa/other",
	} {
		u, err := url.Parse(raw)
		require.NoError(t, err)
		_, _, err = spiffePrincipalClaims(&x509.Certificate{URIs: []*url.URL{u}}, profile)
		require.Error(t, err, raw)
	}
}

func TestSPIFFEFederationIsExplicit(t *testing.T) {
	u, err := url.Parse("spiffe://partner.example/ns/partner/sa/api")
	require.NoError(t, err)
	profile := TrustedIssuer{Name: "federated", Kind: IssuerKindSPIFFE, TrustDomain: "home.example",
		FederatedTrustDomains: []string{"partner.example"},
		AllowedSPIFFEIDs:      []string{"spiffe://partner.example/ns/partner/sa/api"}}
	_, _, err = spiffePrincipalClaims(&x509.Certificate{URIs: []*url.URL{u}}, profile)
	require.NoError(t, err)
	profile.FederatedTrustDomains = nil
	_, _, err = spiffePrincipalClaims(&x509.Certificate{URIs: []*url.URL{u}}, profile)
	require.Error(t, err)
}

type rotatingSVIDSource struct {
	cert *tls.Certificate
	err  error
}

func (s *rotatingSVIDSource) GetX509SVID(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	return s.cert, s.err
}

func TestWorkloadAPITLSConfigAcquiresAtHandshakeAndFailsClosed(t *testing.T) {
	first := &tls.Certificate{Certificate: [][]byte{{1}}}
	second := &tls.Certificate{Certificate: [][]byte{{2}}}
	source := &rotatingSVIDSource{cert: first}
	cfg, err := WorkloadAPITLSConfig(source, x509.NewCertPool())
	require.NoError(t, err)
	require.Empty(t, cfg.Certificates, "SVID private material must not be copied into static TLS configuration")
	got, err := cfg.GetClientCertificate(&tls.CertificateRequestInfo{})
	require.NoError(t, err)
	require.Same(t, first, got)
	source.cert = second // deterministic fixture for Workload API rotation.
	got, err = cfg.GetClientCertificate(&tls.CertificateRequestInfo{})
	require.NoError(t, err)
	require.Same(t, second, got)
	source.err = errors.New("workload API unavailable")
	_, err = cfg.GetClientCertificate(&tls.CertificateRequestInfo{})
	require.ErrorContains(t, err, "unavailable")
}

func TestSPIFFEPolicyRequiresTenantPatternAndJWTAudienceIsExplicit(t *testing.T) {
	base := TrustedIssuer{Name: "prod", Kind: IssuerKindSPIFFE, Issuer: "spiffe-prod",
		ClientCAFile: "bundle.pem", TrustDomain: "prod.example", Namespace: "payments",
		AllowedSPIFFEIDs: []string{"spiffe://prod.example/ns/payments/sa/*"},
		JWTSVIDAudiences: []string{"https://flowstate.example"}}
	require.NoError(t, base.validate())
	base.Namespace = ""
	require.ErrorContains(t, base.validate(), "namespace is required")
}
