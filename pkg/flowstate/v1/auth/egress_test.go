package auth_test

import (
	"crypto/x509"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"

	"github.com/picatz/jose/pkg/jwa"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// TestIdentityFetchesAreRefusedToInternalAddresses is the negative direction of
// the boundary, and the reason this package no longer classifies addresses
// itself.
//
// Each address here is one an identity fetch must never reach. Half of them are
// not [net/netip] predicates at all — shared address space, two cloud metadata
// endpoints, and three IPv6 spellings of 127.0.0.1 — which is exactly why the
// classification is netpolicy's and not a second one written here: a fetch is
// refused because the one classifier in this repository refuses it.
//
// An issuer URL is operator-supplied, but a discovery document's jwks_uri is
// *issuer*-supplied, so the address a key set fetch ends up at is chosen outside
// this deployment. Nothing is dialed by any of these: the denial happens in the
// dialer's control function, before a connection exists.
func TestIdentityFetchesAreRefusedToInternalAddresses(t *testing.T) {
	t.Parallel()

	for _, address := range []string{
		"127.0.0.1",       // loopback
		"10.0.0.1",        // private
		"192.168.1.1",     // private
		"169.254.169.254", // link-local, and AWS/GCP/Azure instance metadata
		"100.64.0.1",      // RFC 6598 shared address space, which IsPrivate does not cover
		"100.100.100.200", // Alibaba Cloud instance metadata
		"192.0.0.192",     // Oracle Cloud instance metadata
	} {
		t.Run(address, func(t *testing.T) {
			t.Parallel()

			verifier, err := auth.NewOIDCVerifier(auth.Policy{
				Issuers: []auth.TrustedIssuer{{
					Name:      "internal",
					Issuer:    "https://" + address,
					Audiences: []string{"flowstate"},
				}},
			})
			require.NoError(t, err)

			err = verifier.Prime(t.Context())
			require.Error(t, err)
			require.ErrorIs(t, err, netpolicy.ErrDenied,
				"an identity fetch to %s must be refused by the egress policy", address)
		})
	}

	// The IPv6 spellings of an internal address are checked against the same
	// policy object the fetches above go through, rather than by dialing them:
	// a host with IPv6 disabled fails to create the socket before the dialer's
	// control function ever runs, which would make this a test of the machine
	// it happens to run on. What is being asserted is the same thing either
	// way — that the one policy identity fetches use refuses these — and none
	// of them is a [net/netip] predicate.
	for _, address := range []string{
		"::1",              // IPv6 loopback
		"::ffff:127.0.0.1", // IPv4-mapped loopback
		"::7f00:1",         // IPv4-compatible loopback
		"64:ff9b::7f00:1",  // NAT64 loopback
		"2002:7f00:1::",    // 6to4 loopback
		"fd00::1",          // unique-local
		"fe80::1",          // IPv6 link-local
	} {
		t.Run(address, func(t *testing.T) {
			t.Parallel()

			err := auth.DefaultEgressPolicy().CheckAddr(netip.AddrPortFrom(netip.MustParseAddr(address), 443))
			require.ErrorIs(t, err, netpolicy.ErrDenied,
				"an identity fetch to %s must be refused by the egress policy", address)
		})
	}
}

// TestExchangeEndpointOnAnInternalAddressIsRefused is the same direction for the
// outbound half. It matters more here than on the verifier, because an exchange
// carries a signed assertion in the request body: a token endpoint that resolved
// to the metadata service would be handed a credential, not just asked for one.
func TestExchangeEndpointOnAnInternalAddressIsRefused(t *testing.T) {
	t.Parallel()

	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	exchanger, err := auth.NewTokenExchanger(auth.TokenExchangeConfig{
		Name:     "metadata",
		TokenURL: "https://169.254.169.254/token",
		Audience: "https://as.example.com",
		Clock:    clock.Now,
	})
	require.NoError(t, err)

	_, err = exchanger.Exchange(t.Context(), mintAssertion(t, issuer, exchanger.Requirement().Audience))
	require.Error(t, err)
	require.ErrorIs(t, err, auth.ErrExchangeFailed)
	// The denial reaches the operator as text rather than as a wrapped sentinel:
	// this package formats a relying party's transport error with %v on purpose,
	// so that nothing below it is unwrapped into a persisted failure.
	require.ErrorContains(t, err, netpolicy.ErrDenied.Error())
	require.ErrorContains(t, err, "cloud metadata addresses are not allowed")
}

// TestAPrivateIssuerIsReachedByANamedOptionAndNotByDisablingTheBoundary is the
// deployment the default refuses on purpose: an issuer inside the network, which
// is what an in-cluster Kubernetes API server is.
//
// The point of the pair is that reaching it is a *loosening* — one named option,
// with everything else still enforced — rather than an escape hatch that hands
// the fetch an unpoliced client. The TLS floor, the body cap, the redirect rules
// and the denial of every other internal range are still in force in the second
// half of this test.
func TestAPrivateIssuerIsReachedByANamedOptionAndNotByDisablingTheBoundary(t *testing.T) {
	t.Parallel()

	var (
		key    = authtest.GenerateKey("primary", jwa.ES256)
		clock  = authtest.NewClock(referenceTime)
		issuer = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))
	)

	policy := auth.Policy{
		Issuers: []auth.TrustedIssuer{{
			Name:      "internal",
			Issuer:    issuer.URL(),
			Audiences: []string{"flowstate"},
		}},
	}
	token := issuer.MintToken(nil, authtest.WithSubject("runner"), authtest.WithAudience("flowstate"))

	refusing, err := auth.NewOIDCVerifier(policy, auth.WithClock(clock.Now))
	require.NoError(t, err)

	_, err = refusing.Verify(t.Context(), token)
	require.ErrorIs(t, err, netpolicy.ErrDenied, "the default policy must refuse an issuer on this machine")

	reaching, err := auth.NewOIDCVerifier(policy,
		auth.WithClock(clock.Now),
		auth.WithEgressPolicy(authtest.EgressPolicy()),
	)
	require.NoError(t, err)

	principal, err := reaching.Verify(t.Context(), token)
	require.NoError(t, err)
	require.Equal(t, "runner", principal.Subject)
}

// TestPolicyBlockedFetchReportsBlockedNotUnavailable is the regression for #1303:
// an egress-policy denial must say "blocked by the identity egress policy", not
// "temporarily unavailable". The error still wraps ErrIssuerUnavailable so
// existing errors.Is checks match, but IssuerBlockedError carries the URL and
// rule, and PublicReason names the cause.
func TestPolicyBlockedFetchReportsBlockedNotUnavailable(t *testing.T) {
	t.Parallel()

	var (
		key    = authtest.GenerateKey("primary", jwa.ES256)
		clock  = authtest.NewClock(referenceTime)
		issuer = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))
	)

	policy := auth.Policy{
		Issuers: []auth.TrustedIssuer{{
			Name:      "internal",
			Issuer:    issuer.URL(),
			Audiences: []string{"flowstate"},
		}},
	}
	token := issuer.MintToken(nil, authtest.WithSubject("runner"), authtest.WithAudience("flowstate"))

	refusing, err := auth.NewOIDCVerifier(policy, auth.WithClock(clock.Now))
	require.NoError(t, err)

	_, err = refusing.Verify(t.Context(), token)

	require.ErrorIs(t, err, auth.ErrIssuerUnavailable, "must still wrap the sentinel")
	require.ErrorIs(t, err, netpolicy.ErrDenied, "must still wrap the denial")

	var blocked *auth.IssuerBlockedError
	require.ErrorAs(t, err, &blocked, "must be an IssuerBlockedError, not a plain wrap")
	require.Equal(t, issuer.URL(), blocked.Issuer)
	require.NotEmpty(t, blocked.URL)
	require.NotNil(t, blocked.Deny)

	reason := auth.PublicReason(err)
	require.Contains(t, reason, "blocked",
		"PublicReason must say 'blocked', not 'temporarily unavailable'")
	require.NotContains(t, reason, "temporarily",
		"a deterministic denial is not temporary")
}

// TestTrustPolicyEgressSectionReachesTheSameBoundary checks the file form: an
// operator who cannot call a Go option still has to be able to name the
// loosening their deployment needs, or the only reachable answer is to turn the
// boundary off.
func TestTrustPolicyEgressSectionReachesTheSameBoundary(t *testing.T) {
	t.Parallel()

	var (
		key    = authtest.GenerateKey("primary", jwa.ES256)
		clock  = authtest.NewClock(referenceTime)
		issuer = newTestIssuer(t, authtest.WithClock(clock.Now), authtest.WithKeys(key))
	)

	policy, err := auth.ParsePolicy([]byte(fmt.Sprintf(`
issuers:
  - name: internal
    issuer: %s
    audiences: [flowstate]
egress:
  allow_loopback: true
  schemes: [http, https]
`, issuer.URL())))
	require.NoError(t, err)
	require.NoError(t, policy.Validate())

	verifier, err := auth.NewOIDCVerifier(policy, auth.WithClock(clock.Now))
	require.NoError(t, err)

	principal, err := verifier.Verify(t.Context(),
		issuer.MintToken(nil, authtest.WithSubject("runner"), authtest.WithAudience("flowstate")))
	require.NoError(t, err)
	require.Equal(t, "runner", principal.Subject)
}

// TestTrustPolicyEgressSectionIsCompiledWhenTheFileLoads is the fail-closed half
// of the section above: a rule that cannot compile is a configuration error at
// start-up, not a surprise on the first fetch of the first token.
func TestTrustPolicyEgressSectionIsCompiledWhenTheFileLoads(t *testing.T) {
	t.Parallel()

	// [auth.ParsePolicy] validates what it parses, so the malformed rule is
	// refused where the file is read.
	_, err := auth.ParsePolicy([]byte(`
issuers:
  - name: ci
    issuer: https://issuer.example.com
    audiences: [flowstate]
egress:
  deny: ['host ==']
`))
	require.Error(t, err)
	require.ErrorIs(t, err, auth.ErrInvalidPolicy)
	require.ErrorContains(t, err, "egress")

	// And a policy built in Go rather than read from a file meets the same
	// refusal, at the verifier rather than at the first fetch.
	verifier, err := auth.NewOIDCVerifier(auth.Policy{
		Issuers: []auth.TrustedIssuer{{
			Name:      "ci",
			Issuer:    "https://issuer.example.com",
			Audiences: []string{"flowstate"},
		}},
		Egress: &netpolicy.EgressConfig{Deny: []string{"host =="}},
	})
	require.Error(t, err)
	require.ErrorIs(t, err, auth.ErrInvalidPolicy)
	require.Nil(t, verifier)
}

// TestAClientAndAnEgressPolicyCannotBothBeConfigured checks the contradiction is
// refused rather than resolved by precedence. Whichever one lost would be
// silently unenforced, and an operator who believes a policy is in force while a
// client bypasses it is worse off than one who is told to pick.
func TestAClientAndAnEgressPolicyCannotBothBeConfigured(t *testing.T) {
	t.Parallel()

	policy := auth.Policy{
		Issuers: []auth.TrustedIssuer{{
			Name:      "ci",
			Issuer:    "https://issuer.example.com",
			Audiences: []string{"flowstate"},
		}},
	}

	_, err := auth.NewOIDCVerifier(policy,
		auth.WithHTTPClient(&http.Client{}),
		auth.WithEgressPolicy(authtest.EgressPolicy()),
	)
	require.ErrorIs(t, err, auth.ErrInvalidPolicy)
	require.Contains(t, err.Error(), "cannot be combined")

	withFile := policy
	withFile.Egress = &netpolicy.EgressConfig{AllowLoopback: true}
	_, err = auth.NewOIDCVerifier(withFile, auth.WithHTTPClient(&http.Client{}))
	require.ErrorIs(t, err, auth.ErrInvalidPolicy)

	_, err = auth.NewTokenExchanger(auth.TokenExchangeConfig{
		Name:         "partner",
		TokenURL:     "https://as.example.com/token",
		Audience:     "https://as.example.com",
		HTTPClient:   &http.Client{},
		EgressPolicy: authtest.EgressPolicy(),
	})
	require.ErrorIs(t, err, auth.ErrInvalidPolicy)
}

// TestAnOversizedErrorBodyIsBoundedByTheTransport is the bound that a library
// option would have missed. The exchange reads the body of a *failed* response
// too, to report the relying party's own error code — and that is the path a
// hostile peer takes, because it is the one it can reach without holding a valid
// trust relationship. The cap is installed on the response by the policy's round
// tripper, below anything this package reaches for, so it applies to the error
// path exactly as it does to the success path.
func TestAnOversizedErrorBodyIsBoundedByTheTransport(t *testing.T) {
	t.Parallel()

	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		chunk := strings.Repeat("a", 64<<10)
		for range 64 { // 4 MiB, four times the cap
			_, _ = w.Write([]byte(chunk))
		}
	}))
	t.Cleanup(server.Close)

	exchanger, err := auth.NewTokenExchanger(auth.TokenExchangeConfig{
		Name:         "partner",
		TokenURL:     server.URL + "/token",
		Audience:     "https://as.example.com",
		Clock:        clock.Now,
		EgressPolicy: authtest.EgressPolicy(),
	})
	require.NoError(t, err)

	_, err = exchanger.Exchange(t.Context(), mintAssertion(t, issuer, exchanger.Requirement().Audience))
	require.Error(t, err)
	require.ErrorIs(t, err, auth.ErrExchangeFailed)
	require.Contains(t, err.Error(), "more than")

	// And the cap an operator sets is the one that applies, below this package's
	// own: a policy with a smaller limit bounds a *successful* body that this
	// package would otherwise have read in full.
	tight, err := netpolicy.New(
		netpolicy.WithAllowLoopback(),
		netpolicy.WithSchemes("http", "https"),
		netpolicy.WithMaxResponseBytes(4<<10),
	)
	require.NoError(t, err)

	big := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, http.StatusOK, map[string]any{
			"access_token":      strings.Repeat("t", 32<<10),
			"issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
			"token_type":        "Bearer",
			"expires_in":        300,
		})
	}))
	t.Cleanup(big.Close)

	bounded, err := auth.NewTokenExchanger(auth.TokenExchangeConfig{
		Name:         "partner",
		TokenURL:     big.URL + "/token",
		Audience:     "https://as.example.com",
		Clock:        clock.Now,
		EgressPolicy: tight,
	})
	require.NoError(t, err)

	_, err = bounded.Exchange(t.Context(), mintAssertion(t, issuer, bounded.Requirement().Audience))
	require.Error(t, err)
	require.ErrorContains(t, err, "more than 4096 bytes")
}

// TestDefaultEgressPolicyIsTheSafeOne states the default in one place, so that a
// change to it is a change to this test rather than a change nobody notices. A
// client handed out by the default policy must refuse the addresses above even
// when nothing in this package is involved.
func TestDefaultEgressPolicyIsTheSafeOne(t *testing.T) {
	t.Parallel()

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	pool := x509.NewCertPool()
	pool.AddCert(server.Certificate())

	_, err := auth.DefaultEgressPolicy().Client().Get(server.URL)
	require.ErrorIs(t, err, netpolicy.ErrDenied)

	// http is not in the default scheme allowlist either, whatever the address.
	_, err = auth.DefaultEgressPolicy().Client().Get("http://example.com")
	require.ErrorIs(t, err, netpolicy.ErrDenied)
}
