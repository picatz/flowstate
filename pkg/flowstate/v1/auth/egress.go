package auth

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// Outbound identity HTTP — OIDC discovery, key set fetches, and token exchange
// — leaves this process the same way a workflow's own requests do: through a
// [netpolicy.Policy].
//
// It is one boundary rather than one per caller because every property this
// package needs is already there and already tested: scheme and port, address
// classification against the address actually dialed (including the IPv6 forms
// that name an IPv4 target), proxies off, a TLS floor with verified
// certificates, phase timeouts, a body cap installed on the response itself,
// a redirect count, and a per-hop re-check that refuses an https-to-http
// downgrade. A second classifier derived from [net/netip]'s predicates would
// have to re-derive all of that, and the deny side is where re-deriving loses:
// shared address space (RFC 6598), the cloud metadata endpoints, and
// ::7f00:1 or 64:ff9b::7f00:1 naming 127.0.0.1 are none of them a netip
// predicate, and all of them are already in [netpolicy] (see #1056).
//
// What stays here is what is not netpolicy's business: [ValidateHTTPSURL]'s
// rules about a URL an operator *wrote down* — absolute, names a host, carries
// no credentials, https unless it is loopback — which are properties of the
// configuration file rather than of a deployment's network, and the exchange
// path's refusal to follow any redirect at all, which is about the assertion in
// a request body rather than about where the request is going.

// defaultEgress is the policy identity fetches use when a deployment configures
// none. It is [netpolicy.New]'s safe default narrowed to https, because a
// discovery document, a key set, and a token exchange all carry or decide
// credentials, and [ValidateHTTPSURL] already refuses to configure one over
// plain http anywhere but loopback.
//
// Built once, lazily: it is shared by every verifier and exchanger that did not
// ask for something else, so they share one connection pool, and a process that
// never fetches anything never builds it.
var defaultEgress = sync.OnceValues(func() (*netpolicy.Policy, error) {
	return netpolicy.New(netpolicy.WithSchemes("https"))
})

// DefaultEgressPolicy returns the egress policy applied to outbound identity
// HTTP when a deployment names none: https only, to public addresses only,
// bounded in every dimension [netpolicy] bounds.
//
// It denies loopback, private, link-local, and carrier-grade NAT addresses, and
// the cloud metadata endpoints, which is the whole point: an issuer URL is
// operator-supplied and a discovery document's jwks_uri is *issuer*-supplied, so
// both are addresses an outside party gets a say in.
//
// A deployment whose issuer really is internal — an in-cluster Kubernetes API
// server, a sidecar — says so with a named option rather than by turning the
// boundary off:
//
//	policy, err := netpolicy.New(netpolicy.WithAllowPrivateNetworks())
//	verifier, err := auth.NewOIDCVerifier(trust, auth.WithEgressPolicy(policy))
//
// or, in the trust policy file itself, with an egress section (see
// [Policy.Egress]). Everything else stays enforced either way, which is the
// difference between loosening a boundary and removing one.
//
// It panics only if [netpolicy.New] rejects its own defaults, which is a
// programming error in this package rather than a configuration error.
func DefaultEgressPolicy() *netpolicy.Policy {
	policy, err := defaultEgress()
	if err != nil {
		panic(fmt.Sprintf("auth: the default identity egress policy does not build: %v", err))
	}
	return policy
}

// identityHTTPClient resolves the client outbound identity HTTP is made with.
//
// The three inputs are mutually exclusive on purpose. A caller that supplies its
// own client is supplying its own boundary — it owns the transport, so nothing
// here can bound what that transport does — and a deployment that has also
// named an egress policy has written down two answers to one question. Rather
// than pick one and leave the other silently unenforced, this refuses: an
// operator who believes a policy is in force while a client bypasses it is the
// fail-open this package exists to prevent.
//
// field names the caller's own spelling of the client option, so the refusal
// says what the operator has to change.
func identityHTTPClient(field string, client *http.Client, policy *netpolicy.Policy) (*http.Client, error) {
	if client != nil && policy != nil {
		return nil, fmt.Errorf("%w: %s and an egress policy cannot be combined: the client owns the "+
			"transport, so the policy would not be enforced on anything it sends. Express what the "+
			"client is for as netpolicy options (roots, proxy, timeouts) and pass only the policy",
			ErrInvalidPolicy, field)
	}

	switch {
	case client != nil:
		return client, nil
	case policy != nil:
		return policy.Client(), nil
	default:
		return DefaultEgressPolicy().Client(), nil
	}
}

// egressPolicyFromConfig builds the policy an `egress:` section in a
// configuration file describes, compiling every CEL rule it carries, so a
// malformed one refuses start-up rather than the first fetch. A nil section
// means the file said nothing, which is the default policy rather than an
// absent one.
func egressPolicyFromConfig(cfg *netpolicy.EgressConfig) (*netpolicy.Policy, error) {
	if cfg == nil {
		return nil, nil
	}

	opts, err := (netpolicy.Config{Egress: *cfg}).Options()
	if err != nil {
		return nil, fmt.Errorf("%w: egress: %w", ErrInvalidPolicy, err)
	}

	// https only unless the file says otherwise, so a section written to loosen
	// one thing — reaching a private issuer — does not quietly loosen the
	// scheme as well. A file that names schemes: is deciding for itself, and its
	// own option comes after this one and wins.
	opts = append([]netpolicy.Option{netpolicy.WithSchemes("https")}, opts...)

	policy, err := netpolicy.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: egress: %w", ErrInvalidPolicy, err)
	}

	return policy, nil
}
