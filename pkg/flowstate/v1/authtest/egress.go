package authtest

import (
	"fmt"
	"sync"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// egress is built once and shared: it is immutable, and every issuer in a test
// binary is on the same loopback interface.
var egress = sync.OnceValues(func() (*netpolicy.Policy, error) {
	return netpolicy.New(
		netpolicy.WithAllowLoopback(),
		netpolicy.WithSchemes("http", "https"),
	)
})

// EgressPolicy returns the egress policy that reaches an in-process [Issuer],
// and nothing else the default policy would refuse.
//
// Outbound identity HTTP goes through a netpolicy.Policy, whose safe default
// denies loopback — which is exactly where an [Issuer] listens, since it is an
// httptest server in this process. A test therefore has to say that its issuer
// is on this machine, in the same named way a deployment with a loopback issuer
// would:
//
//	verifier, err := auth.NewOIDCVerifier(policy,
//		auth.WithEgressPolicy(authtest.EgressPolicy()))
//
// This loosens two things and nothing else: loopback addresses are permitted,
// and http is added to the scheme allowlist, because [Issuer] serves plain
// http. Every other bound — the TLS floor for anything that is https, the phase
// timeouts, the body cap, the redirect rules, and the denial of every other
// internal address range — is the default, so a test cannot accidentally prove
// that a fetch to link-local or private address space is allowed.
//
// It is a policy rather than an [http.Client] on purpose: handing a test its own
// client would replace the boundary instead of loosening it, which is the
// difference this package's own subject matter is about.
func EgressPolicy() *netpolicy.Policy {
	policy, err := egress()
	if err != nil {
		panic(fmt.Sprintf("authtest: the loopback egress policy does not build: %v", err))
	}
	return policy
}
