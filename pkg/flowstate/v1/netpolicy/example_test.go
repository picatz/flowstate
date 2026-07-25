package netpolicy_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"strings"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// Example shows the safe default: public HTTP and HTTPS only.
func Example() {
	policy, err := netpolicy.New()
	if err != nil {
		panic(err)
	}

	client := policy.Client()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://169.254.169.254/latest/meta-data/", nil)
	if err != nil {
		panic(err)
	}

	if _, err := client.Do(req); errors.Is(err, netpolicy.ErrDenied) {
		var denied *netpolicy.DenyError
		if errors.As(err, &denied) {
			fmt.Printf("%s: %s\n", denied.Reason, denied.Detail)
		}
	}

	// Output: address: cloud metadata addresses are not allowed
}

// Example_rules shows an operator expressing egress policy as CEL, in the same
// language workflows are written in.
func Example_rules() {
	policy, err := netpolicy.New(
		// Only these two APIs, over TLS, read-only.
		netpolicy.WithSchemes("https"),
		netpolicy.WithAllowRules(
			`host == "api.github.com"`,
			`host.endsWith(".githubusercontent.com")`,
		),
		netpolicy.WithDenyRules(
			`!(method in ["GET", "HEAD"])`,
			`path.startsWith("/admin")`,
		),
	)
	if err != nil {
		panic(err)
	}

	for _, target := range []string{
		"https://api.github.com/repos/picatz/flowstate",
		"https://evil.example.com/",
	} {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, target, nil)
		if err != nil {
			panic(err)
		}

		if err := policy.CheckURL(req.Context(), req.Method, req.URL); err != nil {
			fmt.Printf("%s: denied\n", req.URL.Host)
			continue
		}
		fmt.Printf("%s: allowed\n", req.URL.Host)
	}

	// Output:
	// api.github.com: allowed
	// evil.example.com: denied
}

// Example_localDevelopment shows the named opt-ins a developer needs to reach
// services on their own machine, and the CIDR allowance an operator uses to reach
// one internal network without opening private space generally.
func Example_localDevelopment() {
	policy, err := netpolicy.New(
		netpolicy.WithAllowLoopback(),
		netpolicy.WithAllowNetworks(netip.MustParsePrefix("10.42.0.0/16")),
	)
	if err != nil {
		panic(err)
	}

	// Still denied: the allowance is scoped to one network.
	fmt.Println(policy.CheckAddr(netip.MustParseAddrPort("10.1.2.3:443")) != nil)

	// Output: true
}

// Example_readBody shows how a task reads a response body under the policy's cap,
// reporting an oversized body as an error rather than truncating it.
func Example_readBody() {
	policy, err := netpolicy.New(netpolicy.WithMaxResponseBytes(64))
	if err != nil {
		panic(err)
	}

	resp := &http.Response{
		Body:          http.NoBody,
		ContentLength: 1 << 20,
	}

	if _, err := policy.ReadResponseBody(resp); errors.Is(err, netpolicy.ErrBodyTooLarge) {
		fmt.Println("body rejected:", strings.Contains(err.Error(), "64 bytes"))
	}

	// Output: body rejected: true
}
