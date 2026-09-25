package netpolicy

import (
	"net/netip"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDenyErrorNamesTheSettingThatAdmits pins [DenyError.Admits] to the
// decisions that set it, through a real policy rather than a hand-built error:
// a remedy composed from the field is only as right as the check that filled
// it in (#1694). The empty cases matter as much as the named ones — a
// metadata address has no key, and a port in deny_ports is not admitted by
// adding it to allow_ports.
func TestDenyErrorNamesTheSettingThatAdmits(t *testing.T) {
	t.Parallel()

	httpsOnly, err := New(WithSchemes("https"))
	require.NoError(t, err)
	narrowed, err := New(WithAllowNetworks(netip.MustParsePrefix("203.0.113.0/24")))
	require.NoError(t, err)
	ports, err := New(WithAllowPorts(443), WithDenyPorts(8555))
	require.NoError(t, err)

	tests := []struct {
		name   string
		err    error
		reason Reason
		admits string
	}{
		{"loopback", httpsOnly.CheckAddr(netip.MustParseAddrPort("127.0.0.1:8555")), ReasonAddress, "allow_loopback"},
		{"private", httpsOnly.CheckAddr(netip.MustParseAddrPort("10.0.0.5:443")), ReasonAddress, "allow_private_networks"},
		{"unique-local", httpsOnly.CheckAddr(netip.MustParseAddrPort("[fd12::1]:443")), ReasonAddress, "allow_private_networks"},
		{"carrier-grade NAT", httpsOnly.CheckAddr(netip.MustParseAddrPort("100.64.0.1:443")), ReasonAddress, "allow_private_networks"},
		{"cloud metadata", httpsOnly.CheckAddr(netip.MustParseAddrPort("169.254.169.254:80")), ReasonAddress, ""},
		{"link-local", httpsOnly.CheckAddr(netip.MustParseAddrPort("169.254.10.10:443")), ReasonAddress, ""},
		{"outside every allowed network", narrowed.CheckAddr(netip.MustParseAddrPort("198.51.100.7:443")), ReasonAddress, "allow_networks"},
		{"port not allowed", ports.CheckAddr(netip.MustParseAddrPort("203.0.113.9:9443")), ReasonPort, "allow_ports"},
		{"port denied", ports.CheckAddr(netip.MustParseAddrPort("203.0.113.9:8555")), ReasonPort, ""},
		{"scheme", httpsOnly.CheckURL(t.Context(), "GET", must(url.Parse("http://203.0.113.9/jwks"))), ReasonScheme, "schemes"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var deny *DenyError
			require.ErrorAs(t, test.err, &deny, "the check did not refuse")
			require.Equal(t, test.reason, deny.Reason)
			require.Equal(t, test.admits, deny.Admits)
		})
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
